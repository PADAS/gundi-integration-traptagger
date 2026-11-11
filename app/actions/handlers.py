import logging
import os
import traceback
from datetime import datetime, timezone

import httpx
from gundi_core import events as system_events
from gundi_core.events.transformers import AttachmentTransformedTrapTagger, EventTransformedTrapTagger
from gundi_core.schemas import v2 as gundi_schemas_v2

from app import settings
from app.services import file_storage, traptagger
from app.services.dispatcher_events import publish_dispatcher_event, send_to_dead_letter_topic
from app.services.traptagger_cache import (
    RateLimiterSemaphore,
    cache_image_metadata,
    get_image_metadata_from_cache,
)
from .configurations import AuthenticateConfig, PushImageMetadataConfig, PushImagesConfig

logger = logging.getLogger(__name__)


async def action_auth(integration, action_config: AuthenticateConfig):
    """Check that TrapTagger accepts the API key, as a destination."""
    if not integration.base_url:
        return {"error": "The TrapTagger address is missing, so the key couldn't be checked."}
    try:
        response = await traptagger.check_api_key(
            base_url=integration.base_url,
            api_key=action_config.api_key.get_secret_value(),
        )
    except ValueError:
        return {"error": f"The key couldn't be checked because the TrapTagger address "
                         f"'{integration.base_url}' isn't a full web address, like https://traptagger.co.uk."}
    except httpx.HTTPError as e:
        logger.exception(f"TrapTagger auth check failed for integration {integration.id}: {type(e).__name__}")
        return {"error": "TrapTagger couldn't be reached to check the key. Try again in a few minutes."}

    if response.status_code in (401, 403):
        return {"valid_credentials": False,
                "message": "TrapTagger didn't accept the API key. Check that it's current and try again."}
    if response.status_code == 400 and traptagger.KEY_ACCEPTED_MESSAGE in response.text.lower():
        # TrapTagger answers 400 "No image provided." once the key is accepted.
        return {"valid_credentials": True}
    if response.status_code >= 500 or response.status_code == 429:
        logger.error(f"TrapTagger auth check got {response.status_code} for integration {integration.id}")
        return {"error": "TrapTagger couldn't check the key right now. Try again in a few minutes."}
    logger.error(f"TrapTagger auth check got unexpected {response.status_code} for integration {integration.id}")
    return {"error": f"The key couldn't be checked because TrapTagger gave an unexpected answer "
                     f"(HTTP {response.status_code}). Check that the TrapTagger address is right."}


# Push actions: deliver camera-trap images routed to a TrapTagger destination.
# Ported from the TrapTagger dispatcher (PADAS/gundi-traptagger-dispatcher),
# keeping its behaviour: the same delivery status events, logs, dead letters
# and retry rules.


class NonRetryableDispatchError(Exception):
    """Retrying can't deliver this message, so it goes to the dead-letter topic."""


class MissingImageMetadataError(Exception):
    """The image arrived before its details (camera, location, time)."""


def _is_null(value) -> bool:
    return value in {None, "", "None", "null"}


def _parse_timestamp(timestamp: str) -> datetime:
    # Pub/Sub sends publish times with or without fractions of a second, and
    # with up to nanosecond precision.
    value = timestamp.rstrip("Z")
    if "." in value:
        whole, fraction = value.split(".", 1)
        value = f"{whole}.{fraction[:6]}"
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    return parsed.replace(tzinfo=timezone.utc)


def _is_too_old(metadata: dict) -> bool:
    timestamp = metadata.get("publish_time")
    if not timestamp:
        return False
    try:
        published_at = _parse_timestamp(timestamp)
    except ValueError:
        logger.warning(f"Unreadable publish time '{timestamp}'. Age check skipped.")
        return False
    # Total seconds, not .seconds: publish times can be in the future.
    age_seconds = (datetime.now(timezone.utc) - published_at).total_seconds()
    return age_seconds > settings.MAX_EVENT_AGE_SECONDS


def _dispatched_observation(metadata: dict, external_id=None) -> gundi_schemas_v2.DispatchedObservation:
    return gundi_schemas_v2.DispatchedObservation(
        gundi_id=metadata.get("gundi_id"),
        related_to=metadata.get("related_to"),
        external_id=external_id,
        data_provider_id=metadata.get("data_provider_id"),
        destination_id=metadata.get("destination_id"),
        delivered_at=datetime.now(timezone.utc),
    )


async def _publish_delivery_failed(metadata: dict, error: str, exception: Exception = None):
    server_response_status = None
    server_response_body = None
    response = getattr(exception, "response", None)
    if response is not None:  # bool(response) is False for error responses
        server_response_status = getattr(response, "status_code", None)
        server_response_body = str(getattr(response, "text", getattr(response, "content", None)) or "")
    await publish_dispatcher_event(
        system_events.ObservationDeliveryFailed(
            payload=system_events.DeliveryErrorDetails(
                error=error,
                error_traceback=traceback.format_exc(),
                server_response_status=server_response_status,
                server_response_body=server_response_body,
                observation=_dispatched_observation(metadata),
            )
        )
    )


async def _publish_custom_log(metadata: dict, title: str, level: gundi_schemas_v2.LogLevel):
    await publish_dispatcher_event(
        system_events.DispatcherCustomLog(
            payload=gundi_schemas_v2.CustomDispatcherLog(
                gundi_id=metadata.get("gundi_id"),
                related_to=metadata.get("related_to"),
                data_provider_id=metadata.get("data_provider_id"),
                destination_id=metadata.get("destination_id"),
                title=title,
                level=level,
            )
        )
    )


def _get_api_key(integration) -> str:
    auth_config = integration.get_action_config(gundi_schemas_v2.TrapTaggerActions.AUTHENTICATE.value)
    if not auth_config:
        raise ValueError(
            f"Authentication settings for integration {integration.id} are missing. "
            "Please fix the integration setup in the portal."
        )
    api_key = auth_config.data.get("api_key")
    if not api_key:
        raise ValueError(
            f"Token for integration {integration.id} is missing. Please fix the integration setup in the portal."
        )
    return api_key


async def _dead_letter_if_too_old(data, metadata: dict):
    """Dead-letter messages past MAX_EVENT_AGE_SECONDS. Returns a result when it did."""
    if not _is_too_old(metadata):
        return None
    logger.warning(
        f"Message discarded (publish time = {metadata.get('publish_time')}). "
        "The message is too old or the retry time limit has been reached."
    )
    await send_to_dead_letter_topic(data.dict(), metadata)
    return {"status": "discarded", "reason": "Message is too old or the retry time limit has been reached"}


async def action_push_image_metadata(
    integration, action_config: PushImageMetadataConfig, data: EventTransformedTrapTagger, metadata: dict
):
    """Keep an image's details in Redis until the image itself arrives."""
    if result := await _dead_letter_if_too_old(data, metadata):
        return result
    if data.schema_version != "v1":
        logger.warning(f"Schema version '{data.schema_version}' not supported. Message discarded.")
        return {"status": "discarded", "reason": f"Schema version '{data.schema_version}' not supported"}
    gundi_id = metadata.get("gundi_id")
    destination_id = metadata.get("destination_id") or str(integration.id)
    try:
        await cache_image_metadata(data=data.payload, gundi_id=gundi_id, destination_id=destination_id)
    except Exception as e:
        error_msg = (
            f"Error caching image metadata for {gundi_id} and {destination_id}: {type(e).__name__}: {e}"
        )
        logger.exception(error_msg)
        await _publish_delivery_failed(metadata, error=error_msg, exception=e)
        raise
    await _publish_custom_log(
        metadata,
        title=f"Observation {gundi_id} buffered in wait for attachment",
        level=gundi_schemas_v2.LogLevel.INFO,
    )
    return {"status": "buffered"}


async def _get_related_image_metadata(data: AttachmentTransformedTrapTagger, metadata: dict, destination_id: str):
    gundi_id = metadata.get("gundi_id")
    related_to = metadata.get("related_to")
    related_metadata = None
    if not _is_null(related_to):
        related_metadata = await get_image_metadata_from_cache(gundi_id=related_to, destination_id=destination_id)
    if related_metadata:
        return related_metadata
    event_time = data.timestamp
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    event_age_seconds = (datetime.now(timezone.utc) - event_time).total_seconds()
    if event_age_seconds < settings.IMAGE_METADATA_CACHE_TTL:
        # Normal race: the image arrived before its details. They may still
        # arrive, so warn and retry.
        await _publish_custom_log(
            metadata,
            title=f"Attachment {gundi_id} received before the metadata of related observation {related_to}. "
                  "It will be retried.",
            level=gundi_schemas_v2.LogLevel.WARNING,
        )
        raise MissingImageMetadataError(f"Error getting related observation {related_to}. Will retry later.")
    # The details' cache time has passed, so this image can never be delivered
    error_msg = (
        f"Error dispatching attachment {gundi_id} to destination {destination_id}: "
        f"metadata of related observation {related_to} was not found and the message is older "
        f"than IMAGE_METADATA_CACHE_TTL ({settings.IMAGE_METADATA_CACHE_TTL} seconds). It will not be retried."
    )
    logger.error(error_msg)
    await _publish_delivery_failed(metadata, error=error_msg)
    raise NonRetryableDispatchError(error_msg)


async def _send_image(integration, image: gundi_schemas_v2.TrapTaggerImage, image_metadata) -> dict:
    api_key = _get_api_key(integration)
    file_path = image.file_path
    try:
        file_data = await file_storage.download_file(file_path)
    except Exception as e:
        logger.exception(f"Error downloading file '{file_path}' from cloud storage: {type(e).__name__}: {e}")
        raise
    # Count requests per upload endpoint, not per configured address, so
    # "https://host", "https://host/" and "https://host/path" share one limit.
    async with RateLimiterSemaphore(url=traptagger.add_image_url(str(integration.base_url))):
        result = await traptagger.add_image(
            base_url=str(integration.base_url),
            api_key=api_key,
            image_metadata=image_metadata.dict(),  # Camera ID, lat/lon, time
            file_name=os.path.basename(file_path),
            file_data=file_data,
        )
    logger.info(f"File {file_path} delivered to TrapTagger with success.")
    if settings.DELETE_FILES_AFTER_DELIVERY:
        await file_storage.delete_file(file_path)
    return result


async def _dispatch_image(integration, data: AttachmentTransformedTrapTagger, image_metadata, metadata: dict):
    gundi_id = metadata.get("gundi_id")
    destination_id = metadata.get("destination_id") or str(integration.id)
    try:
        result = await _send_image(integration, image=data.payload, image_metadata=image_metadata)
    except Exception as e:
        if traptagger.is_missing_site_error(e):
            # Rejected because the camera has no site or location configured.
            # Warn the user with an actionable message instead of a delivery error.
            camera = getattr(image_metadata, "camera", "unknown")
            latitude = getattr(image_metadata, "latitude", "?")
            longitude = getattr(image_metadata, "longitude", "?")
            warning_msg = (
                f"TrapTagger rejected image {gundi_id}: camera '{camera}' needs a site identifier "
                f"or a valid location (current latitude/longitude: {latitude},{longitude}). "
                "Please set the camera location in the data provider."
            )
            logger.warning(warning_msg)
            await _publish_custom_log(metadata, title=warning_msg, level=gundi_schemas_v2.LogLevel.WARNING)
            # Also record a delivery failure so the image doesn't look
            # in flight forever in the portal delivery status
            await _publish_delivery_failed(metadata, error=warning_msg, exception=e)
            raise NonRetryableDispatchError(warning_msg) from e
        error_msg = (
            f"Error dispatching observation {gundi_id} to destination {destination_id}: {type(e).__name__}: {e}"
        )
        logger.exception(error_msg)
        await _publish_delivery_failed(metadata, error=error_msg, exception=e)
        if traptagger.is_permanent_client_error(e):
            # Retrying can't succeed. Dead-letter and acknowledge instead.
            raise NonRetryableDispatchError(error_msg) from e
        raise  # Retried by Pub/Sub
    traptagger_image_id = result.get("image_id")
    await publish_dispatcher_event(
        system_events.ObservationDelivered(
            payload=_dispatched_observation(
                metadata, external_id=str(traptagger_image_id) if traptagger_image_id else None
            )
        )
    )
    logger.debug(f"Observation {gundi_id} delivered with success. TrapTagger response: {result}")
    return result


async def action_push_images(
    integration, action_config: PushImagesConfig, data: AttachmentTransformedTrapTagger, metadata: dict
):
    """Send an image, with the details cached for it, to TrapTagger."""
    if result := await _dead_letter_if_too_old(data, metadata):
        return result
    if data.schema_version != "v1":
        logger.warning(f"Schema version '{data.schema_version}' not supported. Message discarded.")
        return {"status": "discarded", "reason": f"Schema version '{data.schema_version}' not supported"}
    destination_id = metadata.get("destination_id") or str(integration.id)
    try:
        image_metadata = await _get_related_image_metadata(data, metadata, destination_id)
        return await _dispatch_image(integration, data, image_metadata, metadata)
    except NonRetryableDispatchError as e:
        logger.warning(f"Permanent error delivering image: {e}. Sending message to the dead letter topic.")
        await send_to_dead_letter_topic(data.dict(), metadata)
        return {"status": "dead-lettered"}
