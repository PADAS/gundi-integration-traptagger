from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from gundi_core import events as system_events
from gundi_core.events.transformers import AttachmentTransformedTrapTagger, EventTransformedTrapTagger
from gundi_core.schemas.v2 import TrapTaggerImageMetadata

from app.actions import handlers
from app.actions.configurations import PushImageMetadataConfig, PushImagesConfig

DESTINATION_ID = "00000000-0000-4000-8000-0000000000d1"
UPLOAD_URL = "https://traptagger.co.uk/api/v1/addImage"
IMAGE_METADATA = TrapTaggerImageMetadata(camera="cam-1", latitude="-1.29", longitude="36.82", timestamp="2026-11-11 08:15:00")


def _now(delta=timedelta()):
    return (datetime.now(timezone.utc) + delta).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture
def integration():
    auth = SimpleNamespace(data={"api_key": "test-key"})
    return SimpleNamespace(
        id=DESTINATION_ID,
        # Path and mixed case on purpose: the rate limit must key on the upload endpoint
        base_url="https://TrapTagger.co.uk/some/path",
        get_action_config=lambda action: auth if action == "auth" else None,
    )


@pytest.fixture
def metadata():
    return {
        "gundi_id": "att-1",
        "related_to": "obs-1",
        "data_provider_id": "provider-1",
        "destination_id": DESTINATION_ID,
        "stream_type": "att",
        "gundi_version": "v2",
        "publish_time": _now(),
    }


def _image_message(timestamp=None, schema_version="v1"):
    return AttachmentTransformedTrapTagger.parse_obj({
        "event_id": "11111111-1111-4111-8111-111111111111",
        "timestamp": timestamp or _now(),
        "schema_version": schema_version,
        "payload": {"file_path": "traptagger/IMG_0001.JPG"},
    })


def _status_error(status_code, text="error"):
    request = httpx.Request("POST", UPLOAD_URL)
    response = httpx.Response(status_code, text=text, request=request)
    return httpx.HTTPStatusError(f"{status_code}", request=request, response=response)


class _Limiter:
    urls = []

    def __init__(self, url, **kwargs):
        _Limiter.urls.append(url)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
def mocks(mocker):
    _Limiter.urls = []
    return SimpleNamespace(
        cache=mocker.patch.object(handlers, "cache_image_metadata", mocker.AsyncMock()),
        get_cached=mocker.patch.object(handlers, "get_image_metadata_from_cache", mocker.AsyncMock(return_value=IMAGE_METADATA)),
        publish=mocker.patch.object(handlers, "publish_dispatcher_event", mocker.AsyncMock()),
        dead_letter=mocker.patch.object(handlers, "send_to_dead_letter_topic", mocker.AsyncMock()),
        download=mocker.patch.object(handlers.file_storage, "download_file", mocker.AsyncMock(return_value=b"jpeg")),
        delete=mocker.patch.object(handlers.file_storage, "delete_file", mocker.AsyncMock()),
        add_image=mocker.patch.object(handlers.traptagger, "add_image", mocker.AsyncMock(return_value={"image_id": 4242})),
        limiter=mocker.patch.object(handlers, "RateLimiterSemaphore", _Limiter),
    )


def _published(mocks, event_type):
    return [c.args[0] for c in mocks.publish.await_args_list if isinstance(c.args[0], event_type)]


@pytest.mark.asyncio
async def test_image_metadata_is_cached_and_buffered(integration, metadata, mocks):
    data = EventTransformedTrapTagger.parse_obj({
        "event_id": "11111111-1111-4111-8111-111111111110", "timestamp": _now(), "schema_version": "v1", "payload": IMAGE_METADATA.dict(),
    })
    result = await handlers.action_push_image_metadata(integration, PushImageMetadataConfig(), data, metadata)
    assert result == {"status": "buffered"}
    mocks.cache.assert_awaited_once_with(data=data.payload, gundi_id="att-1", destination_id=DESTINATION_ID)
    assert len(_published(mocks, system_events.DispatcherCustomLog)) == 1


@pytest.mark.asyncio
async def test_image_is_delivered(integration, metadata, mocks):
    result = await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert result == {"image_id": 4242}
    mocks.get_cached.assert_awaited_once_with(gundi_id="obs-1", destination_id=DESTINATION_ID)
    kwargs = mocks.add_image.await_args.kwargs
    assert kwargs["api_key"] == "test-key"
    assert kwargs["image_metadata"] == IMAGE_METADATA.dict()
    assert kwargs["file_name"] == "IMG_0001.JPG"
    assert _Limiter.urls == [UPLOAD_URL]
    delivered = _published(mocks, system_events.ObservationDelivered)
    assert len(delivered) == 1 and delivered[0].payload.external_id == "4242"
    mocks.dead_letter.assert_not_awaited()
    mocks.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_image_before_its_metadata_is_retried(integration, metadata, mocks):
    mocks.get_cached.return_value = None
    with pytest.raises(handlers.MissingImageMetadataError):
        await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert len(_published(mocks, system_events.DispatcherCustomLog)) == 1
    mocks.add_image.assert_not_awaited()
    mocks.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_image_whose_metadata_never_came_is_dead_lettered(integration, metadata, mocks):
    mocks.get_cached.return_value = None
    old_image = _image_message(timestamp=_now(-timedelta(hours=2)))
    result = await handlers.action_push_images(integration, PushImagesConfig(), old_image, metadata)
    assert result == {"status": "dead-lettered"}
    assert len(_published(mocks, system_events.ObservationDeliveryFailed)) == 1
    mocks.dead_letter.assert_awaited_once()


@pytest.mark.asyncio
async def test_permanent_client_error_is_dead_lettered(integration, metadata, mocks):
    mocks.add_image.side_effect = _status_error(422)
    result = await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert result == {"status": "dead-lettered"}
    failed = _published(mocks, system_events.ObservationDeliveryFailed)
    assert len(failed) == 1 and failed[0].payload.server_response_status == 422
    mocks.dead_letter.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_site_is_dead_lettered_with_a_camera_warning(integration, metadata, mocks):
    mocks.add_image.side_effect = _status_error(400, "Missing site information.")
    result = await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert result == {"status": "dead-lettered"}
    warnings = _published(mocks, system_events.DispatcherCustomLog)
    assert len(warnings) == 1 and "cam-1" in warnings[0].payload.title
    assert len(_published(mocks, system_events.ObservationDeliveryFailed)) == 1
    mocks.dead_letter.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403, 408, 429, 500, 503])
async def test_transient_errors_are_retried(integration, metadata, mocks, status_code):
    mocks.add_image.side_effect = _status_error(status_code)
    with pytest.raises(httpx.HTTPStatusError):
        await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert len(_published(mocks, system_events.ObservationDeliveryFailed)) == 1
    mocks.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_timeout_is_retried(integration, metadata, mocks):
    mocks.add_image.side_effect = httpx.ReadTimeout("timed out")
    with pytest.raises(httpx.ReadTimeout):
        await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    mocks.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_dead_letter_publish_is_not_acknowledged(integration, metadata, mocks):
    mocks.add_image.side_effect = _status_error(422)
    mocks.dead_letter.side_effect = RuntimeError("Pub/Sub unavailable")
    with pytest.raises(RuntimeError):
        await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)


@pytest.mark.asyncio
async def test_message_older_than_the_age_limit_is_dead_lettered_without_sending(integration, metadata, mocks):
    metadata["publish_time"] = _now(-timedelta(hours=25))
    result = await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    assert result["status"] == "discarded"
    mocks.dead_letter.assert_awaited_once()
    mocks.add_image.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsupported_schema_version_is_discarded(integration, metadata, mocks):
    result = await handlers.action_push_images(integration, PushImagesConfig(), _image_message(schema_version="v2"), metadata)
    assert result["status"] == "discarded"
    mocks.add_image.assert_not_awaited()
    mocks.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_file_is_deleted_after_delivery_when_enabled(integration, metadata, mocks, mocker):
    mocker.patch.object(handlers.settings, "DELETE_FILES_AFTER_DELIVERY", True)
    await handlers.action_push_images(integration, PushImagesConfig(), _image_message(), metadata)
    mocks.delete.assert_awaited_once_with("traptagger/IMG_0001.JPG")
