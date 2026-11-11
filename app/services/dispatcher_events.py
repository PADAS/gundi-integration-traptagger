"""Delivery status events and dead letters, as the TrapTagger dispatcher sends them.

The portal reads ObservationDelivered / ObservationDeliveryFailed and
DispatcherCustomLog from the dispatcher events topic. Messages that can never
be delivered go to the shared dead-letter topic for their data type, where the
dead-letter tooling (gundi-dlq) finds and replays them.
"""
import asyncio
import json
import logging

import aiohttp
import stamina
from gcloud.aio import pubsub
from gundi_core.events import SystemEventBaseModel
from gundi_core.schemas.v2 import StreamPrefixEnum

from app import settings
from app.services.activity_logger import publish_event

logger = logging.getLogger(__name__)

PUBLISH_RETRY = dict(on=(aiohttp.ClientError, asyncio.TimeoutError), attempts=5, wait_initial=1.0, wait_max=10)


async def publish_dispatcher_event(event: SystemEventBaseModel):
    async for attempt in stamina.retry_context(**PUBLISH_RETRY):
        with attempt:
            await publish_event(event=event, topic_name=settings.DISPATCHER_EVENTS_TOPIC)


def get_dlq_topic_for_data_type(data_type) -> str:
    if data_type == StreamPrefixEnum.observation:
        return settings.OBSERVATIONS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.event:
        return settings.EVENTS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.event_update:
        return settings.EVENTS_UPDATES_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.attachment:
        return settings.ATTACHMENTS_DEAD_LETTER_TOPIC
    elif data_type == StreamPrefixEnum.text_message:
        return settings.TEXT_MESSAGES_DEAD_LETTER_TOPIC
    else:
        return settings.LEGACY_DEAD_LETTER_TOPIC


async def send_to_dead_letter_topic(message: dict, attributes: dict):
    """Publish the original message and attributes to the shared dead-letter topic."""
    if attributes.get("gundi_version", "v1") == "v2":
        topic_name = get_dlq_topic_for_data_type(attributes.get("stream_type"))
    else:
        topic_name = settings.LEGACY_DEAD_LETTER_TOPIC
    # Attributes must be strings. Keep only what came in the original message
    # (the runner adds publish_time to the handler's metadata).
    message_attributes = {k: str(v) for k, v in attributes.items() if k != "publish_time" and v is not None}
    binary_payload = json.dumps(message, default=str).encode("utf-8")
    timeout_settings = aiohttp.ClientTimeout(total=20.0)
    async for attempt in stamina.retry_context(**PUBLISH_RETRY):
        with attempt:
            async with aiohttp.ClientSession(raise_for_status=True, timeout=timeout_settings) as session:
                client = pubsub.PublisherClient(session=session)
                topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
                logger.info(f"Sending message to dead letter topic {topic_name}..")
                await client.publish(topic, [pubsub.PubsubMessage(binary_payload, **message_attributes)])
    logger.info(f"Message sent to dead letter topic {topic_name}.")
