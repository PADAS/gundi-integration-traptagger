"""Redis helpers for TrapTagger delivery, ported from the TrapTagger dispatcher.

- Image details (camera, location, time) wait in Redis until their image
  arrives, under the same DB and key format the dispatcher uses.
- A shared counter limits requests per TrapTagger address across instances.
"""
import logging
from typing import Optional

import redis.asyncio as redis
import stamina
from gundi_core.schemas.v2 import TrapTaggerImageMetadata
from redis import exceptions as redis_exceptions
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from app import settings

logger = logging.getLogger(__name__)

# Retries a whole cache read or write after the client's own reconnect
# retries are used up.
CACHE_RETRY = dict(on=redis_exceptions.RedisError, attempts=5, wait_initial=1.0, wait_max=10, wait_jitter=1.0)


class TooManyRequests(Exception):
    pass


# Decrement the rate-limit counter only while it exists and is above zero.
# One script, so the check and the decrement can't be split by another request.
_RELEASE_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]))
if current and current > 0 then
    return redis.call('DECR', KEYS[1])
end
return current or 0
"""


def get_redis_db(retry_on_connection_errors=True):
    """
    retry_on_connection_errors: replay a command that fails on a connection error.
    Turn it off for non-idempotent commands (e.g. INCR/DECR counters), where the
    server may have applied the command before the connection dropped.
    """
    retry_kwargs = {}
    if retry_on_connection_errors:
        retry_kwargs = {
            "retry": Retry(ExponentialBackoff(), settings.REDIS_RETRY_ATTEMPTS),
            "retry_on_error": [
                redis_exceptions.ConnectionError,
                redis_exceptions.TimeoutError,
            ],
        }
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.TRAPTAGGER_REDIS_DB,
        encoding="utf-8",
        decode_responses=True,
        # Check idle connections before reusing them, so a dropped one gets replaced
        health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
        **retry_kwargs,
    )


# Clients connect lazily, on first use.
redis_client = get_redis_db()
# The rate limiter's INCR/DECR must not be replayed after a dropped connection
rate_limiter_redis_client = get_redis_db(retry_on_connection_errors=False)


def image_metadata_key(gundi_id: str, destination_id: str) -> str:
    if not gundi_id or not destination_id:
        raise ValueError("gundi_id and destination_id must be valid")
    return f"traptagger_image_metadata.{gundi_id}.{destination_id}"


async def cache_image_metadata(data: TrapTaggerImageMetadata, gundi_id: str, destination_id: str):
    key = image_metadata_key(gundi_id, destination_id)
    async for attempt in stamina.retry_context(**CACHE_RETRY):
        with attempt:
            await redis_client.setex(name=key, time=settings.IMAGE_METADATA_CACHE_TTL, value=data.json())


async def get_image_metadata_from_cache(gundi_id: str, destination_id: str) -> Optional[TrapTaggerImageMetadata]:
    key = image_metadata_key(gundi_id, destination_id)
    async for attempt in stamina.retry_context(**CACHE_RETRY):
        with attempt:
            cached_data = await redis_client.get(name=key)
    if not cached_data:
        return None
    return TrapTaggerImageMetadata.parse_raw(cached_data)


class RateLimiterSemaphore:
    def __init__(self, url, redis_client=None, **kwargs):
        self.url = url
        self.max_requests = kwargs.get("max_requests", settings.MAX_REQUESTS)
        self.max_requests_time_window_sec = kwargs.get(
            "max_requests_time_window_sec", settings.MAX_REQUESTS_TIME_WINDOW_SEC
        )
        self.redis_client = redis_client or rate_limiter_redis_client

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.release()

    async def acquire(self, auto_release=True):
        """
        Try to acquire the counter semaphore:
            - Increase the number of requests made in the last time window
            - If the number of requests is greater than the limit, raise an exception
        """
        async with self.redis_client.pipeline(transaction=True) as pipe:
            operations = pipe.incr(self.url)
            if auto_release:
                operations = operations.expire(self.url, self.max_requests_time_window_sec)
            res = await operations.execute()
        logger.debug(f"RateLimiterSemaphore<{self.url}>: {res[0]}/{self.max_requests} requests")
        if res[0] > self.max_requests:
            raise TooManyRequests(
                f"Too many requests in the last {self.max_requests_time_window_sec} seconds: {res[0]}"
            )

    async def release(self):
        """Decrease the number of requests made in the last time window.

        The counter expires after the time window. An upload that ends later
        must not decrement the missing key (DECR would create it at -1 with no
        expiry, letting extra requests through), so the script only
        decrements a counter that still exists and is above zero.
        """
        await self.redis_client.eval(_RELEASE_SCRIPT, 1, self.url)

    def __str__(self):
        return f"RateLimiterSemaphore<{self.url}>: {self.max_requests}/{self.max_requests_time_window_sec} sec"

    def __repr__(self):
        return self.__str__()
