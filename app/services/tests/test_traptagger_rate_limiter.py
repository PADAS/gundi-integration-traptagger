import pytest
from unittest.mock import AsyncMock

from app.services.traptagger_cache import _ACQUIRE_SCRIPT, RateLimiterSemaphore, TooManyRequests


@pytest.mark.asyncio
async def test_requests_within_the_limit_go_through():
    redis_client = AsyncMock()
    redis_client.eval.return_value = 3
    async with RateLimiterSemaphore(url="https://traptagger.example.org", redis_client=redis_client, max_requests=3,
                                    max_requests_time_window_sec=1):
        pass
    redis_client.eval.assert_awaited_once_with(_ACQUIRE_SCRIPT, 1, "https://traptagger.example.org", 1)


@pytest.mark.asyncio
async def test_requests_over_the_limit_are_held_back():
    redis_client = AsyncMock()
    redis_client.eval.return_value = 4
    with pytest.raises(TooManyRequests):
        async with RateLimiterSemaphore(url="https://traptagger.example.org", redis_client=redis_client,
                                        max_requests=3, max_requests_time_window_sec=1):
            pass


@pytest.mark.asyncio
async def test_finishing_a_request_does_not_touch_the_counter():
    # Fixed window: requests count until the window closes, so nothing is decremented
    redis_client = AsyncMock()
    redis_client.eval.return_value = 1
    async with RateLimiterSemaphore(url="https://traptagger.example.org", redis_client=redis_client):
        pass
    assert redis_client.eval.await_count == 1
    redis_client.decr.assert_not_called()


def test_acquire_script_only_starts_the_expiry_when_the_window_opens():
    # A rejected request must not push the window's expiry back; a counter
    # without an expiry gets one.
    assert "count == 1 or redis.call('TTL', KEYS[1]) == -1" in _ACQUIRE_SCRIPT
