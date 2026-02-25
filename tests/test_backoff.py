"""Unit tests for retry utility (Kadaster/RCE retries)."""

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
    RETRY_SLEEP_SECONDS,
)


class TestRetryableConstants:
    def test_max_attempts(self):
        assert MAX_ATTEMPTS == 2

    def test_retry_sleep_seconds(self):
        assert RETRY_SLEEP_SECONDS == 3

    def test_retryable_status_codes(self):
        assert RETRYABLE_STATUS_CODES == frozenset({429, 500, 502, 503, 504})

    def test_retryable_network_exceptions(self):
        import asyncio

        import aiohttp

        assert asyncio.TimeoutError in RETRYABLE_NETWORK_EXCEPTIONS
        assert aiohttp.ClientConnectorError in RETRYABLE_NETWORK_EXCEPTIONS
        assert aiohttp.ServerDisconnectedError in RETRYABLE_NETWORK_EXCEPTIONS
        assert len(RETRYABLE_NETWORK_EXCEPTIONS) == 3
