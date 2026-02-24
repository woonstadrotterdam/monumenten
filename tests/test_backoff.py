"""Unit tests for exponential backoff utility (Kadaster/RCE retries)."""

import random
from unittest.mock import MagicMock


from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
    compute_delay,
    get_retry_after_seconds,
)


class TestRetryableConstants:
    def test_max_attempts(self):
        assert MAX_ATTEMPTS == 12

    def test_retryable_status_codes(self):
        assert RETRYABLE_STATUS_CODES == frozenset({429, 500, 502, 503, 504})

    def test_retryable_network_exceptions(self):
        import asyncio

        import aiohttp

        assert asyncio.TimeoutError in RETRYABLE_NETWORK_EXCEPTIONS
        assert aiohttp.ClientConnectorError in RETRYABLE_NETWORK_EXCEPTIONS
        assert aiohttp.ServerDisconnectedError in RETRYABLE_NETWORK_EXCEPTIONS
        assert len(RETRYABLE_NETWORK_EXCEPTIONS) == 3


class TestComputeDelay:
    def test_delay_grows_with_attempt(self):
        random.seed(42)
        delays = [compute_delay(i, None) for i in range(5)]
        # With full jitter, each is in [0, base * 2^attempt] capped at max
        assert delays[0] >= 0 and delays[0] <= 1.0
        assert delays[1] >= 0 and delays[1] <= 2.0
        assert delays[2] >= 0 and delays[2] <= 4.0
        assert delays[3] >= 0 and delays[3] <= 8.0
        assert delays[4] >= 0 and delays[4] <= 16.0

    def test_delay_capped_at_max_delay(self):
        random.seed(123)
        for attempt in range(15):
            d = compute_delay(attempt, None, base=1.0, max_delay=300.0, multiplier=2.0)
            assert 0 <= d <= 300.0

    def test_retry_after_seconds_used_and_capped(self):
        random.seed(99)
        # retry_after_seconds should be used as ceiling (capped at max_delay)
        d = compute_delay(0, retry_after_seconds=60.0)
        assert 0 <= d <= 60.0
        d = compute_delay(0, retry_after_seconds=400.0, max_delay=300.0)
        assert 0 <= d <= 300.0

    def test_full_jitter_range(self):
        random.seed(1)
        base, max_delay, multiplier = 1.0, 300.0, 2.0
        for attempt in range(6):
            exponential = min(base * (multiplier**attempt), max_delay)
            d = compute_delay(
                attempt, None, base=base, max_delay=max_delay, multiplier=multiplier
            )
            assert 0 <= d <= exponential

    def test_retry_after_none_uses_exponential(self):
        random.seed(77)
        d_none = compute_delay(2, None)
        d_explicit_negative = compute_delay(2, -1.0)
        assert 0 <= d_none <= 4.0
        assert 0 <= d_explicit_negative <= 4.0


class TestGetRetryAfterSeconds:
    def test_missing_header_returns_none(self):
        assert get_retry_after_seconds(None) is None
        headers = MagicMock()
        headers.get.return_value = None
        assert get_retry_after_seconds(headers) is None

    def test_integer_seconds(self):
        headers = MagicMock()
        headers.get.return_value = "120"
        assert get_retry_after_seconds(headers) == 120.0
        headers.get.return_value = "0"
        assert get_retry_after_seconds(headers) == 0.0

    def test_integer_seconds_capped_at_max_delay(self):
        headers = MagicMock()
        headers.get.return_value = "500"
        assert get_retry_after_seconds(headers, max_delay=300.0) == 300.0

    def test_http_date_returns_seconds_until_date(self):
        from datetime import datetime, timedelta, timezone

        headers = MagicMock()
        # Date 60 seconds in the future
        future = datetime.now(timezone.utc) + timedelta(seconds=60)
        headers.get.return_value = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = get_retry_after_seconds(headers, max_delay=300.0)
        assert result is not None
        assert 55 <= result <= 65  # allow small clock skew

    def test_http_date_capped_at_max_delay(self):
        from datetime import datetime, timedelta, timezone

        headers = MagicMock()
        future = datetime.now(timezone.utc) + timedelta(seconds=500)
        headers.get.return_value = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = get_retry_after_seconds(headers, max_delay=300.0)
        assert result == 300.0

    def test_invalid_value_returns_none(self):
        headers = MagicMock()
        headers.get.return_value = "not-a-number"
        assert get_retry_after_seconds(headers) is None
        headers.get.return_value = "invalid-date-format"
        assert get_retry_after_seconds(headers) is None

    def test_empty_string_returns_none(self):
        headers = MagicMock()
        headers.get.return_value = "   "
        assert get_retry_after_seconds(headers) is None
