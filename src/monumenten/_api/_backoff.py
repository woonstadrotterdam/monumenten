"""
Exponential backoff + full jitter for external API calls (Kadaster BAG LV, KKG, RCE).

Used by _kadaster and _cultureel_erfgoed to retry on transient HTTP errors (429, 5xx)
and network/connection failures. Retry-After is respected when present (429/503),
capped at max_delay. Non-transient 4xx are not retried.
"""

import asyncio
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Type, Tuple

import aiohttp

# Defaults: 12 attempts, base 1s, x2 per attempt, max 300s, full jitter
MAX_ATTEMPTS = 12
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)


def compute_delay(
    attempt: int,
    retry_after_seconds: Optional[float] = None,
    *,
    base: float = 1.0,
    max_delay: float = 300.0,
    multiplier: float = 2.0,
) -> float:
    """
    Compute sleep duration before the next retry, with full jitter.

    Full jitter: random.uniform(0, computed_delay) to avoid thundering herd.
    If retry_after_seconds is provided (e.g. from Retry-After header), that value
    is used as the ceiling (capped at max_delay), then jitter is applied.

    Args:
        attempt (int): 0-based attempt index (0 = first retry).
        retry_after_seconds (Optional[float]): Optional seconds from Retry-After header.
        base (float): Base delay in seconds.
        max_delay (float): Maximum delay in seconds (also caps Retry-After).
        multiplier (float): Exponential multiplier per attempt.

    Returns:
        float: Sleep duration in seconds (always >= 0, at most max_delay).
    """
    if retry_after_seconds is not None and retry_after_seconds >= 0:
        ceiling = min(retry_after_seconds, max_delay)
    else:
        exponential = min(base * (multiplier**attempt), max_delay)
        ceiling = exponential
    return random.uniform(0, max(ceiling, 0))  # nosec B311 -- jitter only, not crypto


def get_retry_after_seconds(
    headers: Optional[object],
    max_delay: float = 300.0,
) -> Optional[float]:
    """
    Parse Retry-After from response headers (integer seconds or RFC 7231 HTTP-date).

    Args:
        headers (Optional[object]): Response headers (e.g. ClientResponseError.headers).
        max_delay (float): Cap returned value at this many seconds.

    Returns:
        Optional[float]: Seconds to wait, or None if header missing/invalid.
    """
    if headers is None:
        return None
    raw = None
    if hasattr(headers, "get"):
        get_attr = getattr(headers, "get")
        raw = get_attr("Retry-After")
    if raw is None:
        return None
    raw = raw.strip() if isinstance(raw, str) else str(raw)
    if not raw:
        return None
    # Integer seconds
    if raw.isdigit():
        secs = int(raw)
        return float(
            min(max(secs, 0), max_delay) if max_delay is not None else max(secs, 0)
        )
    # HTTP-date (RFC 7231)
    try:
        dt = parsedate_to_datetime(raw)
    except (ValueError, TypeError):
        return None
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = (dt - now).total_seconds()
    if delta <= 0:
        return 0.0
    return float(min(delta, max_delay))
