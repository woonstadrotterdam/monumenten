"""
Simple retry for external API calls (Kadaster BAG LV, KKG, RCE).

Used by _kadaster and _cultureel_erfgoed to retry once on transient HTTP errors (429, 5xx)
and network/connection failures. On retry, sleep for RETRY_SLEEP_SECONDS.
Non-transient 4xx are not retried.
"""

import asyncio
from typing import Tuple, Type

import aiohttp

MAX_ATTEMPTS = 2
RETRY_SLEEP_SECONDS = 3
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)
