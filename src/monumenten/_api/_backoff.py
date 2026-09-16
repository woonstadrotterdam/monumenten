"""
Simple retry for external API calls (Kadaster BAG LV, KKG, RCE).

Used by _kadaster and _cultureel_erfgoed to retry once on transient HTTP errors (429, 5xx)
and network/connection failures. On retry, sleep for RETRY_SLEEP_SECONDS.
Non-transient 4xx are not retried.

Rate limiting (429): wait RATE_LIMIT_SLEEP_SECONDS (or Retry-After) before retrying, and
never split a batch on 429 since that only adds requests. KKG allows 60 requests per minute.
"""

import asyncio
from typing import Tuple, Type

import aiohttp

MAX_ATTEMPTS = 2
RETRY_SLEEP_SECONDS = 3
MIN_BATCH_SIZE = 1
MAX_SPLIT_DEPTH = 10
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_SLEEP_SECONDS = 60
KKG_MAX_REQUESTS_PER_MINUTE = 55  # limiet is 60, houd marge aan
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)
