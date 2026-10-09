"""
Simple retry for external API calls (Kadaster BAG, BRK-PB, RCE).

Used by _kadaster and _cultureel_erfgoed to retry once on transient HTTP errors (429, 5xx)
and network/connection failures. On retry, sleep for RETRY_SLEEP_SECONDS.
Non-transient 4xx are not retried.

Rate limiting (429): wait RATE_LIMIT_SLEEP_SECONDS (or Retry-After) before retrying, and
never split a batch on 429 since that only adds requests.

Outage: splitting only helps when a smaller query can succeed. A source that fails
MAX_CONSECUTIVE_FAILURES requests in a row (each after its retries) without a single
answer in between is treated as down: no more splitting and no new requests to that
source, except one probe request every OUTAGE_PROBE_SECONDS. An answer ends the outage.
A source that goes down before its first answer is not probed: the call stops (see
Bronuitval). IDs a source gave no answer for get one more attempt at the end of the call
(see _processing._query).
"""

import asyncio
from typing import Tuple, Type

import aiohttp

MAX_ATTEMPTS = 2
RETRY_SLEEP_SECONDS = 3
MIN_BATCH_SIZE = 1
MAX_SPLIT_DEPTH = 10
# Er lopen 4 requests per bron tegelijk: twee mislukte splitsniveaus (8 requests)
# blijven eronder
MAX_CONSECUTIVE_FAILURES = 10
# Zo vaak krijgt een gestoorde bron één proefrequest; korter levert minder pd.NA op,
# langer minder requests naar een bron die plat ligt
OUTAGE_PROBE_SECONDS = 10
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_SLEEP_SECONDS = 60
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)
