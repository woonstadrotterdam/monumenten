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
A source that went down before its first answer and is still down after
OUTAGE_GIVE_UP_SECONDS stops the call (see Bronuitval). IDs a source gave no answer for
get one more attempt at the end of the call (see _processing._query).

A failure that belongs to the request rather than the source (a 4xx other than 429, or a
truncated answer) only counts towards an outage once the request cannot be split any
further: one ID that always fails then only leaves that ID without an answer.
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
# Zo lang blijft een bron die nog nooit antwoord gaf gestoord, met proefrequests, voor
# de aanroep stopt. Een korte hapering aan het begin, waarin de requests die tegelijk
# onderweg zijn samen falen, kost zo geen aanroep.
OUTAGE_GIVE_UP_SECONDS = 3 * OUTAGE_PROBE_SECONDS
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_SLEEP_SECONDS = 60
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)


class AfgekaptAntwoordError(RuntimeError):
    """Een WFS gaf het maximale aantal features terug, dus het antwoord is onvolledig."""


def is_fout_van_de_request(fout: BaseException) -> bool:
    """Geef aan of een fout bij de inhoud van de request hoort en niet bij de bron.

    Dat is een HTTP-fout die niet wordt herhaald (een 4xx behalve 429) en een afgekapt
    antwoord. Dezelfde request faalt dan opnieuw, maar een kleinere request kan wel
    lukken. Zo'n fout zegt daarom pas iets over de bron als splitsen niet meer kan.

    Args:
        fout (BaseException): De fout die na retries overbleef

    Returns:
        bool: Of de fout bij de request hoort
    """
    if isinstance(fout, AfgekaptAntwoordError):
        return True
    return (
        isinstance(fout, aiohttp.ClientResponseError)
        and fout.status not in RETRYABLE_STATUS_CODES
    )
