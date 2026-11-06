"""Retry voor de requests aan de externe bronnen (Kadaster BAG en BRK-PB, RCE en de
provincies).

Een request wordt bij een tijdelijke fout (HTTP 429 of 5xx, of een netwerkfout) na
RETRY_SLEEP_SECONDS één keer herhaald; bij een 429 wordt de Retry-After van de bron
gevolgd, anders RATE_LIMIT_SLEEP_SECONDS. Een andere 4xx wordt niet herhaald.

Splitsen (zie _kadaster en _cultureel_erfgoed): faalt een request ook na de retry,
dan wordt de set ID's gehalveerd en elk deel opnieuw geprobeerd, behalve bij een 429,
omdat splitsen dan alleen meer requests oplevert. Splitsen helpt alleen als een
kleinere request wel kan lukken. Een bron waarvan MAX_CONSECUTIVE_FAILURES requests
op rij mislukken (elk na zijn retry), zonder één antwoord ertussen, heeft een storing:
er wordt niet meer gesplitst en de bron wordt overgeslagen, op één proefrequest per
OUTAGE_PROBE_SECONDS na. Een antwoord beëindigt de storing. Verblijfsobjecten waarvoor
een bron geen antwoord gaf krijgen aan het einde van de aanroep één herkansing (zie
_processing._query).

Een fout die bij de request hoort en niet bij de bron (een 4xx behalve 429, of een
afgekapt antwoord) telt pas mee voor een storing als de request niet verder kan
worden gesplitst: één ID dat altijd faalt laat dan alleen dat ID zonder antwoord.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Tuple, Type, TypeVar

import aiohttp

logger = logging.getLogger("monumenten.api")

MAX_ATTEMPTS = 2
RETRY_SLEEP_SECONDS = 3
MIN_BATCH_SIZE = 1
MAX_SPLIT_DEPTH = 10
# Zoveel requests lopen er per bron tegelijk
MAX_PARALLELLE_REQUESTS = 4
# Twee mislukte splitsniveaus (8 requests) blijven onder deze grens
MAX_CONSECUTIVE_FAILURES = 10
# Zo vaak krijgt een bron met een storing één proefrequest; korter levert minder pd.NA
# op, langer minder requests naar een bron die niet antwoordt
OUTAGE_PROBE_SECONDS = 10
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_SLEEP_SECONDS = 60
RETRYABLE_NETWORK_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.TimeoutError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
)

T = TypeVar("T")


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


def is_rate_limited(fout: BaseException) -> bool:
    """Geef aan of een fout een rate limit (HTTP 429) is.

    Args:
        fout (BaseException): De fout die na retries overbleef

    Returns:
        bool: Of de fout een rate limit is
    """
    return isinstance(fout, aiohttp.ClientResponseError) and fout.status == 429


def _wachttijd(fout: aiohttp.ClientResponseError) -> int:
    """De wachttijd voor de retry na een HTTP-fout, bij een 429 uit de Retry-After."""
    if not is_rate_limited(fout):
        return RETRY_SLEEP_SECONDS
    retry_after = fout.headers.get("Retry-After", "") if fout.headers else ""
    return int(retry_after) if retry_after.isdigit() else RATE_LIMIT_SLEEP_SECONDS


async def met_retries(
    verzoek: Callable[[], Awaitable[T]],
    context: str,
    *,
    herhaalbaar: Tuple[Type[BaseException], ...] = RETRYABLE_NETWORK_EXCEPTIONS,
) -> T:
    """Voer een request uit en herhaal haar bij een tijdelijke fout.

    Args:
        verzoek (Callable[[], Awaitable[T]]): Doet de request en geeft het antwoord
        context (str): Wat er wordt opgehaald, voor de logregels
        herhaalbaar (Tuple[Type[BaseException], ...]): Fouten die naast een HTTP-fout
            met een status uit RETRYABLE_STATUS_CODES worden herhaald

    Returns:
        T: Het antwoord van de eerste poging die slaagde

    Raises:
        Exception: De fout van de laatste poging, of direct een fout die niet wordt
            herhaald
    """
    for poging in range(1, MAX_ATTEMPTS + 1):
        fout: BaseException
        try:
            antwoord = await verzoek()
        except aiohttp.ClientResponseError as e:
            if e.status not in RETRYABLE_STATUS_CODES or poging == MAX_ATTEMPTS:
                raise
            fout, wacht = e, _wachttijd(e)
        except herhaalbaar as e:
            if poging == MAX_ATTEMPTS:
                raise
            fout, wacht = e, RETRY_SLEEP_SECONDS
        else:
            if poging > 1:
                logger.info(
                    "Poging %d/%d voor %s geslaagd na eerdere mislukking",
                    poging,
                    MAX_ATTEMPTS,
                    context,
                )
            return antwoord
        logger.warning(
            "Poging %d/%d voor %s mislukt (%s: %s). Opnieuw proberen over %ds...",
            poging,
            MAX_ATTEMPTS,
            context,
            type(fout).__name__,
            fout,
            wacht,
        )
        await asyncio.sleep(wacht)
    raise AssertionError("De laatste poging geeft een antwoord of een fout")
