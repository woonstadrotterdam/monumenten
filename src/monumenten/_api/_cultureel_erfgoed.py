import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
    RETRY_SLEEP_SECONDS,
)

# Create a module-level logger
logger = logging.getLogger("monumenten.api.cultureel_erfgoed")

_CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)

_RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
SELECT ?identificatie (MAX(?nummer) as ?rijksmonument_nummer)
WHERE {{
    ?monument ceo:heeftJuridischeStatus rn2:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
              ceo:rijksmonumentnummer ?nummer ;
              ceo:heeftBasisregistratieRelatie ?basisregistratieRelatie .
    ?basisregistratieRelatie ceo:heeftBAGRelatie ?bagRelatie .
    ?bagRelatie ceo:verblijfsobjectIdentificatie ?identificatie .
    VALUES ?identificatie {{ {identificaties} }}
}}
GROUP BY ?identificatie
"""

_BESCHERMDE_GEZICHTEN_QUERY = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX rn2:<https://data.cultureelerfgoed.nl/term/id/rn/2/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?gezicht ?beschermd_gezicht_naam ?gezichtWKT
WHERE {{
  ?gezicht
      ceo:heeftGeometrie ?gezichtGeometrie ;
      ceo:heeftGezichtsstatus rn2:fd968529-bf70-4afa-8564-7c6c2fcfcc54;
      ceo:heeftNaam/ceo:naam ?beschermd_gezicht_naam.
  ?gezichtGeometrie geo:asWKT ?gezichtWKT.
}}
"""

_cultureel_erfgoed_semaphore: Optional[asyncio.Semaphore] = None


def _get_semaphore(loop: asyncio.AbstractEventLoop) -> asyncio.Semaphore:
    """
    Verkrijgt een semaphore voor het beperken van gelijktijdige aanvragen naar de Cultureel Erfgoed API.

    Args:
        loop (asyncio.AbstractEventLoop): De asyncio event loop

    Returns:
        asyncio.Semaphore: Een asyncio.Semaphore object dat het aantal gelijktijdige aanvragen beperkt tot 4
    """
    global _cultureel_erfgoed_semaphore
    if _cultureel_erfgoed_semaphore is None:
        _cultureel_erfgoed_semaphore = asyncio.Semaphore(4)
    return _cultureel_erfgoed_semaphore


async def _query_rijksmonumenten(
    session: aiohttp.ClientSession, identificaties: List[str]
) -> List[Dict[str, Any]]:
    """
    Voert een SPARQL-query uit om rijksmonumenten op te halen voor gegeven BAG-identificaties.

    Args:
        session (aiohttp.ClientSession): De aiohttp ClientSession voor het uitvoeren van de HTTP-aanvraag
        identificaties (List[str]): Lijst van BAG-identificaties waarvoor rijksmonumenten worden opgezocht

    Returns:
        List[Dict[str, Any]]: Lijst van dictionaries met informatie over gevonden rijksmonumenten

    Raises:
        aiohttp.ClientResponseError: Bij fouten in de HTTP-aanvraag na alle pogingen
    """
    async with _get_semaphore(asyncio.get_running_loop()):
        identificaties_str = " ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = _RIJKSMONUMENTEN_QUERY_TEMPLATE.format(
            identificaties=identificaties_str
        )
        data = {"query": query, "format": "json"}
        last_error: Optional[BaseException] = None
        for poging in range(MAX_ATTEMPTS):
            try:
                async with session.post(
                    _CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=data
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return resultaat
                    else:
                        logger.warning(
                            "Unexpected response format on attempt %d: %s",
                            poging + 1,
                            resultaat,
                        )
            except aiohttp.ClientResponseError as e:
                last_error = e
                if e.status not in RETRYABLE_STATUS_CODES:
                    raise
                if poging == MAX_ATTEMPTS - 1:
                    raise
                logger.warning(
                    "Poging %d/%d voor rijksmonumenten query mislukt: %s. Opnieuw proberen over %ds...",
                    poging + 1,
                    MAX_ATTEMPTS,
                    str(e),
                    RETRY_SLEEP_SECONDS,
                )
                await asyncio.sleep(RETRY_SLEEP_SECONDS)
            except RETRYABLE_NETWORK_EXCEPTIONS as e:
                last_error = e
                if poging == MAX_ATTEMPTS - 1:
                    raise
                logger.warning(
                    "Poging %d/%d voor rijksmonumenten query mislukt (netwerk): %s. Opnieuw proberen over %ds...",
                    poging + 1,
                    MAX_ATTEMPTS,
                    str(e),
                    RETRY_SLEEP_SECONDS,
                )
                await asyncio.sleep(RETRY_SLEEP_SECONDS)
        if last_error is not None:
            raise last_error
        return []


async def _query_beschermde_gezichten(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """
    Voert een SPARQL-query uit om beschermde stads- en dorpsgezichten op te halen.

    Args:
        session (aiohttp.ClientSession): De aiohttp ClientSession voor het uitvoeren van de HTTP-aanvraag

    Returns:
        List[Dict[str, Any]]: Lijst van dictionaries met informatie over beschermde stads- en dorpsgezichten

    Raises:
        aiohttp.ClientResponseError: Bij fouten in de HTTP-aanvraag na alle pogingen
    """
    data = {"query": _BESCHERMDE_GEZICHTEN_QUERY, "format": "json"}
    last_error: Optional[BaseException] = None
    for poging in range(MAX_ATTEMPTS):
        try:
            async with session.post(
                _CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=data
            ) as response:
                response.raise_for_status()
                resultaat = await response.json()
                if isinstance(resultaat, list):
                    return resultaat
                else:
                    logger.warning(
                        "Onverwacht response formaat bij poging %d: %s",
                        poging + 1,
                        resultaat,
                    )
        except aiohttp.ClientResponseError as e:
            last_error = e
            if e.status not in RETRYABLE_STATUS_CODES:
                raise
            if poging == MAX_ATTEMPTS - 1:
                raise
            logger.warning(
                "Poging %d/%d voor beschermde gezichten query mislukt: %s. Opnieuw proberen over %ds...",
                poging + 1,
                MAX_ATTEMPTS,
                str(e),
                RETRY_SLEEP_SECONDS,
            )
            await asyncio.sleep(RETRY_SLEEP_SECONDS)
        except RETRYABLE_NETWORK_EXCEPTIONS as e:
            last_error = e
            if poging == MAX_ATTEMPTS - 1:
                raise
            logger.warning(
                "Poging %d/%d voor beschermde gezichten query mislukt (netwerk): %s. Opnieuw proberen over %ds...",
                poging + 1,
                MAX_ATTEMPTS,
                str(e),
                RETRY_SLEEP_SECONDS,
            )
            await asyncio.sleep(RETRY_SLEEP_SECONDS)
    if last_error is not None:
        raise last_error
    return []
