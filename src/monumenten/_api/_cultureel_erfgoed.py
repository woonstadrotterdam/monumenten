import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

# Create a module-level logger
logger = logging.getLogger("monumenten.api.cultureel_erfgoed")

_CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)

_RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn:<https://data.cultureelerfgoed.nl/term/id/rn/>
SELECT ?identificatie (MAX(?nummer) as ?rijksmonument_nummer)
WHERE {{
    ?monument ceo:heeftJuridischeStatus rn:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
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
PREFIX rn:<https://data.cultureelerfgoed.nl/term/id/rn/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?gezicht ?beschermd_gezicht_naam ?gezichtWKT
WHERE {{
  ?gezicht
      ceo:heeftGeometrie ?gezichtGeometrie ;
      ceo:heeftGezichtsstatus rn:fd968529-bf70-4afa-8564-7c6c2fcfcc54;
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
        aiohttp.ClientResponseError: Bij fouten in de HTTP-aanvraag na 3 pogingen
    """
    async with _get_semaphore(asyncio.get_running_loop()):
        identificaties_str = " ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = _RIJKSMONUMENTEN_QUERY_TEMPLATE.format(
            identificaties=identificaties_str
        )
        data = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
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
                if poging != retries - 1:
                    logger.warning(
                        "Poging %d/%d voor rijksmonumenten query mislukt: %s. Opnieuw proberen over 1 seconde...",
                        poging + 1,
                        retries,
                        str(e),
                    )
                    await asyncio.sleep(1)
                else:
                    raise
        return List[Dict[str, Any]]()


async def _query_beschermde_gebieden(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    """
    Voert een SPARQL-query uit om beschermde stads- en dorpsgezichten op te halen.

    Args:
        session (aiohttp.ClientSession): De aiohttp ClientSession voor het uitvoeren van de HTTP-aanvraag

    Returns:
        List[Dict[str, Any]]: Lijst van dictionaries met informatie over beschermde stads- en dorpsgezichten

    Raises:
        aiohttp.ClientResponseError: Bij fouten in de HTTP-aanvraag na 3 pogingen
    """
    retries = 3
    for poging in range(retries):
        try:
            data = {"query": _BESCHERMDE_GEZICHTEN_QUERY, "format": "json"}

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
            if poging != retries - 1:
                logger.warning(
                    "Poging %d/%d voor beschermde gebieden query mislukt: %s. Opnieuw proberen over 1 seconde...",
                    poging + 1,
                    retries,
                    str(e),
                )
                await asyncio.sleep(1)
            else:
                raise
    return List[Dict[str, Any]]()
