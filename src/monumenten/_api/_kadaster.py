import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

_KADASTER_SPARQL_ENDPOINT = (
    "https://api.labs.kadaster.nl/datasets/dst/kkg/services/default/sparql"
)

_VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """
PREFIX sor: <https://data.kkg.kadaster.nl/sor/model/def/>
PREFIX nen3610: <https://data.kkg.kadaster.nl/nen3610/model/def/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT DISTINCT ?identificatie ?verblijfsobjectWKT
WHERE {{
  ?verblijfsobject sor:geregistreerdMet/nen3610:identificatie ?identificatie .
  ?verblijfsobject geo:hasGeometry/geo:asWKT ?verblijfsobjectWKT.
  FILTER (?identificatie IN ( {identificaties} ))
}}
"""

_kadaster_semaphore: Optional[asyncio.Semaphore] = None

# Create a module-level logger
logger = logging.getLogger("monumenten.api.kadaster")


def _get_semaphore(loop: asyncio.AbstractEventLoop) -> asyncio.Semaphore:
    global _kadaster_semaphore
    if _kadaster_semaphore is None:
        _kadaster_semaphore = asyncio.Semaphore(4)
    return _kadaster_semaphore


async def _query_verblijfsobjecten(
    session: aiohttp.ClientSession, identificaties: List[str]
) -> List[Dict[str, Any]]:
    async with _get_semaphore(asyncio.get_running_loop()):
        identificaties_str = ", ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = _VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
            identificaties=identificaties_str
        )
        data = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with session.post(
                    _KADASTER_SPARQL_ENDPOINT, data=data
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
                        "Poging %d/%d voor verblijfsobjecten query mislukt: %s. Opnieuw proberen over 1 seconde...",
                        poging + 1,
                        retries,
                        str(e),
                    )
                    await asyncio.sleep(1)
                else:
                    logger.error(
                        "Alle pogingen voor verblijfsobjecten query mislukt: %s", str(e)
                    )
                    raise
        return List[Dict[str, Any]]()
