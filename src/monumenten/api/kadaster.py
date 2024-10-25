import asyncio
from typing import Any, List, Dict

import aiohttp
from loguru import logger

KADASTER_SPARQL_ENDPOINT = (
    "https://api.labs.kadaster.nl/datasets/dst/kkg/services/default/sparql"
)
KADASTER_SEMAPHORE = asyncio.Semaphore(4)

VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """
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


async def query_verblijfsobjecten(
    session: aiohttp.ClientSession, identificaties: List[str]
) -> List[Dict[str, Any]]:
    async with KADASTER_SEMAPHORE:
        identificaties_str = ", ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
            identificaties=identificaties_str
        )
        data = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with session.post(
                    KADASTER_SPARQL_ENDPOINT, data=data
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return resultaat
                    else:
                        logger.warning(
                            f"Onverwacht antwoordformaat bij poging {poging + 1}: {resultaat}"
                        )
            except aiohttp.ClientResponseError:
                if poging != retries - 1:
                    logger.warning(
                        f"Poging {poging + 1}/{retries} voor verblijfsobjecten query mislukt. Opnieuw proberen in 1 seconde..."
                    )
                    await asyncio.sleep(1)
                else:
                    raise
        return List[Dict[str, Any]]()
