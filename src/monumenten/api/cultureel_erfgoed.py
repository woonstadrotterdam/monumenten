import asyncio
from typing import Any, List, Dict

import aiohttp
from loguru import logger

CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)
CULTUREEL_ERFGOED_SEMAPHORE = asyncio.Semaphore(4)

RIJKSMONUMENTEN_QUERY_TEMPLATE = """
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

BESCHERMDE_GEZICHTEN_QUERY = """
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


async def query_rijksmonumenten(
    session: aiohttp.ClientSession, identificaties: List[str]
) -> List[Dict[str, Any]]:
    async with CULTUREEL_ERFGOED_SEMAPHORE:
        identificaties_str = " ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = RIJKSMONUMENTEN_QUERY_TEMPLATE.format(identificaties=identificaties_str)
        data = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with session.post(
                    CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=data
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return resultaat
                    else:
                        logger.warning(
                            f"Onverwacht antwoord bij poging {poging + 1}: {resultaat}"
                        )
            except aiohttp.ClientResponseError as e:
                logger.debug(response.headers)
                if poging != retries - 1:
                    logger.warning(
                        f"Poging {poging + 1}/{retries} voor rijksmonumenten query mislukt. {e} Opnieuw proberen over 1 seconde..."
                    )
                    await asyncio.sleep(1)
                else:
                    raise
        return List[Dict[str, Any]]()


async def query_beschermde_gebieden(
    session: aiohttp.ClientSession,
) -> List[Dict[str, Any]]:
    retries = 3
    for poging in range(retries):
        try:
            data = {"query": BESCHERMDE_GEZICHTEN_QUERY, "format": "json"}

            async with session.post(
                CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=data
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
                    f"Poging {poging + 1}/{retries} voor beschermde gebieden query mislukt. Opnieuw proberen in 1 seconde..."
                )
                await asyncio.sleep(1)
            else:
                raise
    return List[Dict[str, Any]]()
