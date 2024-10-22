import asyncio

import aiohttp
from loguru import logger

CULTUREEL_ERFGOED_SPARQL_ENDPOINT = (
    "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql"
)
CULTUREEL_ERFGOED_SEMAPHORE = asyncio.Semaphore(2)

RIJKSMONUMENTEN_QUERY_TEMPLATE = """
PREFIX ceo:<https://linkeddata.cultureelerfgoed.nl/def/ceo#>
PREFIX bag:<http://bag.basisregistraties.overheid.nl/bag/id/>
PREFIX rn:<https://data.cultureelerfgoed.nl/term/id/rn/>
SELECT DISTINCT ?identificatie ?rijksmonument_nummer
WHERE {{
    ?monument ceo:heeftJuridischeStatus rn:b2d9a59a-fe1e-4552-9a05-3c2acddff864 ;
              ceo:rijksmonumentnummer ?rijksmonument_nummer ;
              ceo:heeftBasisregistratieRelatie ?basisregistratieRelatie .
    ?basisregistratieRelatie ceo:heeftBAGRelatie ?bagRelatie .
    ?bagRelatie ceo:verblijfsobjectIdentificatie ?identificatie .
    VALUES ?identificatie {{ {identificaties} }}
}}
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


async def query_rijksmonumenten(sessie, identificaties):
    async with CULTUREEL_ERFGOED_SEMAPHORE:
        identificaties_str = " ".join(
            f'"{identificatie}"' for identificatie in identificaties
        )
        query = RIJKSMONUMENTEN_QUERY_TEMPLATE.format(identificaties=identificaties_str)
        params = {"query": query, "format": "json"}
        retries = 3
        for poging in range(retries):
            try:
                async with sessie.post(
                    CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=params
                ) as response:
                    response.raise_for_status()
                    resultaat = await response.json()
                    if isinstance(resultaat, list):
                        return resultaat
                    else:
                        logger.warning(
                            f"Onverwacht antwoord bij poging {poging + 1}: {resultaat}"
                        )
                        return {}
            except aiohttp.ClientResponseError:
                if poging != retries - 1:
                    logger.warning(
                        f"Poging {poging + 1}/{retries} voor rijksmonumenten query mislukt. Opnieuw proberen over 1 seconde..."
                    )
                    await asyncio.sleep(1)
                else:
                    raise


async def query_beschermde_gebieden(sessie):
    retries = 3
    for poging in range(retries):
        try:
            params = {"query": BESCHERMDE_GEZICHTEN_QUERY, "format": "json"}
            async with sessie.post(
                CULTUREEL_ERFGOED_SPARQL_ENDPOINT, data=params
            ) as response:
                response.raise_for_status()
                resultaat = await response.json()
                if isinstance(resultaat, list):
                    return resultaat
                else:
                    logger.warning(
                        f"Onverwacht antwoordformaat bij poging {poging + 1}: {resultaat}"
                    )
                    return []
        except aiohttp.ClientResponseError:
            if poging != retries - 1:
                logger.warning(
                    f"Poging {poging + 1}/{retries} voor beschermde gebieden query mislukt. Opnieuw proberen in 1 seconde..."
                )
                await asyncio.sleep(1)
            else:
                raise
