import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

_KADASTER_SPARQL_ENDPOINT = "https://data.kkg.kadaster.nl/service/sparql"

_VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """
prefix geo: <http://www.opengis.net/ont/geosparql#>
prefix prov: <http://www.w3.org/ns/prov#>
prefix imx: <http://modellen.geostandaarden.nl/def/imx-geo#>
prefix geof: <http://www.opengis.net/def/function/geosparql/>

select distinct ?identificatie ?verblijfsobjectWKT ?grondslagcode ?grondslag_gemeentelijk_monument
where {{
  values ?verblijfsobjectIri {{
    {uri_list}
  }}

  ?adres prov:wasDerivedFrom ?verblijfsobjectIri;
         imx:isHoofdadres true;
         geo:hasGeometry/geo:asWKT ?verblijfsobjectWKT.

  OPTIONAL {{
    ?adres imx:isAdresVanGebouw ?gebouw.
    ?gebouw imx:bevindtZichOpPerceel ?perceel.
    ?perceel geo:hasGeometry/geo:asWKT ?perceelWKT.
    FILTER (geof:sfWithin(?verblijfsobjectWKT, ?perceelWKT))
    OPTIONAL {{
      ?beperking imx:isBeperkingOpPerceel ?perceel.
      ?beperking geo:hasGeometry/geo:asWKT ?beperkingWKT.
      ?beperking imx:grondslagcode ?grondslagcode.
      ?beperking imx:grondslag ?grondslag_gemeentelijk_monument.
      values ?grondslagcode {{
        "GG"  # Besluit monument, Gemeentewet
        "GWA" # Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)
        "EWE" # Erfgoedwet: Afschrift inschrijving monument of archeologisch monument in rijksmonumentenregister door minister OCW
        "EWD" # Erfgoedwet: Toezending ontwerpbesluit aanwijzing rijksmonument door minister OCW (voorbescherming)
      }}
      FILTER (geof:sfWithin(?verblijfsobjectWKT, ?beperkingWKT))
    }}
  }}

  bind(strafter(str(?verblijfsobjectIri), "https://bag.basisregistraties.overheid.nl/id/verblijfsobject/") as ?identificatie)
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
        uri_list = " ".join(
            f"<https://bag.basisregistraties.overheid.nl/id/verblijfsobject/{identificatie}>"
            for identificatie in identificaties
        )

        query = _VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(uri_list=uri_list)

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
        return []
