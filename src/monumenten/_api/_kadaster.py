import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

# New endpoints following the BAG LV + KKG two-stage approach
_BAG_LV_ENDPOINT = "https://api.labs.kadaster.nl/datasets/bag/lv/services/baglv/sparql"
_KKG_ENDPOINT = "https://api.labs.kadaster.nl/datasets/kadaster/kkg/services/kkg/sparql"

# Stage 1 – BAG LV: verblijfsobject ID -> Nummeraanduiding URI
_BAG_NUMMERAANDUIDING_QUERY_TEMPLATE = """
PREFIX bag: <https://bag.basisregistraties.overheid.nl/def/bag#>
PREFIX nen3610: <http://modellen.geostandaarden.nl/def/nen3610#>

SELECT DISTINCT ?voId ?nummeraanduiding
WHERE {{
  VALUES ?voId {{ {id_values} }}

  ?vo a bag:Verblijfsobject ;
      nen3610:identificatie ?voId ;
      bag:heeftAlsHoofdadres ?nummeraanduiding .
}}
"""

# Stage 2 – KKG: Nummeraanduiding URI -> geometrie + beperkingen
_KKG_VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """
PREFIX imx: <http://modellen.geostandaarden.nl/def/imx-geo#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT DISTINCT ?nummeraanduiding ?verblijfsobjectWKT ?grondslagcode ?grondslag_gemeentelijk_monument
WHERE {{
  VALUES ?nummeraanduiding {{ {nummeraanduiding_values} }}

  # Adres gekoppeld aan BAG Nummeraanduiding via prov:wasDerivedFrom
  ?adres a imx:Adres ;
         prov:wasDerivedFrom ?nummeraanduiding ;
         geo:hasGeometry/geo:asWKT ?verblijfsobjectWKT .

  # Eventuele beperkingen via Gebouw -> Perceel -> Beperking
  OPTIONAL {{
    ?gebouw a imx:Gebouw ;
            imx:heeftAlsAdres ?adres ;
            imx:bevindtZichOpPerceel ?perceel .

    OPTIONAL {{
      ?beperking imx:isBeperkingOpPerceel ?perceel .
      ?beperking imx:grondslagcode ?grondslagcode .
      ?beperking imx:grondslag ?grondslag_gemeentelijk_monument .
      VALUES ?grondslagcode {{
        "GG"  # Besluit monument, Gemeentewet
        "GWA" # Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)
        "EWE" # Erfgoedwet: Afschrift inschrijving monument of archeologisch monument in rijksmonumentenregister door minister OCW
        "EWD" # Erfgoedwet: Toezending ontwerpbesluit aanwijzing rijksmonument door minister OCW (voorbescherming)
      }}
    }}
  }}
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


async def _post_sparql_json(
    session: aiohttp.ClientSession, endpoint: str, query: str, context: str
) -> Any:
    """Generic helper to POST a SPARQL query and return JSON with retries."""
    data = {"query": query, "format": "json"}
    retries = 3
    for poging in range(retries):
        try:
            async with session.post(endpoint, data=data) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as e:
            if poging != retries - 1:
                logger.warning(
                    "Poging %d/%d voor %s mislukt: %s. Opnieuw proberen over 1 seconde...",
                    poging + 1,
                    retries,
                    context,
                    str(e),
                )
                await asyncio.sleep(1)
            else:
                logger.error(
                    "Alle pogingen voor %s mislukt tegen %s: %s",
                    context,
                    endpoint,
                    str(e),
                )
                raise


async def _query_verblijfsobjecten(
    session: aiohttp.ClientSession, identificaties: List[str]
) -> List[Dict[str, Any]]:
    """Query BAG LV + KKG to obtain geometrie en beperkingen per verblijfsobject."""
    async with _get_semaphore(asyncio.get_running_loop()):
        if not identificaties:
            return []

        # -------------------------
        # Stage 1 – BAG LV
        # -------------------------
        id_values = " ".join(f'"{identificatie}"' for identificatie in identificaties)
        bag_query = _BAG_NUMMERAANDUIDING_QUERY_TEMPLATE.format(id_values=id_values)

        bag_data = await _post_sparql_json(
            session, _BAG_LV_ENDPOINT, bag_query, "BAG nummeraanduiding query"
        )

        bag_results: List[Dict[str, Any]] = []
        if isinstance(bag_data, list):
            bag_results = bag_data
        elif isinstance(bag_data, dict):
            bindings = bag_data.get("results", {}).get("bindings", [])
            for b in bindings:
                bag_results.append(
                    {
                        "voId": b.get("voId", {}).get("value", ""),
                        "nummeraanduiding": b.get("nummeraanduiding", {}).get(
                            "value", ""
                        ),
                    }
                )

        if not bag_results:
            # Geen geldige BAG koppelingen gevonden
            return []

        # Map Nummeraanduiding URI -> set van verblijfsobject IDs
        na_to_vo_ids: Dict[str, List[str]] = {}
        for row in bag_results:
            vo_id = row.get("voId")
            na_uri = row.get("nummeraanduiding")
            if not vo_id or not na_uri:
                continue
            na_to_vo_ids.setdefault(na_uri, []).append(vo_id)

        if not na_to_vo_ids:
            return []

        nummeraanduiding_values = " ".join(f"<{uri}>" for uri in na_to_vo_ids.keys())

        # -------------------------
        # Stage 2 – KKG
        # -------------------------
        kkg_query = _KKG_VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
            nummeraanduiding_values=nummeraanduiding_values
        )

        kkg_data = await _post_sparql_json(
            session, _KKG_ENDPOINT, kkg_query, "KKG verblijfsobjecten query"
        )

        kkg_results: List[Dict[str, Any]] = []
        if isinstance(kkg_data, list):
            kkg_results = kkg_data
        elif isinstance(kkg_data, dict):
            bindings = kkg_data.get("results", {}).get("bindings", [])
            for b in bindings:
                kkg_results.append(
                    {
                        "nummeraanduiding": b.get("nummeraanduiding", {})
                        .get("value", "")
                        .strip(),
                        "verblijfsobjectWKT": b.get("verblijfsobjectWKT", {}).get(
                            "value", ""
                        ),
                        "grondslagcode": b.get("grondslagcode", {}).get("value", ""),
                        "grondslag_gemeentelijk_monument": b.get(
                            "grondslag_gemeentelijk_monument", {}
                        ).get("value", ""),
                    }
                )

        if not kkg_results:
            # We hebben wel geometrie-nummers maar geen beperkingen/geometry uit KKG
            return []

        resultaten: List[Dict[str, Any]] = []
        for row in kkg_results:
            na_uri = row.get("nummeraanduiding", "")
            if not na_uri:
                continue
            vo_ids = na_to_vo_ids.get(na_uri, [])
            if not vo_ids:
                continue

            for vo_id in vo_ids:
                resultaten.append(
                    {
                        "identificatie": vo_id,
                        "verblijfsobjectWKT": row.get("verblijfsobjectWKT"),
                        "grondslagcode": row.get("grondslagcode") or None,
                        "grondslag_gemeentelijk_monument": row.get(
                            "grondslag_gemeentelijk_monument"
                        )
                        or None,
                    }
                )

        return resultaten
