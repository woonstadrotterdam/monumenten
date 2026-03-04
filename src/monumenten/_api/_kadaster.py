import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    MAX_SPLIT_DEPTH,
    MIN_BATCH_SIZE,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
    RETRY_SLEEP_SECONDS,
)

# New endpoints following the BAG LV + KKG two-stage approach
_BAG_LV_ENDPOINT = "https://api.labs.kadaster.nl/datasets/bag/lv/services/baglv/sparql"
_KKG_ENDPOINT = "https://data.kkg.kadaster.nl/service/sparql"

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
    """Generic helper to POST a SPARQL query and return JSON with exponential backoff retries."""
    data = {"query": query, "format": "json"}
    last_error: Optional[BaseException] = None
    for poging in range(MAX_ATTEMPTS):
        try:
            async with session.post(endpoint, data=data) as response:
                response.raise_for_status()
                if poging >= 1:
                    logger.info(
                        "Poging %d/%d voor %s geslaagd na eerdere mislukking",
                        poging + 1,
                        MAX_ATTEMPTS,
                        context,
                    )
                return await response.json()
        except aiohttp.ClientResponseError as e:
            last_error = e
            if e.status not in RETRYABLE_STATUS_CODES:
                logger.error(
                    "Niet-herhaalbare HTTP-fout voor %s: %s (status %s)",
                    context,
                    str(e),
                    e.status,
                )
                raise
            if poging == MAX_ATTEMPTS - 1:
                logger.error(
                    "Alle %d pogingen voor %s mislukt tegen %s: %s",
                    MAX_ATTEMPTS,
                    context,
                    endpoint,
                    str(e),
                )
                raise
            logger.warning(
                "Poging %d/%d voor %s mislukt: %s. Opnieuw proberen over %ds...",
                poging + 1,
                MAX_ATTEMPTS,
                context,
                str(e),
                RETRY_SLEEP_SECONDS,
            )
            await asyncio.sleep(RETRY_SLEEP_SECONDS)
        except RETRYABLE_NETWORK_EXCEPTIONS as e:
            last_error = e
            if poging == MAX_ATTEMPTS - 1:
                logger.error(
                    "Alle %d pogingen voor %s mislukt (netwerk/verbinding): %s",
                    MAX_ATTEMPTS,
                    context,
                    str(e),
                )
                raise
            logger.warning(
                "Poging %d/%d voor %s mislukt (netwerk): %s. Opnieuw proberen over %ds...",
                poging + 1,
                MAX_ATTEMPTS,
                context,
                str(e),
                RETRY_SLEEP_SECONDS,
            )
            await asyncio.sleep(RETRY_SLEEP_SECONDS)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Geen response ontvangen")


async def _query_kkg(
    session: aiohttp.ClientSession,
    na_to_vo_ids: Dict[str, List[str]],
    *,
    _depth: int = 0,
) -> List[Dict[str, Any]]:
    """Stage 2: query KKG for given nummeraanduiding URIs.
    Bij falen na retries wordt de set nummeraanduidingen in tweeën gesplitst en opnieuw geprobeerd.
    """
    na_uris = list(na_to_vo_ids.keys())
    try:
        async with _get_semaphore(asyncio.get_running_loop()):
            nummeraanduiding_values = " ".join(f"<{uri}>" for uri in na_uris)
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
                            "grondslagcode": b.get("grondslagcode", {}).get(
                                "value", ""
                            ),
                            "grondslag_gemeentelijk_monument": b.get(
                                "grondslag_gemeentelijk_monument", {}
                            ).get("value", ""),
                        }
                    )

            if not kkg_results:
                # Geen geometrie/beperkingen gevonden in KKG
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
            if _depth > 0:
                logger.info(
                    "KKG query geslaagd na splitsing (depth %d)",
                    _depth,
                )
            return resultaten
    except Exception:
        if len(na_uris) > MIN_BATCH_SIZE and _depth < MAX_SPLIT_DEPTH:
            mid = len(na_uris) // 2
            logger.info(
                "KKG query mislukt, splitsen in 2 batches van %d en %d nummeraanduidingen (depth %d)",
                mid,
                len(na_uris) - mid,
                _depth + 1,
            )
            left_na = {k: na_to_vo_ids[k] for k in na_uris[:mid]}
            right_na = {k: na_to_vo_ids[k] for k in na_uris[mid:]}
            left = await _query_kkg(session, left_na, _depth=_depth + 1)
            right = await _query_kkg(session, right_na, _depth=_depth + 1)
            return left + right
        logger.warning(
            "KKG query definitief overgeslagen voor %d nummeraanduidingen",
            len(na_uris),
        )
        return []


async def _query_verblijfsobjecten(
    session: aiohttp.ClientSession,
    identificaties: List[str],
    *,
    _depth: int = 0,
) -> List[Dict[str, Any]]:
    """Query BAG LV + KKG to obtain geometrie en beperkingen per verblijfsobject.

    Stage 1 (BAG LV) failure splits on identificaties and restarts the full pipeline.
    Stage 2 (KKG) failure splits on nummeraanduiding URIs, leaving BAG LV untouched.
    """
    if not identificaties:
        return []

    try:
        async with _get_semaphore(asyncio.get_running_loop()):
            # -------------------------
            # Stage 1 – BAG LV
            # -------------------------
            id_values = " ".join(
                f'"{identificatie}"' for identificatie in identificaties
            )
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
    except Exception:
        if len(identificaties) > MIN_BATCH_SIZE and _depth < MAX_SPLIT_DEPTH:
            mid = len(identificaties) // 2
            logger.info(
                "BAG nummeraanduiding query mislukt, splitsen in 2 batches van %d en %d IDs (depth %d)",
                mid,
                len(identificaties) - mid,
                _depth + 1,
            )
            left = await _query_verblijfsobjecten(
                session, identificaties[:mid], _depth=_depth + 1
            )
            right = await _query_verblijfsobjecten(
                session, identificaties[mid:], _depth=_depth + 1
            )
            return left + right
        logger.warning(
            "BAG nummeraanduiding query definitief overgeslagen voor %d IDs",
            len(identificaties),
        )
        return []

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

    if _depth > 0:
        logger.info(
            "BAG nummeraanduiding query geslaagd na splitsing (depth %d)",
            _depth,
        )
    # -------------------------
    # Stage 2 – KKG (own split-on-failure on nummeraanduiding URIs)
    # -------------------------
    return await _query_kkg(session, na_to_vo_ids)
