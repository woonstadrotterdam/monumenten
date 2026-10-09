import asyncio
import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import geopandas as gpd

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    MAX_SPLIT_DEPTH,
    MIN_BATCH_SIZE,
    RATE_LIMIT_SLEEP_SECONDS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
    RETRY_SLEEP_SECONDS,
)
from monumenten._bronfouten import BAG, BRK_PB, Bronuitval

# PDOK WFS endpoints following the BAG + BRK-PB two-stage approach
_BAG_ENDPOINT = "https://service.pdok.nl/kadaster/bag/wfs/v2_0"
_BRK_PB_ENDPOINT = (
    "https://service.pdok.nl/kadaster/brk-publiekrechtelijke-beperkingen-wkpb/wfs/v1_0"
)

# De BAG WFS weigert een filter met enkele honderden ID's
_MAX_PER_BAG_REQUEST = 100
_MAX_PER_BRK_PB_REQUEST = 500
# De BRK-PB WFS doorzoekt de omhullende rechthoek van alle adrespunten in een request.
# De duur groeit met het aantal adrespunten maal de oppervlakte van die rechthoek;
# boven deze waarde (adrespunten × km²) zijn twee kleinere requests sneller dan één.
_MAX_ADRESPUNTEN_KEER_KM2 = 150_000
# Een WFS response bevat maximaal 1000 features; bij meer is het resultaat afgekapt
_WFS_MAX_FEATURES = 1000

_RD = "EPSG:28992"

# Stage 1 – BAG: verblijfsobject ID -> adrespunt
_BAG_VERBLIJFSOBJECTEN_QUERY_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature service="WFS" version="2.0.0" outputFormat="application/json"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:bag="http://bag.geonovum.nl">
  <wfs:Query typeNames="bag:verblijfsobject" srsName="urn:ogc:def:crs:EPSG::28992">
    <wfs:PropertyName>identificatie</wfs:PropertyName>
    <wfs:PropertyName>geom</wfs:PropertyName>
    <fes:Filter>{identificatie_filter}</fes:Filter>
  </wfs:Query>
</wfs:GetFeature>
"""

_IDENTIFICATIE_FILTER_TEMPLATE = (
    "<fes:PropertyIsEqualTo>"
    "<fes:ValueReference>identificatie</fes:ValueReference>"
    "<fes:Literal>{identificatie}</fes:Literal>"
    "</fes:PropertyIsEqualTo>"
)

# Stage 2 – BRK-PB: adrespunten -> beperkingen
# Een beperking geldt voor haar beperkingsgebied (BAG-pand, BGT-object, handmatige
# contour of het perceel zelf). De WFS geeft alle monumentbeperkingen terug waarvan
# het gebied minstens één van de adrespunten raakt; welk adrespunt binnen welk gebied
# ligt wordt daarna lokaal bepaald (zie _koppel_beperkingen).
_BRK_PB_BEPERKINGEN_QUERY_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:GetFeature service="WFS" version="2.0.0" outputFormat="application/json"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:fes="http://www.opengis.net/fes/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:wkpb="http://wkpb.geonovum.nl">
  <wfs:Query typeNames="wkpb:pb_multipolygon" srsName="urn:ogc:def:crs:EPSG::28992">
    <wfs:PropertyName>grondslagCode</wfs:PropertyName>
    <wfs:PropertyName>grondslagOmschrijving</wfs:PropertyName>
    <wfs:PropertyName>geom</wfs:PropertyName>
    <fes:Filter>
      <fes:And>
        <fes:Or>
          <!-- Besluit monument, Gemeentewet -->
          <fes:PropertyIsEqualTo>
            <fes:ValueReference>grondslagCode</fes:ValueReference>
            <fes:Literal>GG</fes:Literal>
          </fes:PropertyIsEqualTo>
          <!-- Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift) -->
          <fes:PropertyIsEqualTo>
            <fes:ValueReference>grondslagCode</fes:ValueReference>
            <fes:Literal>GWA</fes:Literal>
          </fes:PropertyIsEqualTo>
          <!-- Erfgoedwet: Afschrift inschrijving monument of archeologisch monument in rijksmonumentenregister door minister OCW -->
          <fes:PropertyIsEqualTo>
            <fes:ValueReference>grondslagCode</fes:ValueReference>
            <fes:Literal>EWE</fes:Literal>
          </fes:PropertyIsEqualTo>
          <!-- Erfgoedwet: Toezending ontwerpbesluit aanwijzing rijksmonument door minister OCW (voorbescherming) -->
          <fes:PropertyIsEqualTo>
            <fes:ValueReference>grondslagCode</fes:ValueReference>
            <fes:Literal>EWD</fes:Literal>
          </fes:PropertyIsEqualTo>
        </fes:Or>
        <fes:Intersects>
          <fes:ValueReference>geom</fes:ValueReference>
          <gml:MultiPoint gml:id="adrespunten" srsName="urn:ogc:def:crs:EPSG::28992">
            {adrespunten}
          </gml:MultiPoint>
        </fes:Intersects>
      </fes:And>
    </fes:Filter>
  </wfs:Query>
</wfs:GetFeature>
"""

_ADRESPUNT_TEMPLATE = (
    "<gml:pointMember>"
    '<gml:Point gml:id="adrespunt.{nummer}"><gml:pos>{x} {y}</gml:pos></gml:Point>'
    "</gml:pointMember>"
)

_Adrespunten = Dict[str, Tuple[float, float]]

_kadaster_semaphore: Optional[asyncio.Semaphore] = None

# Create a module-level logger
logger = logging.getLogger("monumenten.api.kadaster")


def _get_semaphore(loop: asyncio.AbstractEventLoop) -> asyncio.Semaphore:
    global _kadaster_semaphore
    if _kadaster_semaphore is None:
        _kadaster_semaphore = asyncio.Semaphore(4)
    return _kadaster_semaphore


def _is_rate_limited(error: BaseException) -> bool:
    return isinstance(error, aiohttp.ClientResponseError) and error.status == 429


async def _post_wfs_json(
    session: aiohttp.ClientSession, endpoint: str, query: str, context: str
) -> Any:
    """Generic helper to POST a WFS GetFeature query and return JSON with exponential backoff retries."""
    last_error: Optional[BaseException] = None
    for poging in range(MAX_ATTEMPTS):
        try:
            async with session.post(
                endpoint,
                data=query,
                headers={"Content-Type": "application/xml"},
            ) as response:
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
            wacht = RETRY_SLEEP_SECONDS
            if e.status == 429:
                retry_after = e.headers.get("Retry-After", "") if e.headers else ""
                wacht = (
                    int(retry_after)
                    if retry_after.isdigit()
                    else RATE_LIMIT_SLEEP_SECONDS
                )
            logger.warning(
                "Poging %d/%d voor %s mislukt: %s. Opnieuw proberen over %ds...",
                poging + 1,
                MAX_ATTEMPTS,
                context,
                str(e),
                wacht,
            )
            await asyncio.sleep(wacht)
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


def _koppel_beperkingen(
    adrespunten: _Adrespunten, beperkingen: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Bepaal per adrespunt binnen welke beperkingsgebieden het ligt.

    Een beperking telt alleen als het adrespunt binnen haar gebied ligt, anders krijgt
    elk adres in een gebouw dat het perceel raakt de status.

    Args:
        adrespunten (_Adrespunten): Adrespunt (x, y in RD) per verblijfsobject ID
        beperkingen (List[Dict[str, Any]]): GeoJSON features van de BRK-PB WFS

    Returns:
        List[Dict[str, Any]]: Eén rij per verblijfsobject en beperking, en één rij
            zonder grondslag voor een verblijfsobject zonder beperking
    """
    xs, ys = zip(*adrespunten.values())
    punten = gpd.GeoDataFrame(
        {"identificatie": list(adrespunten)},
        geometry=gpd.points_from_xy(xs, ys),
        crs=_RD,
    )
    punten["verblijfsobjectWKT"] = punten.geometry.to_wkt()
    if beperkingen:
        punten = punten.sjoin(
            gpd.GeoDataFrame.from_features(beperkingen, crs=_RD),
            how="left",
            predicate="within",
        )
    gekoppeld = punten.reindex(
        columns=[
            "identificatie",
            "verblijfsobjectWKT",
            "grondslagCode",
            "grondslagOmschrijving",
        ]
    ).astype(object)
    return [
        {
            "identificatie": identificatie,
            "verblijfsobjectWKT": verblijfsobject_wkt,
            "grondslagcode": grondslagcode,
            "grondslag_gemeentelijk_monument": grondslag,
        }
        for identificatie, verblijfsobject_wkt, grondslagcode, grondslag in (
            gekoppeld.where(gekoppeld.notna(), None).itertuples(index=False)
        )
    ]


async def _query_beperkingen(
    session: aiohttp.ClientSession,
    adrespunten: _Adrespunten,
    uitval: Bronuitval,
    *,
    _depth: int = 0,
) -> List[Dict[str, Any]]:
    """Stage 2: query BRK-PB for the beperkingen at the given adrespunten.

    De BRK-PB WFS doorzoekt de omhullende rechthoek van alle adrespunten in een request
    en wordt traag als die groot is. Opeenvolgende adrespunten horen daarom bij elkaar
    te liggen; per request gaan er maximaal _MAX_PER_BRK_PB_REQUEST mee. Is de rechthoek
    te groot voor het aantal adrespunten, dan wordt de set gesplitst op de grootste
    sprong tussen twee opeenvolgende adrespunten.
    Bij falen na retries wordt de set adrespunten in tweeën gesplitst en opnieuw geprobeerd,
    behalve bij een rate limit (429): splitsen levert dan alleen meer requests op.
    Is de BRK-PB gestoord, dan wordt de request overgeslagen (zie Bronuitval).
    In uitval wordt vastgelegd voor welke verblijfsobjecten de BRK-PB wel en geen
    antwoord gaf. Zonder antwoord komt het adrespunt terug zonder beperkingen: daarmee
    is het rijksbeschermd gezicht nog wel te bepalen.
    """
    if not adrespunten:
        return []

    punten = list(adrespunten.items())
    if len(punten) > _MAX_PER_BRK_PB_REQUEST:
        delen = await asyncio.gather(
            *(
                _query_beperkingen(
                    session, dict(punten[i : i + _MAX_PER_BRK_PB_REQUEST]), uitval
                )
                for i in range(0, len(punten), _MAX_PER_BRK_PB_REQUEST)
            )
        )
        return [rij for deel in delen for rij in deel]

    xs, ys = zip(*adrespunten.values())
    omhullende_km2 = (max(xs) - min(xs)) * (max(ys) - min(ys)) / 1e6
    if len(punten) * omhullende_km2 > _MAX_ADRESPUNTEN_KEER_KM2:
        # Zoek de sprong in de middelste helft, zodat beide delen groot genoeg blijven
        kwart = len(punten) // 4
        sprong = max(
            range(max(kwart, 1), len(punten) - kwart),
            key=lambda i: math.dist(punten[i - 1][1], punten[i][1]),
        )
        left, right = await asyncio.gather(
            _query_beperkingen(session, dict(punten[:sprong]), uitval),
            _query_beperkingen(session, dict(punten[sprong:]), uitval),
        )
        return left + right

    try:
        async with _get_semaphore(asyncio.get_running_loop()):
            if uitval.sla_over_bij_storing(BRK_PB, adrespunten):
                return _koppel_beperkingen(adrespunten, [])
            brk_pb_query = _BRK_PB_BEPERKINGEN_QUERY_TEMPLATE.format(
                adrespunten="".join(
                    _ADRESPUNT_TEMPLATE.format(nummer=nummer, x=x, y=y)
                    for nummer, (x, y) in enumerate(adrespunten.values())
                )
            )
            brk_pb_data = await _post_wfs_json(
                session, _BRK_PB_ENDPOINT, brk_pb_query, "BRK-PB beperkingen query"
            )

        beperkingen = brk_pb_data["features"]
        if len(beperkingen) >= _WFS_MAX_FEATURES:
            raise RuntimeError("BRK-PB response is afgekapt")

        if _depth > 0:
            logger.info(
                "BRK-PB query geslaagd na splitsing (depth %d)",
                _depth,
            )
        rijen = _koppel_beperkingen(adrespunten, beperkingen)
        uitval.registreer_antwoord(BRK_PB, len(adrespunten))
        return rijen
    except Exception as e:
        uitval.registreer_mislukte_request(BRK_PB, e)
        if (
            not uitval.is_gestoord(BRK_PB)
            and not _is_rate_limited(e)
            and len(adrespunten) > MIN_BATCH_SIZE
            and _depth < MAX_SPLIT_DEPTH
        ):
            mid = len(punten) // 2
            logger.info(
                "BRK-PB query mislukt, splitsen in 2 batches van %d en %d adrespunten (depth %d)",
                mid,
                len(punten) - mid,
                _depth + 1,
            )
            left = await _query_beperkingen(
                session, dict(punten[:mid]), uitval, _depth=_depth + 1
            )
            right = await _query_beperkingen(
                session, dict(punten[mid:]), uitval, _depth=_depth + 1
            )
            return left + right
        logger.warning(
            "BRK-PB query definitief overgeslagen voor %d adrespunten",
            len(adrespunten),
        )
        uitval.registreer_fout(BRK_PB, adrespunten, e)
        return _koppel_beperkingen(adrespunten, [])


async def _query_adrespunten(
    session: aiohttp.ClientSession,
    identificaties: List[str],
    uitval: Bronuitval,
    *,
    _depth: int = 0,
) -> _Adrespunten:
    """Stage 1: query BAG for the adrespunt of the given verblijfsobject IDs.

    Per request gaan er maximaal _MAX_PER_BAG_REQUEST verblijfsobjecten mee.
    Bij falen na retries wordt de batch in tweeën gesplitst en opnieuw geprobeerd,
    behalve bij een rate limit (429): splitsen levert dan alleen meer requests op.
    Is de BAG gestoord, dan wordt de request overgeslagen (zie Bronuitval).
    In uitval wordt vastgelegd voor welke verblijfsobjecten de BAG wel en, ook na
    retries en splitsen, geen antwoord gaf.
    """
    if not identificaties:
        return {}

    if len(identificaties) > _MAX_PER_BAG_REQUEST:
        gevonden: _Adrespunten = {}
        for deel in await asyncio.gather(
            *(
                _query_adrespunten(
                    session, identificaties[i : i + _MAX_PER_BAG_REQUEST], uitval
                )
                for i in range(0, len(identificaties), _MAX_PER_BAG_REQUEST)
            )
        ):
            gevonden.update(deel)
        return gevonden

    try:
        async with _get_semaphore(asyncio.get_running_loop()):
            if uitval.sla_over_bij_storing(BAG, identificaties):
                return {}
            filters = [
                _IDENTIFICATIE_FILTER_TEMPLATE.format(identificatie=identificatie)
                for identificatie in identificaties
            ]
            bag_query = _BAG_VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
                # Een fes:Or vereist minimaal twee filters
                identificatie_filter=filters[0]
                if len(filters) == 1
                else f"<fes:Or>{''.join(filters)}</fes:Or>"
            )
            bag_data = await _post_wfs_json(
                session, _BAG_ENDPOINT, bag_query, "BAG verblijfsobjecten query"
            )

        adrespunten: _Adrespunten = {}
        for verblijfsobject in bag_data["features"]:
            x, y = verblijfsobject["geometry"]["coordinates"][:2]
            adrespunten[verblijfsobject["properties"]["identificatie"]] = (x, y)
    except Exception as e:
        uitval.registreer_mislukte_request(BAG, e)
        if (
            not uitval.is_gestoord(BAG)
            and not _is_rate_limited(e)
            and len(identificaties) > MIN_BATCH_SIZE
            and _depth < MAX_SPLIT_DEPTH
        ):
            mid = len(identificaties) // 2
            logger.info(
                "BAG verblijfsobjecten query mislukt, splitsen in 2 batches van %d en %d IDs (depth %d)",
                mid,
                len(identificaties) - mid,
                _depth + 1,
            )
            left = await _query_adrespunten(
                session, identificaties[:mid], uitval, _depth=_depth + 1
            )
            right = await _query_adrespunten(
                session, identificaties[mid:], uitval, _depth=_depth + 1
            )
            return {**left, **right}
        logger.warning(
            "BAG verblijfsobjecten query definitief overgeslagen voor %d IDs",
            len(identificaties),
        )
        uitval.registreer_fout(BAG, identificaties, e)
        return {}

    # Ook een leeg antwoord is een antwoord: het verblijfsobject staat dan niet in de BAG
    uitval.registreer_antwoord(BAG, len(identificaties))

    if _depth > 0:
        logger.info(
            "BAG verblijfsobjecten query geslaagd na splitsing (depth %d)",
            _depth,
        )
    return adrespunten
