import asyncio
import logging
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
import geopandas as gpd

from monumenten._api._backoff import AfgekaptAntwoordError, met_retries
from monumenten._bronfouten import BAG, BRK_PB, Bronuitval

# De PDOK WFS-endpoints van de twee stappen: eerst de BAG, dan de BRK-PB
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

# Stap 1 – BAG: verblijfsobject ID -> adrespunt
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

# Stap 2 – BRK-PB: adrespunten -> beperkingen
# Een beperking geldt voor haar beperkingsgebied (BAG-pand, BGT-object, handmatige
# contour of het perceel zelf). De WFS geeft alle monumentbeperkingen terug waarvan
# het gebied minstens één van de adrespunten raakt; welk adrespunt binnen welk gebied
# ligt wordt daarna lokaal bepaald (zie _koppel_beperkingen).
# Alleen pb_multipolygon wordt bevraagd, niet pb_multipoint of pb_multilinestring:
# een adrespunt ligt niet zinvol binnen een punt of lijn. De oude KKG-query
# (geof:sfWithin) had dezelfde beperking.
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
          <!-- Erfgoedwet: Afschrift inschrijving monument of archeologisch monument in rijksmonumentenregister door minister OCW (= rijksmonument) -->
          <fes:PropertyIsEqualTo>
            <fes:ValueReference>grondslagCode</fes:ValueReference>
            <fes:Literal>EWE</fes:Literal>
          </fes:PropertyIsEqualTo>
          <!-- Erfgoedwet: Toezending ontwerpbesluit aanwijzing rijksmonument door minister OCW (voorbescherming; geen rijksmonument, zie rijksmonument_voorbescherming) -->
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

# Verschuiving van het hulppunt dat een lijnvormige puntenset een rechthoek maakt
_HULPPUNT_VERSCHUIVING_M = 0.01

logger = logging.getLogger("monumenten.api.kadaster")


async def _post_wfs_json(
    session: aiohttp.ClientSession, endpoint: str, query: str, context: str
) -> Any:
    """POST een WFS GetFeature-query en geef de JSON, met retries (zie _backoff)."""

    async def post() -> Any:
        async with session.post(
            endpoint, data=query, headers={"Content-Type": "application/xml"}
        ) as response:
            response.raise_for_status()
            return await response.json()

    return await met_retries(post, context)


def _punten_voor_query(adrespunten: _Adrespunten) -> List[Tuple[float, float]]:
    """Geef de punten voor het BRK-PB request, zo nodig met een hulppunt.

    De BRK-PB WFS vindt niets als alle punten exact op één horizontale of verticale
    lijn liggen, zodat de omhullende rechthoek geen breedte of hoogte heeft. Gemeten
    op 5 november 2026: 500 punten op één horizontale lijn door Amsterdam gaven 0
    beperkingen, met één extra punt 1 cm hoger 111. Bij adrespunten op hele meters
    (zoals in Amsterdam en Maastricht) liggen de adressen van één pand vaak zo.
    Een los punt (rechthoek zonder breedte én hoogte) werkt wel.

    Een hulppunt naast de lijn maakt er een rechthoek van. Het hulppunt kan extra
    beperkingen opleveren, maar die vallen weg in _koppel_beperkingen, dat alleen naar
    de echte adrespunten kijkt.
    """
    punten = list(adrespunten.values())
    xs, ys = zip(*punten)
    if len(set(punten)) > 1 and (min(xs) == max(xs) or min(ys) == max(ys)):
        punten.append(
            (min(xs) + _HULPPUNT_VERSCHUIVING_M, min(ys) + _HULPPUNT_VERSCHUIVING_M)
        )
    return punten


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
    if beperkingen:
        punten = punten.sjoin(
            gpd.GeoDataFrame.from_features(beperkingen, crs=_RD),
            how="left",
            predicate="within",
        )
    gekoppeld = punten.reindex(
        columns=[
            "identificatie",
            "grondslagCode",
            "grondslagOmschrijving",
        ]
    ).astype(object)
    return [
        {
            "identificatie": identificatie,
            "grondslagcode": grondslagcode,
            "grondslag_gemeentelijk_monument": grondslag,
        }
        for identificatie, grondslagcode, grondslag in (
            gekoppeld.where(gekoppeld.notna(), None).itertuples(index=False)
        )
    ]


async def _query_beperkingen(
    session: aiohttp.ClientSession,
    adrespunten: _Adrespunten,
    uitval: Bronuitval,
) -> List[Dict[str, Any]]:
    """Stap 2: vraag de BRK-PB om de beperkingen bij de adrespunten.

    Eerst gaan alle requests de deur uit (zie _vraag_beperkingen), daarna wordt per
    request bepaald binnen welke beperkingen de adrespunten liggen. Een fout bij dat
    koppelen is geen uitval van de BRK-PB, die wel antwoord gaf: zij bereikt de
    aanroeper, die de batch als mislukte verwerking vastlegt. Pas koppelen als alle
    requests klaar zijn, voorkomt dat een request na zo'n fout nog ongezien doorloopt.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        adrespunten (_Adrespunten): Adrespunt (x, y in RD) per verblijfsobject ID
        uitval (Bronuitval): Hierin legt de BRK-PB vast voor welke verblijfsobjecten
            zij wel en geen antwoord gaf

    Returns:
        List[Dict[str, Any]]: Eén rij per verblijfsobject en beperking, en één rij
            zonder grondslag voor een verblijfsobject zonder beperking. Zonder
            antwoord van de BRK-PB volgen er voor een verblijfsobject geen rijen.
    """
    antwoorden = await _vraag_beperkingen(session, adrespunten, uitval)
    return [
        rij
        for punten, beperkingen in antwoorden
        for rij in _koppel_beperkingen(punten, beperkingen)
    ]


def _verdeel_in_requests(
    punten: List[Tuple[str, Tuple[float, float]]],
) -> List[List[str]]:
    """Verdeel adrespunten in BRK-PB requests: de verblijfsobject ID's per request.

    De BRK-PB WFS doorzoekt de omhullende rechthoek van alle adrespunten in een
    request en wordt traag als die groot is. Opeenvolgende adrespunten horen daarom
    bij elkaar te liggen; per request gaan er maximaal _MAX_PER_BRK_PB_REQUEST mee. Is
    de rechthoek te groot voor het aantal adrespunten, dan wordt de set gesplitst op
    de grootste sprong tussen twee opeenvolgende adrespunten.
    """
    if len(punten) > _MAX_PER_BRK_PB_REQUEST:
        return [
            request
            for i in range(0, len(punten), _MAX_PER_BRK_PB_REQUEST)
            for request in _verdeel_in_requests(punten[i : i + _MAX_PER_BRK_PB_REQUEST])
        ]
    xs, ys = zip(*(punt for _, punt in punten))
    omhullende_km2 = (max(xs) - min(xs)) * (max(ys) - min(ys)) / 1e6
    if len(punten) * omhullende_km2 > _MAX_ADRESPUNTEN_KEER_KM2:
        # Zoek de sprong in de middelste helft, zodat beide delen groot genoeg blijven
        kwart = len(punten) // 4
        sprong = max(
            range(max(kwart, 1), len(punten) - kwart),
            key=lambda i: math.dist(punten[i - 1][1], punten[i][1]),
        )
        return _verdeel_in_requests(punten[:sprong]) + _verdeel_in_requests(
            punten[sprong:]
        )
    return [[identificatie for identificatie, _ in punten]]


async def _vraag_beperkingen(
    session: aiohttp.ClientSession,
    adrespunten: _Adrespunten,
    uitval: Bronuitval,
) -> List[Tuple[_Adrespunten, List[Dict[str, Any]]]]:
    """Vraag de BRK-PB om de beperkingen bij de adrespunten.

    De adrespunten gaan in requests die ruimtelijk compact zijn (zie
    _verdeel_in_requests). Retries, splitsen bij falen en overslaan bij een storing:
    zie Bronuitval.bevraag. Geeft per beantwoorde request de adrespunten en de
    GeoJSON features die de BRK-PB teruggaf.
    """
    if not adrespunten:
        return []

    async def vraag(ids: List[str]) -> List[Tuple[_Adrespunten, List[Dict[str, Any]]]]:
        punten = {i: adrespunten[i] for i in ids}
        query = _BRK_PB_BEPERKINGEN_QUERY_TEMPLATE.format(
            adrespunten="".join(
                _ADRESPUNT_TEMPLATE.format(nummer=nummer, x=x, y=y)
                for nummer, (x, y) in enumerate(_punten_voor_query(punten))
            )
        )
        data = await _post_wfs_json(
            session, _BRK_PB_ENDPOINT, query, "BRK-PB beperkingen query"
        )
        beperkingen = data["features"]
        if len(beperkingen) >= _WFS_MAX_FEATURES:
            # Eigen logregel: lukt splitsen niet meer (bijvoorbeeld bij één adrespunt
            # met zoveel beperkingen), dan is anders alleen "definitief overgeslagen"
            # te zien en niet waarom.
            logger.warning(
                "BRK-PB response afgekapt op %d features voor %d adrespunten",
                len(beperkingen),
                len(punten),
            )
            raise AfgekaptAntwoordError("BRK-PB response is afgekapt")
        return [(punten, beperkingen)]

    delen = await asyncio.gather(
        *(
            uitval.bevraag(BRK_PB, ids, vraag)
            for ids in _verdeel_in_requests(list(adrespunten.items()))
        )
    )
    return [antwoord for deel in delen for antwoord in deel]


async def _query_adrespunten(
    session: aiohttp.ClientSession,
    identificaties: List[str],
    uitval: Bronuitval,
    *,
    voortgang: Optional[Callable[[int], object]] = None,
) -> _Adrespunten:
    """Stap 1: vraag de BAG om het adrespunt van de verblijfsobjecten.

    Per request gaan er maximaal _MAX_PER_BAG_REQUEST verblijfsobjecten mee. Retries,
    splitsen bij falen en overslaan bij een storing: zie Bronuitval.bevraag.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        identificaties (List[str]): De verblijfsobject ID's
        uitval (Bronuitval): Hierin legt de BAG vast voor welke verblijfsobjecten
            zij wel en geen antwoord gaf
        voortgang (Optional[Callable[[int], object]]): Krijgt per afgehandelde groep
            het aantal verblijfsobjecten, met of zonder antwoord; samen precies het
            aantal identificaties

    Returns:
        _Adrespunten: Adrespunt per verblijfsobject ID dat in de BAG staat. Een ID
            dat niet in de BAG staat is een geldig leeg antwoord.
    """

    async def vraag(ids: List[str]) -> List[Tuple[str, Tuple[float, float]]]:
        filters = [_IDENTIFICATIE_FILTER_TEMPLATE.format(identificatie=i) for i in ids]
        query = _BAG_VERBLIJFSOBJECTEN_QUERY_TEMPLATE.format(
            # Een fes:Or vereist minimaal twee filters
            identificatie_filter=filters[0]
            if len(filters) == 1
            else f"<fes:Or>{''.join(filters)}</fes:Or>"
        )
        data = await _post_wfs_json(
            session, _BAG_ENDPOINT, query, "BAG verblijfsobjecten query"
        )
        return [
            (
                verblijfsobject["properties"]["identificatie"],
                tuple(verblijfsobject["geometry"]["coordinates"][:2]),
            )
            for verblijfsobject in data["features"]
        ]

    async def groep(ids: List[str]) -> List[Tuple[str, Tuple[float, float]]]:
        gevonden = await uitval.bevraag(BAG, ids, vraag)
        if voortgang is not None:
            voortgang(len(ids))
        return gevonden

    delen = await asyncio.gather(
        *(
            groep(identificaties[i : i + _MAX_PER_BAG_REQUEST])
            for i in range(0, len(identificaties), _MAX_PER_BAG_REQUEST)
        )
    )
    return dict(paar for deel in delen for paar in deel)
