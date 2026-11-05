"""Provinciale monumenten uit de kaartservices van Noord-Holland en Drenthe.

Alleen deze twee provincies wijzen provinciale monumenten aan. Het Kadaster
registreert ze niet (de grondslagcodes MP en PVA zijn leeg) en de landelijke
PDOK-dataset loopt achter op de provincies zelf. Daarom worden de ArcGIS
REST-services van de provincies rechtstreeks bevraagd. Beide slaan de
monumentvlakken op in RD (EPSG:28992) en leveren ze in dat stelsel als GeoJSON.

Welke adrespunten in een provincie liggen, volgt uit de provinciegrenzen van
PDOK (Bestuurlijke Gebieden, afgeleid uit de BRK).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

import aiohttp
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    RETRY_SLEEP_SECONDS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
)

logger = logging.getLogger("monumenten.api.provincies")

# De service van Noord-Holland gaf time-outs bij pagina's van 500 vlakken.
_PAGINA_GROOTTE = 250

_RD = "EPSG:28992"

_PROVINCIEGRENZEN_URL = "https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0"


class ProvincialeMonumentenError(RuntimeError):
    """De provinciale monumenten of de provinciegrenzen konden niet worden opgehaald.

    De fout blijft intern. Zonder de monumentvlakken van een provincie wordt
    ``provinciaal_monument`` voor haar adressen ``pd.NA`` in plaats van stil
    ``False``, omdat die status meetelt in de woningwaardering.
    """


@dataclass(frozen=True)
class Provincie:
    """Een provincie met provinciale monumenten en de service waar ze staan.

    Attributes:
        naam (str): Naam van de provincie, voor logging en foutmeldingen.
        code (str): Provinciecode van de BRK, waarmee de provinciegrens bij PDOK
            wordt opgevraagd.
        url (str): ArcGIS REST ``query``-endpoint van de laag met monumentvlakken.
        velden (Tuple[str, ...]): Attributen die per vlak worden opgevraagd.
        bbox (Tuple[float, float, float, float]): Ruime omhullende rechthoek van
            de provincie als (min_lon, min_lat, max_lon, max_lat) in WGS84. De
            grens en de service worden alleen bevraagd als een adrespunt
            hierbinnen ligt.
        omschrijving (Callable[[Dict[str, Any]], Optional[str]]): Bepaalt uit de
            attributen van een vlak de omschrijving van het monument, of ``None``
            als het vlak niet als provinciaal monument telt.
    """

    naam: str
    code: str
    url: str
    velden: Tuple[str, ...]
    bbox: Tuple[float, float, float, float]
    omschrijving: Callable[[Dict[str, Any]], Optional[str]]


@lru_cache(maxsize=None)
def _waarschuw_onbekend(provincie: str, veld: str, waarde: str) -> None:
    """Logt één keer per proces dat een onbekende waarde is genegeerd.

    Args:
        provincie (str): Naam van de provincie.
        veld (str): Naam van het attribuut met de onbekende waarde.
        waarde (str): ``repr`` van de onbekende waarde; een string, zodat ook een
            list of dict uit de service hashbaar is voor de cache.
    """
    logger.warning(
        "Provinciale monumenten %s: onbekende waarde %s voor %s genegeerd; "
        "controleer of de service is gewijzigd",
        provincie,
        waarde,
        veld,
    )


# Noord-Holland deelt het erfgoedregister in categorieën in. Alleen categorieën
# waarvan het vlak een gebouw of object omvat tellen mee. Bij een dijk
# (Keringselementen) is de dijk zelf het monument, niet de bebouwing erop; bij
# een archeologisch terrein of beschermd dorpsgezicht evenmin. Gemeten op
# 16 september 2026 gaven de dijken 249 onterechte treffers (vakantiehuisjes op
# de Zeedijk bij Uitdam, woningen in Wervershoof) tegenover 456 terechte in de
# overige categorieën.
_NH_CATEGORIEEN: FrozenSet[str] = frozenset(
    {
        "Kleine objecten",
        "Stelling van Amsterdam",
        "Stelling van Den Helder",
        "Waterstaatkundige werken",
    }
)
_NH_UITGESLOTEN_CATEGORIEEN: FrozenSet[str] = frozenset(
    {"Keringselementen", "Archeologie", "Dorpsgezicht"}
)


def _omschrijving_noord_holland(attributen: Dict[str, Any]) -> Optional[str]:
    """Omschrijving van een Noord-Hollands monumentvlak: de categorie.

    Args:
        attributen (Dict[str, Any]): Attributen van het vlak uit de service.

    Returns:
        Optional[str]: De categorie, of ``None`` als het vlak niet meetelt.
    """
    categorie = attributen.get("siteName_Spelling")
    if isinstance(categorie, str):
        if categorie in _NH_CATEGORIEEN:
            return categorie
        if categorie in _NH_UITGESLOTEN_CATEGORIEEN:
            return None
    _waarschuw_onbekend("Noord-Holland", "siteName_Spelling", repr(categorie))
    return None


# Alleen een door gedeputeerde staten aangewezen monument telt (Besluit
# huurprijzen woonruimte, bijlage I). Een voorbeschermd object is nog niet
# aangewezen.
_DR_STATUS_BESCHERMD = "beschermd"
_DR_UITGESLOTEN_STATUSSEN: FrozenSet[str] = frozenset({"voorbeschermd"})


def _omschrijving_drenthe(attributen: Dict[str, Any]) -> Optional[str]:
    """Omschrijving van een Drents monumentvlak: nummer en naam.

    Args:
        attributen (Dict[str, Any]): Attributen van het vlak uit de service.

    Returns:
        Optional[str]: Bijvoorbeeld ``"PM1-0001 Dwarshuisboerderij Rolde"``, of
        ``None`` als het monument niet de status ``beschermd`` heeft. Een
        beschermd monument zonder nummer en naam krijgt een vaste omschrijving,
        zodat het niet stil wegvalt.
    """
    status = attributen.get("PM_status")
    if status != _DR_STATUS_BESCHERMD:
        if not (isinstance(status, str) and status in _DR_UITGESLOTEN_STATUSSEN):
            _waarschuw_onbekend("Drenthe", "PM_status", repr(status))
        return None
    nummer = str(attributen.get("PM_nummer") or "").strip()
    naam = " ".join(str(attributen.get("PM_object") or "").split())
    return (
        " ".join(deel for deel in (nummer, naam) if deel)
        or "Provinciaal monument Drenthe"
    )


NOORD_HOLLAND = Provincie(
    naam="Noord-Holland",
    code="27",
    url=(
        "https://geoservices.noord-holland.nl/ags/rest/services/"
        "oi_dataservice_protected_sites/MapServer/2/query"
    ),
    velden=("siteName_Spelling",),
    bbox=(4.40, 52.10, 5.40, 53.30),
    omschrijving=_omschrijving_noord_holland,
)

DRENTHE = Provincie(
    naam="Drenthe",
    code="22",
    url=(
        "https://kaartportaal.drenthe.nl/server/rest/services/37/"
        "Provinciale_monumenten_Drenthe/MapServer/3/query"
    ),
    velden=("PM_nummer", "PM_object", "PM_status"),
    bbox=(6.00, 52.55, 7.15, 53.25),
    omschrijving=_omschrijving_drenthe,
)

PROVINCIES: Tuple[Provincie, ...] = (NOORD_HOLLAND, DRENTHE)

# Een adrespunt ligt nooit ``within`` een punt of lijn, dus zo'n object zou stil
# niet meetellen. Beide services leveren nu alleen polygonen.
_VLAK_TYPEN: FrozenSet[str] = frozenset({"Polygon", "MultiPolygon"})


async def _get_json(
    session: aiohttp.ClientSession, url: str, params: Dict[str, str], context: str
) -> Dict[str, Any]:
    """Haalt één JSON-antwoord op bij een kaartservice, met retries.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP-requests.
        url (str): Het endpoint.
        params (Dict[str, str]): Queryparameters voor het endpoint.
        context (str): Wat er wordt opgehaald, voor logging en foutmeldingen.

    Returns:
        Dict[str, Any]: De JSON-response.

    Raises:
        ProvincialeMonumentenError: Als de service na alle pogingen geen geldig
            antwoord geeft.
    """
    for poging in range(MAX_ATTEMPTS):
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                # ArcGIS antwoordt met application/geo+json
                data = await response.json(content_type=None)
                if poging >= 1:
                    logger.info(
                        "Poging %d/%d voor %s geslaagd na eerdere mislukking",
                        poging + 1,
                        MAX_ATTEMPTS,
                        context,
                    )
        except aiohttp.ClientResponseError as e:
            if e.status not in RETRYABLE_STATUS_CODES or poging == MAX_ATTEMPTS - 1:
                raise ProvincialeMonumentenError(
                    f"Ophalen van {context} mislukt: HTTP {e.status}"
                ) from e
        except RETRYABLE_NETWORK_EXCEPTIONS as e:
            if poging == MAX_ATTEMPTS - 1:
                raise ProvincialeMonumentenError(
                    f"Ophalen van {context} mislukt (netwerk): {e}"
                ) from e
        else:
            if not isinstance(data, dict):
                raise ProvincialeMonumentenError(
                    f"Onverwacht antwoord van {context}: {type(data).__name__}"
                )
            if "error" in data:
                # ArcGIS meldt fouten met HTTP 200 en een error-object
                raise ProvincialeMonumentenError(f"Fout van {context}: {data['error']}")
            return data
        logger.warning(
            "Poging %d/%d voor %s mislukt. Opnieuw proberen over %ds...",
            poging + 1,
            MAX_ATTEMPTS,
            context,
            RETRY_SLEEP_SECONDS,
        )
        await asyncio.sleep(RETRY_SLEEP_SECONDS)
    raise ProvincialeMonumentenError(f"Ophalen van {context} mislukt")


async def _query_provinciale_monumenten(
    session: aiohttp.ClientSession, provincie: Provincie
) -> List[Dict[str, Any]]:
    """Haalt alle monumentvlakken van een provincie op die als monument tellen.

    De service wordt pagina voor pagina uitgelezen. Vlakken waarvoor
    ``provincie.omschrijving`` ``None`` geeft, worden weggelaten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP-requests.
        provincie (Provincie): De provincie waarvan de monumenten worden opgehaald.

    Returns:
        List[Dict[str, Any]]: GeoJSON-features (RD) met als enige property
        ``provinciaal_monument_omschrijving``.

    Raises:
        ProvincialeMonumentenError: Als de service faalt of geen enkel vlak
            oplevert.
    """
    features: List[Dict[str, Any]] = []
    aantal_vlakken = 0
    offset = 0
    while True:
        params = {
            "where": "1=1",
            "outFields": ",".join(provincie.velden),
            "outSR": "28992",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(_PAGINA_GROOTTE),
            # Zonder vaste volgorde kan offset-paginering vlakken overslaan of
            # dubbel geven.
            "orderByFields": "OBJECTID",
        }
        pagina = await _get_json(
            session,
            provincie.url,
            params,
            f"provinciale monumenten {provincie.naam}",
        )
        nieuwe = pagina.get("features") or []
        for feature in nieuwe:
            aantal_vlakken += 1
            omschrijving = provincie.omschrijving(feature.get("properties") or {})
            geometrie = feature.get("geometry")
            if omschrijving is None or geometrie is None:
                continue
            if geometrie.get("type") not in _VLAK_TYPEN:
                _waarschuw_onbekend(
                    provincie.naam, "geometrietype", repr(geometrie.get("type"))
                )
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": {"provinciaal_monument_omschrijving": omschrijving},
                }
            )
        # Bij f=geojson staat de vlag top-level of onder properties.
        meer = pagina.get("exceededTransferLimit") or (
            pagina.get("properties") or {}
        ).get("exceededTransferLimit")
        if not nieuwe and meer:
            logger.warning(
                "Provinciale monumenten %s: lege pagina bij offset %d terwijl de "
                "service meer vlakken meldt; ophalen gestopt",
                provincie.naam,
                offset,
            )
        if not nieuwe or (len(nieuwe) < _PAGINA_GROOTTE and not meer):
            break
        offset += len(nieuwe)

    if not features:
        raise ProvincialeMonumentenError(
            f"Geen provinciale monumenten gevonden voor {provincie.naam} "
            f"({aantal_vlakken} vlakken opgehaald)"
        )
    logger.debug(
        "Provinciale monumenten %s: %d van %d vlakken tellen mee",
        provincie.naam,
        len(features),
        aantal_vlakken,
    )
    return features


def _filter_op_codes(codes: List[str]) -> str:
    """FES-filter dat de provinciegebieden met de gegeven codes selecteert."""
    gelijk = "".join(
        "<fes:PropertyIsEqualTo><fes:ValueReference>code</fes:ValueReference>"
        f"<fes:Literal>{code}</fes:Literal></fes:PropertyIsEqualTo>"
        for code in codes
    )
    if len(codes) > 1:
        gelijk = f"<fes:Or>{gelijk}</fes:Or>"
    return (
        f'<fes:Filter xmlns:fes="http://www.opengis.net/fes/2.0">{gelijk}</fes:Filter>'
    )


async def _query_provinciegrenzen(
    session: aiohttp.ClientSession, provincies: Tuple[Provincie, ...]
) -> Dict[str, BaseGeometry]:
    """Haalt de grenzen van provincies op bij PDOK, in RD.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP-requests.
        provincies (Tuple[Provincie, ...]): De provincies waarvan de grens nodig is.

    Returns:
        Dict[str, BaseGeometry]: Per provinciecode de grens als (multi)polygoon.

    Raises:
        ProvincialeMonumentenError: Als PDOK faalt of niet voor elke provincie
            precies één (multi)polygoon geeft.
    """
    codes = [provincie.code for provincie in provincies]
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "bestuurlijkegebieden:Provinciegebied",
        "srsName": _RD,
        "outputFormat": "application/json",
        "propertyName": "code,geom",
        "FILTER": _filter_op_codes(codes),
    }
    data = await _get_json(session, _PROVINCIEGRENZEN_URL, params, "provinciegrenzen")
    grenzen: Dict[str, List[BaseGeometry]] = {code: [] for code in codes}
    for feature in data.get("features") or []:
        code = (feature.get("properties") or {}).get("code")
        geometrie = feature.get("geometry") or {}
        if code in grenzen and geometrie.get("type") in _VLAK_TYPEN:
            grenzen[code].append(shape(geometrie))
    afwijkend = {
        code: len(vlakken) for code, vlakken in grenzen.items() if len(vlakken) != 1
    }
    if afwijkend:
        raise ProvincialeMonumentenError(
            f"Onverwachte provinciegrenzen van PDOK: aantal vlakken per code {afwijkend}"
        )
    return {code: vlakken[0] for code, vlakken in grenzen.items()}
