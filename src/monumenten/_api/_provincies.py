"""Provinciale monumenten uit de kaartservices van Noord-Holland en Drenthe.

Alleen deze twee provincies wijzen provinciale monumenten aan. Het Kadaster
registreert ze niet (de grondslagcodes MP en PVA zijn leeg) en de landelijke
PDOK-dataset loopt achter op de provincies zelf. Daarom worden de ArcGIS
REST-services van de provincies rechtstreeks bevraagd. Beide leveren
monumentvlakken als GeoJSON in WGS84, hetzelfde stelsel als de adrespunten
van het Kadaster, zodat er geen herprojectie nodig is.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

import aiohttp

from monumenten._api._backoff import (
    MAX_ATTEMPTS,
    RETRY_SLEEP_SECONDS,
    RETRYABLE_NETWORK_EXCEPTIONS,
    RETRYABLE_STATUS_CODES,
)

logger = logging.getLogger("monumenten.api.provincies")

# De service van Noord-Holland gaf time-outs bij pagina's van 500 vlakken.
_PAGINA_GROOTTE = 250


class ProvincialeMonumentenError(RuntimeError):
    """De provinciale monumenten van een provincie konden niet worden opgehaald.

    Deze fout wordt niet stil overgeslagen: zonder de data zou elk
    verblijfsobject in die provincie onterecht ``provinciaal_monument = False``
    krijgen, terwijl die status meetelt in de woningwaardering.
    """


@dataclass(frozen=True)
class Provincie:
    """Een provincie met provinciale monumenten en de service waar ze staan.

    Attributes:
        naam (str): Naam van de provincie, voor logging en foutmeldingen.
        url (str): ArcGIS REST ``query``-endpoint van de laag met monumentvlakken.
        velden (Tuple[str, ...]): Attributen die per vlak worden opgevraagd.
        bbox (Tuple[float, float, float, float]): Ruime omhullende rechthoek van
            de provincie als (min_lon, min_lat, max_lon, max_lat) in WGS84. De
            service wordt alleen bevraagd als een adrespunt hierbinnen ligt.
        omschrijving (Callable[[Dict[str, Any]], Optional[str]]): Bepaalt uit de
            attributen van een vlak de omschrijving van het monument, of ``None``
            als het vlak niet als provinciaal monument telt.
    """

    naam: str
    url: str
    velden: Tuple[str, ...]
    bbox: Tuple[float, float, float, float]
    omschrijving: Callable[[Dict[str, Any]], Optional[str]]


@lru_cache(maxsize=None)
def _waarschuw_onbekend(provincie: str, veld: str, waarde: object) -> None:
    """Logt één keer per proces dat een onbekende waarde is genegeerd.

    Args:
        provincie (str): Naam van de provincie.
        veld (str): Naam van het attribuut met de onbekende waarde.
        waarde (object): De onbekende waarde.
    """
    logger.warning(
        "Provinciale monumenten %s: onbekende waarde %r voor %s genegeerd; "
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
    if isinstance(categorie, str) and categorie in _NH_CATEGORIEEN:
        return categorie
    if categorie not in _NH_UITGESLOTEN_CATEGORIEEN:
        _waarschuw_onbekend("Noord-Holland", "siteName_Spelling", categorie)
    return None


# Alleen een door gedeputeerde staten aangewezen monument telt (Besluit
# huurprijzen woonruimte, bijlage I). Een voorbeschermd object is nog niet
# aangewezen.
_DR_STATUS_BESCHERMD = "beschermd"


def _omschrijving_drenthe(attributen: Dict[str, Any]) -> Optional[str]:
    """Omschrijving van een Drents monumentvlak: nummer en naam.

    Args:
        attributen (Dict[str, Any]): Attributen van het vlak uit de service.

    Returns:
        Optional[str]: Bijvoorbeeld ``"PM1-0001 Dwarshuisboerderij Rolde"``, of
        ``None`` als het monument niet de status ``beschermd`` heeft.
    """
    status = attributen.get("PM_status")
    if status != _DR_STATUS_BESCHERMD:
        _waarschuw_onbekend("Drenthe", "PM_status", status)
        return None
    nummer = str(attributen.get("PM_nummer") or "").strip()
    naam = " ".join(str(attributen.get("PM_object") or "").split())
    return " ".join(deel for deel in (nummer, naam) if deel) or None


NOORD_HOLLAND = Provincie(
    naam="Noord-Holland",
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
    url=(
        "https://kaartportaal.drenthe.nl/server/rest/services/37/"
        "Provinciale_monumenten_Drenthe/MapServer/3/query"
    ),
    velden=("PM_nummer", "PM_object", "PM_status"),
    bbox=(6.00, 52.55, 7.15, 53.25),
    omschrijving=_omschrijving_drenthe,
)

PROVINCIES: Tuple[Provincie, ...] = (NOORD_HOLLAND, DRENTHE)


async def _get_json(
    session: aiohttp.ClientSession, provincie: Provincie, params: Dict[str, str]
) -> Dict[str, Any]:
    """Haalt één pagina op bij de service van een provincie, met retries.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP-requests.
        provincie (Provincie): De provincie waarvan de service wordt bevraagd.
        params (Dict[str, str]): Queryparameters voor het ArcGIS-endpoint.

    Returns:
        Dict[str, Any]: De JSON-response.

    Raises:
        ProvincialeMonumentenError: Als de service na alle pogingen geen geldig
            antwoord geeft.
    """
    context = f"provinciale monumenten {provincie.naam}"
    for poging in range(MAX_ATTEMPTS):
        try:
            async with session.get(provincie.url, params=params) as response:
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
        List[Dict[str, Any]]: GeoJSON-features (WGS84) met als enige property
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
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(_PAGINA_GROOTTE),
        }
        pagina = await _get_json(session, provincie, params)
        nieuwe = pagina.get("features") or []
        for feature in nieuwe:
            aantal_vlakken += 1
            omschrijving = provincie.omschrijving(feature.get("properties") or {})
            if omschrijving is None or feature.get("geometry") is None:
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": {"provinciaal_monument_omschrijving": omschrijving},
                }
            )
        if len(nieuwe) < _PAGINA_GROOTTE and not pagina.get("exceededTransferLimit"):
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
