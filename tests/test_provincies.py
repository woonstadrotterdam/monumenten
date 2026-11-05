"""Tests voor het ophalen van provinciale monumenten uit de provinciale services."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from monumenten._api._provincies import (
    _PAGINA_GROOTTE,
    DRENTHE,
    NOORD_HOLLAND,
    PROVINCIES,
    ProvincialeMonumentenError,
    _omschrijving_drenthe,
    _omschrijving_noord_holland,
    _query_provinciale_monumenten,
    _query_provinciegrenzen,
    _waarschuw_onbekend,
)


@pytest.fixture(autouse=True)
def reset_waarschuwingen():
    """Onbekende waarden worden één keer per proces gelogd; reset per test."""
    _waarschuw_onbekend.cache_clear()


def test_omschrijving_noord_holland_categorieen(caplog):
    with caplog.at_level(logging.WARNING):
        assert (
            _omschrijving_noord_holland({"siteName_Spelling": "Stelling van Amsterdam"})
            == "Stelling van Amsterdam"
        )
        # Bekende, bewust uitgesloten categorie: geen waarschuwing
        assert (
            _omschrijving_noord_holland({"siteName_Spelling": "Keringselementen"})
            is None
        )
        assert caplog.records == []
        # Onbekende categorie: genegeerd met één waarschuwing
        assert _omschrijving_noord_holland({"siteName_Spelling": "Nieuw type"}) is None
        assert _omschrijving_noord_holland({"siteName_Spelling": "Nieuw type"}) is None
    assert len(caplog.records) == 1
    assert "Nieuw type" in caplog.records[0].message


def test_omschrijving_noord_holland_onhashbare_waarde(caplog):
    """Een list uit de service geeft een waarschuwing, geen TypeError."""
    with caplog.at_level(logging.WARNING):
        assert _omschrijving_noord_holland({"siteName_Spelling": ["a", "b"]}) is None
    assert len(caplog.records) == 1
    assert "['a', 'b']" in caplog.records[0].message


def test_omschrijving_drenthe(caplog):
    assert (
        _omschrijving_drenthe(
            {
                "PM_nummer": "PM2-0310 ",
                "PM_object": "Woningen voor ouden van dagen\r\n",
                "PM_status": "beschermd",
            }
        )
        == "PM2-0310 Woningen voor ouden van dagen"
    )
    with caplog.at_level(logging.WARNING):
        # Alleen een aangewezen monument telt; voorbescherming niet, en die
        # bekende status geeft geen waarschuwing
        assert (
            _omschrijving_drenthe(
                {
                    "PM_nummer": "PM2-0400",
                    "PM_object": "X",
                    "PM_status": "voorbeschermd",
                }
            )
            is None
        )
        assert caplog.records == []
        # Onbekende status: genegeerd met één waarschuwing
        assert _omschrijving_drenthe({"PM_status": "vervallen"}) is None
    assert len(caplog.records) == 1
    assert "vervallen" in caplog.records[0].message


def test_omschrijving_drenthe_beschermd_zonder_nummer_en_naam():
    """Een beschermd monument zonder omschrijving valt niet stil weg."""
    assert (
        _omschrijving_drenthe(
            {"PM_nummer": " ", "PM_object": None, "PM_status": "beschermd"}
        )
        == "Provinciaal monument Drenthe"
    )


_PUNT = {"type": "Point", "coordinates": [6.5, 52.9]}
_VLAK = {
    "type": "Polygon",
    "coordinates": [[[6.5, 52.9], [6.6, 52.9], [6.6, 53.0], [6.5, 52.9]]],
}


def _feature(properties, geometry=_VLAK):
    return {"type": "Feature", "properties": properties, "geometry": geometry}


def _sessie(antwoorden):
    """Session-mock waarvan session.get(...) achtereenvolgens ``antwoorden`` geeft.

    Een antwoord is een dict (JSON-body) of een exception die raise_for_status gooit.
    """
    aanroepen = []

    def get(url, params=None):
        aanroepen.append(params)
        antwoord = antwoorden[len(aanroepen) - 1]
        response = MagicMock()
        if isinstance(antwoord, BaseException):
            response.raise_for_status = MagicMock(side_effect=antwoord)
        else:
            response.raise_for_status = MagicMock()
            response.json = AsyncMock(return_value=antwoord)
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=None)
        return response

    session = MagicMock()
    session.get = MagicMock(side_effect=get)
    return session, aanroepen


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_pagineert_en_filtert():
    pagina_1 = {
        "type": "FeatureCollection",
        "exceededTransferLimit": True,
        "features": [
            _feature(
                {
                    "PM_nummer": f"PM1-{i:04d}",
                    "PM_object": "Boerderij",
                    "PM_status": "beschermd",
                }
            )
            for i in range(_PAGINA_GROOTTE)
        ],
    }
    pagina_2 = {
        "type": "FeatureCollection",
        "features": [
            _feature(
                {"PM_nummer": "PM9-0001", "PM_object": "Kerk", "PM_status": "beschermd"}
            ),
            _feature(
                {
                    "PM_nummer": "PM9-0002",
                    "PM_object": "Weg",
                    "PM_status": "voorbeschermd",
                }
            ),
            _feature(
                {
                    "PM_nummer": "PM9-0003",
                    "PM_object": "Zonder vlak",
                    "PM_status": "beschermd",
                },
                geometry=None,
            ),
        ],
    }
    session, aanroepen = _sessie([pagina_1, pagina_2])

    features = await _query_provinciale_monumenten(session, DRENTHE)

    assert [p["resultOffset"] for p in aanroepen] == ["0", str(_PAGINA_GROOTTE)]
    assert aanroepen[0]["orderByFields"] == "OBJECTID"
    assert aanroepen[0]["outSR"] == "28992"
    assert aanroepen[0]["f"] == "geojson"
    # voorbeschermd en zonder geometrie vallen af
    assert len(features) == _PAGINA_GROOTTE + 1
    assert features[-1]["properties"] == {
        "provinciaal_monument_omschrijving": "PM9-0001 Kerk"
    }
    assert set(features[-1]) == {"type", "geometry", "properties"}


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_arcgis_fout():
    """ArcGIS meldt fouten met HTTP 200 en een error-object."""
    session, _ = _sessie([{"error": {"code": 400, "message": "Unable to complete"}}])
    with pytest.raises(ProvincialeMonumentenError, match="Unable to complete"):
        await _query_provinciale_monumenten(session, NOORD_HOLLAND)


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_geen_vlakken_is_fout():
    session, _ = _sessie([{"type": "FeatureCollection", "features": []}])
    with pytest.raises(ProvincialeMonumentenError, match="Geen provinciale monumenten"):
        await _query_provinciale_monumenten(session, DRENTHE)


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_retry_dan_fout():
    """Een tijdelijke HTTP-fout wordt herhaald; daarna een duidelijke fout."""
    fout = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=503, message="Service Unavailable"
    )
    session, aanroepen = _sessie([fout, fout])
    with patch("monumenten._api._provincies.asyncio.sleep", new=AsyncMock()):
        with pytest.raises(ProvincialeMonumentenError, match="HTTP 503"):
            await _query_provinciale_monumenten(session, NOORD_HOLLAND)
    assert len(aanroepen) == 2


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_niet_herhaalbare_fout():
    fout = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=404, message="Not Found"
    )
    session, aanroepen = _sessie([fout])
    with pytest.raises(ProvincialeMonumentenError, match="HTTP 404"):
        await _query_provinciale_monumenten(session, NOORD_HOLLAND)
    assert len(aanroepen) == 1


def _drents_vlak(nummer, geometry=_VLAK):
    return _feature(
        {"PM_nummer": nummer, "PM_object": "Boerderij", "PM_status": "beschermd"},
        geometry=geometry,
    )


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_vlag_onder_properties():
    """Een korte pagina met exceededTransferLimit onder properties: doorgaan."""
    pagina_1 = {
        "type": "FeatureCollection",
        "properties": {"exceededTransferLimit": True},
        "features": [_drents_vlak("PM1-0001")],
    }
    pagina_2 = {"type": "FeatureCollection", "features": [_drents_vlak("PM1-0002")]}
    session, aanroepen = _sessie([pagina_1, pagina_2])

    features = await _query_provinciale_monumenten(session, DRENTHE)

    assert [p["resultOffset"] for p in aanroepen] == ["0", "1"]
    assert len(features) == 2


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_lege_pagina_met_vlag_stopt(caplog):
    """Een lege pagina met de vlag aan geeft geen oneindige lus."""
    pagina_1 = {
        "type": "FeatureCollection",
        "exceededTransferLimit": True,
        "features": [_drents_vlak(f"PM1-{i:04d}") for i in range(_PAGINA_GROOTTE)],
    }
    pagina_2 = {
        "type": "FeatureCollection",
        "exceededTransferLimit": True,
        "features": [],
    }
    session, aanroepen = _sessie([pagina_1, pagina_2])

    with caplog.at_level(logging.WARNING):
        features = await _query_provinciale_monumenten(session, DRENTHE)

    assert len(aanroepen) == 2
    assert len(features) == _PAGINA_GROOTTE
    assert "lege pagina" in caplog.text


@pytest.mark.asyncio
async def test_query_provinciale_monumenten_alleen_vlakken(caplog):
    """Een punt of lijn kan geen adrespunt bevatten: weglaten met waarschuwing."""
    lijn = {"type": "LineString", "coordinates": [[6.5, 52.9], [6.6, 53.0]]}
    pagina = {
        "type": "FeatureCollection",
        "features": [
            _drents_vlak("PM1-0001"),
            _drents_vlak("PM1-0002", geometry=_PUNT),
            _drents_vlak("PM1-0003", geometry=lijn),
        ],
    }
    session, _ = _sessie([pagina])

    with caplog.at_level(logging.WARNING):
        features = await _query_provinciale_monumenten(session, DRENTHE)

    assert [f["properties"]["provinciaal_monument_omschrijving"] for f in features] == [
        "PM1-0001 Boerderij"
    ]
    assert "'Point'" in caplog.text
    assert "'LineString'" in caplog.text


def _grens(code, geometry=_VLAK):
    return {"type": "Feature", "properties": {"code": code}, "geometry": geometry}


@pytest.mark.asyncio
async def test_query_provinciegrenzen_vraagt_codes_in_rd():
    session, aanroepen = _sessie(
        [{"type": "FeatureCollection", "features": [_grens("27"), _grens("22")]}]
    )

    grenzen = await _query_provinciegrenzen(session, PROVINCIES)

    assert set(grenzen) == {"27", "22"}
    assert grenzen["22"].geom_type == "Polygon"
    params = aanroepen[0]
    assert params["typeNames"] == "bestuurlijkegebieden:Provinciegebied"
    assert params["srsName"] == "EPSG:28992"
    assert "<fes:Or>" in params["FILTER"]
    assert "<fes:Literal>27</fes:Literal>" in params["FILTER"]
    assert "<fes:Literal>22</fes:Literal>" in params["FILTER"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "features",
    [
        [_grens("27")],  # Drenthe ontbreekt
        [_grens("27"), _grens("22"), _grens("22")],  # Drenthe dubbel
        [_grens("27"), _grens("22", geometry=_PUNT)],  # geen vlak
    ],
)
async def test_query_provinciegrenzen_onvolledig_is_fout(features):
    session, _ = _sessie([{"type": "FeatureCollection", "features": features}])
    with pytest.raises(ProvincialeMonumentenError, match="'22'"):
        await _query_provinciegrenzen(session, PROVINCIES)


@pytest.mark.asyncio
async def test_query_provinciegrenzen_http_400_is_fout():
    """PDOK meldt een ongeldige request met HTTP 400 en een ExceptionReport."""
    fout = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=400, message="Bad Request"
    )
    session, aanroepen = _sessie([fout])
    with pytest.raises(ProvincialeMonumentenError, match="HTTP 400"):
        await _query_provinciegrenzen(session, PROVINCIES)
    assert len(aanroepen) == 1
