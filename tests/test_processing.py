"""Tests for batch processing and API-level retry/split logic."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest
import shapely
from shapely.geometry import Point, box

from monumenten._api._backoff import MAX_SPLIT_DEPTH, MIN_BATCH_SIZE
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import (
    _MAX_PER_BAG_REQUEST,
    _WFS_MAX_FEATURES,
    _koppel_beperkingen,
    _punten_voor_query,
    _query_adrespunten,
    _query_beperkingen,
)
from monumenten._api._provincies import (
    DRENTHE,
    ProvincialeMonumentenError,
)
from monumenten._processing import (
    _GRENS_BUFFER_METER,
    _QUERY_BATCH_GROOTTE,
    _bepaal_provinciale_monumenten,
    _get_beschermde_gezichten,
    _get_provinciegrenzen,
    ProvincieOnbepaald,
    _process_batch,
    _query,
)


def _make_batch_result(identificaties, count=None):
    """Minimal valid (rm, beschermd_gezicht, gemeentelijk) frames + count."""
    n = count if count is not None else len(identificaties)
    ids_df = pd.DataFrame({"identificatie": list(identificaties)[:n]})
    rm = ids_df.assign(
        rijksmonument_nummer="",
        rijksmonument_bron=None,
        rijksmonument_voorbescherming=False,
    )
    bg = ids_df.assign(rijksbeschermd_gezicht_naam=pd.NA)
    gm = ids_df.assign(grondslag_gemeentelijk_monument=pd.NA)
    return (rm, bg, gm, n)


@pytest.fixture
def empty_geodataframe():
    return gpd.GeoDataFrame(columns=["rijksbeschermd_gezicht_naam", "geometry"])


@pytest.mark.asyncio
async def test_query_success(empty_geodataframe):
    """_query returns merged result when _process_batch succeeds."""
    ids = ["id1", "id2", "id3"]

    async def mock_process_batch(session, batch, bg_df, adrespunten):
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._query_adrespunten", return_value={}),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with aiohttp.ClientSession() as session:
            result, onbepaald = await _query(session, ids)

    assert onbepaald == {}
    assert len(result) == 3
    assert set(result["identificatie"]) == {"id1", "id2", "id3"}
    assert list(result.columns) == [
        "identificatie",
        "rijksmonument_nummer",
        "rijksmonument_bron",
        "rijksmonument_voorbescherming",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
        "provinciaal_monument_omschrijving",
    ]


@pytest.mark.asyncio
async def test_query_batch_failure_logged(empty_geodataframe):
    """When _process_batch raises, batch is skipped and result has no rows for that batch."""
    ids = ["only_one"]

    async def mock_process_batch(session, batch, bg_df, adrespunten):
        raise RuntimeError("always fail")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._query_adrespunten", return_value={}),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with aiohttp.ClientSession() as session:
            result, _ = await _query(session, ids)

    assert len(result) == 0
    assert list(result.columns) == [
        "identificatie",
        "rijksmonument_nummer",
        "rijksmonument_bron",
        "rijksmonument_voorbescherming",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
        "provinciaal_monument_omschrijving",
    ]


@pytest.mark.asyncio
async def test_query_sorteert_batches_ruimtelijk(empty_geodataframe):
    """Een batch bevat verblijfsobjecten die bij elkaar liggen, ongeacht de invoervolgorde."""
    adrespunten = {
        "rotterdam1": (92000.0, 437000.0),
        "groningen1": (233000.0, 582000.0),
        "rotterdam2": (92100.0, 437100.0),
        "groningen2": (233100.0, 582100.0),
    }
    batches = []

    async def mock_process_batch(session, batch, bg_df, adrespunten):
        batches.append(set(batch))
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._query_adrespunten", return_value=adrespunten),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
        patch("monumenten._processing._QUERY_BATCH_GROOTTE", 2),
    ):
        async with aiohttp.ClientSession() as session:
            result, _ = await _query(session, [*adrespunten, "onbekend"])

    assert {"rotterdam1", "rotterdam2"} in batches
    assert {"groningen1", "groningen2"} in batches
    # Een verblijfsobject zonder adrespunt wordt nog steeds verwerkt
    assert set(result["identificatie"]) == {*adrespunten, "onbekend"}


def _make_cm_response(identificaties=None, fail=False):
    """Return a synchronous context manager mock for session.post(...)."""
    resp = MagicMock()
    if fail:
        resp.raise_for_status = MagicMock(side_effect=RuntimeError("transient fail"))
    else:
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(
            return_value=[
                {"identificatie": i, "rijksmonument_nummer": "1"}
                for i in (identificaties or [])
            ]
        )
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=None)
    return resp


@pytest.mark.asyncio
async def test_rijksmonumenten_splits_on_failure():
    """_query_rijksmonumenten splits batch on failure and returns combined result."""
    post_calls = []

    def mock_post_side_effect(*args, **kwargs):
        query = kwargs.get("data", {}).get("query", "")
        post_calls.append(query)
        if len(post_calls) == 1:
            return _make_cm_response(fail=True)
        if '"id1"' in query:
            return _make_cm_response(["id1"])
        return _make_cm_response(["id2"])

    noop_semaphore = MagicMock()
    noop_semaphore.__aenter__ = AsyncMock(return_value=None)
    noop_semaphore.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.post = MagicMock(side_effect=mock_post_side_effect)

    with patch(
        "monumenten._api._cultureel_erfgoed._get_semaphore",
        return_value=noop_semaphore,
    ):
        result = await _query_rijksmonumenten(session, ["id1", "id2"])

    assert len(post_calls) == 3
    assert len(result) == 2
    assert {r["identificatie"] for r in result} == {"id1", "id2"}


def _bag_response(identificaties):
    """Minimal BAG WFS response: one verblijfsobject per identificatie."""
    return {
        "features": [
            {
                "properties": {"identificatie": identificatie},
                "geometry": {"type": "Point", "coordinates": [nummer, 0.0]},
            }
            for nummer, identificatie in enumerate(identificaties)
        ]
    }


@pytest.mark.asyncio
async def test_adrespunten_bag_failure_splits_on_identificaties():
    """BAG failure → split on identificaties → both halves are retried.

    BAG call 1 (["id1","id2"]) fails → split into ["id1"] and ["id2"].
    BAG is called twice more (once per half).
    """
    queries = []

    async def mock_post_wfs(session, endpoint, query, context):
        queries.append(query)
        if len(queries) == 1:
            raise RuntimeError("BAG transient fail")
        return _bag_response([i for i in ("id1", "id2") if i in query])

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_adrespunten(AsyncMock(), ["id1", "id2"])

    assert set(result) == {"id1", "id2"}
    assert len(queries) == 3  # 1 failed + 2 per-half successes


@pytest.mark.asyncio
async def test_adrespunten_maximaal_per_request():
    """Meer dan _MAX_PER_BAG_REQUEST identificaties gaan in meerdere requests."""
    identificaties = [
        f"id{nummer:04d}" for nummer in range(2 * _MAX_PER_BAG_REQUEST + 1)
    ]
    aantallen = []

    async def mock_post_wfs(session, endpoint, query, context):
        gevraagd = [i for i in identificaties if i in query]
        aantallen.append(len(gevraagd))
        return _bag_response(gevraagd)

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_adrespunten(AsyncMock(), identificaties)

    assert set(result) == set(identificaties)
    assert sorted(aantallen) == [1, _MAX_PER_BAG_REQUEST, _MAX_PER_BAG_REQUEST]


@pytest.mark.asyncio
async def test_beperkingen_brk_pb_failure_splits_on_adrespunten():
    """BRK-PB failure → split on adrespunten → both halves are retried.

    BRK-PB call 1 (both adrespunten) fails → split into one adrespunt each.
    BRK-PB is called twice more (once per half).
    """
    call_count = 0

    async def mock_post_wfs(session, endpoint, query, context):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("BRK-PB transient fail")
        return {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_beperkingen(
            AsyncMock(), {"id1": (0.0, 0.0), "id2": (1.0, 0.0)}
        )

    assert {r["identificatie"] for r in result} == {"id1", "id2"}
    assert call_count == 3  # 1 failed + 2 per-half successes


@pytest.mark.asyncio
async def test_beperkingen_splitst_op_grootste_sprong():
    """Adrespunten die ver uit elkaar liggen gaan niet samen in één request."""
    adrespunten = {
        **{f"rotterdam{n}": (92000.0 + n, 437000.0 + n) for n in range(6)},
        **{f"groningen{n}": (233000.0 + n, 582000.0 + n) for n in range(6)},
    }
    queries = []

    async def mock_post_wfs(session, endpoint, query, context):
        queries.append(query)
        return {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_beperkingen(AsyncMock(), adrespunten)

    assert {r["identificatie"] for r in result} == set(adrespunten)
    assert len(queries) == 2
    rotterdam, groningen = sorted(queries, key=lambda q: "233000.0" in q)
    assert rotterdam.count("<gml:Point ") == groningen.count("<gml:Point ") == 6
    assert "233000.0" not in rotterdam and "92000.0" not in groningen


@pytest.mark.parametrize(
    "adrespunten",
    [
        {"a": (100.0, 5.0), "b": (200.0, 5.0)},  # horizontaal
        {"a": (5.0, 100.0), "b": (5.0, 200.0), "c": (5.0, 150.0)},  # verticaal
    ],
)
def test_punten_voor_query_hulppunt_bij_lijn(adrespunten):
    """Liggen alle adrespunten op één as-lijn, dan krijgt het request een hulppunt."""
    punten = _punten_voor_query(adrespunten)

    assert punten[:-1] == list(adrespunten.values())
    xs, ys = zip(*punten)
    assert max(xs) > min(xs) and max(ys) > min(ys)


@pytest.mark.parametrize(
    "adrespunten",
    [
        {"a": (5.0, 5.0)},  # één punt werkt al
        {"a": (5.0, 5.0), "b": (5.0, 5.0)},  # zelfde punt
        {"a": (0.0, 0.0), "b": (1.0, 1.0)},  # rechthoek
    ],
)
def test_punten_voor_query_geen_hulppunt(adrespunten):
    """Een los punt of een echte rechthoek krijgt geen hulppunt."""
    assert _punten_voor_query(adrespunten) == list(adrespunten.values())


@pytest.mark.asyncio
async def test_beperkingen_hulppunt_niet_in_uitvoer():
    """Het hulppunt gaat mee in het request maar levert geen rij in de uitvoer."""
    queries = []

    async def mock_post_wfs(session, endpoint, query, context):
        queries.append(query)
        return {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_beperkingen(
            AsyncMock(), {"a": (100.0, 5.0), "b": (200.0, 5.0)}
        )

    assert queries[0].count("<gml:Point ") == 3
    assert [r["identificatie"] for r in result] == ["a", "b"]


def test_koppel_beperkingen_alleen_binnen_beperkingsgebied():
    """Alleen het adrespunt binnen het beperkingsgebied krijgt de grondslag."""
    beperking = {
        "type": "Feature",
        "properties": {
            "grondslagCode": "GWA",
            "grondslagOmschrijving": "Gemeentewet: Aanwijzing gemeentelijk monument",
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        },
    }

    result = _koppel_beperkingen(
        {"binnen": (5.0, 5.0), "buiten": (15.0, 5.0)}, [beperking]
    )

    assert result == [
        {
            "identificatie": "binnen",
            "grondslagcode": "GWA",
            "grondslag_gemeentelijk_monument": "Gemeentewet: Aanwijzing gemeentelijk monument",
        },
        {
            "identificatie": "buiten",
            "grondslagcode": None,
            "grondslag_gemeentelijk_monument": None,
        },
    ]


@pytest.mark.asyncio
async def test_rijksmonumenten_single_id_failure_returns_empty():
    """_query_rijksmonumenten returns [] when batch size 1 fails and cannot split."""
    noop_semaphore = MagicMock()
    noop_semaphore.__aenter__ = AsyncMock(return_value=None)
    noop_semaphore.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.post = MagicMock(return_value=_make_cm_response(fail=True))

    with patch(
        "monumenten._api._cultureel_erfgoed._get_semaphore",
        return_value=noop_semaphore,
    ):
        result = await _query_rijksmonumenten(session, ["only_one"])

    assert result == []


def _beschermde_gezichten(naam, minx, miny, maxx, maxy):
    """GeoDataFrame in RD met één beschermd gezicht als rechthoek."""
    return gpd.GeoDataFrame(
        {"rijksbeschermd_gezicht_naam": [naam]},
        geometry=[shapely.box(minx, miny, maxx, maxy)],
        crs="EPSG:28992",
    )


@pytest.mark.asyncio
async def test_process_batch_zonder_adrespunten_houdt_rce_rijksmonumenten():
    """Zonder adrespunten in de batch blijven de rijksmonumenten van de RCE behouden."""
    with (
        patch(
            "monumenten._processing._query_rijksmonumenten",
            return_value=[{"identificatie": "ligplaats", "rijksmonument_nummer": "1"}],
        ),
        patch("monumenten._processing._query_beperkingen", return_value=[]),
    ):
        rijksmonumenten, gezichten, gemeentelijk, aantal = await _process_batch(
            AsyncMock(),
            ["ligplaats", "onbekend"],
            _beschermde_gezichten("Gezicht", 0, 0, 10, 10),
            {},
        )

    assert aantal == 2
    assert rijksmonumenten.to_dict("records") == [
        {
            "identificatie": "ligplaats",
            "rijksmonument_nummer": "1",
            "rijksmonument_bron": "RCE",
            "rijksmonument_voorbescherming": False,
        }
    ]
    assert gezichten.empty
    assert gemeentelijk.empty


@pytest.mark.asyncio
async def test_process_batch_beschermd_gezicht_zonder_brk_pb():
    """Faalt de BRK-PB voor een batch, dan volgt het beschermd gezicht nog uit het adrespunt."""
    with (
        patch("monumenten._processing._query_rijksmonumenten", return_value=[]),
        patch("monumenten._processing._query_beperkingen", return_value=[]),
    ):
        _, gezichten, _, _ = await _process_batch(
            AsyncMock(),
            ["binnen", "buiten"],
            _beschermde_gezichten("Gezicht", 0, 0, 10, 10),
            {"binnen": (5.0, 5.0), "buiten": (15.0, 5.0)},
        )

    assert dict(
        zip(gezichten["identificatie"], gezichten["rijksbeschermd_gezicht_naam"])
    ) == {"binnen": "Gezicht", "buiten": pytest.approx(float("nan"), nan_ok=True)}


@pytest.mark.asyncio
async def test_query_bag_faalt_definitief_houdt_rce_rijksmonumenten(
    empty_geodataframe,
):
    """Faalt de BAG definitief, dan staan de uitkomsten van de RCE nog in de uitvoer."""

    async def mock_post_wfs(session, endpoint, query, context):
        raise RuntimeError("BAG onbereikbaar")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs),
        patch(
            "monumenten._processing._query_rijksmonumenten",
            return_value=[{"identificatie": "id1", "rijksmonument_nummer": "1"}],
        ),
    ):
        result, _ = await _query(AsyncMock(), ["id1", "id2"])

    assert result[
        ["identificatie", "rijksmonument_nummer", "rijksmonument_bron"]
    ].to_dict("records") == [
        {
            "identificatie": "id1",
            "rijksmonument_nummer": "1",
            "rijksmonument_bron": "RCE",
        }
    ]


@pytest.mark.asyncio
async def test_beperkingen_afgekapt_antwoord_splitst():
    """Een afgekapt BRK-PB antwoord (1.000 features) leidt tot splitsen en nieuwe requests."""
    afgekapt = {
        "features": [
            {
                "type": "Feature",
                "properties": {"grondslagCode": "GG", "grondslagOmschrijving": ""},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
        ]
        * _WFS_MAX_FEATURES
    }
    queries = []

    async def mock_post_wfs(session, endpoint, query, context):
        queries.append(query)
        return afgekapt if len(queries) == 1 else {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        result = await _query_beperkingen(
            AsyncMock(), {"id1": (0.0, 0.0), "id2": (1.0, 0.0)}
        )

    assert {r["identificatie"] for r in result} == {"id1", "id2"}
    assert len(queries) == 3  # 1 afgekapt + 2 per helft
    assert all(q.count("<gml:Point ") == 1 for q in queries[1:])


@pytest.mark.asyncio
async def test_beschermde_gezichten_buiten_nederland_geeft_fout():
    """Levert de RCE geen WGS84 meer, dan volgt een fout in plaats van een lege join."""
    # De cache negeert de session; leeg hem zodat een eerdere test hem niet vult
    await _get_beschermde_gezichten.cache.clear()
    with patch(
        "monumenten._processing._query_beschermde_gezichten",
        return_value=[
            {
                "rijksbeschermd_gezicht_naam": "In RD",
                "gezichtWKT": "POLYGON ((92000 437000, 93000 437000, 93000 438000, 92000 437000))",
            }
        ],
    ):
        with pytest.raises(ValueError, match="buiten Nederland"):
            await _get_beschermde_gezichten(MagicMock())


def test_constants():
    """Batch and split constants match design."""
    assert _QUERY_BATCH_GROOTTE == 500
    assert MIN_BATCH_SIZE == 1
    assert MAX_SPLIT_DEPTH == 10


# ---------------------------------------------------------------------------
# Provinciale monumenten: kandidaten via bbox en provinciegrens, daarna sjoin
# ---------------------------------------------------------------------------

# Adrespunten in RD, zoals PDOK ze levert
_ROLDE = Point(239495.0, 556475.0)  # Kerkbrink 7, PM1-0001
_ROLDE_NAAST = Point(239600.0, 556600.0)  # wel in Drenthe, geen monument
_HAARLEM = Point(103800.0, 488600.0)
_LEIDEN = Point(93911.705, 464260.065)  # in de bbox van Noord-Holland, niet erin
_ROTTERDAM = Point(92500.0, 437500.0)

_DRENTSE_GRENS = box(200000, 520000, 270000, 580000)
_NH_GRENS = box(100000, 470000, 150000, 580000)


def _adrespunten(punten):
    """Adrespunten zoals _query_adrespunten ze levert: (x, y) in RD per ID."""
    return {identificatie: (p.x, p.y) for identificatie, p in punten.items()}


def _drenthe_monumenten():
    return gpd.GeoDataFrame(
        {"provinciaal_monument_omschrijving": ["PM1-0001 Dwarshuisboerderij Rolde"]},
        geometry=[_ROLDE.buffer(5)],
        crs="EPSG:28992",
    )


def _nh_monumenten():
    return gpd.GeoDataFrame(
        {"provinciaal_monument_omschrijving": ["Stelling van Amsterdam"]},
        geometry=[_HAARLEM.buffer(5)],
        crs="EPSG:28992",
    )


async def _bepaal(adrespunten, grenzen=None, monumenten=None):
    """Draai _bepaal_provinciale_monumenten met gemockte grenzen en vlakken.

    ``grenzen`` is een dict of een exception; ``monumenten`` per provincienaam een
    GeoDataFrame of een exception. Geeft het resultaat en de opgehaalde provincies.
    """
    opgehaald = []

    async def get_monumenten(session, provincie):
        opgehaald.append(provincie.naam)
        antwoord = (monumenten or {})[provincie.naam]
        if isinstance(antwoord, BaseException):
            raise antwoord
        return antwoord

    with (
        patch(
            "monumenten._processing._get_provinciegrenzen",
            AsyncMock(
                side_effect=grenzen
                if isinstance(grenzen, BaseException)
                else lambda session: grenzen
            ),
        ) as get_grenzen,
        patch(
            "monumenten._processing._get_provinciale_monumenten",
            side_effect=get_monumenten,
        ),
    ):
        resultaat = await _bepaal_provinciale_monumenten(MagicMock(), adrespunten)
    return resultaat, opgehaald, get_grenzen


_GRENZEN = {"27": _NH_GRENS, "22": _DRENTSE_GRENS}


@pytest.mark.asyncio
async def test_bepaal_provinciaal_alleen_benodigde_provincie():
    """Alleen provincies met een kandidaat worden opgehaald."""
    adrespunten = _adrespunten(
        {"rolde_in": _ROLDE, "rolde_uit": _ROLDE_NAAST, "rotterdam": _ROTTERDAM}
    )
    (result, onbepaald), opgehaald, _ = await _bepaal(
        adrespunten, _GRENZEN, {"Drenthe": _drenthe_monumenten()}
    )

    assert opgehaald == [DRENTHE.naam]
    assert onbepaald == {}
    assert result.to_dict(orient="records") == [
        {
            "identificatie": "rolde_in",
            "provinciaal_monument_omschrijving": "PM1-0001 Dwarshuisboerderij Rolde",
        }
    ]


@pytest.mark.asyncio
async def test_bepaal_provinciaal_buiten_bbox_geen_enkele_request():
    """Buiten de bboxen wordt ook de grens niet opgehaald."""
    (result, onbepaald), opgehaald, get_grenzen = await _bepaal(
        _adrespunten({"rotterdam": _ROTTERDAM}), AssertionError("geen grenzen")
    )

    assert opgehaald == []
    get_grenzen.assert_not_called()
    assert onbepaald == {}
    assert result.empty
    assert list(result.columns) == [
        "identificatie",
        "provinciaal_monument_omschrijving",
    ]


@pytest.mark.asyncio
async def test_bepaal_provinciaal_in_bbox_buiten_grens_geen_download():
    """Leiden ligt in de bbox van Noord-Holland, maar niet binnen de grens: False,
    ook als Noord-Holland plat ligt."""
    (result, onbepaald), opgehaald, _ = await _bepaal(
        _adrespunten({"leiden": _LEIDEN}),
        _GRENZEN,
        {"Noord-Holland": ProvincialeMonumentenError("Noord-Holland plat")},
    )

    assert opgehaald == []
    assert onbepaald == {}
    assert result.empty


@pytest.mark.asyncio
async def test_bepaal_provinciaal_provincie_plat_geeft_onbepaald(caplog):
    """Een kandidaat in een uitgevallen provincie is onbepaald; de andere
    provincie wordt gewoon bepaald."""
    adrespunten = _adrespunten(
        {"haarlem": _HAARLEM, "leiden": _LEIDEN, "rolde": _ROLDE}
    )
    with caplog.at_level(logging.WARNING):
        (result, onbepaald), opgehaald, _ = await _bepaal(
            adrespunten,
            _GRENZEN,
            {
                "Noord-Holland": ProvincialeMonumentenError("Noord-Holland plat"),
                "Drenthe": _drenthe_monumenten(),
            },
        )

    assert sorted(opgehaald) == ["Drenthe", "Noord-Holland"]
    assert onbepaald == {
        "Noord-Holland": ProvincieOnbepaald("Noord-Holland plat", ["haarlem"])
    }
    assert result["identificatie"].tolist() == ["rolde"]
    assert "Noord-Holland plat" in caplog.text


@pytest.mark.asyncio
async def test_bepaal_provinciaal_grenzen_falen_terugval_op_bbox(caplog):
    """Zonder grenzen is elk punt in de bbox kandidaat, ook Leiden."""
    with caplog.at_level(logging.WARNING):
        (result, onbepaald), opgehaald, _ = await _bepaal(
            _adrespunten({"haarlem": _HAARLEM, "leiden": _LEIDEN}),
            ProvincialeMonumentenError("PDOK plat"),
            {"Noord-Holland": ProvincialeMonumentenError("Noord-Holland plat")},
        )

    assert opgehaald == ["Noord-Holland"]
    assert sorted(onbepaald["Noord-Holland"].verblijfsobject_ids) == [
        "haarlem",
        "leiden",
    ]
    assert result.empty
    assert "PDOK plat" in caplog.text


@pytest.mark.asyncio
async def test_bepaal_provinciaal_onverwachte_fout_geeft_onbepaald(caplog):
    """Een fout buiten ProvincialeMonumentenError (zoals een geometrie die
    from_features niet leest) breekt de aanroep niet af."""
    with caplog.at_level(logging.WARNING):
        (result, onbepaald), _, _ = await _bepaal(
            _adrespunten({"haarlem": _HAARLEM, "rolde": _ROLDE}),
            _GRENZEN,
            {
                "Noord-Holland": ValueError("ongeldige geometrie"),
                "Drenthe": _drenthe_monumenten(),
            },
        )

    assert onbepaald["Noord-Holland"].verblijfsobject_ids == ["haarlem"]
    assert "ValueError: ongeldige geometrie" in onbepaald["Noord-Holland"].oorzaak
    assert result["identificatie"].tolist() == ["rolde"]
    # De traceback staat in de log
    assert any(record.exc_info for record in caplog.records)


@pytest.mark.asyncio
async def test_bepaal_provinciaal_onverwachte_fout_grenzen_terugval(caplog):
    """Ook een onverwachte fout bij de grenzen valt terug op de bbox."""
    with caplog.at_level(logging.WARNING):
        (result, onbepaald), opgehaald, _ = await _bepaal(
            _adrespunten({"haarlem": _HAARLEM}),
            TypeError("onverwacht"),
            {"Noord-Holland": _nh_monumenten()},
        )

    assert opgehaald == ["Noord-Holland"]
    assert onbepaald == {}
    assert result["identificatie"].tolist() == ["haarlem"]
    assert "Provinciegrenzen niet op te halen" in caplog.text


@pytest.mark.asyncio
async def test_query_mislukte_batch_bepaalt_provinciaal_monument_wel(
    empty_geodataframe,
):
    """Faalt een batch, dan wordt het provinciale monument toch bepaald: de
    adrespunten zijn er al voor de batches beginnen."""

    async def mislukte_batch(session, batch, bg_df, adrespunten):
        raise RuntimeError("RCE onbereikbaar")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch(
            "monumenten._processing._query_adrespunten",
            return_value=_adrespunten({"rolde": _ROLDE}),
        ),
        patch("monumenten._processing._process_batch", side_effect=mislukte_batch),
        patch(
            "monumenten._processing._get_provinciegrenzen",
            AsyncMock(return_value=_GRENZEN),
        ),
        patch(
            "monumenten._processing._get_provinciale_monumenten",
            AsyncMock(return_value=_drenthe_monumenten()),
        ),
    ):
        result, onbepaald = await _query(AsyncMock(), ["rolde"])

    assert onbepaald == {}
    assert result[["identificatie", "provinciaal_monument_omschrijving"]].to_dict(
        "records"
    ) == [
        {
            "identificatie": "rolde",
            "provinciaal_monument_omschrijving": "PM1-0001 Dwarshuisboerderij Rolde",
        }
    ]


@pytest.mark.asyncio
async def test_get_provinciegrenzen_buffer():
    """Een punt 50 m buiten de grens is kandidaat, een punt op 150 m niet."""
    grens = box(100000, 470000, 150000, 580000)
    with patch(
        "monumenten._processing._query_provinciegrenzen",
        AsyncMock(return_value={"27": grens, "22": _DRENTSE_GRENS}),
    ):
        # Zonder de cache, zodat andere tests er geen last van hebben
        gebieden = await _get_provinciegrenzen.__wrapped__(MagicMock())

    assert _GRENS_BUFFER_METER == 100
    assert gebieden["27"].contains(Point(99950, 500000))
    assert not gebieden["27"].contains(Point(99850, 500000))


@pytest.mark.asyncio
async def test_query_provinciaal_samen_met_rijks_en_gemeentelijk():
    """Een provinciaal monument op een ID met ook een rijks- en gemeentelijke
    status geeft één rij met alle drie; een ID met alleen een provinciaal monument
    krijgt een eigen rij."""
    adrespunten = {
        "rolde_alles": (_ROLDE.x, _ROLDE.y),
        "rolde_provinciaal": (_ROLDE.x + 1, _ROLDE.y + 1),
    }
    beperkingen = [
        {
            "identificatie": "rolde_alles",
            "grondslagcode": "GG",
            "grondslag_gemeentelijk_monument": "Gemeentewet",
        },
    ]
    drenthe_df = gpd.GeoDataFrame(
        {"provinciaal_monument_omschrijving": ["PM1-0001 Dwarshuisboerderij Rolde"]},
        geometry=[_ROLDE.buffer(5)],
        crs="EPSG:28992",
    )
    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=gpd.GeoDataFrame(
                columns=["rijksbeschermd_gezicht_naam", "geometry"]
            ),
        ),
        patch(
            "monumenten._processing._query_rijksmonumenten",
            AsyncMock(
                return_value=[
                    {"identificatie": "rolde_alles", "rijksmonument_nummer": "12345"}
                ]
            ),
        ),
        patch(
            "monumenten._processing._query_adrespunten",
            AsyncMock(return_value=adrespunten),
        ),
        patch(
            "monumenten._processing._query_beperkingen",
            AsyncMock(return_value=beperkingen),
        ),
        patch(
            "monumenten._processing._get_provinciegrenzen",
            AsyncMock(return_value=_GRENZEN),
        ),
        patch(
            "monumenten._processing._get_provinciale_monumenten",
            AsyncMock(return_value=drenthe_df),
        ),
    ):
        async with aiohttp.ClientSession() as session:
            result, onbepaald = await _query(
                session, ["rolde_alles", "rolde_provinciaal"]
            )

    assert onbepaald == {}
    assert sorted(result["identificatie"]) == ["rolde_alles", "rolde_provinciaal"]
    alles = _rij(result, "rolde_alles")
    assert alles["rijksmonument_nummer"] == "12345"
    assert alles["rijksmonument_bron"] == "RCE"
    assert alles["grondslag_gemeentelijk_monument"] == "Gemeentewet"
    assert (
        alles["provinciaal_monument_omschrijving"]
        == "PM1-0001 Dwarshuisboerderij Rolde"
    )
    alleen = _rij(result, "rolde_provinciaal")
    assert pd.isna(alleen["rijksmonument_nummer"])
    assert pd.isna(alleen["grondslag_gemeentelijk_monument"])
    assert (
        alleen["provinciaal_monument_omschrijving"]
        == "PM1-0001 Dwarshuisboerderij Rolde"
    )


# ---------------------------------------------------------------------------
# Grondslagcodes: EWE = rijksmonument, EWD = voorbescherming (geen rijksmonument)
# ---------------------------------------------------------------------------


def _beperking(identificatie, grondslagcode=None, grondslag=None):
    return {
        "identificatie": identificatie,
        "grondslagcode": grondslagcode,
        "grondslag_gemeentelijk_monument": grondslag,
    }


_EWD_GRONDSLAG = (
    "Erfgoedwet: Toezending ontwerpbesluit aanwijzing rijksmonument "
    "door minister OCW (voorbescherming)"
)
_EWE_GRONDSLAG = (
    "Erfgoedwet: Afschrift inschrijving monument of archeologisch monument "
    "in rijksmonumentenregister door minister OCW"
)


async def _run_process_batch(rijksmonumenten, beperkingen, beschermde_gezichten_df):
    ids = sorted({r["identificatie"] for r in beperkingen})
    # Rotterdam ligt buiten de bbox van Noord-Holland en Drenthe, dus er wordt
    # geen provincie bevraagd
    adrespunten = {i: (_ROTTERDAM.x, _ROTTERDAM.y) for i in ids}
    with (
        patch(
            "monumenten._processing._query_rijksmonumenten",
            AsyncMock(return_value=rijksmonumenten),
        ),
        patch(
            "monumenten._processing._query_beperkingen",
            AsyncMock(return_value=beperkingen),
        ),
    ):
        async with aiohttp.ClientSession() as session:
            return await _process_batch(
                session, ids, beschermde_gezichten_df, adrespunten
            )


def _rij(rm_df, identificatie):
    rows = rm_df[rm_df["identificatie"] == identificatie]
    assert len(rows) == 1, (
        f"verwacht precies 1 rij voor {identificatie}, kreeg {len(rows)}"
    )
    return rows.iloc[0]


@pytest.mark.asyncio
async def test_process_batch_ewd_is_geen_rijksmonument(empty_geodataframe):
    """EWD (voorbescherming) zet alleen rijksmonument_voorbescherming, geen bron."""
    rm_df, _, gm_df, n = await _run_process_batch(
        rijksmonumenten=[],
        beperkingen=[_beperking("vo_ewd", "EWD", _EWD_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    assert n == 1
    assert list(rm_df.columns) == [
        "identificatie",
        "rijksmonument_nummer",
        "rijksmonument_bron",
        "rijksmonument_voorbescherming",
    ]
    row = _rij(rm_df, "vo_ewd")
    assert pd.isna(row["rijksmonument_nummer"])
    assert pd.isna(row["rijksmonument_bron"])
    assert bool(row["rijksmonument_voorbescherming"]) is True
    # EWD is ook geen gemeentelijk monument
    assert gm_df.empty


@pytest.mark.asyncio
async def test_process_batch_ewe_is_rijksmonument_zonder_voorbescherming(
    empty_geodataframe,
):
    rm_df, _, _, _ = await _run_process_batch(
        rijksmonumenten=[],
        beperkingen=[_beperking("vo_ewe", "EWE", _EWE_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_ewe")
    assert row["rijksmonument_bron"] == "Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is False


@pytest.mark.asyncio
async def test_process_batch_ewe_en_ewd_op_zelfde_verblijfsobject(empty_geodataframe):
    """EWE + EWD: één rij, bron Kadaster én voorbescherming True."""
    rm_df, _, _, _ = await _run_process_batch(
        rijksmonumenten=[],
        beperkingen=[
            _beperking("vo_beide", "EWE", _EWE_GRONDSLAG),
            _beperking("vo_beide", "EWD", _EWD_GRONDSLAG),
        ],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_beide")
    assert row["rijksmonument_bron"] == "Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is True


@pytest.mark.asyncio
async def test_process_batch_rce_en_ewd(empty_geodataframe):
    """RCE-nummer + EWD: bron is alleen RCE (EWD telt niet als Kadaster-bron)."""
    rm_df, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": "12345"}],
        beperkingen=[_beperking("vo_rce", "EWD", _EWD_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert row["rijksmonument_nummer"] == "12345"
    assert row["rijksmonument_bron"] == "RCE"
    assert bool(row["rijksmonument_voorbescherming"]) is True


@pytest.mark.asyncio
async def test_process_batch_rce_en_ewe(empty_geodataframe):
    rm_df, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": "12345"}],
        beperkingen=[_beperking("vo_rce", "EWE", _EWE_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert row["rijksmonument_bron"] == "RCE, Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is False


@pytest.mark.asyncio
async def test_process_batch_zonder_beperking(empty_geodataframe):
    """Verblijfsobject zonder beperking komt niet in rijksmonumenten_df."""
    rm_df, _, gm_df, _ = await _run_process_batch(
        rijksmonumenten=[],
        beperkingen=[_beperking("vo_leeg")],
        beschermde_gezichten_df=empty_geodataframe,
    )

    assert rm_df.empty
    assert gm_df.empty


@pytest.mark.asyncio
async def test_process_batch_geen_bron_is_none(empty_geodataframe):
    """Een rij zonder RCE-nummer en zonder EWE krijgt None als bron, geen "".

    De client leidt rijksmonument af uit een gevulde bron (notna), dus een lege
    string zou zo'n rij ten onrechte als rijksmonument laten tellen.
    """
    rm_df, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": None}],
        beperkingen=[_beperking("vo_rce")],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert pd.isna(row["rijksmonument_bron"])
