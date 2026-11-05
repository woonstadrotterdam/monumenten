"""Tests for batch processing and API-level retry/split logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest
import shapely

from monumenten._api._backoff import MAX_SPLIT_DEPTH, MIN_BATCH_SIZE
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import (
    _MAX_PER_BAG_REQUEST,
    _WFS_MAX_FEATURES,
    _koppel_beperkingen,
    _query_adrespunten,
    _query_beperkingen,
)
from monumenten._processing import (
    _QUERY_BATCH_GROOTTE,
    _get_beschermde_gezichten,
    _process_batch,
    _query,
)


def _make_batch_result(identificaties, count=None):
    """Minimal valid (rm, beschermd_gezicht, gemeentelijk) triple + count."""
    n = count if count is not None else len(identificaties)
    ids_df = pd.DataFrame({"identificatie": list(identificaties)[:n]})
    rm = ids_df.assign(rijksmonument_nummer="", rijksmonument_bron="")
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
            result = await _query(session, ids)

    assert len(result) == 3
    assert set(result["identificatie"]) == {"id1", "id2", "id3"}
    assert list(result.columns) == [
        "identificatie",
        "rijksmonument_nummer",
        "rijksmonument_bron",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
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
            result = await _query(session, ids)

    assert len(result) == 0
    assert list(result.columns) == [
        "identificatie",
        "rijksmonument_nummer",
        "rijksmonument_bron",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
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
            result = await _query(session, [*adrespunten, "onbekend"])

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
        result = await _query(AsyncMock(), ["id1", "id2"])

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
