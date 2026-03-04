"""Tests for batch processing and API-level retry/split logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest

from monumenten._api._backoff import MAX_SPLIT_DEPTH, MIN_BATCH_SIZE
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import _query_verblijfsobjecten
from monumenten._processing import _QUERY_BATCH_GROOTTE, _query


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

    async def mock_process_batch(session, batch, bg_df):
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
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

    async def mock_process_batch(session, batch, bg_df):
        raise RuntimeError("always fail")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
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


@pytest.mark.asyncio
async def test_verblijfsobjecten_bag_failure_splits_on_identificaties():
    """BAG LV failure → split on identificaties → both halves run full pipeline (BAG + KKG).

    BAG LV call 1 (["id1","id2"]) fails → split into ["id1"] and ["id2"].
    BAG LV is called twice more (once per half). KKG is called twice (once per half).
    BAG LV is never called again for the full batch after the failure.
    """
    call_order = []

    async def mock_post_sparql(session, endpoint, query, context):
        call_order.append(context)
        if len(call_order) == 1:
            raise RuntimeError("BAG transient fail")
        if "BAG" in context:
            if '"id1"' in query:
                return [{"voId": "id1", "nummeraanduiding": "http://na/1"}]
            if '"id2"' in query:
                return [{"voId": "id2", "nummeraanduiding": "http://na/2"}]
            return []
        # KKG
        if "na/1" in query:
            return [
                {
                    "nummeraanduiding": "http://na/1",
                    "verblijfsobjectWKT": "POINT(0 0)",
                    "grondslagcode": None,
                    "grondslag_gemeentelijk_monument": None,
                }
            ]
        return [
            {
                "nummeraanduiding": "http://na/2",
                "verblijfsobjectWKT": "POINT(0 0)",
                "grondslagcode": None,
                "grondslag_gemeentelijk_monument": None,
            }
        ]

    with patch(
        "monumenten._api._kadaster._post_sparql_json", side_effect=mock_post_sparql
    ):
        session = AsyncMock()
        result = await _query_verblijfsobjecten(session, ["id1", "id2"])

    assert len(result) == 2
    assert {r["identificatie"] for r in result} == {"id1", "id2"}
    bag_calls = [c for c in call_order if "BAG" in c]
    kkg_calls = [c for c in call_order if "KKG" in c]
    assert len(bag_calls) == 3  # 1 failed + 2 per-half successes
    assert len(kkg_calls) == 2  # one KKG per half


@pytest.mark.asyncio
async def test_verblijfsobjecten_kkg_failure_splits_on_nummeraanduidingen():
    """KKG failure → split on nummeraanduiding URIs → BAG LV is NOT re-run.

    BAG LV call 1 (["id1","id2"]) succeeds → na/1 + na/2.
    KKG call 1 (both URIs) fails → split into [na/1] and [na/2].
    KKG is called twice more (once per half). BAG LV is called exactly once total.
    """
    call_order = []
    kkg_call_count = 0

    async def mock_post_sparql(session, endpoint, query, context):
        nonlocal kkg_call_count
        call_order.append(context)
        if "BAG" in context:
            return [
                {"voId": "id1", "nummeraanduiding": "http://na/1"},
                {"voId": "id2", "nummeraanduiding": "http://na/2"},
            ]
        # KKG
        kkg_call_count += 1
        if kkg_call_count == 1:
            raise RuntimeError("KKG transient fail")
        if "na/1" in query:
            return [
                {
                    "nummeraanduiding": "http://na/1",
                    "verblijfsobjectWKT": "POINT(0 0)",
                    "grondslagcode": None,
                    "grondslag_gemeentelijk_monument": None,
                }
            ]
        return [
            {
                "nummeraanduiding": "http://na/2",
                "verblijfsobjectWKT": "POINT(0 0)",
                "grondslagcode": None,
                "grondslag_gemeentelijk_monument": None,
            }
        ]

    with patch(
        "monumenten._api._kadaster._post_sparql_json", side_effect=mock_post_sparql
    ):
        session = AsyncMock()
        result = await _query_verblijfsobjecten(session, ["id1", "id2"])

    assert len(result) == 2
    assert {r["identificatie"] for r in result} == {"id1", "id2"}
    bag_calls = [c for c in call_order if "BAG" in c]
    assert len(bag_calls) == 1  # BAG LV called exactly once, never re-run
    assert kkg_call_count == 3  # 1 failed + 2 per-half successes


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


def test_constants():
    """Batch and split constants match design."""
    assert _QUERY_BATCH_GROOTTE == 500
    assert MIN_BATCH_SIZE == 1
    assert MAX_SPLIT_DEPTH == 10
