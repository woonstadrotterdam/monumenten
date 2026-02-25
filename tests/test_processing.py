"""Tests for batch retry and split logic in _query."""

from unittest.mock import patch

import geopandas as gpd
import pandas as pd
import pytest

from monumenten._processing import (
    _MAX_BATCH_ATTEMPTS,
    _MAX_SPLIT_DEPTH,
    _MIN_BATCH_SIZE,
    _QUERY_BATCH_GROOTTE,
    _query,
)


def _make_batch_result(identificaties, count=None):
    """Minimal valid (rm, beschermd_gezicht, gemeentelijk) triple + count."""
    n = count if count is not None else len(identificaties)
    ids_df = pd.DataFrame({"identificatie": list(identificaties)[:n]})
    rm = ids_df.assign(rijksmonument_nummer="", rijksmonument_bron="")
    bg = ids_df.assign(beschermd_gezicht_naam=pd.NA)
    gm = ids_df.assign(grondslag_gemeentelijk_monument=pd.NA)
    return (rm, bg, gm, n)


@pytest.fixture
def empty_geodataframe():
    return gpd.GeoDataFrame(columns=["beschermd_gezicht_naam", "geometry"])


@pytest.mark.asyncio
async def test_batch_retry_then_defer(empty_geodataframe):
    """After 2 failures a batch is deferred; phase 2 then retries it."""
    ids = [f"id_{i}" for i in range(_QUERY_BATCH_GROOTTE)]
    call_count = 0

    async def mock_process_batch(session, batch, bg_df):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("transient")
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with __import__("aiohttp").ClientSession() as session:
            result = await _query(session, ids)

    # Phase 1: 2 attempts then defer. Phase 2: one attempt succeeds.
    assert call_count == 3
    assert len(result) == len(ids)
    assert set(result["identificatie"]) == set(ids)


@pytest.mark.asyncio
async def test_deferred_batch_splits_on_failure(empty_geodataframe):
    """Deferred batch that fails again is split in two; halves are retried."""
    # One batch of 4 IDs. Phase 1: 2 attempts, all fail -> deferred.
    # Phase 2: try full batch once -> fail -> split to [a,b] and [c,d]. Both halves succeed.
    ids = ["a", "b", "c", "d"]
    call_log = []

    async def mock_process_batch(session, batch, bg_df):
        call_log.append(list(batch))
        if len(batch) == 4:
            raise RuntimeError("fail")
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with __import__("aiohttp").ClientSession() as session:
            result = await _query(session, ids)

    # Phase 1: 2 calls (batch of 4). Phase 2: 1 call (batch of 4) -> split; 2 calls (halves).
    assert len([c for c in call_log if len(c) == 4]) == 3
    assert len([c for c in call_log if c == ["a", "b"]]) == 1
    assert len([c for c in call_log if c == ["c", "d"]]) == 1
    assert len(result) == 4
    assert set(result["identificatie"]) == {"a", "b", "c", "d"}


@pytest.mark.asyncio
async def test_single_id_batch_skipped_after_failure(empty_geodataframe):
    """A deferred batch of size 1 that fails is logged and skipped (no split)."""
    ids = ["only_one"]
    process_calls = []

    async def mock_process_batch(session, batch, bg_df):
        process_calls.append(len(batch))
        raise RuntimeError("always fail")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with __import__("aiohttp").ClientSession() as session:
            result = await _query(session, ids)

    # Phase 1: 2 attempts (size 1). Phase 2: 1 attempt, then cannot split (min size 1).
    assert len(process_calls) >= 3
    assert all(n == 1 for n in process_calls)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_constants():
    """Batch retry/split constants match design."""
    assert _MAX_BATCH_ATTEMPTS == 2
    assert _MIN_BATCH_SIZE == 1
    assert _MAX_SPLIT_DEPTH == 10
    assert _QUERY_BATCH_GROOTTE == 500
