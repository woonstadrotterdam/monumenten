"""Tests for batch processing and API-level retry/split logic."""

import asyncio
import contextlib
import re
import warnings
from collections import Counter
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from monumenten._api._backoff import (
    MAX_CONSECUTIVE_FAILURES,
    MAX_SPLIT_DEPTH,
    MIN_BATCH_SIZE,
    OUTAGE_PROBE_SECONDS,
)
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import (
    _MAX_PER_BAG_REQUEST,
    _koppel_beperkingen,
    _query_adrespunten,
    _query_beperkingen,
)
from monumenten._bronfouten import (
    BAG,
    BRK_PB,
    RCE,
    BronFoutError,
    Bronuitval,
    MonumentStatusOnbepaaldWarning,
)
from monumenten._processing import (
    _BATCH_CONCURRENCY,
    _QUERY_BATCH_GROOTTE,
    _process_batch,
    _query,
)
from monumenten.client import MonumentenClient

_RD = "EPSG:28992"


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


class _Klok:
    """Stands in for the time and asyncio modules in _bronfouten: time only passes
    when a test says so, or when the code under test sleeps."""

    def __init__(self):
        self.nu = 1000.0

    def monotonic(self):
        return self.nu

    async def sleep(self, seconden):
        self.nu += seconden


@pytest.fixture(autouse=True)
def klok():
    """Freeze the clock that decides when a source that is down gets a probe request,
    so that no test depends on how fast it runs or really waits."""
    klok = _Klok()
    with (
        patch("monumenten._bronfouten.time", klok),
        patch("monumenten._bronfouten.asyncio", klok),
    ):
        yield klok


@pytest.mark.asyncio
async def test_query_success(empty_geodataframe):
    """_query returns merged result when _process_batch succeeds."""
    ids = ["id1", "id2", "id3"]

    async def mock_process_batch(session, batch, bg_df, adrespunten, uitval):
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
            result, uitval = await _query(session, ids)

    assert uitval.mislukt == {}
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
async def test_query_batch_failure_is_not_swallowed(empty_geodataframe):
    """When _process_batch raises, the error reaches the caller instead of an empty
    result (which would read as "no monument"), unwrapped from the TaskGroup."""
    ids = ["only_one"]

    async def mock_process_batch(session, batch, bg_df, adrespunten, uitval):
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
            with pytest.raises(RuntimeError, match="always fail"):
                await _query(session, ids)


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

    async def mock_process_batch(session, batch, bg_df, adrespunten, uitval):
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
        uitval = Bronuitval()
        result = await _query_rijksmonumenten(session, ["id1", "id2"], uitval)

    assert uitval.mislukt == {}
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
        uitval = Bronuitval()
        result = await _query_adrespunten(AsyncMock(), ["id1", "id2"], uitval)

    assert uitval.mislukt == {}
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
        result = await _query_adrespunten(AsyncMock(), identificaties, Bronuitval())

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
        uitval = Bronuitval()
        result = await _query_beperkingen(
            AsyncMock(), {"id1": (0.0, 0.0), "id2": (1.0, 0.0)}, uitval
        )

    assert uitval.mislukt == {}
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
        result = await _query_beperkingen(AsyncMock(), adrespunten, Bronuitval())

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
            "verblijfsobjectWKT": "POINT (5 5)",
            "grondslagcode": "GWA",
            "grondslag_gemeentelijk_monument": "Gemeentewet: Aanwijzing gemeentelijk monument",
        },
        {
            "identificatie": "buiten",
            "verblijfsobjectWKT": "POINT (15 5)",
            "grondslagcode": None,
            "grondslag_gemeentelijk_monument": None,
        },
    ]


@pytest.mark.asyncio
async def test_rijksmonumenten_single_id_failure_is_registered():
    """A batch of size 1 that fails cannot split: no rows, and the ID is registered as
    unanswered so it is not mistaken for "no rijksmonument"."""
    noop_semaphore = MagicMock()
    noop_semaphore.__aenter__ = AsyncMock(return_value=None)
    noop_semaphore.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock()
    session.post = MagicMock(return_value=_make_cm_response(fail=True))

    with patch(
        "monumenten._api._cultureel_erfgoed._get_semaphore",
        return_value=noop_semaphore,
    ):
        uitval = Bronuitval()
        result = await _query_rijksmonumenten(session, ["only_one"], uitval)

    assert result == []
    assert list(uitval.mislukt) == [RCE]
    assert list(uitval.mislukt[RCE]) == ["only_one"]
    assert str(uitval.mislukt[RCE]["only_one"]) == "transient fail"


def test_constants():
    """Batch and split constants match design."""
    assert _QUERY_BATCH_GROOTTE == 500
    assert MIN_BATCH_SIZE == 1
    assert MAX_SPLIT_DEPTH == 10
    assert MAX_CONSECUTIVE_FAILURES == 10
    assert OUTAGE_PROBE_SECONDS == 10


# ---------------------------------------------------------------------------
# A source that gives no answer is not the same as "not a monument"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rijksmonumenten_partial_failure_registers_only_failed_ids():
    """id1 answers after the split, id2 keeps failing: only id2 is unanswered."""

    def mock_post_side_effect(*args, **kwargs):
        query = kwargs.get("data", {}).get("query", "")
        if '"id2"' in query:
            return _make_cm_response(fail=True)
        return _make_cm_response(["id1"])

    session = MagicMock()
    session.post = MagicMock(side_effect=mock_post_side_effect)

    uitval = Bronuitval()
    result = await _query_rijksmonumenten(session, ["id1", "id2"], uitval)

    assert [r["identificatie"] for r in result] == ["id1"]
    assert list(uitval.mislukt[RCE]) == ["id2"]
    uitval.controleer()  # the RCE answered for id1, so it is not down


@pytest.mark.asyncio
async def test_rijksmonumenten_unexpected_format_is_not_an_empty_answer():
    """A response that is not a list is no answer and must not read as "no rijksmonument"."""
    response = _make_cm_response()
    response.json = AsyncMock(return_value={"error": "boom"})
    session = MagicMock()
    session.post = MagicMock(return_value=response)

    uitval = Bronuitval()
    result = await _query_rijksmonumenten(session, ["only_one"], uitval)

    assert result == []
    assert list(uitval.mislukt[RCE]) == ["only_one"]


@pytest.mark.asyncio
async def test_adrespunten_unknown_id_is_an_answer_not_a_failure():
    """An ID that is not in the BAG gives a valid empty answer: no adrespunt, no failure."""

    async def mock_post_wfs(session, endpoint, query, context):
        return {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        uitval = Bronuitval()
        result = await _query_adrespunten(AsyncMock(), ["unknown"], uitval)

    assert result == {}
    assert uitval.mislukt == {}
    uitval.controleer()
    assert uitval.waarschuwing() is None


@pytest.mark.asyncio
async def test_adrespunten_bag_failure_is_registered():
    """The same empty result after a BAG error is a failure. The BAG answered for no ID
    at all, so controleer raises."""

    async def mock_post_wfs(session, endpoint, query, context):
        raise RuntimeError("BAG down")

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        uitval = Bronuitval()
        result = await _query_adrespunten(AsyncMock(), ["id1", "id2"], uitval)

    assert result == {}
    assert list(uitval.mislukt) == [BAG]
    with pytest.raises(BronFoutError, match="BAG down") as exc_info:
        uitval.controleer()
    assert exc_info.value.verblijfsobject_ids == {BAG: ["id1", "id2"]}
    assert str(exc_info.value.__cause__) == "BAG down"


@pytest.mark.asyncio
async def test_beperkingen_brk_pb_failure_is_registered_per_verblijfsobject():
    """BRK-PB keeps failing for id2 only: id2 is unanswered by BRK-PB. Its adrespunt
    still comes back, without beperkingen, so that its rijksbeschermd gezicht can be
    determined."""

    async def mock_post_wfs(session, endpoint, query, context):
        if "<gml:pos>1.0 0.0</gml:pos>" in query:
            raise RuntimeError("BRK-PB down")
        return {"features": []}

    with patch("monumenten._api._kadaster._post_wfs_json", side_effect=mock_post_wfs):
        uitval = Bronuitval()
        result = await _query_beperkingen(
            AsyncMock(), {"id1": (0.0, 0.0), "id2": (1.0, 0.0)}, uitval
        )

    assert [(r["identificatie"], r["verblijfsobjectWKT"]) for r in result] == [
        ("id1", "POINT (0 0)"),
        ("id2", "POINT (1 0)"),
    ]
    assert list(uitval.mislukt) == [BRK_PB]
    uitval.controleer()  # BRK-PB answered for id1, so it is not down
    waarschuwing = uitval.waarschuwing()
    assert isinstance(waarschuwing, MonumentStatusOnbepaaldWarning)
    assert waarschuwing.verblijfsobject_ids == {BRK_PB: ["id2"]}
    assert "id2" in str(waarschuwing)
    assert "BRK-PB down" in str(waarschuwing)


@pytest.mark.asyncio
async def test_process_batch_keeps_rce_result_without_adrespunt(empty_geodataframe):
    """Without any adrespunt for the batch, what the RCE knows is kept."""
    with patch(
        "monumenten._processing._query_rijksmonumenten",
        AsyncMock(
            return_value=[{"identificatie": "id1", "rijksmonument_nummer": "123"}]
        ),
    ):
        rm_df, bg_df, gm_df, n = await _process_batch(
            MagicMock(), ["id1", "id2"], empty_geodataframe, {}, Bronuitval()
        )

    assert rm_df.to_dict(orient="records") == [
        {
            "identificatie": "id1",
            "rijksmonument_nummer": "123",
            "rijksmonument_bron": "RCE",
        }
    ]
    assert bg_df.empty
    assert gm_df.empty
    assert n == 2


@pytest.mark.asyncio
async def test_query_raises_when_source_fails_for_all_ids(empty_geodataframe):
    """A source that answers for no ID at all, over all batches, stops the call."""

    async def mock_process_batch(session, batch, bg_df, adrespunten, uitval):
        uitval.registreer_antwoord(RCE, len(batch))
        uitval.registreer_fout(BRK_PB, batch, RuntimeError("503"))
        return _make_batch_result([])

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._query_adrespunten", return_value={}),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
        patch("monumenten._processing._QUERY_BATCH_GROOTTE", 1),
    ):
        async with aiohttp.ClientSession() as session:
            with pytest.raises(BronFoutError, match="BRK-PB") as exc_info:
                await _query(session, ["id1", "id2"])

    assert sorted(exc_info.value.verblijfsobject_ids[BRK_PB]) == ["id1", "id2"]
    assert RCE not in exc_info.value.verblijfsobject_ids


@pytest.mark.asyncio
async def test_query_returns_partial_failure(empty_geodataframe):
    """A source that fails for one batch but answers for another is not down: the
    result comes back together with the IDs that are still unanswered after the
    retry at the end."""
    pogingen = Counter()

    async def mock_process_batch(session, batch, bg_df, adrespunten, uitval):
        pogingen[tuple(batch)] += 1
        if batch == ["id1"]:
            uitval.registreer_fout(BRK_PB, batch, RuntimeError("503"))
            return _make_batch_result([])
        uitval.registreer_antwoord(BRK_PB, len(batch))
        return _make_batch_result(batch)

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._query_adrespunten", return_value={}),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
        patch("monumenten._processing._QUERY_BATCH_GROOTTE", 1),
    ):
        async with aiohttp.ClientSession() as session:
            result, uitval = await _query(session, ["id1", "id2"])

    assert pogingen == {("id1",): 2, ("id2",): 1}
    assert list(result["identificatie"]) == ["id2"]
    assert list(uitval.mislukt) == [BRK_PB]
    assert list(uitval.mislukt[BRK_PB]) == ["id1"]


# Client with all three sources mocked ---------------------------------------

_VO_RIJKS = "0599010000000001"  # rijksmonument according to the RCE
_VO_GEEN = "0599010000000002"  # no monument
_VO_GEMEENTE = "0599010000000003"  # gemeentelijk monument in a rijksbeschermd gezicht
_VO_KADASTER_RIJKS = "0599010000000004"  # rijksmonument according to Kadaster only
_VO_ONBEKEND = "0599010000000005"  # not in the BAG
_ALLE = [_VO_RIJKS, _VO_GEEN, _VO_GEMEENTE, _VO_KADASTER_RIJKS, _VO_ONBEKEND]
_ID_KOLOM = "bag_verblijfsobject_id"
_STATUSSEN = ["rijksmonument", "rijksbeschermd_gezicht", "gemeentelijk_monument"]
# Adrespunt (RD) per verblijfsobject in the BAG; the gezicht is the square (0, 0)-(10, 10)
_ADRESPUNT = {
    _VO_RIJKS: (50.0, 50.0),
    _VO_GEEN: (60.0, 60.0),
    _VO_GEMEENTE: (5.0, 5.0),
    _VO_KADASTER_RIJKS: (70.0, 70.0),
}


def _pos(vo_id):
    """The adrespunt of a verblijfsobject as it appears in a BRK-PB query."""
    x, y = _ADRESPUNT[vo_id]
    return f"<gml:pos>{x} {y}</gml:pos>"


def _beperking(vo_id, grondslagcode, grondslag):
    """A beperking whose gebied is a small square around the adrespunt."""
    x, y = _ADRESPUNT[vo_id]
    return {
        "type": "Feature",
        "properties": {
            "grondslagCode": grondslagcode,
            "grondslagOmschrijving": grondslag,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [x - 1, y - 1],
                    [x + 1, y - 1],
                    [x + 1, y + 1],
                    [x - 1, y + 1],
                    [x - 1, y - 1],
                ]
            ],
        },
    }


_BEPERKINGEN = {
    _VO_GEMEENTE: _beperking(_VO_GEMEENTE, "GWA", "Gemeentewet: Aanwijzing"),
    _VO_KADASTER_RIJKS: _beperking(_VO_KADASTER_RIJKS, "EWE", "Erfgoedwet: Afschrift"),
}


@contextlib.contextmanager
def _mocked_sources(rce_down=(), bag_down=(), brk_pb_down=()):
    """Yield a client whose sources are mocked for the verblijfsobjecten above.

    A source fails every request that contains one of its ``*_down`` IDs, so after
    splitting exactly those IDs stay unanswered. The collections are read at every
    request: a test can bring a source back by emptying its list.
    """

    def rce_post(*args, **kwargs):
        query = kwargs["data"]["query"]
        if any(vo_id in query for vo_id in rce_down):
            return _make_cm_response(fail=True)
        return _make_cm_response([i for i in (_VO_RIJKS,) if i in query])

    async def kadaster_post(session, endpoint, query, context):
        if "BAG" in context:
            if any(vo_id in query for vo_id in bag_down):
                raise RuntimeError("BAG 503")
            return {
                "features": [
                    {
                        "properties": {"identificatie": vo_id},
                        "geometry": {"type": "Point", "coordinates": list(punt)},
                    }
                    for vo_id, punt in _ADRESPUNT.items()
                    if vo_id in query
                ]
            }
        if any(_pos(vo_id) in query for vo_id in brk_pb_down if vo_id in _ADRESPUNT):
            raise RuntimeError("BRK-PB 503")
        return {
            "features": [
                beperking
                for vo_id, beperking in _BEPERKINGEN.items()
                if _pos(vo_id) in query
            ]
        }

    session = MagicMock()
    session.post = MagicMock(side_effect=rce_post)
    gezichten = gpd.GeoDataFrame(
        {"rijksbeschermd_gezicht_naam": ["Testgezicht"]},
        geometry=[box(0, 0, 10, 10)],
        crs=_RD,
    )
    with (
        patch("monumenten._api._kadaster._post_wfs_json", side_effect=kadaster_post),
        patch(
            "monumenten._processing._get_beschermde_gezichten", return_value=gezichten
        ),
    ):
        yield MonumentenClient(session=session)


def _statussen(result, **per_status):
    """Expected status columns, in the order of _ALLE."""
    return pd.DataFrame(per_status, index=result.index, dtype="boolean")[_STATUSSEN]


def _onbepaald(record):
    """The IDs per source of the one MonumentStatusOnbepaaldWarning in the record."""
    (waarschuwing,) = [
        w.message
        for w in record
        if issubclass(w.category, MonumentStatusOnbepaaldWarning)
    ]
    return {bron: sorted(ids) for bron, ids in waarschuwing.verblijfsobject_ids.items()}


@pytest.mark.asyncio
async def test_client_all_sources_answer():
    """Baseline: every status is a nullable boolean without NA, and nothing is warned.
    The ID that is not in the BAG is a valid empty answer and stays False."""
    with _mocked_sources() as client, warnings.catch_warnings():
        warnings.simplefilter("error", MonumentStatusOnbepaaldWarning)
        result = await client.process_from_df(
            pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
        )

    assert list(result[_ID_KOLOM]) == _ALLE
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, False, False, True, False],
            rijksbeschermd_gezicht=[False, False, True, False, False],
            gemeentelijk_monument=[False, False, True, False, False],
        ),
    )
    assert list(result["rijksmonument_bron"].fillna("")) == [
        "RCE",
        "",
        "",
        "Kadaster",
        "",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("down", "bron"), [("rce_down", RCE), ("bag_down", BAG), ("brk_pb_down", BRK_PB)]
)
async def test_client_raises_when_a_source_is_down(down, bron):
    """The incident of 7 October 2026: with a Kadaster source down, a known
    rijksmonument came back as rijksmonument False. Now the call fails."""
    with _mocked_sources(**{down: _ALLE}) as client:
        with pytest.raises(BronFoutError, match=bron) as exc_info:
            await client.process_from_list(_ALLE)

    assert list(exc_info.value.verblijfsobject_ids) == [bron]


@pytest.mark.asyncio
async def test_client_single_id_never_gets_an_undetermined_status():
    """With one ID a failing source fails for all IDs, so the caller gets an error."""
    with _mocked_sources(brk_pb_down=[_VO_RIJKS]) as client:
        with pytest.raises(BronFoutError, match="BRK-PB"):
            await client.process_from_list([_VO_RIJKS], to_vera=True)


@pytest.mark.asyncio
async def test_client_partial_brk_pb_failure_gives_na_and_warning():
    """BRK-PB fails for two IDs. What depends on the beperkingen is NA for those two;
    the rijksmonument the RCE knows stays True, and the rijksbeschermd gezicht only
    needs the adrespunt from the BAG."""
    brk_pb_down = [_VO_RIJKS, _VO_GEMEENTE]
    with _mocked_sources(brk_pb_down=brk_pb_down) as client:
        with pytest.warns(MonumentStatusOnbepaaldWarning, match="BRK-PB 503") as record:
            result = await client.process_from_df(
                pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
            )

    assert _onbepaald(record) == {BRK_PB: sorted(brk_pb_down)}
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, False, pd.NA, True, False],
            rijksbeschermd_gezicht=[False, False, True, False, False],
            gemeentelijk_monument=[pd.NA, False, pd.NA, False, False],
        ),
    )


@pytest.mark.asyncio
async def test_client_partial_bag_failure_gives_na_and_warning():
    """The BAG fails for two IDs. Without an adrespunt nothing from the Kadaster can
    be determined for them; the rijksmonument the RCE knows stays True."""
    bag_down = [_VO_RIJKS, _VO_GEEN]
    with _mocked_sources(bag_down=bag_down) as client:
        with pytest.warns(MonumentStatusOnbepaaldWarning, match="BAG 503") as record:
            result = await client.process_from_df(
                pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
            )

    assert _onbepaald(record) == {BAG: sorted(bag_down)}
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, pd.NA, False, True, False],
            rijksbeschermd_gezicht=[pd.NA, pd.NA, True, False, False],
            gemeentelijk_monument=[pd.NA, pd.NA, True, False, False],
        ),
    )


@pytest.mark.asyncio
async def test_client_partial_rce_failure_only_affects_rijksmonument():
    """The RCE fails for two IDs. Only rijksmonument is NA, and only where the
    Kadaster does not already say it is one."""
    rce_down = [_VO_GEEN, _VO_KADASTER_RIJKS]
    with _mocked_sources(rce_down=rce_down) as client:
        with pytest.warns(MonumentStatusOnbepaaldWarning) as record:
            result = await client.process_from_df(
                pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
            )

    assert _onbepaald(record) == {RCE: sorted(rce_down)}
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, pd.NA, False, True, False],
            rijksbeschermd_gezicht=[False, False, True, False, False],
            gemeentelijk_monument=[False, False, True, False, False],
        ),
    )


@pytest.mark.asyncio
async def test_client_partial_failure_from_list():
    """In process_from_list an undetermined status is None; in VERA format the whole
    value is None, because a list would be incomplete."""
    with _mocked_sources(brk_pb_down=[_VO_RIJKS, _VO_GEEN]) as client:
        with pytest.warns(MonumentStatusOnbepaaldWarning):
            result = await client.process_from_list(_ALLE)
        with pytest.warns(MonumentStatusOnbepaaldWarning):
            vera = await client.process_from_list(_ALLE, to_vera=True)

    assert result[_VO_RIJKS]["rijksmonument"] is True
    assert result[_VO_RIJKS]["rijksmonument_bron"] == ["RCE"]
    assert result[_VO_RIJKS]["rijksbeschermd_gezicht"] is False
    assert result[_VO_RIJKS]["gemeentelijk_monument"] is None
    assert [result[_VO_GEEN][status] for status in _STATUSSEN] == [None, False, None]
    assert [result[_VO_GEMEENTE][status] for status in _STATUSSEN] == [
        False,
        True,
        True,
    ]
    assert [result[_VO_ONBEKEND][status] for status in _STATUSSEN] == [
        False,
        False,
        False,
    ]

    assert vera[_VO_RIJKS] is None
    assert vera[_VO_GEEN] is None
    assert [status["code"] for status in vera[_VO_GEMEENTE]] == ["SGR", "GEM"]
    assert [status["code"] for status in vera[_VO_KADASTER_RIJKS]] == ["RIJ"]
    assert vera[_VO_ONBEKEND] == []


# ---------------------------------------------------------------------------
# A source that is down: stop instead of splitting down to every single ID
# ---------------------------------------------------------------------------


def _ids(aantal):
    return [f"vo{nummer:04d}" for nummer in range(aantal)]


def _ids_in(query):
    return re.findall(r"vo\d{4}", query)


def test_source_is_down_after_consecutive_failed_requests():
    """MAX_CONSECUTIVE_FAILURES failed requests in a row mark a source as down. An
    answer in between resets the count."""
    uitval = Bronuitval()
    fout = RuntimeError("503")

    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    assert not uitval.is_gestoord(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {}

    uitval.registreer_mislukte_request(BRK_PB, fout)
    assert uitval.is_gestoord(BRK_PB)
    assert not uitval.is_gestoord(RCE)

    # BRK-PB answered before it went down, so the call itself can still succeed
    assert uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    assert uitval.mislukt == {BRK_PB: {"id1": fout}}
    assert not uitval.sla_over_als_bron_plat_ligt(["id2"])
    uitval.controleer()


def test_source_that_went_down_halfway_is_probed_until_it_answers(klok):
    """A source that answered before it went down gets one probe request per
    OUTAGE_PROBE_SECONDS. Every other request is skipped until one is answered."""
    uitval = Bronuitval()
    fout, proef_fout = RuntimeError("503"), RuntimeError("still 503")
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BRK_PB, fout)

    assert uitval.sla_over_bij_storing(BRK_PB, ["id1"])
    klok.nu += OUTAGE_PROBE_SECONDS
    assert not uitval.sla_over_bij_storing(BRK_PB, ["proef1"])  # the probe request
    assert uitval.sla_over_bij_storing(BRK_PB, ["id2"])

    # The probe fails: still down, and the pause starts again
    uitval.registreer_mislukte_request(BRK_PB, proef_fout)
    assert uitval.is_gestoord(BRK_PB)
    klok.nu += OUTAGE_PROBE_SECONDS - 1
    assert uitval.sla_over_bij_storing(BRK_PB, ["id3"])
    klok.nu += 1
    assert not uitval.sla_over_bij_storing(BRK_PB, ["proef2"])

    # The probe is answered: the source is asked again
    uitval.registreer_antwoord(BRK_PB, 1)
    assert not uitval.is_gestoord(BRK_PB)
    assert not uitval.sla_over_bij_storing(BRK_PB, ["id4"])
    assert uitval.mislukt == {BRK_PB: {"id1": fout, "id2": fout, "id3": proef_fout}}

    # It is back for real: going down again takes a full run of failed requests
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    assert not uitval.is_gestoord(BRK_PB)


def test_source_down_before_its_first_answer_is_not_probed(klok):
    """Without a single answer the call fails, so there is nothing to recover."""
    uitval = Bronuitval()
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(RCE, RuntimeError("503"))

    klok.nu += OUTAGE_PROBE_SECONDS
    assert uitval.sla_over_bij_storing(RCE, ["id1"])
    assert uitval.sla_over_als_bron_plat_ligt(["id2"])


def test_source_down_before_its_first_answer_fails_the_call():
    """A source that goes down before it answered once fails the call, also when a
    request that was still under way answers after all: batches have been skipped."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(RCE, fout)

    assert uitval.sla_over_als_bron_plat_ligt(["id1", "id2"])
    uitval.registreer_antwoord(RCE, 1)

    with pytest.raises(BronFoutError, match="RCE") as exc_info:
        uitval.controleer()
    assert exc_info.value.verblijfsobject_ids == {RCE: ["id1", "id2"]}
    assert exc_info.value.__cause__ is fout


def test_retry_is_only_for_sources_that_are_not_down():
    """An ID gets a retry when a source that is up again gave no answer for it. Asking
    a source that is still down has no use."""
    uitval = Bronuitval()
    fout = RuntimeError("503")
    uitval.registreer_antwoord(BRK_PB, 1)
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        uitval.registreer_mislukte_request(BRK_PB, fout)
    uitval.registreer_fout(BRK_PB, ["id1", "id2"], fout)
    uitval.registreer_fout(RCE, ["id2", "id3"], fout)

    assert uitval.gestoorde_bronnen() == [BRK_PB]
    assert uitval.herkansbaar() == ["id2", "id3"]

    uitval.vergeet(["id2", "id3"])
    assert uitval.mislukt == {BRK_PB: {"id1": fout}}


@pytest.mark.asyncio
async def test_rijksmonumenten_stops_splitting_when_rce_is_down():
    """Splitting a full batch down to single IDs would cost 999 failed requests."""
    ids = _ids(_QUERY_BATCH_GROOTTE)
    session = MagicMock()
    session.post = MagicMock(return_value=_make_cm_response(fail=True))

    uitval = Bronuitval()
    result = await _query_rijksmonumenten(session, ids, uitval)

    assert result == []
    assert session.post.call_count == MAX_CONSECUTIVE_FAILURES
    assert sorted(uitval.mislukt[RCE]) == ids
    with pytest.raises(BronFoutError, match="transient fail"):
        uitval.controleer()


@pytest.mark.asyncio
async def test_adrespunten_stops_splitting_when_bag_is_down():
    """The BAG gets MAX_CONSECUTIVE_FAILURES requests, not one per ID and split."""
    ids = _ids(_MAX_PER_BAG_REQUEST)
    post = AsyncMock(side_effect=RuntimeError("BAG 503"))

    with patch("monumenten._api._kadaster._post_wfs_json", post):
        uitval = Bronuitval()
        result = await _query_adrespunten(AsyncMock(), ids, uitval)

    assert result == {}
    assert post.await_count == MAX_CONSECUTIVE_FAILURES
    assert sorted(uitval.mislukt[BAG]) == ids


@pytest.mark.asyncio
async def test_beperkingen_stops_splitting_when_brk_pb_is_down():
    """BRK-PB gets MAX_CONSECUTIVE_FAILURES requests, not one per adrespunt and split.
    Every adrespunt still comes back, without beperkingen."""
    ids = _ids(_QUERY_BATCH_GROOTTE)
    adrespunten = {vo_id: (float(nummer), 0.0) for nummer, vo_id in enumerate(ids)}
    post = AsyncMock(side_effect=RuntimeError("BRK-PB 503"))

    with patch("monumenten._api._kadaster._post_wfs_json", post):
        uitval = Bronuitval()
        result = await _query_beperkingen(AsyncMock(), adrespunten, uitval)

    assert post.await_count == MAX_CONSECUTIVE_FAILURES
    assert sorted(r["identificatie"] for r in result) == ids
    assert {r["grondslagcode"] for r in result} == {None}
    assert sorted(uitval.mislukt[BRK_PB]) == ids


@pytest.mark.asyncio
async def test_rijksmonumenten_keeps_splitting_while_smaller_queries_succeed():
    """What splitting is for: a query that is too heavy. More than
    MAX_CONSECUTIVE_FAILURES requests fail in total, but never in a row."""
    ids = _ids(64)
    mislukt = []

    def mock_post_side_effect(*args, **kwargs):
        bevraagd = _ids_in(kwargs["data"]["query"])
        if len(bevraagd) > 4:
            mislukt.append(len(bevraagd))
            return _make_cm_response(fail=True)
        return _make_cm_response(bevraagd)

    session = MagicMock()
    session.post = MagicMock(side_effect=mock_post_side_effect)

    uitval = Bronuitval()
    result = await _query_rijksmonumenten(session, ids, uitval)

    assert len(mislukt) > MAX_CONSECUTIVE_FAILURES
    assert not uitval.is_gestoord(RCE)
    assert uitval.mislukt == {}
    assert [r["identificatie"] for r in result] == ids


@pytest.mark.asyncio
async def test_full_batch_can_split_down_to_single_ids_without_being_down():
    """The limit leaves room for the deepest split of one full batch: when only
    single-ID queries succeed, every ID is still answered."""
    ids = _ids(_QUERY_BATCH_GROOTTE)

    def mock_post_side_effect(*args, **kwargs):
        bevraagd = _ids_in(kwargs["data"]["query"])
        return _make_cm_response(bevraagd, fail=len(bevraagd) > 1)

    session = MagicMock()
    session.post = MagicMock(side_effect=mock_post_side_effect)

    uitval = Bronuitval()
    result = await _query_rijksmonumenten(session, ids, uitval)

    assert uitval.mislukt == {}
    assert [r["identificatie"] for r in result] == ids


@contextlib.contextmanager
def _counting_sources(fails, batch_grootte, kadaster_wacht=False):
    """Yield a session and a request count per source, for sources that know any ID.

    ``fails(bron, nummer)`` tells whether the request with that number (per source,
    counted from 1) fails. With ``kadaster_wacht`` a Kadaster request gives way to
    other requests before it is answered, as a real request does. The batches keep
    the order of the input, so that a test can tell which IDs were in which batch.
    """
    requests = Counter()

    def rce_post(*args, **kwargs):
        requests[RCE] += 1
        return _make_cm_response(fail=fails(RCE, requests[RCE]))

    async def kadaster_post(session, endpoint, query, context):
        bron = BAG if "BAG" in context else BRK_PB
        requests[bron] += 1
        mislukt = fails(bron, requests[bron])
        if kadaster_wacht:
            await asyncio.sleep(0)
        if mislukt:
            raise RuntimeError(f"{bron} 503")
        if bron == BAG:
            return _bag_response(_ids_in(query))
        return {"features": []}

    session = MagicMock()
    session.post = MagicMock(side_effect=rce_post)
    gezichten = gpd.GeoDataFrame(
        {"rijksbeschermd_gezicht_naam": ["Testgezicht"]},
        geometry=[box(0, 0, 10, 10)],
        crs=_RD,
    )
    with (
        patch("monumenten._api._kadaster._post_wfs_json", side_effect=kadaster_post),
        patch(
            "monumenten._processing._get_beschermde_gezichten", return_value=gezichten
        ),
        patch("monumenten._processing._QUERY_BATCH_GROOTTE", batch_grootte),
        patch(
            "monumenten._processing._sorteer_ruimtelijk",
            side_effect=lambda verblijfsobject_ids, adrespunten: verblijfsobject_ids,
        ),
    ):
        yield session, requests


@pytest.mark.asyncio
@pytest.mark.parametrize("bron", [RCE, BAG, BRK_PB])
async def test_query_stops_early_when_a_source_is_down_from_the_start(bron):
    """20 batches and one source that never answers. That source gets
    MAX_CONSECUTIVE_FAILURES requests instead of one per ID and split, and the other
    sources are only asked for what had already started."""
    ids = _ids(20 * 16)

    with _counting_sources(lambda b, nummer: b == bron, batch_grootte=16) as (
        session,
        requests,
    ):
        with pytest.raises(BronFoutError, match=bron) as exc_info:
            await _query(session, ids)

    assert requests[bron] == MAX_CONSECUTIVE_FAILURES
    for andere_bron in {RCE, BAG, BRK_PB} - {bron}:
        assert requests[andere_bron] <= _BATCH_CONCURRENCY
    assert list(exc_info.value.verblijfsobject_ids) == [bron]
    assert sorted(exc_info.value.verblijfsobject_ids[bron]) == ids
    assert str(exc_info.value.__cause__) in {"transient fail", f"{bron} 503"}


@pytest.mark.asyncio
async def test_query_stops_before_the_other_sources_when_bag_is_down():
    """Without the BAG there is no adrespunt, so nothing else is asked at all."""
    with _counting_sources(lambda b, nummer: b == BAG, batch_grootte=16) as (
        session,
        requests,
    ):
        with pytest.raises(BronFoutError, match="BAG"):
            await _query(session, _ids(20 * 16))

    assert requests == {BAG: MAX_CONSECUTIVE_FAILURES}


@pytest.mark.asyncio
async def test_query_continues_without_a_source_that_goes_down_halfway():
    """BRK-PB answers for the first two batches and then goes down for good. The call
    still returns: after MAX_CONSECUTIVE_FAILURES failed requests BRK-PB is only
    asked once more, by the probe request at the end. That fails, so nothing is
    retried, and only the IDs BRK-PB did not answer for are reported."""
    ids = _ids(20 * 16)

    with _counting_sources(
        lambda b, nummer: b == BRK_PB and nummer > 2, batch_grootte=16
    ) as (session, requests):
        result, uitval = await _query(session, ids)

    assert requests == {RCE: 20, BAG: 4, BRK_PB: 2 + MAX_CONSECUTIVE_FAILURES + 1}
    assert list(uitval.mislukt) == [BRK_PB]
    assert sorted(uitval.mislukt[BRK_PB]) == ids[2 * 16 :]
    assert sorted(result["identificatie"]) == ids
    assert isinstance(uitval.waarschuwing(), MonumentStatusOnbepaaldWarning)


@pytest.mark.asyncio
async def test_query_resumes_when_a_source_recovers():
    """BRK-PB answers for two batches, fails MAX_CONSECUTIVE_FAILURES requests and
    then answers again. The probe request brings BRK-PB back right away, so every
    later batch is answered. Only what was under way while it was down is retried at
    the end, and after that no ID is left open."""
    ids = _ids(20 * 16)

    def fails(bron, nummer):
        return bron == BRK_PB and 2 < nummer <= 2 + MAX_CONSECUTIVE_FAILURES

    with (
        _counting_sources(fails, batch_grootte=16) as (session, requests),
        patch("monumenten._bronfouten.OUTAGE_PROBE_SECONDS", 0),
    ):
        result, uitval = await _query(session, ids)

    assert not uitval.is_gestoord(BRK_PB)
    assert uitval.mislukt == {}
    assert sorted(result["identificatie"]) == ids
    assert 20 < requests[RCE] <= 20 + _BATCH_CONCURRENCY
    assert requests[BAG] == 4


@pytest.mark.asyncio
async def test_query_retry_at_the_end_fills_what_a_short_outage_left_open(klok):
    """BRK-PB goes down after two batches and stays down while all other batches
    pass, so it is skipped for every one of them. At the end the call waits for the
    probe request, BRK-PB answers again, and all 18 batches without an answer are
    processed after all."""
    ids = _ids(20 * 16)
    start = klok.nu

    def fails(bron, nummer):
        # Down until the wait for the probe request at the end moves the clock
        return bron == BRK_PB and nummer > 2 and klok.nu == start

    with _counting_sources(fails, batch_grootte=16, kadaster_wacht=True) as (
        session,
        requests,
    ):
        result, uitval = await _query(session, ids)

    assert klok.nu - start == OUTAGE_PROBE_SECONDS
    assert requests[RCE] == 20 + 18
    assert requests[BAG] == 4
    assert requests[BRK_PB] >= 2 + MAX_CONSECUTIVE_FAILURES + 1 + 18
    assert uitval.mislukt == {}
    assert uitval.waarschuwing() is None
    assert sorted(result["identificatie"]) == ids


@pytest.mark.asyncio
async def test_client_retry_at_the_end_replaces_the_incomplete_answer():
    """BRK-PB fails for two IDs and answers at the retry. The retry replaces what the
    first round knew: the rijksmonument that then only had the RCE as its source now
    has both, in a single row, and nothing is left undetermined."""
    brk_pb_down = [_VO_RIJKS, _VO_KADASTER_RIJKS]
    met_ewe = {_VO_RIJKS: _beperking(_VO_RIJKS, "EWE", "Erfgoedwet: Afschrift")}

    with (
        _mocked_sources(brk_pb_down=brk_pb_down) as client,
        patch.dict(_BEPERKINGEN, met_ewe),
        # BRK-PB comes back while the call waits for the retry
        patch.object(
            Bronuitval,
            "wacht_op_proefrequest",
            AsyncMock(side_effect=brk_pb_down.clear),
        ),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error", MonumentStatusOnbepaaldWarning)
        result = await client.process_from_df(
            pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
        )

    assert list(result[_ID_KOLOM]) == _ALLE
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, False, False, True, False],
            rijksbeschermd_gezicht=[False, False, True, False, False],
            gemeentelijk_monument=[False, False, True, False, False],
        ),
    )
    assert list(result["rijksmonument_bron"].fillna("")) == [
        "RCE, Kadaster",
        "",
        "",
        "Kadaster",
        "",
    ]


@pytest.mark.asyncio
async def test_client_retry_at_the_end_fetches_the_missing_adrespunten():
    """The BAG fails for two IDs and answers at the retry. Their adrespunten are
    fetched after all, so the beperkingen and the rijksbeschermd gezicht that depend
    on them are determined too."""
    bag_down = [_VO_GEMEENTE, _VO_GEEN]

    with (
        _mocked_sources(bag_down=bag_down) as client,
        # The BAG comes back while the call waits for the retry
        patch.object(
            Bronuitval, "wacht_op_proefrequest", AsyncMock(side_effect=bag_down.clear)
        ),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error", MonumentStatusOnbepaaldWarning)
        result = await client.process_from_df(
            pd.DataFrame({_ID_KOLOM: _ALLE}), _ID_KOLOM
        )

    assert list(result[_ID_KOLOM]) == _ALLE
    pd.testing.assert_frame_equal(
        result[_STATUSSEN],
        _statussen(
            result,
            rijksmonument=[True, False, False, True, False],
            rijksbeschermd_gezicht=[False, False, True, False, False],
            gemeentelijk_monument=[False, False, True, False, False],
        ),
    )
