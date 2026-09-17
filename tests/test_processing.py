"""Tests for batch processing and API-level retry/split logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest

from monumenten._api._backoff import MAX_SPLIT_DEPTH, MIN_BATCH_SIZE
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import _query_verblijfsobjecten
from monumenten._api._provincies import (
    DRENTHE,
    NOORD_HOLLAND,
    ProvincialeMonumentenError,
)
from monumenten._processing import (
    _QUERY_BATCH_GROOTTE,
    _koppel_provinciale_monumenten,
    _process_batch,
    _query,
)


def _make_batch_result(identificaties, count=None):
    """Minimal valid (rm, beschermd_gezicht, gemeentelijk, provinciaal) frames + count."""
    n = count if count is not None else len(identificaties)
    ids_df = pd.DataFrame({"identificatie": list(identificaties)[:n]})
    rm = ids_df.assign(
        rijksmonument_nummer="",
        rijksmonument_bron="",
        rijksmonument_voorbescherming=False,
    )
    bg = ids_df.assign(rijksbeschermd_gezicht_naam=pd.NA)
    gm = ids_df.assign(grondslag_gemeentelijk_monument=pd.NA)
    pm = ids_df.assign(provinciaal_monument_omschrijving=pd.NA)
    return (rm, bg, gm, pm, n)


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
        "rijksmonument_voorbescherming",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
        "provinciaal_monument_omschrijving",
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
        "rijksmonument_voorbescherming",
        "rijksbeschermd_gezicht_naam",
        "grondslag_gemeentelijk_monument",
        "provinciaal_monument_omschrijving",
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


@pytest.mark.asyncio
async def test_query_provinciale_fout_wordt_niet_overgeslagen(empty_geodataframe):
    """Een ProvincialeMonumentenError mag niet als lege batch eindigen: dan zou een
    hele provincie stil False krijgen. De fout komt onverpakt (geen ExceptionGroup)
    bij de aanroeper."""

    async def mock_process_batch(session, batch, bg_df):
        raise ProvincialeMonumentenError("Drenthe onbereikbaar")

    with (
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            return_value=empty_geodataframe,
        ),
        patch("monumenten._processing._process_batch", side_effect=mock_process_batch),
    ):
        async with aiohttp.ClientSession() as session:
            with pytest.raises(
                ProvincialeMonumentenError, match="Drenthe onbereikbaar"
            ):
                await _query(session, ["id1", "id2"])


@pytest.mark.asyncio
async def test_koppel_provinciale_monumenten_alleen_benodigde_provincie():
    """Alleen provincies met een adrespunt in hun bounding box worden opgehaald."""
    from shapely.geometry import Point, box

    geo_df = gpd.GeoDataFrame(
        {"identificatie": ["rolde_in", "rolde_uit", "rotterdam"]},
        geometry=[Point(6.646, 52.9885), Point(6.70, 52.95), Point(4.48, 51.92)],
    )
    drenthe_df = gpd.GeoDataFrame(
        {"provinciaal_monument_omschrijving": ["PM1-0001 Dwarshuisboerderij Rolde"]},
        geometry=[box(6.645, 52.988, 6.647, 52.989)],
    )
    opgehaald = []

    async def mock_get(session, provincie):
        opgehaald.append(provincie.naam)
        assert provincie is DRENTHE
        return drenthe_df

    with patch(
        "monumenten._processing._get_provinciale_monumenten", side_effect=mock_get
    ):
        result = await _koppel_provinciale_monumenten(MagicMock(), geo_df)

    assert opgehaald == ["Drenthe"]
    assert NOORD_HOLLAND.naam not in opgehaald
    assert result.to_dict(orient="records") == [
        {
            "identificatie": "rolde_in",
            "provinciaal_monument_omschrijving": "PM1-0001 Dwarshuisboerderij Rolde",
        }
    ]


@pytest.mark.asyncio
async def test_koppel_provinciale_monumenten_buiten_nh_en_dr_geen_download():
    from shapely.geometry import Point

    geo_df = gpd.GeoDataFrame(
        {"identificatie": ["rotterdam"]}, geometry=[Point(4.48, 51.92)]
    )
    with patch(
        "monumenten._processing._get_provinciale_monumenten",
        side_effect=AssertionError("mag niet worden aangeroepen"),
    ):
        result = await _koppel_provinciale_monumenten(MagicMock(), geo_df)

    assert result.empty
    assert list(result.columns) == [
        "identificatie",
        "provinciaal_monument_omschrijving",
    ]


# ---------------------------------------------------------------------------
# Grondslagcodes: EWE = rijksmonument, EWD = voorbescherming (geen rijksmonument)
# ---------------------------------------------------------------------------


def _beperking(identificatie, grondslagcode=None, grondslag=None):
    # POINT (0 0) ligt buiten Nederland, dus er wordt geen provincie bevraagd
    return {
        "identificatie": identificatie,
        "verblijfsobjectWKT": "POINT (0 0)",
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


async def _run_process_batch(
    rijksmonumenten, verblijfsobjecten, beschermde_gezichten_df
):
    ids = sorted({r["identificatie"] for r in verblijfsobjecten})
    with (
        patch(
            "monumenten._processing._query_rijksmonumenten",
            AsyncMock(return_value=rijksmonumenten),
        ),
        patch(
            "monumenten._processing._query_verblijfsobjecten",
            AsyncMock(return_value=verblijfsobjecten),
        ),
    ):
        async with aiohttp.ClientSession() as session:
            return await _process_batch(session, ids, beschermde_gezichten_df)


def _rij(rm_df, identificatie):
    rows = rm_df[rm_df["identificatie"] == identificatie]
    assert len(rows) == 1, (
        f"verwacht precies 1 rij voor {identificatie}, kreeg {len(rows)}"
    )
    return rows.iloc[0]


@pytest.mark.asyncio
async def test_process_batch_ewd_is_geen_rijksmonument(empty_geodataframe):
    """EWD (voorbescherming) zet alleen rijksmonument_voorbescherming, geen bron."""
    rm_df, _, gm_df, _, n = await _run_process_batch(
        rijksmonumenten=[],
        verblijfsobjecten=[_beperking("vo_ewd", "EWD", _EWD_GRONDSLAG)],
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
    assert pd.isna(row["rijksmonument_bron"]) or row["rijksmonument_bron"] == ""
    assert bool(row["rijksmonument_voorbescherming"]) is True
    # EWD is ook geen gemeentelijk monument
    assert gm_df.empty


@pytest.mark.asyncio
async def test_process_batch_ewe_is_rijksmonument_zonder_voorbescherming(
    empty_geodataframe,
):
    rm_df, _, _, _, _ = await _run_process_batch(
        rijksmonumenten=[],
        verblijfsobjecten=[_beperking("vo_ewe", "EWE", _EWE_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_ewe")
    assert row["rijksmonument_bron"] == "Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is False


@pytest.mark.asyncio
async def test_process_batch_ewe_en_ewd_op_zelfde_verblijfsobject(empty_geodataframe):
    """EWE + EWD: één rij, bron Kadaster én voorbescherming True."""
    rm_df, _, _, _, _ = await _run_process_batch(
        rijksmonumenten=[],
        verblijfsobjecten=[
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
    rm_df, _, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": "12345"}],
        verblijfsobjecten=[_beperking("vo_rce", "EWD", _EWD_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert row["rijksmonument_nummer"] == "12345"
    assert row["rijksmonument_bron"] == "RCE"
    assert bool(row["rijksmonument_voorbescherming"]) is True


@pytest.mark.asyncio
async def test_process_batch_rce_en_ewe(empty_geodataframe):
    rm_df, _, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": "12345"}],
        verblijfsobjecten=[_beperking("vo_rce", "EWE", _EWE_GRONDSLAG)],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert row["rijksmonument_bron"] == "RCE, Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is False


@pytest.mark.asyncio
async def test_process_batch_zonder_beperking(empty_geodataframe):
    """Verblijfsobject zonder beperking komt niet in rijksmonumenten_df."""
    rm_df, _, gm_df, pm_df, _ = await _run_process_batch(
        rijksmonumenten=[],
        verblijfsobjecten=[_beperking("vo_leeg")],
        beschermde_gezichten_df=empty_geodataframe,
    )

    assert rm_df.empty
    assert gm_df.empty
    assert pm_df.empty
