"""Tests for batch processing and API-level retry/split logic."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, box

from monumenten._api._backoff import MAX_SPLIT_DEPTH, MIN_BATCH_SIZE
from monumenten._api._cultureel_erfgoed import _query_rijksmonumenten
from monumenten._api._kadaster import _query_verblijfsobjecten
from monumenten._api._provincies import (
    DRENTHE,
    ProvincialeMonumentenError,
)
from monumenten._processing import (
    _GRENS_BUFFER_METER,
    _QUERY_BATCH_GROOTTE,
    _bepaal_provinciale_monumenten,
    _get_provinciegrenzen,
    _lege_adrespunten,
    _process_batch,
    _query,
)


def _make_batch_result(identificaties, count=None):
    """Minimal valid (rm, beschermd_gezicht, gemeentelijk, adrespunten) frames + count."""
    n = count if count is not None else len(identificaties)
    ids_df = pd.DataFrame({"identificatie": list(identificaties)[:n]})
    rm = ids_df.assign(
        rijksmonument_nummer="",
        rijksmonument_bron=None,
        rijksmonument_voorbescherming=False,
    )
    bg = ids_df.assign(rijksbeschermd_gezicht_naam=pd.NA)
    gm = ids_df.assign(grondslag_gemeentelijk_monument=pd.NA)
    return (rm, bg, gm, _lege_adrespunten(), n)


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


def _adrespunten(punten, crs="EPSG:28992"):
    return gpd.GeoDataFrame(
        {"identificatie": list(punten)}, geometry=list(punten.values()), crs=crs
    )


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
    assert onbepaald == {"Noord-Holland": ["haarlem"]}
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
    assert sorted(onbepaald["Noord-Holland"]) == ["haarlem", "leiden"]
    assert result.empty
    assert "PDOK plat" in caplog.text


@pytest.mark.asyncio
async def test_bepaal_provinciaal_wgs84_en_rd_geven_hetzelfde():
    """De KKG levert WGS84, PDOK RD: de uitkomst is gelijk."""
    rd = _adrespunten({"rolde": _ROLDE, "haarlem": _HAARLEM, "leiden": _LEIDEN})
    wgs84 = rd.to_crs("EPSG:4326")
    monumenten = {"Drenthe": _drenthe_monumenten(), "Noord-Holland": _nh_monumenten()}

    (uit_rd, _), _, _ = await _bepaal(rd, _GRENZEN, monumenten)
    (uit_wgs84, _), _, _ = await _bepaal(wgs84, _GRENZEN, monumenten)

    assert sorted(uit_rd["identificatie"]) == ["haarlem", "rolde"]
    assert sorted(uit_wgs84["identificatie"]) == sorted(uit_rd["identificatie"])


@pytest.mark.asyncio
async def test_bepaal_provinciaal_dubbele_id_een_keer_onbepaald():
    """Een verblijfsobject met meerdere beperkingen staat één keer in de melding."""
    adrespunten = _adrespunten({"haarlem": _HAARLEM})
    adrespunten = pd.concat([adrespunten, adrespunten], ignore_index=True)
    (_, onbepaald), _, _ = await _bepaal(
        adrespunten,
        _GRENZEN,
        {"Noord-Holland": ProvincialeMonumentenError("Noord-Holland plat")},
    )

    assert onbepaald == {"Noord-Holland": ["haarlem"]}


@pytest.mark.asyncio
async def test_bepaal_provinciaal_verwisselde_assen_geeft_fout(caplog):
    """POINT(lat lon) valt buiten Nederland en zou stil nooit in een bbox liggen."""
    (_, _), opgehaald, _ = await _bepaal(
        _adrespunten({"rolde": Point(52.9885, 6.646)}, crs="EPSG:4326"),
        AssertionError("geen grenzen"),
    )

    assert opgehaald == []
    assert "buiten het bereik van Nederland" in caplog.text


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
    verblijfsobjecten = [
        {
            "identificatie": "rolde_alles",
            "verblijfsobjectWKT": "POINT (6.646 52.9885)",
            "grondslagcode": "GG",
            "grondslag_gemeentelijk_monument": "Gemeentewet",
        },
        {
            "identificatie": "rolde_provinciaal",
            "verblijfsobjectWKT": "POINT (6.6461 52.9886)",
            "grondslagcode": None,
            "grondslag_gemeentelijk_monument": None,
        },
    ]
    drenthe_df = gpd.GeoDataFrame(
        {"provinciaal_monument_omschrijving": ["PM1-0001 Dwarshuisboerderij Rolde"]},
        geometry=[box(6.645, 52.988, 6.647, 52.989)],
        crs="EPSG:4326",
    ).to_crs("EPSG:28992")
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
            "monumenten._processing._query_verblijfsobjecten",
            AsyncMock(return_value=verblijfsobjecten),
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
    # Rotterdam ligt buiten de bbox van Noord-Holland en Drenthe, dus er wordt
    # geen provincie bevraagd
    return {
        "identificatie": identificatie,
        "verblijfsobjectWKT": "POINT (4.48 51.92)",
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
    assert pd.isna(row["rijksmonument_bron"])
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
    rm_df, _, gm_df, adrespunten, _ = await _run_process_batch(
        rijksmonumenten=[],
        verblijfsobjecten=[_beperking("vo_leeg")],
        beschermde_gezichten_df=empty_geodataframe,
    )

    assert rm_df.empty
    assert gm_df.empty
    # Het adrespunt gaat door naar de provinciale stap, in WGS84
    assert adrespunten["identificatie"].tolist() == ["vo_leeg"]
    assert adrespunten.crs == "EPSG:4326"


@pytest.mark.asyncio
async def test_process_batch_geen_bron_is_none(empty_geodataframe):
    """Een rij zonder RCE-nummer en zonder EWE krijgt None als bron, geen "".

    De client leidt rijksmonument af uit een gevulde bron (notna), dus een lege
    string zou zo'n rij ten onrechte als rijksmonument laten tellen.
    """
    rm_df, _, _, _, _ = await _run_process_batch(
        rijksmonumenten=[{"identificatie": "vo_rce", "rijksmonument_nummer": None}],
        verblijfsobjecten=[_beperking("vo_rce")],
        beschermde_gezichten_df=empty_geodataframe,
    )

    row = _rij(rm_df, "vo_rce")
    assert pd.isna(row["rijksmonument_bron"])
