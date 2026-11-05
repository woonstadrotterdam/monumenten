"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Tuple

import aiohttp
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from aiocache import cached_stampede
from pandas import DataFrame
from shapely.geometry.base import BaseGeometry
from tqdm.asyncio import tqdm_asyncio

from monumenten._api._cultureel_erfgoed import (
    _query_beschermde_gezichten,
    _query_rijksmonumenten,
)
from monumenten._api._kadaster import _query_verblijfsobjecten
from monumenten._api._provincies import (
    PROVINCIES,
    Provincie,
    ProvincialeMonumentenError,
    _query_provinciale_monumenten,
    _query_provinciegrenzen,
)

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
_BATCH_CONCURRENCY = 4

# Kadaster grondslagcodes (publiekrechtelijke beperkingen, BRK)
# EWE: inschrijving in het rijksmonumentenregister -> rijksmonument (art. 1.1 Erfgoedwet)
_RIJKSMONUMENT_GRONDSLAGCODES = ["EWE"]
# EWD: toezending ontwerpbesluit aanwijzing (voorbescherming) -> nog geen rijksmonument
_VOORBESCHERMING_GRONDSLAGCODES = ["EWD"]
# GG/GWA: gemeentelijk monument (GWA bundelt voorbescherming, aanwijzing en afschrift)
_GEMEENTELIJK_GRONDSLAGCODES = ["GG", "GWA"]

# Ruim bereik van Nederland in WGS84, om verwisselde assen te herkennen
_NL_LON = (3.0, 7.5)
_NL_LAT = (50.5, 53.8)

_WGS84 = "EPSG:4326"
_RD = "EPSG:28992"

# Marge om de provinciegrens. Enkele Drentse monumentvlakken steken tot 1,3 m
# buiten de grens van PDOK, door verschillen in nauwkeurigheid; de grens wordt
# bovendien maar jaarlijks bijgewerkt. Een te ruime marge kost alleen wat extra
# kandidaten, een te krappe geeft stil False.
_GRENS_BUFFER_METER = 100

_BatchFrames = Tuple[DataFrame, DataFrame, DataFrame, gpd.GeoDataFrame]
_BatchResult = Tuple[DataFrame, DataFrame, DataFrame, gpd.GeoDataFrame, int]

_RESULTAAT_KOLOMMEN = [
    "identificatie",
    "rijksmonument_nummer",
    "rijksmonument_bron",
    "rijksmonument_voorbescherming",
    "rijksbeschermd_gezicht_naam",
    "grondslag_gemeentelijk_monument",
    "provinciaal_monument_omschrijving",
]


def _lege_batch_frames() -> _BatchFrames:
    """Lege resultaatframes voor een batch, in de volgorde van _process_batch."""
    return (
        pd.DataFrame(
            columns=[
                "identificatie",
                "rijksmonument_nummer",
                "rijksmonument_bron",
                "rijksmonument_voorbescherming",
            ]
        ),
        pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"]),
        pd.DataFrame(columns=["identificatie", "grondslag_gemeentelijk_monument"]),
        _lege_adrespunten(),
    )


def _lege_adrespunten() -> gpd.GeoDataFrame:
    """Leeg frame met adrespunten (WGS84), zoals _process_batch dat teruggeeft."""
    return gpd.GeoDataFrame(
        {"identificatie": pd.Series(dtype="string")},
        geometry=gpd.GeoSeries([], crs=_WGS84),
    )


def _lege_provinciale_monumenten() -> DataFrame:
    """Leeg resultaat van _bepaal_provinciale_monumenten."""
    return pd.DataFrame(columns=["identificatie", "provinciaal_monument_omschrijving"])


async def _process_batch(
    session: aiohttp.ClientSession,
    batch: List[str],
    beschermde_gezichten_df: gpd.GeoDataFrame,
) -> _BatchResult:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        beschermde_gezichten_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten

    Returns:
        _BatchResult: Tuple met rijksmonumenten, beschermde gezichten,
            gemeentelijke monumenten, adrespunten (WGS84) en aantal verwerkte objecten.
            De provinciale monumenten worden na alle batches bepaald.
    """
    # Get the current event loop
    loop = asyncio.get_running_loop()

    # Create tasks using the current loop
    rijksmonumenten_taak = loop.create_task(_query_rijksmonumenten(session, batch))
    verblijfsobjecten_taak = loop.create_task(_query_verblijfsobjecten(session, batch))

    # Wait for both tasks to complete
    rijksmonumenten, verblijfsobjecten = await asyncio.gather(
        rijksmonumenten_taak, verblijfsobjecten_taak
    )

    if not verblijfsobjecten:
        logger.warning(
            "Geen geldige BAG verblijfsobjecten gevonden voor batch van %d ID's",
            len(batch),
        )
        return (*_lege_batch_frames(), len(batch))

    verblijfsobjecten_df = pd.DataFrame(verblijfsobjecten).astype(
        {"identificatie": "string"}
    )

    rijksmonumenten_df = pd.merge(
        pd.DataFrame(
            rijksmonumenten,
            columns=["identificatie", "rijksmonument_nummer"],
            dtype="string",
        ),
        verblijfsobjecten_df[
            verblijfsobjecten_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
        ][["identificatie", "grondslagcode"]],
        on="identificatie",
        how="outer",
    )

    # voeg bron voor rijksmonumenten toe in de kolom rijksmonument_bron
    condition_choice_map = {
        "RCE, Kadaster": (
            rijksmonumenten_df["rijksmonument_nummer"].notna()
            & rijksmonumenten_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
        ),
        "RCE": (
            rijksmonumenten_df["rijksmonument_nummer"].notna()
            & ~rijksmonumenten_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
        ),
        "Kadaster": (
            rijksmonumenten_df["rijksmonument_nummer"].isna()
            & rijksmonumenten_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
        ),
    }

    rijksmonumenten_df["rijksmonument_bron"] = np.select(
        list(condition_choice_map.values()),
        list(condition_choice_map.keys()),
        # None en geen "": de client leidt rijksmonument af uit een gevulde bron.
        # Als object-array, omdat de numpy-stubs geen kale None accepteren.
        default=np.array(None, dtype=object),
    )

    rijksmonumenten_df = rijksmonumenten_df.drop(columns=["grondslagcode"])

    # Voorbescherming (EWD) is geen rijksmonument in de zin van art. 1.1 Erfgoedwet
    # en telt dus niet mee voor rijksmonument/rijksmonument_bron. Wel apart teruggeven.
    voorbescherming_df = (
        verblijfsobjecten_df[
            verblijfsobjecten_df["grondslagcode"].isin(_VOORBESCHERMING_GRONDSLAGCODES)
        ][["identificatie"]]
        .drop_duplicates()
        .assign(rijksmonument_voorbescherming=True)
    )
    rijksmonumenten_df = pd.merge(
        rijksmonumenten_df, voorbescherming_df, on="identificatie", how="outer"
    )
    rijksmonumenten_df["rijksmonument_voorbescherming"] = rijksmonumenten_df[
        "rijksmonument_voorbescherming"
    ].eq(True)

    # Process gemeentelijke monumenten
    gemeentelijke_monumenten_df = verblijfsobjecten_df[
        verblijfsobjecten_df["grondslagcode"].isin(_GEMEENTELIJK_GRONDSLAGCODES)
    ][["identificatie", "grondslag_gemeentelijk_monument"]]

    # Process beschermde gezichten
    geo_df = gpd.GeoDataFrame(
        verblijfsobjecten_df[["identificatie", "verblijfsobjectWKT"]].assign(
            geometry=lambda x: gpd.GeoSeries.from_wkt(x["verblijfsobjectWKT"])
        )[["identificatie", "geometry"]],
        geometry="geometry",
    )

    # Find objects within beschermde gezichten
    verblijfsobjecten_in_beschermde_gezichten_df = gpd.sjoin(
        geo_df,
        beschermde_gezichten_df,
        how="left",
        predicate="within",
    )[["identificatie", "rijksbeschermd_gezicht_naam"]]

    # De KKG levert POINT(lon lat) in WGS84
    adrespunten = geo_df.set_crs(_WGS84)

    return (
        rijksmonumenten_df,
        verblijfsobjecten_in_beschermde_gezichten_df,
        gemeentelijke_monumenten_df,
        adrespunten,
        len(batch),
    )


@cached_stampede(ttl=60 * 60 * 24 * 7, noself=True)  # Cache resultaat voor 7 dagen
async def _get_beschermde_gezichten(
    session: aiohttp.ClientSession,
) -> gpd.GeoDataFrame:
    """Haal beschermde gezichten op."""
    beschermde_gezichten = await _query_beschermde_gezichten(session)
    beschermde_gezichten_df = gpd.GeoDataFrame()

    if not beschermde_gezichten:
        raise ValueError("Geen beschermde gezichten gevonden")

    beschermde_gezichten_df = pd.DataFrame(beschermde_gezichten)
    beschermde_gezichten_df["geometry"] = gpd.GeoSeries.from_wkt(
        beschermde_gezichten_df["gezichtWKT"]
    )
    beschermde_gezichten_df = gpd.GeoDataFrame(
        beschermde_gezichten_df[["rijksbeschermd_gezicht_naam", "geometry"]],
        geometry="geometry",
    )

    return beschermde_gezichten_df


# Cache 7 dagen. De lease (standaard 2 s) dekt het pagineren van Noord-Holland,
# zodat gelijktijdige aanroepen niet allemaal zelf downloaden.
@cached_stampede(lease=60, ttl=60 * 60 * 24 * 7, noself=True)
async def _get_provinciale_monumenten(
    session: aiohttp.ClientSession, provincie: Provincie
) -> gpd.GeoDataFrame:
    """Haal de monumentvlakken van een provincie op als GeoDataFrame (RD)."""
    features = await _query_provinciale_monumenten(session, provincie)
    return gpd.GeoDataFrame.from_features(features, crs=_RD)[
        ["provinciaal_monument_omschrijving", "geometry"]
    ]


@cached_stampede(ttl=60 * 60 * 24 * 7, noself=True)  # Cache resultaat voor 7 dagen
async def _get_provinciegrenzen(
    session: aiohttp.ClientSession,
) -> Dict[str, BaseGeometry]:
    """Haal de grenzen van alle provincies in PROVINCIES op, met de marge erom (RD).

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests

    Returns:
        Dict[str, BaseGeometry]: Per provinciecode het gebied waarbinnen een
            adrespunt kandidaat is voor die provincie
    """
    grenzen = await _query_provinciegrenzen(session, PROVINCIES)
    gebieden = {
        code: grens.buffer(_GRENS_BUFFER_METER) for code, grens in grenzen.items()
    }
    for gebied in gebieden.values():
        # Eén keer voorbereiden; anders kost elk adrespunt een volledige doorloop
        # van de grens.
        shapely.prepare(gebied)
    return gebieden


async def _bepaal_provinciale_monumenten(
    session: aiohttp.ClientSession, adrespunten: gpd.GeoDataFrame
) -> Tuple[DataFrame, Dict[str, List[str]]]:
    """Bepaal welke adrespunten binnen een provinciaal monumentvlak liggen.

    Een adrespunt is kandidaat voor een provincie als het in haar ruime bounding
    box ligt en binnen haar grens (met een marge van ``_GRENS_BUFFER_METER``).
    Alleen provincies met kandidaten worden gedownload; ligt geen adrespunt in een
    bounding box, dan volgt er geen enkele request. Zijn de grenzen niet op te
    halen, dan is elk adrespunt in de bounding box kandidaat.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        adrespunten (gpd.GeoDataFrame): Adrespunten met kolom ``identificatie`` en
            een coördinatenstelsel

    Returns:
        Tuple[DataFrame, Dict[str, List[str]]]: De gevonden monumenten (kolommen
            ``identificatie`` en ``provinciaal_monument_omschrijving``, één rij per
            combinatie), en per provincie waarvan de monumentvlakken niet op te halen
            waren de ID's van haar kandidaten
    """
    onbepaald: Dict[str, List[str]] = {}
    # Een verblijfsobject met meerdere beperkingen komt meerdere keren voor
    adrespunten = adrespunten.drop_duplicates(subset="identificatie")
    if adrespunten.empty:
        return _lege_provinciale_monumenten(), onbepaald

    wgs84 = adrespunten.geometry.to_crs(_WGS84)
    lon, lat = wgs84.x, wgs84.y
    in_nederland = lon.between(*_NL_LON) & lat.between(*_NL_LAT)
    if in_nederland.mean() < 0.5:
        logger.error(
            "%d van %d adrespunten liggen buiten het bereik van Nederland; "
            "controleer de volgorde van lengte- en breedtegraad. Provinciale "
            "monumenten worden voor deze punten niet gevonden.",
            (~in_nederland).sum(),
            len(adrespunten),
        )

    in_bbox: Dict[Provincie, pd.Series[bool]] = {}
    for provincie in PROVINCIES:
        min_lon, min_lat, max_lon, max_lat = provincie.bbox
        masker = lon.between(min_lon, max_lon) & lat.between(min_lat, max_lat)
        if masker.any():
            in_bbox[provincie] = masker
    if not in_bbox:
        return _lege_provinciale_monumenten(), onbepaald

    rd = adrespunten.to_crs(_RD)
    grenzen: Dict[str, BaseGeometry] | None
    try:
        grenzen = await _get_provinciegrenzen(session)
    except ProvincialeMonumentenError as e:
        logger.warning(
            "Provinciegrenzen niet op te halen (%s); elk adrespunt in de bounding "
            "box van een provincie telt als kandidaat",
            e,
        )
        grenzen = None

    delen: List[DataFrame] = []
    for provincie, masker in in_bbox.items():
        kandidaten = rd[masker]
        if grenzen is not None:
            kandidaten = kandidaten[
                shapely.contains_xy(
                    grenzen[provincie.code],
                    kandidaten.geometry.x,
                    kandidaten.geometry.y,
                )
            ]
        if kandidaten.empty:
            continue
        try:
            monumenten_df = await _get_provinciale_monumenten(session, provincie)
        except ProvincialeMonumentenError as e:
            logger.warning(
                "%s; provinciaal_monument is onbepaald voor %d verblijfsobjecten",
                e,
                len(kandidaten),
            )
            onbepaald[provincie.naam] = kandidaten["identificatie"].tolist()
            continue
        delen.append(
            gpd.sjoin(
                kandidaten,
                monumenten_df,
                how="inner",
                predicate="within",
            )[["identificatie", "provinciaal_monument_omschrijving"]]
        )
    if not delen:
        return _lege_provinciale_monumenten(), onbepaald
    return pd.concat(delen, ignore_index=True), onbepaald


def _voeg_namen_samen(df: DataFrame, kolom: str) -> DataFrame:
    """Voeg per identificatie de unieke waarden van ``kolom`` samen met ", ".

    Args:
        df (DataFrame): Rijen met kolommen ``identificatie`` en ``kolom``;
            lege waarden worden genegeerd
        kolom (str): Naam van de kolom die wordt samengevoegd

    Returns:
        DataFrame: Eén rij per identificatie; ``None`` als er geen waarden zijn
    """
    # Deduplicate (id, name) pairs so we only aggregate unique names per id.
    df = df.drop_duplicates(subset=["identificatie", kolom])
    # Use numpy for aggregation
    ids = df["identificatie"].to_numpy(dtype=object)
    names = df[kolom].to_numpy(dtype=object)
    # Sort by id so all rows with the same identificatie are contiguous.
    order = np.argsort(ids)
    ids, names = ids[order], names[order]
    # Get start index of each group; end is start of next group or length.
    unique_ids, group_start = np.unique(ids, return_index=True)
    group_end = np.concatenate([group_start[1:], [len(ids)]])
    # For each id, join its names (drop NaNs) with ", ".
    parts = []
    for i in range(len(unique_ids)):
        valid = names[group_start[i] : group_end[i]]
        valid = valid[~pd.isna(valid)]
        parts.append(", ".join(valid.astype(str)) or None)
    return pd.DataFrame({"identificatie": unique_ids, kolom: parts})


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules. De provinciale
    monumenten worden daarna in één stap bepaald.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        Tuple[pd.DataFrame, Dict[str, List[str]]]: Eén rij per verblijfsobject met
            een monumentstatus, met de kolommen uit ``_RESULTAAT_KOLOMMEN``; en per
            provincie die niet op te halen was de ID's waarvan
            ``provinciaal_monument`` onbepaald is
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)

    batches = [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]

    progress_bar = tqdm_asyncio(
        total=len(verblijfsobject_ids), disable=len(batches) <= 1
    )

    results_list: List[_BatchResult] = []

    batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)

    async def _run_batch(batch: List[str]) -> None:
        async with batch_semaphore:
            try:
                result = await _process_batch(session, batch, beschermde_gezichten_df)
                results_list.append(result)
                progress_bar.update(result[4])
            except Exception:
                logger.error(
                    "Batch (size %d) mislukt na retry/split in API-laag, overgeslagen",
                    len(batch),
                )
                results_list.append((*_lege_batch_frames(), len(batch)))
                progress_bar.update(len(batch))

    try:
        async with asyncio.TaskGroup() as tg:
            for batch in batches:
                tg.create_task(_run_batch(batch))
    finally:
        progress_bar.close()

    # Per monumenttype de frames van alle batches samenvoegen
    samengevoegd: List[DataFrame] = (
        [
            pd.concat(frames, ignore_index=True)
            for frames in zip(*(r[:4] for r in results_list))
        ]
        if results_list
        else list(_lege_batch_frames())
    )
    (
        rijksmonumenten_result,
        verblijfsobjecten_in_beschermd_gezicht_result,
        gemeentelijke_monumenten_result,
        adrespunten,
    ) = samengevoegd

    provinciale_monumenten_result, onbepaald = await _bepaal_provinciale_monumenten(
        session, adrespunten
    )

    if all(
        frame.empty
        for frame in (
            rijksmonumenten_result,
            verblijfsobjecten_in_beschermd_gezicht_result,
            gemeentelijke_monumenten_result,
            provinciale_monumenten_result,
        )
    ):
        return pd.DataFrame(columns=_RESULTAAT_KOLOMMEN), onbepaald

    # The filtering in _process_batch already separates the data correctly:
    # - EWE rows go to rijksmonumenten_df (EWD only sets rijksmonument_voorbescherming)
    # - GG/GWA rows go to gemeentelijke_monumenten_df
    # We only remove truly duplicate rows (all columns identical) to preserve unique information

    if not rijksmonumenten_result.empty:
        rijksmonumenten_result = rijksmonumenten_result.drop_duplicates(keep="first")

    if not verblijfsobjecten_in_beschermd_gezicht_result.empty:
        verblijfsobjecten_in_beschermd_gezicht_result = _voeg_namen_samen(
            verblijfsobjecten_in_beschermd_gezicht_result,
            "rijksbeschermd_gezicht_naam",
        )

    if not gemeentelijke_monumenten_result.empty:
        gemeentelijke_monumenten_result = (
            gemeentelijke_monumenten_result.drop_duplicates(keep="first")
        )

    if not provinciale_monumenten_result.empty:
        provinciale_monumenten_result = _voeg_namen_samen(
            provinciale_monumenten_result, "provinciaal_monument_omschrijving"
        )

    result = (
        rijksmonumenten_result.merge(
            verblijfsobjecten_in_beschermd_gezicht_result,
            on="identificatie",
            how="outer",
        )
        .merge(gemeentelijke_monumenten_result, on="identificatie", how="outer")
        .merge(provinciale_monumenten_result, on="identificatie", how="outer")
    )

    return result, onbepaald
