"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from typing import List, Tuple

import aiohttp
import geopandas as gpd
import numpy as np
import pandas as pd
from aiocache import cached_stampede
from pandas import DataFrame
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
)

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
_BATCH_CONCURRENCY = 4

_BatchFrames = Tuple[DataFrame, DataFrame, DataFrame, DataFrame]
_BatchResult = Tuple[DataFrame, DataFrame, DataFrame, DataFrame, int]

_RESULTAAT_KOLOMMEN = [
    "identificatie",
    "rijksmonument_nummer",
    "rijksmonument_bron",
    "rijksbeschermd_gezicht_naam",
    "grondslag_gemeentelijk_monument",
    "provinciaal_monument_omschrijving",
]


def _lege_batch_frames() -> _BatchFrames:
    """Lege resultaatframes voor een batch, in de volgorde van _process_batch."""
    return (
        pd.DataFrame(
            columns=["identificatie", "rijksmonument_nummer", "rijksmonument_bron"]
        ),
        pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"]),
        pd.DataFrame(columns=["identificatie", "grondslag_gemeentelijk_monument"]),
        pd.DataFrame(columns=["identificatie", "provinciaal_monument_omschrijving"]),
    )


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
            gemeentelijke monumenten, provinciale monumenten en aantal verwerkte objecten
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
            verblijfsobjecten_df["grondslagcode"].isin(["EWE", "EWD"])
        ][["identificatie", "grondslagcode"]],
        on="identificatie",
        how="outer",
    )

    # voeg bron voor rijksmonumenten toe in de kolom rijksmonument_bron
    condition_choice_map = {
        "RCE, Kadaster": (
            rijksmonumenten_df["rijksmonument_nummer"].notna()
            & rijksmonumenten_df["grondslagcode"].isin(["EWE", "EWD"])
        ),
        "RCE": (
            rijksmonumenten_df["rijksmonument_nummer"].notna()
            & ~rijksmonumenten_df["grondslagcode"].isin(["EWE", "EWD"])
        ),
        "Kadaster": (
            rijksmonumenten_df["rijksmonument_nummer"].isna()
            & rijksmonumenten_df["grondslagcode"].isin(["EWE", "EWD"])
        ),
    }

    rijksmonumenten_df["rijksmonument_bron"] = np.select(
        list(condition_choice_map.values()),
        list(condition_choice_map.keys()),
        default="",
    )

    rijksmonumenten_df.drop(columns=["grondslagcode"], inplace=True)

    # Process gemeentelijke monumenten
    gemeentelijke_monumenten_df = verblijfsobjecten_df[
        verblijfsobjecten_df["grondslagcode"].isin(["GG", "GWA"])
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

    provinciale_monumenten_df = await _koppel_provinciale_monumenten(session, geo_df)

    return (
        rijksmonumenten_df,
        verblijfsobjecten_in_beschermde_gezichten_df,
        gemeentelijke_monumenten_df,
        provinciale_monumenten_df,
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


@cached_stampede(ttl=60 * 60 * 24 * 7, noself=True)  # Cache resultaat voor 7 dagen
async def _get_provinciale_monumenten(
    session: aiohttp.ClientSession, provincie: Provincie
) -> gpd.GeoDataFrame:
    """Haal de monumentvlakken van een provincie op als GeoDataFrame (WGS84)."""
    features = await _query_provinciale_monumenten(session, provincie)
    return gpd.GeoDataFrame.from_features(features)[
        ["provinciaal_monument_omschrijving", "geometry"]
    ]


async def _koppel_provinciale_monumenten(
    session: aiohttp.ClientSession, geo_df: gpd.GeoDataFrame
) -> DataFrame:
    """Bepaal welke adrespunten binnen een provinciaal monumentvlak liggen.

    Alleen de provincies waarvan de ruime bounding box een adrespunt uit de batch
    bevat worden bevraagd; voor de rest van Nederland is er geen download.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        geo_df (gpd.GeoDataFrame): Adrespunten (WGS84) met kolom ``identificatie``

    Returns:
        DataFrame: Kolommen ``identificatie`` en ``provinciaal_monument_omschrijving``,
            één rij per gevonden combinatie
    """
    delen: List[DataFrame] = []
    lon, lat = geo_df.geometry.x, geo_df.geometry.y
    for provincie in PROVINCIES:
        min_lon, min_lat, max_lon, max_lat = provincie.bbox
        in_bbox = (
            (lon >= min_lon) & (lon <= max_lon) & (lat >= min_lat) & (lat <= max_lat)
        )
        if not in_bbox.any():
            continue
        monumenten_df = await _get_provinciale_monumenten(session, provincie)
        delen.append(
            gpd.sjoin(
                geo_df[in_bbox],
                monumenten_df,
                how="inner",
                predicate="within",
            )[["identificatie", "provinciaal_monument_omschrijving"]]
        )
    if not delen:
        return _lege_batch_frames()[3]
    return pd.concat(delen, ignore_index=True)


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


def _eerste_exceptie(groep: BaseExceptionGroup[BaseException]) -> BaseException:
    """Geef de eerste niet-gegroepeerde exceptie uit een (geneste) ExceptionGroup."""
    for exceptie in groep.exceptions:
        if isinstance(exceptie, BaseExceptionGroup):
            return _eerste_exceptie(exceptie)
        return exceptie
    return groep


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> pd.DataFrame:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        pd.DataFrame: Eén rij per verblijfsobject met een monumentstatus, met de
            kolommen uit ``_RESULTAAT_KOLOMMEN``

    Raises:
        ProvincialeMonumentenError: Als de provinciale monumenten van een benodigde
            provincie niet konden worden opgehaald.
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
            except ProvincialeMonumentenError:
                # Niet overslaan: dan zou de hele provincie stil False worden.
                raise
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
    except BaseExceptionGroup as groep:
        provinciaal, _ = groep.split(ProvincialeMonumentenError)
        if provinciaal is None:
            raise
        # De TaskGroup verpakt de fout; geef de oorspronkelijke fout door.
        raise _eerste_exceptie(provinciaal) from None
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
        provinciale_monumenten_result,
    ) = samengevoegd

    if all(frame.empty for frame in samengevoegd):
        return pd.DataFrame(columns=_RESULTAAT_KOLOMMEN)

    # The filtering in _process_batch already separates the data correctly:
    # - EWE/EWD rows go to rijksmonumenten_df
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

    return result
