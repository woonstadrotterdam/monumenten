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

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
_BATCH_CONCURRENCY = 4


async def _process_batch(
    session: aiohttp.ClientSession,
    batch: List[str],
    beschermde_gezichten_df: gpd.GeoDataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame, int]:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        beschermde_gezichten_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame, int]: Tuple met rijksmonumenten,
            beschermde gezichten, gemeentelijke monumenten en aantal verwerkte objecten
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
        return (
            pd.DataFrame(
                columns=["identificatie", "rijksmonument_nummer", "rijksmonument_bron"]
            ),
            pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"]),
            pd.DataFrame(columns=["identificatie", "grondslag_gemeentelijk_monument"]),
            len(batch),
        )

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

    return (
        rijksmonumenten_df,
        verblijfsobjecten_in_beschermde_gezichten_df,
        gemeentelijke_monumenten_df,
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


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> pd.DataFrame:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules.
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)

    batches = [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]

    progress_bar = tqdm_asyncio(
        total=len(verblijfsobject_ids), disable=len(batches) <= 1
    )

    _BatchResult = Tuple[DataFrame, DataFrame, DataFrame, int]
    results_list: List[_BatchResult] = []

    _empty_batch_result = (
        pd.DataFrame(
            columns=["identificatie", "rijksmonument_nummer", "rijksmonument_bron"]
        ),
        pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"]),
        pd.DataFrame(columns=["identificatie", "grondslag_gemeentelijk_monument"]),
    )

    batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)

    async def _run_batch(batch: List[str]) -> None:
        async with batch_semaphore:
            try:
                result = await _process_batch(session, batch, beschermde_gezichten_df)
                results_list.append(result)
                progress_bar.update(result[3])
            except Exception:
                logger.error(
                    "Batch (size %d) mislukt na retry/split in API-laag, overgeslagen",
                    len(batch),
                )
                results_list.append((*_empty_batch_result, len(batch)))
                progress_bar.update(len(batch))

    async with asyncio.TaskGroup() as tg:
        for batch in batches:
            tg.create_task(_run_batch(batch))

    rijksmonumenten_parts = [r[0] for r in results_list]
    beschermd_gezicht_parts = [r[1] for r in results_list]
    gemeentelijke_parts = [r[2] for r in results_list]

    rijksmonumenten_result = (
        pd.concat(rijksmonumenten_parts, ignore_index=True)
        if rijksmonumenten_parts
        else pd.DataFrame(
            columns=["identificatie", "rijksmonument_nummer", "rijksmonument_bron"]
        )
    )
    verblijfsobjecten_in_beschermd_gezicht_result = (
        pd.concat(beschermd_gezicht_parts, ignore_index=True)
        if beschermd_gezicht_parts
        else pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"])
    )
    gemeentelijke_monumenten_result = (
        pd.concat(gemeentelijke_parts, ignore_index=True)
        if gemeentelijke_parts
        else pd.DataFrame(columns=["identificatie", "grondslag_gemeentelijk_monument"])
    )

    progress_bar.close()

    if (
        rijksmonumenten_result.empty
        and verblijfsobjecten_in_beschermd_gezicht_result.empty
        and gemeentelijke_monumenten_result.empty
    ):
        return pd.DataFrame(
            columns=[
                "identificatie",
                "rijksmonument_nummer",
                "rijksmonument_bron",
                "rijksbeschermd_gezicht_naam",
                "grondslag_gemeentelijk_monument",
            ]
        )

    # The filtering in _process_batch already separates the data correctly:
    # - EWE/EWD rows go to rijksmonumenten_df
    # - GG/GWA rows go to gemeentelijke_monumenten_df
    # We only remove truly duplicate rows (all columns identical) to preserve unique information

    if not rijksmonumenten_result.empty:
        rijksmonumenten_result = rijksmonumenten_result.drop_duplicates(keep="first")

    if not verblijfsobjecten_in_beschermd_gezicht_result.empty:
        # Deduplicate (id, name) pairs so we only aggregate unique names per id.
        df_bg = verblijfsobjecten_in_beschermd_gezicht_result.drop_duplicates(
            subset=["identificatie", "rijksbeschermd_gezicht_naam"]
        )
        # Use numpy for aggregation
        ids = df_bg["identificatie"].to_numpy(dtype=object)
        names = df_bg["rijksbeschermd_gezicht_naam"].to_numpy(dtype=object)
        # Sort by id so all rows with the same identificatie are contiguous.
        order = np.argsort(ids)
        ids, names = ids[order], names[order]
        # Get start index of each group; end is start of next group or length.
        unique_ids, group_start = np.unique(ids, return_index=True)
        group_end = np.concatenate([group_start[1:], [len(ids)]])
        # For each id, join its rijksbeschermd_gezicht names (drop NaNs) with ", ".
        parts = []
        for i in range(len(unique_ids)):
            valid = names[group_start[i] : group_end[i]]
            valid = valid[~pd.isna(valid)]
            parts.append(", ".join(valid.astype(str)) or None)
        verblijfsobjecten_in_beschermd_gezicht_result = pd.DataFrame(
            {"identificatie": unique_ids, "rijksbeschermd_gezicht_naam": parts}
        )

    if not gemeentelijke_monumenten_result.empty:
        gemeentelijke_monumenten_result = (
            gemeentelijke_monumenten_result.drop_duplicates(keep="first")
        )

    result = rijksmonumenten_result.merge(
        verblijfsobjecten_in_beschermd_gezicht_result, on="identificatie", how="outer"
    ).merge(gemeentelijke_monumenten_result, on="identificatie", how="outer")

    return result
