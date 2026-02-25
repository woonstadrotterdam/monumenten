"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import List, Tuple, cast

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
_MAX_BATCH_ATTEMPTS = 2  # one retry per batch before deferring to end
_MIN_BATCH_SIZE = 1  # do not split below this size
_MAX_SPLIT_DEPTH = 10  # 500 -> ~1 ID


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
            pd.DataFrame(columns=["identificatie", "beschermd_gezicht_naam"]),
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
    )[["identificatie", "beschermd_gezicht_naam"]]

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
        beschermde_gezichten_df[["beschermd_gezicht_naam", "geometry"]],
        geometry="geometry",
    )

    return beschermde_gezichten_df


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> pd.DataFrame:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden één keer opnieuw geprobeerd; na twee mislukkingen gaan ze naar een
    uitgestelde wachtrij. Aan het eind worden uitgestelde batches opnieuw geprobeerd;
    bij opnieuw falen worden ze in tweeën gedeeld en opnieuw in de wachtrij gezet,
    tot ze slagen of te klein zijn om verder te splitsen.
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)

    rijksmonumenten_result = pd.DataFrame()
    verblijfsobjecten_in_beschermd_gezicht_result = pd.DataFrame()
    gemeentelijke_monumenten_result = pd.DataFrame()

    batches = [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]

    progress_bar = tqdm_asyncio(
        total=len(verblijfsobject_ids), disable=len(batches) <= 1
    )

    # Phase 1: process all batches, up to _MAX_BATCH_ATTEMPTS per batch; defer rest
    _BatchResult = Tuple[DataFrame, DataFrame, DataFrame, int]
    task_to_info: dict[asyncio.Future[_BatchResult], Tuple[List[str], int, str]] = {}
    all_tasks: set[asyncio.Future[_BatchResult]] = set()
    deferred: List[Tuple[List[str], str]] = []

    for batch in batches:
        t = cast(
            asyncio.Future[_BatchResult],
            asyncio.ensure_future(
                _process_batch(session, batch, beschermde_gezichten_df)
            ),
        )
        all_tasks.add(t)
        task_to_info[t] = (batch, 0, uuid.uuid4().hex[:8])

    try:
        while all_tasks:
            done, _ = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                all_tasks.discard(t)
                batch, attempt, batch_id = task_to_info.pop(t)
                try:
                    batch_result = t.result()
                except Exception:
                    if attempt < _MAX_BATCH_ATTEMPTS - 1:
                        t2 = cast(
                            asyncio.Future[_BatchResult],
                            asyncio.ensure_future(
                                _process_batch(session, batch, beschermde_gezichten_df)
                            ),
                        )
                        all_tasks.add(t2)
                        task_to_info[t2] = (batch, attempt + 1, batch_id)
                        logger.warning(
                            "Batch [%s] (size %d) mislukt, poging %d/%d, opnieuw proberen",
                            batch_id,
                            len(batch),
                            attempt + 1,
                            _MAX_BATCH_ATTEMPTS,
                        )
                    else:
                        deferred.append((batch, batch_id))
                        logger.error(
                            "Batch [%s] (size %d) na %d pogingen mislukt, uitstellen",
                            batch_id,
                            len(batch),
                            _MAX_BATCH_ATTEMPTS,
                        )
                    progress_bar.update(len(batch))
                    continue
                (
                    rijksmonumenten,
                    verblijfsobjecten_in_beschermd_gezicht,
                    gemeentelijke_monumenten,
                    aantal,
                ) = batch_result
                rijksmonumenten_result = pd.concat(
                    [rijksmonumenten_result, rijksmonumenten]
                )
                verblijfsobjecten_in_beschermd_gezicht_result = pd.concat(
                    [
                        verblijfsobjecten_in_beschermd_gezicht_result,
                        verblijfsobjecten_in_beschermd_gezicht,
                    ]
                )
                gemeentelijke_monumenten_result = pd.concat(
                    [gemeentelijke_monumenten_result, gemeentelijke_monumenten]
                )
                progress_bar.update(aantal)
    finally:
        for t in all_tasks:
            if not t.done():
                t.cancel()
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

    # Phase 2: retry deferred batches; on failure, split in half and re-queue
    # queue entries are (batch, split_depth, batch_id)
    queue: List[Tuple[List[str], int, str]] = [(b, 0, bid) for b, bid in deferred]
    while queue:
        batch, depth, batch_id = queue.pop(0)
        try:
            (
                rijksmonumenten,
                verblijfsobjecten_in_beschermd_gezicht,
                gemeentelijke_monumenten,
                aantal,
            ) = await _process_batch(session, batch, beschermde_gezichten_df)
            rijksmonumenten_result = pd.concat(
                [rijksmonumenten_result, rijksmonumenten]
            )
            verblijfsobjecten_in_beschermd_gezicht_result = pd.concat(
                [
                    verblijfsobjecten_in_beschermd_gezicht_result,
                    verblijfsobjecten_in_beschermd_gezicht,
                ]
            )
            gemeentelijke_monumenten_result = pd.concat(
                [gemeentelijke_monumenten_result, gemeentelijke_monumenten]
            )
            progress_bar.update(aantal)
        except Exception:
            if len(batch) > _MIN_BATCH_SIZE and depth < _MAX_SPLIT_DEPTH:
                mid = len(batch) // 2
                queue.append((batch[:mid], depth + 1, batch_id))
                queue.append((batch[mid:], depth + 1, batch_id))
                logger.info(
                    "Uitgestelde batch [%s] (size %d) opnieuw mislukt, gesplitst in %d en %d",
                    batch_id,
                    len(batch),
                    mid,
                    len(batch) - mid,
                )
            else:
                logger.warning(
                    "Batch [%s] (size %d) definitief overgeslagen na splitsen",
                    batch_id,
                    len(batch),
                )
                progress_bar.update(len(batch))

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
                "beschermd_gezicht_naam",
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
        # Aggregate beschermd gezicht names for the same identificatie
        def _join_beschermd_gezicht_naam(x: pd.Series[str]) -> str | None:
            if x.dropna().any():
                return ", ".join(str(v) for v in x.dropna().unique())
            return None

        verblijfsobjecten_in_beschermd_gezicht_result = (
            verblijfsobjecten_in_beschermd_gezicht_result.groupby("identificatie")
            .agg({"beschermd_gezicht_naam": _join_beschermd_gezicht_naam})
            .reset_index()
        )

    if not gemeentelijke_monumenten_result.empty:
        gemeentelijke_monumenten_result = (
            gemeentelijke_monumenten_result.drop_duplicates(keep="first")
        )

    result = rijksmonumenten_result.merge(
        verblijfsobjecten_in_beschermd_gezicht_result, on="identificatie", how="outer"
    ).merge(gemeentelijke_monumenten_result, on="identificatie", how="outer")

    return result
