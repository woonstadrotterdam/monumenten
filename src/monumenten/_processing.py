"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
from typing import List, Tuple

from aiocache import cached_stampede
import aiohttp
import geopandas as gpd
import pandas as pd
from pandas import DataFrame
from tqdm.asyncio import tqdm_asyncio

from monumenten._api._cultureel_erfgoed import (
    _query_beschermde_gebieden,
    _query_rijksmonumenten,
)
from monumenten._api._kadaster import _query_verblijfsobjecten

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance


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

    verblijfsobjecten_df = pd.DataFrame(verblijfsobjecten)

    rijksmonumenten_df = pd.merge(
        pd.DataFrame(
            rijksmonumenten,
            columns=["identificatie", "rijksmonument_nummer"],
        ),
        verblijfsobjecten_df[
            verblijfsobjecten_df["grondslagcode"].isin(["EWE", "EWD"])
        ][["identificatie"]],
        on="identificatie",
        how="outer",
    ).fillna({"rijksmonument_nummer": "REGISTRATIE_ONTBREEKT_BIJ_RCE"})

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
async def _get_beschermde_gebieden(
    session: aiohttp.ClientSession,
) -> gpd.GeoDataFrame:
    """Haal beschermde gebieden op."""
    beschermde_gebieden = await _query_beschermde_gebieden(session)
    bg_df = gpd.GeoDataFrame()

    if not beschermde_gebieden:
        raise ValueError("Geen beschermde gebieden gevonden")

    bg_df = pd.DataFrame(beschermde_gebieden)
    bg_df["geometry"] = gpd.GeoSeries.from_wkt(bg_df["gezichtWKT"])
    bg_df = gpd.GeoDataFrame(
        bg_df[["beschermd_gezicht_naam", "geometry"]], geometry="geometry"
    )

    return bg_df


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> pd.DataFrame:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        pd.DataFrame: DataFrame met monumentinformatie
    """
    # Load 'beschermde_gebieden' and convert to GeoDataFrame
    bg_df = await _get_beschermde_gebieden(session)

    rijksmonumenten_result = pd.DataFrame()
    verblijfsobjecten_in_beschermd_gezicht_result = pd.DataFrame()
    gemeentelijke_monumenten_result = pd.DataFrame()

    # Prepare batches
    batches = [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]

    # Create tasks for each batch
    tasks = [_process_batch(session, batch, bg_df) for batch in batches]

    progress_bar = tqdm_asyncio(total=len(verblijfsobject_ids), disable=len(tasks) <= 1)

    for task in asyncio.as_completed(tasks):
        (
            rijksmonumenten,
            verblijfsobjecten_in_beschermd_gezicht,
            gemeentelijke_monumenten,
            aantal,
        ) = await task

        rijksmonumenten_result = pd.concat([rijksmonumenten_result, rijksmonumenten])
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

    progress_bar.close()

    result = rijksmonumenten_result.merge(
        verblijfsobjecten_in_beschermd_gezicht_result, on="identificatie", how="outer"
    ).merge(gemeentelijke_monumenten_result, on="identificatie", how="outer")

    return result
