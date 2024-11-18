"""Interne processing functies voor de monumenten package."""

import asyncio
from typing import Any, Dict, List, Tuple

import aiohttp
import geopandas as gpd
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from monumenten._api._cultureel_erfgoed import (
    _query_beschermde_gebieden,
    _query_rijksmonumenten,
)
from monumenten._api._kadaster import _query_verblijfsobjecten

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance


async def _process_batch(
    session: aiohttp.ClientSession, batch: List[str], bg_df: gpd.GeoDataFrame
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        bg_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]: Tuple met rijksmonumenten,
            beschermde gezichten en aantal verwerkte objecten
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

    # Process 'verblijfsobjecten' into GeoDataFrame
    if verblijfsobjecten:
        vo_df = pd.DataFrame(verblijfsobjecten)
        vo_df["geometry"] = gpd.GeoSeries.from_wkt(vo_df["verblijfsobjectWKT"])
        vo_df = gpd.GeoDataFrame(
            vo_df[["identificatie", "geometry"]], geometry="geometry"
        )
    else:
        vo_df = gpd.GeoDataFrame(columns=["identificatie", "geometry"])

    # Perform spatial join
    if not vo_df.empty:
        joined_df = gpd.sjoin(vo_df, bg_df, how="left", predicate="within")

        verblijfsobjecten_in_beschermd_gezicht = joined_df[
            ["identificatie", "beschermd_gezicht_naam"]
        ].to_dict("records")
    else:
        verblijfsobjecten_in_beschermd_gezicht = list[Dict[str, Any]]()

    return rijksmonumenten, verblijfsobjecten_in_beschermd_gezicht, len(batch)


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
    beschermde_gebieden = await _query_beschermde_gebieden(session)
    bg_df = gpd.GeoDataFrame()

    if beschermde_gebieden:
        bg_df = pd.DataFrame(beschermde_gebieden)
        bg_df["geometry"] = gpd.GeoSeries.from_wkt(bg_df["gezichtWKT"])
        bg_df = gpd.GeoDataFrame(
            bg_df[["beschermd_gezicht_naam", "geometry"]], geometry="geometry"
        )

    # Ensure spatial index is built
    bg_df.sindex

    rijksmonumenten_result = list[Dict[str, Any]]()
    verblijfsobjecten_in_beschermd_gezicht_result = list[Dict[str, Any]]()

    # Prepare batches
    batches = [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]

    # Create tasks for each batch
    tasks = [_process_batch(session, batch, bg_df) for batch in batches]

    progress_bar = tqdm_asyncio(total=len(verblijfsobject_ids))

    for task in asyncio.as_completed(tasks):
        rijksmonumenten, verblijfsobjecten, aantal = await task
        rijksmonumenten_result.extend(rijksmonumenten)
        verblijfsobjecten_in_beschermd_gezicht_result.extend(verblijfsobjecten)
        progress_bar.update(aantal)

    progress_bar.close()

    df_rijksmonumenten = (
        pd.DataFrame(rijksmonumenten_result)
        if rijksmonumenten_result
        else pd.DataFrame({"identificatie": [], "rijksmonument_nummer": []})
    )

    df_beschermd_gezicht = (
        pd.DataFrame(verblijfsobjecten_in_beschermd_gezicht_result)
        if verblijfsobjecten_in_beschermd_gezicht_result
        else pd.DataFrame({"identificatie": [], "beschermd_gezicht_naam": []})
    )

    result = pd.merge(
        df_rijksmonumenten, df_beschermd_gezicht, on="identificatie", how="outer"
    )

    return result
