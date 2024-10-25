import asyncio
import signal

import aiohttp
import geopandas as gpd
import pandas as pd
from loguru import logger
from tqdm.asyncio import tqdm_asyncio

from monumenten.api.cultureel_erfgoed import (
    query_beschermde_gebieden,
    query_rijksmonumenten,
)
from monumenten.api.kadaster import query_verblijfsobjecten


QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance


async def verzamel_data(session, identificaties_batch):
    rijksmonumenten_taak = query_rijksmonumenten(session, identificaties_batch)
    verblijfsobjecten_taak = query_verblijfsobjecten(session, identificaties_batch)
    rijksmonumenten, verblijfsobjecten = await asyncio.gather(
        rijksmonumenten_taak, verblijfsobjecten_taak
    )
    return rijksmonumenten, verblijfsobjecten


async def process_batch(session, batch, bg_df):
    # Retrieve data asynchronously
    rijksmonumenten, verblijfsobjecten = await verzamel_data(session, batch)

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
        verblijfsobjecten_in_beschermd_gezicht = []

    return rijksmonumenten, verblijfsobjecten_in_beschermd_gezicht, len(batch)


async def voer_queries_uit(session, verblijfsobject_ids: list[str]):
    # Load 'beschermde_gebieden' and convert to GeoDataFrame
    beschermde_gebieden = await query_beschermde_gebieden(session)
    bg_df = gpd.GeoDataFrame()

    if beschermde_gebieden:
        bg_df = pd.DataFrame(beschermde_gebieden)
        bg_df["geometry"] = gpd.GeoSeries.from_wkt(bg_df["gezichtWKT"])
        bg_df = gpd.GeoDataFrame(
            bg_df[["beschermd_gezicht_naam", "geometry"]], geometry="geometry"
        )

    # Ensure spatial index is built
    bg_df.sindex

    rijksmonumenten_result = []
    verblijfsobjecten_in_beschermd_gezicht_result = []

    # Prepare batches
    batches = [
        verblijfsobject_ids[i : i + QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), QUERY_BATCH_GROOTTE)
    ]

    # Create tasks for each batch
    tasks = [process_batch(session, batch, bg_df) for batch in batches]

    rijksmonumenten_result = []
    verblijfsobjecten_in_beschermd_gezicht_result = []

    progress_bar = tqdm_asyncio(total=len(verblijfsobject_ids))

    for task in asyncio.as_completed(tasks):
        result = await task
        rijksmonumenten_result.extend(result[0])
        verblijfsobjecten_in_beschermd_gezicht_result.extend(result[1])
        progress_bar.update(result[2])

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


def handle_sigint(signal, frame):
    logger.info("SIGINT ontvangen, taken annuleren...")
    for taak in asyncio.all_tasks():
        taak.cancel()


async def main():
    input = pd.read_csv("verblijfsobjecten.csv", dtype=str)

    verblijfsobject_ids = input[
        "bag_verblijfsobject_id"
    ].drop_duplicates()  # TODO: verwijder deze lijst na testen

    async with aiohttp.ClientSession() as session:
        results = await voer_queries_uit(session, verblijfsobject_ids)
        merged = pd.merge(
            input, results, left_on="bag_verblijfsobject_id", right_on="identificatie"
        )

        if "identificatie" not in input.columns:
            merged = merged.drop(columns=["identificatie"])

        rijksmonument_nummer_position = merged.columns.get_loc("rijksmonument_nummer")

        merged.insert(
            rijksmonument_nummer_position + 1,
            "rijksmonument_url",
            merged.apply(
                lambda row: f"https://monumenten.nl/monument/{int(row['rijksmonument_nummer'])}"
                if pd.notnull(row["rijksmonument_nummer"])
                else None,
                axis=1,
            ),
        )

        merged.insert(
            rijksmonument_nummer_position,
            "is_rijksmonument",
            merged["rijksmonument_nummer"].notna(),
        )

        beschermd_gezicht_naam_position = merged.columns.get_loc(
            "beschermd_gezicht_naam"
        )

        merged.insert(
            beschermd_gezicht_naam_position,
            "is_beschermd_gezicht",
            merged["beschermd_gezicht_naam"].notna(),
        )

        merged.to_csv("monumenten.csv", index=False)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    asyncio.run(main())
