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

ontbrekende_identificaties = set()
identificatie_mapping = {}

QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance


async def verzamel_data(sessie, identificaties_batch):
    rijksmonumenten_taak = query_rijksmonumenten(sessie, identificaties_batch)
    verblijfsobjecten_taak = query_verblijfsobjecten(sessie, identificaties_batch)
    rijksmonumenten, verblijfsobjecten = await asyncio.gather(
        rijksmonumenten_taak, verblijfsobjecten_taak
    )
    return rijksmonumenten, verblijfsobjecten


async def voer_queries_uit(sessie, verblijfsobject_ids: list[str]):
    # Load 'beschermde_gebieden' and convert to GeoDataFrame
    beschermde_gebieden = await query_beschermde_gebieden(sessie)
    bg_df = gpd.GeoDataFrame()

    if beschermde_gebieden:
        bg_df = pd.DataFrame(beschermde_gebieden)
        bg_df["geometry"] = gpd.GeoSeries.from_wkt(bg_df["gezichtWKT"])
        bg_df = gpd.GeoDataFrame(
            bg_df[["beschermd_gezicht_naam", "geometry"]], geometry="geometry"
        )

    # Ensure spatial index is built
    bg_df.sindex

    rijksmonumenten_totaal = []
    verblijfsobjecten_in_beschermd_gezicht_totaal = []

    # Prepare batches
    batches = [
        verblijfsobject_ids[i : i + QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), QUERY_BATCH_GROOTTE)
    ]

    # Semaphore to limit the number of concurrent tasks
    semaphore = asyncio.Semaphore(10)

    async def process_batch(batch):
        async with semaphore:
            # Retrieve data asynchronously
            rijksmonumenten, verblijfsobjecten = await verzamel_data(sessie, batch)

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

    # Create tasks for each batch
    tasks = [process_batch(batch) for batch in batches]

    rijksmonumenten_totaal = []
    verblijfsobjecten_in_beschermd_gezicht_totaal = []

    # Use tqdm.asyncio to display a progress bar for asynchronous tasks
    for future in tqdm_asyncio.as_completed(
        tasks, desc="Verwerking verzoeken ", total=len(tasks)
    ):
        (
            rijksmonumenten,
            verblijfsobjecten_in_beschermd_gezicht,
            batch_size,
        ) = await future
        rijksmonumenten_totaal.extend(rijksmonumenten)
        verblijfsobjecten_in_beschermd_gezicht_totaal.extend(
            verblijfsobjecten_in_beschermd_gezicht
        )

    # Merge results into final DataFrame
    df_identificatie = pd.DataFrame({"identificatie": verblijfsobject_ids})
    df_rijksmonumenten = (
        pd.DataFrame(rijksmonumenten_totaal)
        if rijksmonumenten_totaal
        else pd.DataFrame({"identificatie": [], "monument_nummer": []})
    )
    df_beschermd_gezicht = (
        pd.DataFrame(verblijfsobjecten_in_beschermd_gezicht_totaal)
        if verblijfsobjecten_in_beschermd_gezicht_totaal
        else pd.DataFrame({"identificatie": [], "beschermd_gezicht_naam": []})
    )

    result = df_identificatie.merge(
        df_rijksmonumenten, on="identificatie", how="left"
    ).merge(df_beschermd_gezicht, on="identificatie", how="left")

    return result


def handle_sigint(signal, frame):
    logger.info("SIGINT ontvangen, taken annuleren...")
    for taak in asyncio.all_tasks():
        taak.cancel()


async def main():
    verblijfsobject_ids = pd.read_csv("verblijfsobjecten.csv", dtype=str)[
        "bag_verblijfsobject_id"
    ].tolist()  # TODO: verwijder deze lijst na testen

    async with aiohttp.ClientSession() as sessie:
        results = await voer_queries_uit(sessie, verblijfsobject_ids)

    results.to_csv("results.csv", index=False)
    print(results)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    asyncio.run(main())
