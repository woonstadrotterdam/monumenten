import asyncio
import signal

import aiohttp
import pandas as pd
import shapely
from loguru import logger
from rtree import index
from tqdm.asyncio import tqdm

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
    beschermde_gebieden = await query_beschermde_gebieden(sessie)
    beschermde_gezicht_geometrieën = [
        (gebied["beschermd_gezicht_naam"], shapely.io.from_wkt(gebied["gezichtWKT"]))
        for gebied in beschermde_gebieden
    ]
    idx = index.Index()

    rijksmonumenten_totaal = []
    verblijfsobjecten_in_beschermd_gezicht_totaal = []

    for pos, (_, geom) in enumerate(beschermde_gezicht_geometrieën):
        idx.insert(pos, shapely.bounds(geom))
    with tqdm(total=len(verblijfsobject_ids), desc="Verwerking verzoeken ") as pbar:
        for i in range(0, len(verblijfsobject_ids), QUERY_BATCH_GROOTTE):
            batch = verblijfsobject_ids[i : i + QUERY_BATCH_GROOTTE]
            identificaties_batch = batch

            rijksmonumenten, verblijfsobjecten = await verzamel_data(
                sessie, identificaties_batch
            )

            verblijfsobject_geometrieën = [
                (
                    verblijfsobject["identificatie"],
                    shapely.io.from_wkt(verblijfsobject["verblijfsobjectWKT"]),
                )
                for verblijfsobject in verblijfsobjecten
            ]

            def controleer_punt(punt_id, punt_geom):
                mogelijke_overeenkomsten = idx.intersection(shapely.bounds(punt_geom))
                for match in mogelijke_overeenkomsten:
                    naam, geom = beschermde_gezicht_geometrieën[match]
                    if shapely.contains(geom, punt_geom):
                        return naam

            verblijfsobjecten_in_beschermd_gezicht = [
                {
                    "identificatie": identificatie,
                    "beschermd_gezicht_naam": controleer_punt(identificatie, punt_geom),
                }
                for identificatie, punt_geom in verblijfsobject_geometrieën
            ]

            # rijksmonumenten [{"identificatie": monument_nummer}, ...]
            # verblijfsobjecten_in_beschermd_gezicht [{"identificatie": beschermd_gezicht_naam}, ...]
            rijksmonumenten_totaal.extend(rijksmonumenten)
            verblijfsobjecten_in_beschermd_gezicht_totaal.extend(
                verblijfsobjecten_in_beschermd_gezicht
            )

            pbar.update(len(batch))

    return pd.merge(
        pd.merge(
            pd.DataFrame({"identificatie": verblijfsobject_ids}),
            pd.DataFrame(rijksmonumenten_totaal)
            if rijksmonumenten_totaal
            else pd.DataFrame({"identificatie": [], "monument_nummer": []}),
            on="identificatie",
            how="left",
        ),
        pd.DataFrame(verblijfsobjecten_in_beschermd_gezicht_totaal)
        if verblijfsobjecten_in_beschermd_gezicht_totaal
        else pd.DataFrame({"identificatie": [], "beschermd_gezicht_naam": []}),
        on="identificatie",
        how="left",
    )


def handle_sigint(signal, frame):
    logger.info("SIGINT ontvangen, taken annuleren...")
    for taak in asyncio.all_tasks():
        taak.cancel()


async def check_bag_verblijfsobjecten(verblijfsobject_ids: list[str]):
    async with aiohttp.ClientSession() as sessie:
        results = await voer_queries_uit(sessie, verblijfsobject_ids)
        return results


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    verblijfsobject_ids = pd.read_csv("verblijfsobjecten.csv", dtype=str)[
        "bag_verblijfsobject_id"
    ].tolist()  # TODO: verwijder deze lijst na testen
    results = asyncio.run(check_bag_verblijfsobjecten(verblijfsobject_ids)).to_csv(
        "results.csv", index=False
    )
    print(results)
