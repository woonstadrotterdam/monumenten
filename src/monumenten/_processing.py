"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from typing import List, Set, Tuple

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
from monumenten._api._kadaster import (
    _RD,
    _Adrespunten,
    _query_adrespunten,
    _query_beperkingen,
)
from monumenten._bronfouten import BAG, RCE, Bronuitval

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
_BATCH_CONCURRENCY = 4
# Bereik van RD-coördinaten in Nederland: minx, miny, maxx, maxy
_RD_BEREIK = (0, 300_000, 300_000, 650_000)

_BatchResult = Tuple[DataFrame, DataFrame, DataFrame, int]


async def _process_batch(
    session: aiohttp.ClientSession,
    batch: List[str],
    beschermde_gezichten_df: gpd.GeoDataFrame,
    adrespunten: _Adrespunten,
    uitval: Bronuitval,
) -> _BatchResult:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        beschermde_gezichten_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten
        adrespunten (_Adrespunten): Adrespunt per verblijfsobject ID uit de BAG
        uitval (Bronuitval): Hierin leggen de bronnen vast voor welke
            verblijfsobjecten zij wel en geen antwoord gaven

    Returns:
        _BatchResult: Tuple met rijksmonumenten, beschermde gezichten,
            gemeentelijke monumenten en aantal verwerkte objecten
    """
    # Get the current event loop
    loop = asyncio.get_running_loop()

    # Create tasks using the current loop
    rijksmonumenten_taak = loop.create_task(
        _query_rijksmonumenten(session, batch, uitval)
    )
    verblijfsobjecten_taak = loop.create_task(
        _query_beperkingen(
            session, {i: adrespunten[i] for i in batch if i in adrespunten}, uitval
        )
    )

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
            # Wat de RCE wel weet blijft staan, ook zonder adrespunt uit de BAG
            pd.DataFrame(
                rijksmonumenten,
                columns=["identificatie", "rijksmonument_nummer"],
                dtype="string",
            ).assign(rijksmonument_bron="RCE"),
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
        crs=_RD,
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
    # De RCE levert de gezichten in WGS84, de adrespunten van het Kadaster zijn in RD
    beschermde_gezichten_df = gpd.GeoDataFrame(
        beschermde_gezichten_df[["rijksbeschermd_gezicht_naam", "geometry"]],
        geometry="geometry",
        crs="EPSG:4326",
    ).to_crs(_RD)

    return beschermde_gezichten_df


def _in_batches(verblijfsobject_ids: List[str]) -> List[List[str]]:
    return [
        verblijfsobject_ids[i : i + _QUERY_BATCH_GROOTTE]
        for i in range(0, len(verblijfsobject_ids), _QUERY_BATCH_GROOTTE)
    ]


def _sorteer_ruimtelijk(
    verblijfsobject_ids: List[str], adrespunten: _Adrespunten
) -> List[str]:
    """Sorteer verblijfsobjecten langs een Hilbert-curve.

    De BRK-PB requests zijn alleen snel als de adrespunten in een request bij elkaar
    liggen. Na het sorteren liggen opeenvolgende ID's bij elkaar in de buurt.
    Verblijfsobjecten zonder adrespunt komen achteraan.
    """
    punten = {i: adrespunten[i] for i in verblijfsobject_ids if i in adrespunten}
    if not punten:
        return verblijfsobject_ids
    xs, ys = zip(*punten.values())
    hilbert_afstand = dict(
        zip(
            punten,
            gpd.GeoSeries(gpd.points_from_xy(xs, ys)).hilbert_distance(
                total_bounds=_RD_BEREIK
            ),
        )
    )
    return sorted(
        verblijfsobject_ids, key=lambda i: hilbert_afstand.get(i, float("inf"))
    )


def _zonder(result: _BatchResult, verblijfsobject_ids: Set[str]) -> _BatchResult:
    """Laat uit een batchresultaat de rijen van de gegeven verblijfsobjecten weg."""
    rijksmonumenten, beschermde_gezichten, gemeentelijke_monumenten, aantal = result
    return (
        rijksmonumenten[~rijksmonumenten["identificatie"].isin(verblijfsobject_ids)],
        beschermde_gezichten[
            ~beschermde_gezichten["identificatie"].isin(verblijfsobject_ids)
        ],
        gemeentelijke_monumenten[
            ~gemeentelijke_monumenten["identificatie"].isin(verblijfsobject_ids)
        ],
        aantal,
    )


def _eerste_exceptie(groep: BaseExceptionGroup[BaseException]) -> BaseException:
    """Geef de eerste niet-gegroepeerde exceptie uit een (geneste) ExceptionGroup."""
    for exceptie in groep.exceptions:
        if isinstance(exceptie, BaseExceptionGroup):
            return _eerste_exceptie(exceptie)
        return exceptie
    return groep


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> Tuple[pd.DataFrame, Bronuitval]:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules. Raakt een bron gestoord
    voor zij één antwoord gaf, dan worden de resterende batches overgeslagen.
    Verblijfsobjecten waarvoor een bron geen antwoord gaf krijgen aan het einde één
    herkansing.

    Gaf een bron voor geen enkel bevraagd verblijfsobject antwoord, dan volgt een
    BronFoutError. Mislukt de verwerking van een batch, dan bereikt de oorspronkelijke
    fout de aanroeper.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        Tuple[pd.DataFrame, Bronuitval]: Eén rij per verblijfsobject met een
            monumentstatus, en per bron de verblijfsobjecten waarvoor zij, ook na
            retries, splitsen en de herkansing, geen antwoord gaf
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)
    uitval = Bronuitval()

    # Eerst alle adrespunten ophalen: de BRK-PB heeft ze nodig, en de batches worden
    # erop gesorteerd. Zonder de BAG valt er verder niets te bepalen.
    adrespunten = await _query_adrespunten(session, verblijfsobject_ids, uitval)
    uitval.controleer()

    batches = _in_batches(_sorteer_ruimtelijk(verblijfsobject_ids, adrespunten))

    progress_bar = tqdm_asyncio(
        total=len(verblijfsobject_ids), disable=len(batches) <= 1
    )

    batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)

    async def _verwerk(
        batches: List[List[str]], herkansing: bool = False
    ) -> List[_BatchResult]:
        resultaten: List[_BatchResult] = []

        async def _run_batch(batch: List[str]) -> None:
            # Een fout wordt hier niet opgevangen: een overgeslagen batch zou voor al
            # zijn verblijfsobjecten stil False opleveren. Uitval van een bron loopt
            # via uitval.
            async with batch_semaphore:
                # Ligt een bron plat, dan kan de aanroep niet meer slagen: bevraag ook
                # de andere bronnen niet meer. uitval.controleer() geeft dan de
                # BronFoutError.
                if uitval.sla_over_als_bron_plat_ligt(batch):
                    return
                result = await _process_batch(
                    session, batch, beschermde_gezichten_df, adrespunten, uitval
                )
                resultaten.append(result)
                if not herkansing:
                    progress_bar.update(result[3])

        try:
            async with asyncio.TaskGroup() as tg:
                for batch in batches:
                    tg.create_task(_run_batch(batch))
        except BaseExceptionGroup as groep:
            # De TaskGroup verpakt de fout; geef de oorspronkelijke fout door
            raise _eerste_exceptie(groep) from None
        return resultaten

    async def _proefrequest(bron: str, verblijfsobject_id: str) -> None:
        if bron == RCE:
            await _query_rijksmonumenten(session, [verblijfsobject_id], uitval)
        elif bron == BAG:
            await _query_adrespunten(session, [verblijfsobject_id], uitval)
        else:
            await _query_beperkingen(
                session, {verblijfsobject_id: adrespunten[verblijfsobject_id]}, uitval
            )

    try:
        results_list = await _verwerk(batches)
    finally:
        progress_bar.close()

    uitval.controleer()

    # Herkansing: een bron die halverwege haperde geeft aan het einde vaak weer
    # antwoord. De verblijfsobjecten zonder antwoord worden daarom één keer opnieuw
    # verwerkt; hun eerdere, onvolledige rijen vervallen.
    if uitval.mislukt:
        # Een bron die nog gestoord is krijgt eerst één proefrequest voor één
        # verblijfsobject. Blijft zij gestoord, dan heeft opnieuw verwerken geen zin.
        await uitval.wacht_op_proefrequest()
        for bron in uitval.gestoorde_bronnen():
            await _proefrequest(bron, next(iter(uitval.mislukt[bron])))
        opnieuw = uitval.herkansbaar()
        if opnieuw:
            logger.info(
                "Herkansing voor %d verblijfsobjecten waarvoor een bron geen antwoord gaf",
                len(opnieuw),
            )
            zonder_adrespunt = [i for i in opnieuw if i in uitval.mislukt.get(BAG, {})]
            uitval.vergeet(opnieuw)
            adrespunten.update(
                await _query_adrespunten(session, zonder_adrespunt, uitval)
            )
            herkansing = await _verwerk(
                _in_batches(_sorteer_ruimtelijk(opnieuw, adrespunten)), herkansing=True
            )
            results_list = [_zonder(r, set(opnieuw)) for r in results_list] + herkansing
            uitval.controleer()

    # Lege frames doen niet mee: pandas waarschuwt bij het samenvoegen daarvan
    rijksmonumenten_parts = [r[0] for r in results_list if not r[0].empty]
    beschermd_gezicht_parts = [r[1] for r in results_list if not r[1].empty]
    gemeentelijke_parts = [r[2] for r in results_list if not r[2].empty]

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

    if (
        rijksmonumenten_result.empty
        and verblijfsobjecten_in_beschermd_gezicht_result.empty
        and gemeentelijke_monumenten_result.empty
    ):
        return (
            pd.DataFrame(
                columns=[
                    "identificatie",
                    "rijksmonument_nummer",
                    "rijksmonument_bron",
                    "rijksbeschermd_gezicht_naam",
                    "grondslag_gemeentelijk_monument",
                ]
            ),
            uitval,
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

    return result, uitval
