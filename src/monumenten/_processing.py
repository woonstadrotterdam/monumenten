"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
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
from monumenten._api._kadaster import (
    _RD,
    _Adrespunten,
    _query_adrespunten,
    _query_beperkingen,
)
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
# Bereik van RD-coördinaten in Nederland: minx, miny, maxx, maxy
_RD_BEREIK = (0, 300_000, 300_000, 650_000)

# Kadaster grondslagcodes (publiekrechtelijke beperkingen, BRK)
# EWE: inschrijving in het rijksmonumentenregister -> rijksmonument (art. 1.1 Erfgoedwet)
_RIJKSMONUMENT_GRONDSLAGCODES = ["EWE"]
# EWD: toezending ontwerpbesluit aanwijzing (voorbescherming) -> nog geen rijksmonument
_VOORBESCHERMING_GRONDSLAGCODES = ["EWD"]
# GG/GWA: gemeentelijk monument (GWA bundelt voorbescherming, aanwijzing en afschrift)
_GEMEENTELIJK_GRONDSLAGCODES = ["GG", "GWA"]

_WGS84 = "EPSG:4326"

# Marge om de provinciegrens. Enkele Drentse monumentvlakken steken tot 1,3 m
# buiten de grens van PDOK, door verschillen in nauwkeurigheid; de grens wordt
# bovendien maar jaarlijks bijgewerkt. Een te ruime marge kost alleen wat extra
# kandidaten, een te krappe geeft stil False.
_GRENS_BUFFER_METER = 100


@dataclass(frozen=True)
class ProvincieOnbepaald:
    """Een provincie waarvan de monumentvlakken niet op te halen waren.

    Attributes:
        oorzaak (str): Waarom het ophalen mislukte, voor de waarschuwing aan de
            gebruiker
        verblijfsobject_ids (List[str]): De kandidaten in die provincie, waarvoor
            ``provinciaal_monument`` onbepaald is
    """

    oorzaak: str
    verblijfsobject_ids: List[str]


_BatchFrames = Tuple[DataFrame, DataFrame, DataFrame]
_BatchResult = Tuple[DataFrame, DataFrame, DataFrame, int]

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
    )


def _lege_provinciale_monumenten() -> DataFrame:
    """Leeg resultaat van _bepaal_provinciale_monumenten."""
    return pd.DataFrame(columns=["identificatie", "provinciaal_monument_omschrijving"])


async def _process_batch(
    session: aiohttp.ClientSession,
    batch: List[str],
    beschermde_gezichten_df: gpd.GeoDataFrame,
    adrespunten: _Adrespunten,
) -> _BatchResult:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        beschermde_gezichten_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten
        adrespunten (_Adrespunten): Adrespunt per verblijfsobject ID uit de BAG

    Returns:
        _BatchResult: Tuple met rijksmonumenten, beschermde gezichten,
            gemeentelijke monumenten en aantal verwerkte objecten. De provinciale
            monumenten worden na alle batches bepaald, uit alle adrespunten.
    """
    # Get the current event loop
    loop = asyncio.get_running_loop()
    batch_adrespunten = {i: adrespunten[i] for i in batch if i in adrespunten}

    # Create tasks using the current loop
    rijksmonumenten_taak = loop.create_task(_query_rijksmonumenten(session, batch))
    beperkingen_taak = loop.create_task(_query_beperkingen(session, batch_adrespunten))

    # Wait for both tasks to complete
    rijksmonumenten, beperkingen = await asyncio.gather(
        rijksmonumenten_taak, beperkingen_taak
    )

    # Geen vroege return als de beperkingen leeg zijn: de RCE-query heeft geen
    # adrespunt nodig, en het beschermd gezicht volgt uit het adrespunt zelf.
    beperkingen_df = pd.DataFrame(
        beperkingen,
        columns=["identificatie", "grondslagcode", "grondslag_gemeentelijk_monument"],
    ).astype({"identificatie": "string"})

    rijksmonumenten_df = pd.merge(
        pd.DataFrame(
            rijksmonumenten,
            columns=["identificatie", "rijksmonument_nummer"],
            dtype="string",
        ),
        beperkingen_df[
            beperkingen_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
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
        beperkingen_df[
            beperkingen_df["grondslagcode"].isin(_VOORBESCHERMING_GRONDSLAGCODES)
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
    gemeentelijke_monumenten_df = beperkingen_df[
        beperkingen_df["grondslagcode"].isin(_GEMEENTELIJK_GRONDSLAGCODES)
    ][["identificatie", "grondslag_gemeentelijk_monument"]]

    # Process beschermde gezichten
    xs = [x for x, _ in batch_adrespunten.values()]
    ys = [y for _, y in batch_adrespunten.values()]
    geo_df = gpd.GeoDataFrame(
        {"identificatie": pd.Series(list(batch_adrespunten), dtype="string")},
        geometry=gpd.points_from_xy(xs, ys),
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
        crs=_WGS84,
    ).to_crs(_RD)
    # Levert de RCE ooit iets anders dan WGS84, dan vindt de spatial join stilletjes
    # niets meer. Na omrekening moeten de gezichten daarom binnen Nederland liggen.
    minx, miny, maxx, maxy = beschermde_gezichten_df.total_bounds
    bereik_minx, bereik_miny, bereik_maxx, bereik_maxy = _RD_BEREIK
    if not (
        bereik_minx <= minx <= maxx <= bereik_maxx
        and bereik_miny <= miny <= maxy <= bereik_maxy
    ):
        raise ValueError(
            "Beschermde gezichten liggen na omrekening naar RD buiten Nederland; "
            "levert de RCE de geometrie nog in WGS84?"
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
    session: aiohttp.ClientSession, adrespunten: _Adrespunten
) -> Tuple[DataFrame, Dict[str, ProvincieOnbepaald]]:
    """Bepaal welke adrespunten binnen een provinciaal monumentvlak liggen.

    Een adrespunt is kandidaat voor een provincie als het in haar ruime bounding
    box ligt en binnen haar grens (met een marge van ``_GRENS_BUFFER_METER``).
    Alleen provincies met kandidaten worden gedownload; ligt geen adrespunt in een
    bounding box, dan volgt er geen enkele request. Zijn de grenzen niet op te
    halen, dan is elk adrespunt in de bounding box kandidaat.

    Een fout bij een provincie breekt de aanroep niet af: haar kandidaten worden
    onbepaald.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        adrespunten (_Adrespunten): Adrespunt (RD) per verblijfsobject ID uit de BAG

    Returns:
        Tuple[DataFrame, Dict[str, ProvincieOnbepaald]]: De gevonden monumenten
            (kolommen ``identificatie`` en ``provinciaal_monument_omschrijving``,
            één rij per combinatie), en per provincie waarvan de monumentvlakken
            niet op te halen waren de oorzaak en de ID's van haar kandidaten
    """
    onbepaald: Dict[str, ProvincieOnbepaald] = {}
    if not adrespunten:
        return _lege_provinciale_monumenten(), onbepaald

    xs, ys = zip(*adrespunten.values())
    rd = gpd.GeoDataFrame(
        {"identificatie": pd.Series(list(adrespunten), dtype="string")},
        geometry=gpd.points_from_xy(xs, ys),
        crs=_RD,
    )
    x, y = rd.geometry.x, rd.geometry.y

    in_bbox: Dict[Provincie, pd.Series[bool]] = {}
    for provincie in PROVINCIES:
        min_x, min_y, max_x, max_y = provincie.bbox
        masker = x.between(min_x, max_x) & y.between(min_y, max_y)
        if masker.any():
            in_bbox[provincie] = masker
    if not in_bbox:
        return _lege_provinciale_monumenten(), onbepaald

    grenzen: Dict[str, BaseGeometry] | None
    try:
        grenzen = await _get_provinciegrenzen(session)
    except Exception as e:
        logger.warning(
            "Provinciegrenzen niet op te halen (%s); elk adrespunt in de bounding "
            "box van een provincie telt als kandidaat",
            e,
            # Bij een onverwachte fout ook de traceback
            exc_info=not isinstance(e, ProvincialeMonumentenError),
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
        except Exception as e:
            # Ook een onverwachte fout, zoals een geometrie die from_features niet
            # leest, mag de rest van de aanroep niet afbreken.
            oorzaak = (
                str(e)
                if isinstance(e, ProvincialeMonumentenError)
                else f"Onverwachte fout bij {provincie.naam}: {type(e).__name__}: {e}"
            )
            logger.warning(
                "%s; provinciaal_monument is onbepaald voor %d verblijfsobjecten",
                oorzaak,
                len(kandidaten),
                exc_info=not isinstance(e, ProvincialeMonumentenError),
            )
            onbepaald[provincie.naam] = ProvincieOnbepaald(
                oorzaak, kandidaten["identificatie"].tolist()
            )
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
) -> Tuple[pd.DataFrame, Dict[str, ProvincieOnbepaald]]:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules. De provinciale
    monumenten worden daarna in één stap bepaald.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        Tuple[pd.DataFrame, Dict[str, ProvincieOnbepaald]]: Eén rij per
            verblijfsobject met een monumentstatus, met de kolommen uit
            ``_RESULTAAT_KOLOMMEN``; en per provincie die niet op te halen was de
            oorzaak en de ID's waarvan ``provinciaal_monument`` onbepaald is
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)

    # De BRK-PB requests zijn alleen snel als de adrespunten in een request bij elkaar
    # liggen. Daarom eerst alle adrespunten ophalen en de verblijfsobjecten langs een
    # Hilbert-curve sorteren: opeenvolgende ID's liggen dan bij elkaar in de buurt.
    adrespunten = await _query_adrespunten(session, verblijfsobject_ids)
    zonder_adrespunt = len(verblijfsobject_ids) - len(adrespunten)
    if zonder_adrespunt:
        logger.warning(
            "%d van %d verblijfsobjecten hebben geen adrespunt uit de BAG; die krijgen "
            "geen Kadaster-beperking, geen beschermd gezicht en geen provinciaal monument",
            zonder_adrespunt,
            len(verblijfsobject_ids),
        )
    if adrespunten:
        xs, ys = zip(*adrespunten.values())
        hilbert_afstand = dict(
            zip(
                adrespunten,
                gpd.GeoSeries(gpd.points_from_xy(xs, ys)).hilbert_distance(
                    total_bounds=_RD_BEREIK
                ),
            )
        )
        verblijfsobject_ids = sorted(
            verblijfsobject_ids, key=lambda i: hilbert_afstand.get(i, float("inf"))
        )

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
                result = await _process_batch(
                    session, batch, beschermde_gezichten_df, adrespunten
                )
                results_list.append(result)
                progress_bar.update(result[3])
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
            for frames in zip(*(r[:3] for r in results_list))
        ]
        if results_list
        else list(_lege_batch_frames())
    )
    (
        rijksmonumenten_result,
        verblijfsobjecten_in_beschermd_gezicht_result,
        gemeentelijke_monumenten_result,
    ) = samengevoegd

    # Uit alle adrespunten en niet per batch: ook een mislukte batch heeft ze al
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
