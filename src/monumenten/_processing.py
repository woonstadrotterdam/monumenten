"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Set, Tuple

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
from monumenten._bronfouten import BAG, RCE, VERWERKING, Bronuitval

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
# Vanaf zoveel verblijfsobjecten toont de BAG-fase een voortgangsbalk
_BAG_VOORTGANG_VANAF = 10_000
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


_BatchFrames = Tuple[DataFrame, DataFrame, DataFrame]

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
    uitval: Bronuitval,
) -> _BatchFrames:
    """Verwerk een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        batch (List[str]): Lijst met verblijfsobject ID's
        beschermde_gezichten_df (gpd.GeoDataFrame): GeoDataFrame met beschermde gezichten
        adrespunten (_Adrespunten): Adrespunt per verblijfsobject ID uit de BAG
        uitval (Bronuitval): Hierin leggen de bronnen vast voor welke
            verblijfsobjecten zij wel en geen antwoord gaven

    Returns:
        _BatchFrames: Tuple met rijksmonumenten, beschermde gezichten en
            gemeentelijke monumenten. De provinciale monumenten worden na alle
            batches bepaald, uit alle adrespunten.
    """
    batch_adrespunten = {i: adrespunten[i] for i in batch if i in adrespunten}

    rijksmonumenten_taak = asyncio.create_task(
        _query_rijksmonumenten(session, batch, uitval)
    )
    beperkingen_taak = asyncio.create_task(
        _query_beperkingen(session, batch_adrespunten, uitval)
    )
    try:
        rijksmonumenten, beperkingen = await asyncio.gather(
            rijksmonumenten_taak, beperkingen_taak
        )
    finally:
        # Faalt één query onverwacht, dan mag de andere niet ongezien doorlopen: de
        # batch is dan al als mislukt vastgelegd, en zij zou daarna, ook tijdens de
        # herkansing, nog antwoorden en fouten in uitval kunnen vastleggen.
        rijksmonumenten_taak.cancel()
        beperkingen_taak.cancel()

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
    session: aiohttp.ClientSession, adrespunten: _Adrespunten, uitval: Bronuitval
) -> DataFrame:
    """Bepaal welke adrespunten binnen een provinciaal monumentvlak liggen.

    Een adrespunt is kandidaat voor een provincie als het in haar ruime bounding
    box ligt en binnen haar grens (met een marge van ``_GRENS_BUFFER_METER``).
    Alleen provincies met kandidaten worden gedownload; ligt geen adrespunt in een
    bounding box, dan volgt er geen enkele request. Zijn de grenzen niet op te
    halen, dan is elk adrespunt in de bounding box kandidaat.

    Een fout bij een provincie breekt de aanroep niet af: haar kandidaten worden in
    uitval vastgelegd met de provincie als bron, zodat ``provinciaal_monument`` voor
    hen onbepaald is.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        adrespunten (_Adrespunten): Adrespunt (RD) per verblijfsobject ID uit de BAG
        uitval (Bronuitval): Hierin worden de kandidaten vastgelegd van een
            provincie waarvan de monumentvlakken niet op te halen waren

    Returns:
        DataFrame: De gevonden monumenten (kolommen ``identificatie`` en
            ``provinciaal_monument_omschrijving``, één rij per combinatie)
    """
    if not adrespunten:
        return _lege_provinciale_monumenten()

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
        return _lege_provinciale_monumenten()

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
            logger.warning(
                "Monumentvlakken van %s niet op te halen (%s: %s); "
                "provinciaal_monument is onbepaald voor %d verblijfsobjecten",
                provincie.naam,
                type(e).__name__,
                e,
                len(kandidaten),
                # Bij een onverwachte fout ook de traceback
                exc_info=not isinstance(e, ProvincialeMonumentenError),
            )
            uitval.registreer_fout(
                provincie.naam, kandidaten["identificatie"].tolist(), e
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
        return _lege_provinciale_monumenten()
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


def _voeg_frames_samen(frames: List[DataFrame], leeg: DataFrame) -> DataFrame:
    """Voeg de frames van alle batches samen; ``leeg`` als ze allemaal leeg zijn.

    Lege frames doen niet mee: pandas waarschuwt bij het samenvoegen daarvan.
    """
    gevuld = [frame for frame in frames if not frame.empty]
    return pd.concat(gevuld, ignore_index=True) if gevuld else leeg


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


def _zonder(result: _BatchFrames, verblijfsobject_ids: Set[str]) -> _BatchFrames:
    """Laat uit een batchresultaat de rijen van de gegeven verblijfsobjecten weg."""
    rijksmonumenten, beschermde_gezichten, gemeentelijke_monumenten = result
    return (
        rijksmonumenten[~rijksmonumenten["identificatie"].isin(verblijfsobject_ids)],
        beschermde_gezichten[
            ~beschermde_gezichten["identificatie"].isin(verblijfsobject_ids)
        ],
        gemeentelijke_monumenten[
            ~gemeentelijke_monumenten["identificatie"].isin(verblijfsobject_ids)
        ],
    )


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> Tuple[pd.DataFrame, Bronuitval]:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Batches worden parallel verwerkt. Retry en split-on-failure gebeuren per API
    (cultureel erfgoed en kadaster) in hun eigen modules. Raakt een bron gestoord
    voor zij één antwoord gaf, dan worden de resterende batches overgeslagen.
    Mislukt de verwerking van een batch onverwacht, dan worden zijn statussen
    onbepaald, zodat één fout niet het werk van een hele aanroep weggooit.
    Verblijfsobjecten waarvoor een bron geen antwoord gaf krijgen aan het einde één
    herkansing. De provinciale monumenten worden daarna in één stap bepaald.

    Gaf een bron voor geen enkel bevraagd verblijfsobject antwoord, dan volgt een
    BronFoutError. Lukte de verwerking van geen enkele batch, dan bereikt de
    oorspronkelijke fout de aanroeper.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): Lijst met verblijfsobject ID's

    Returns:
        Tuple[pd.DataFrame, Bronuitval]: Eén rij per verblijfsobject met een
            monumentstatus, met de kolommen uit ``_RESULTAAT_KOLOMMEN``; en per bron
            (ook "verwerking" en de provincies) de verblijfsobjecten waarvoor zij,
            ook na retries, splitsen en de herkansing, geen antwoord gaf
    """
    beschermde_gezichten_df = await _get_beschermde_gezichten(session)
    uitval = Bronuitval()

    # Eerst alle adrespunten ophalen: de BRK-PB heeft ze nodig, en de batches worden
    # erop gesorteerd. Zonder de BAG valt er verder niets te bepalen.
    with tqdm_asyncio(
        total=len(verblijfsobject_ids),
        desc="Adrespunten (BAG)",
        disable=len(verblijfsobject_ids) < _BAG_VOORTGANG_VANAF,
    ) as bag_voortgang:
        adrespunten = await _query_adrespunten(
            session, verblijfsobject_ids, uitval, voortgang=bag_voortgang.update
        )
    uitval.controleer()

    # Verblijfsobjecten waarvoor de BAG geen antwoord gaf, krijgen een herkansing en
    # anders pd.NA; alleen de rest staat echt niet in de BAG.
    bag_mislukt = uitval.mislukt.get(BAG, set())
    zonder_adrespunt = sum(
        1 for i in verblijfsobject_ids if i not in adrespunten and i not in bag_mislukt
    )
    if zonder_adrespunt:
        logger.warning(
            "%d van %d verblijfsobjecten hebben geen adrespunt uit de BAG; die krijgen "
            "geen Kadaster-beperking, geen beschermd gezicht en geen provinciaal monument",
            zonder_adrespunt,
            len(verblijfsobject_ids),
        )

    batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)
    # De traceback van een onverwachte fout één keer per fouttype loggen, niet voor
    # elke batch opnieuw
    gelogde_fouttypes: Set[type] = set()

    async def _verwerk_batch(batch: List[str]) -> Optional[_BatchFrames]:
        # Ligt een bron plat, dan kan de aanroep niet meer slagen: bevraag ook de
        # andere bronnen niet meer. Hetzelfde geldt als de verwerking kansloos is.
        # uitval.controleer() geeft daarna de fout.
        if uitval.sla_over_als_bron_plat_ligt(
            batch
        ) or uitval.sla_over_als_verwerking_kansloos(batch):
            return None
        try:
            result = await _process_batch(
                session, batch, beschermde_gezichten_df, adrespunten, uitval
            )
        except Exception as fout:
            # Niet stil overslaan: dan kreeg de batch overal False. Uitval van een
            # bron loopt via uitval; dit is een onverwachte fout.
            logger.warning(
                "Verwerking van een batch van %d verblijfsobjecten mislukt (%s: %s); "
                "hun statussen worden onbepaald",
                len(batch),
                type(fout).__name__,
                fout,
                exc_info=type(fout) not in gelogde_fouttypes,
            )
            gelogde_fouttypes.add(type(fout))
            uitval.registreer_mislukte_verwerking(batch, fout)
            return None
        uitval.registreer_antwoord(VERWERKING, len(batch))
        return result

    async def _verwerk(batches: List[List[str]], fase: str) -> List[_BatchFrames]:
        resultaten: List[_BatchFrames] = []
        onvolledig = 0

        async def _run_batch(batch: List[str]) -> None:
            nonlocal onvolledig
            async with batch_semaphore:
                result = await _verwerk_batch(batch)
            if result is not None:
                resultaten.append(result)
            # Ook een mislukte of overgeslagen batch is afgehandeld: de balk loopt vol,
            # en ernaast staat hoeveel verblijfsobjecten een onvolledig resultaat hebben
            onvolledig += uitval.aantal_onvolledig(batch)
            voortgang.update(len(batch))
            voortgang.set_postfix(onvolledig=onvolledig, refresh=False)

        with tqdm_asyncio(
            total=sum(len(batch) for batch in batches),
            desc=fase,
            disable=len(batches) <= 1,
        ) as voortgang:
            # _verwerk_batch vangt elke gewone fout op; alleen bijvoorbeeld een
            # KeyboardInterrupt breekt de TaskGroup af, en die geeft zij onverpakt door.
            async with asyncio.TaskGroup() as tg:
                for batch in batches:
                    tg.create_task(_run_batch(batch))
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

    results_list = await _verwerk(
        _in_batches(_sorteer_ruimtelijk(verblijfsobject_ids, adrespunten)),
        "Monumentstatus",
    )
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
            bag_mislukt = uitval.mislukt.get(BAG, set())
            zonder_bag_antwoord = [i for i in opnieuw if i in bag_mislukt]
            uitval.vergeet(opnieuw)
            adrespunten.update(
                await _query_adrespunten(session, zonder_bag_antwoord, uitval)
            )
            herkansing = await _verwerk(
                _in_batches(_sorteer_ruimtelijk(opnieuw, adrespunten)), "Herkansing"
            )
            # Eén set voor alle batches: bij miljoenen verblijfsobjecten telt dat
            opnieuw_set = set(opnieuw)
            results_list = [_zonder(r, opnieuw_set) for r in results_list] + herkansing
            uitval.controleer()

    # Per monumenttype de frames van alle batches samenvoegen
    lege_rm, lege_bg, lege_gm = _lege_batch_frames()
    rijksmonumenten_result = _voeg_frames_samen([r[0] for r in results_list], lege_rm)
    verblijfsobjecten_in_beschermd_gezicht_result = _voeg_frames_samen(
        [r[1] for r in results_list], lege_bg
    )
    gemeentelijke_monumenten_result = _voeg_frames_samen(
        [r[2] for r in results_list], lege_gm
    )

    # Uit alle adrespunten en niet per batch, na de herkansing: dan zijn ook de
    # adrespunten bekend die de BAG pas in de herkansing gaf
    provinciale_monumenten_result = await _bepaal_provinciale_monumenten(
        session, adrespunten, uitval
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
        return pd.DataFrame(columns=_RESULTAAT_KOLOMMEN), uitval

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

    return result, uitval
