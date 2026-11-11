"""Interne processing functies voor de monumenten package."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Dict, List, Optional, Set, Tuple

import aiohttp
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from aiocache import cached_stampede
from shapely.geometry.base import BaseGeometry
from tqdm.asyncio import tqdm_asyncio

from monumenten._api._cultureel_erfgoed import (
    _query_beschermde_gezichten,
    _query_rijksmonumenten,
    _query_rijksmonumenten_via_aktes,
)
from monumenten._api._kadaster import (
    _GEMEENTELIJK_GRONDSLAGCODES,
    _RD,
    _RELEVANTE_GRONDSLAGCODES,
    _RIJKSMONUMENT_GRONDSLAGCODES,
    _VOORBESCHERMING_GRONDSLAGCODES,
    _Adrespunten,
    _in_groepen,
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
from monumenten._bronfouten import BAG, BRK_PB, RCE, VERWERKING, Bronuitval

logger = logging.getLogger("monumenten.processing")

_QUERY_BATCH_GROOTTE = 500  # lijkt meest optimaal qua performance
# Vanaf zoveel verblijfsobjecten toont de BAG-fase een voortgangsbalk
_BAG_VOORTGANG_VANAF = 10_000
_BATCH_CONCURRENCY = 4
# Bereik van RD-coördinaten in Nederland: minx, miny, maxx, maxy
_RD_BEREIK = (0, 300_000, 300_000, 650_000)

_WGS84 = "EPSG:4326"

# Marge om de provinciegrens. Enkele Drentse monumentvlakken steken tot 1,3 m
# buiten de grens van PDOK, door verschillen in nauwkeurigheid; de grens wordt
# bovendien maar jaarlijks bijgewerkt. Een te ruime marge kost alleen wat extra
# kandidaten, een te krappe geeft stil False.
_GRENS_BUFFER_METER = 100


_RCE_KOLOMMEN = ["identificatie", "rijksmonument_nummer"]
_BEPERKING_KOLOMMEN = [
    "identificatie",
    "grondslagcode",
    "grondslag_gemeentelijk_monument",
    "akte",
]
_AKTE_NUMMER_KOLOMMEN = ["akte", "rijksmonument_nummer"]

# De antwoorden van de RCE en de BRK-PB voor een batch. De statussen worden pas na
# de herkansing afgeleid, uit de antwoorden van alle batches: zo hoeft de herkansing
# alleen de bron opnieuw te bevragen die geen antwoord gaf.
_BatchAntwoorden = Tuple[pd.DataFrame, pd.DataFrame]

_RESULTAAT_KOLOMMEN = [
    "identificatie",
    "rijksmonument_nummer",
    "rijksmonument_bron",
    "rijksmonument_voorbescherming",
    "rijksbeschermd_gezicht_naam",
    "grondslag_gemeentelijk_monument",
    "provinciaal_monument_omschrijving",
]


def _rce_frame(rijksmonumenten: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rijksmonumenten, columns=_RCE_KOLOMMEN, dtype="string")


def _beperkingen_frame(beperkingen: List[Dict[str, Any]]) -> pd.DataFrame:
    # De akte als object, ook als geen enkele beperking er een heeft: anders wordt de
    # kolom float en lukt het koppelen aan de nummers per akte niet
    beperkingen_df = pd.DataFrame(beperkingen, columns=_BEPERKING_KOLOMMEN).astype(
        {"identificatie": "string", "akte": "object"}
    )
    # Andere beperkingen vallen per batch al weg, zodat bij miljoenen
    # verblijfsobjecten alleen de rijen bewaard blijven waar een status van afhangt
    return beperkingen_df[
        beperkingen_df["grondslagcode"].isin(_RELEVANTE_GRONDSLAGCODES)
    ].reset_index(drop=True)


def _akte_nummers_frame(akte_nummers: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(akte_nummers, columns=_AKTE_NUMMER_KOLOMMEN, dtype="string")


def _lege_provinciale_monumenten() -> pd.DataFrame:
    """Leeg resultaat van _bepaal_provinciale_monumenten."""
    return pd.DataFrame(columns=["identificatie", "provinciaal_monument_omschrijving"])


async def _process_batch(
    session: aiohttp.ClientSession,
    rce_ids: List[str],
    brk_pb_adrespunten: _Adrespunten,
    uitval: Bronuitval,
) -> _BatchAntwoorden:
    """Bevraag de RCE en de BRK-PB voor een batch verblijfsobjecten.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        rce_ids (List[str]): De verblijfsobject ID's voor de RCE
        brk_pb_adrespunten (_Adrespunten): De adrespunten voor de BRK-PB
        uitval (Bronuitval): Hierin leggen de bronnen vast voor welke
            verblijfsobjecten zij wel en geen antwoord gaven

    Returns:
        _BatchAntwoorden: De rijksmonumenten van de RCE en de beperkingen van de
            BRK-PB met een grondslagcode waar een status van afhangt
    """
    rijksmonumenten_taak = asyncio.create_task(
        _query_rijksmonumenten(session, rce_ids, uitval)
    )
    beperkingen_taak = asyncio.create_task(
        _query_beperkingen(session, brk_pb_adrespunten, uitval)
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
    return _rce_frame(rijksmonumenten), _beperkingen_frame(beperkingen)


def _bepaal_rijks_en_gemeentelijke_monumenten(
    rce_df: pd.DataFrame, beperkingen_df: pd.DataFrame, akte_nummers_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Leid de rijks- en gemeentelijke monumenten af uit de antwoorden.

    Een rijksmonument dat alleen het Kadaster vindt, krijgt het nummer via zijn akte
    (zie _nummers_via_akte). De bron blijft dan Kadaster.

    Args:
        rce_df (pd.DataFrame): De rijksmonumenten van de RCE
        beperkingen_df (pd.DataFrame): De beperkingen van de BRK-PB
        akte_nummers_df (pd.DataFrame): Per akte de rijksmonumentnummers die de RCE
            bij die akte vond

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: De rijksmonumenten (met bron en voorbescherming)
            en de gemeentelijke monumenten
    """
    rijksmonumenten_df = pd.merge(
        rce_df,
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

    alleen_kadaster = rijksmonumenten_df["rijksmonument_bron"] == "Kadaster"
    rijksmonumenten_df.loc[alleen_kadaster, "rijksmonument_nummer"] = (
        rijksmonumenten_df.loc[alleen_kadaster, "identificatie"]
        .map(_nummers_via_akte(beperkingen_df, akte_nummers_df))
        .astype("string")
    )

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

    gemeentelijke_monumenten_df = beperkingen_df[
        beperkingen_df["grondslagcode"].isin(_GEMEENTELIJK_GRONDSLAGCODES)
    ][["identificatie", "grondslag_gemeentelijk_monument"]]

    return rijksmonumenten_df, gemeentelijke_monumenten_df


def _nummers_via_akte(
    beperkingen_df: pd.DataFrame, akte_nummers_df: pd.DataFrame
) -> pd.Series[str]:
    """Bepaal per verblijfsobject het rijksmonumentnummer via de aktes van zijn
    rijksmonument-beperkingen.

    Alleen een eenduidig nummer telt: elke akte van het verblijfsobject moet bij de
    RCE gevonden zijn, en samen bij precies één monument horen. Meerdere aktes op één
    adrespunt horen in de praktijk bij verschillende monumenten op hetzelfde perceel;
    welk daarvan het verblijfsobject is, weet het Kadaster niet.

    Args:
        beperkingen_df (pd.DataFrame): De beperkingen van de BRK-PB
        akte_nummers_df (pd.DataFrame): Per akte de rijksmonumentnummers die de RCE
            bij die akte vond

    Returns:
        pd.Series[str]: Het nummer per identificatie, alleen voor verblijfsobjecten
            met een eenduidig nummer
    """
    aktes = beperkingen_df[
        beperkingen_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
    ][["identificatie", "akte"]].drop_duplicates()
    # Een akte zonder nummer bij de RCE geeft na de left merge een leeg nummer
    gekoppeld = aktes.merge(
        akte_nummers_df.astype({"akte": "object"}), on="akte", how="left"
    ).groupby("identificatie")["rijksmonument_nummer"]
    eenduidig = (gekoppeld.count() == gekoppeld.size()) & (gekoppeld.nunique() == 1)
    return gekoppeld.first()[eenduidig]


def _bepaal_beschermde_gezichten(
    adrespunten: _Adrespunten, beschermde_gezichten_df: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Bepaal welke adrespunten in een rijksbeschermd gezicht liggen.

    Args:
        adrespunten (_Adrespunten): Adrespunt (RD) per verblijfsobject ID uit de BAG
        beschermde_gezichten_df (gpd.GeoDataFrame): De beschermde gezichten (RD)

    Returns:
        pd.DataFrame: Eén rij per verblijfsobject en gezicht waarin het ligt
    """
    if not adrespunten:
        return pd.DataFrame(columns=["identificatie", "rijksbeschermd_gezicht_naam"])
    xs, ys = zip(*adrespunten.values())
    punten = gpd.GeoDataFrame(
        {"identificatie": pd.Series(list(adrespunten), dtype="string")},
        geometry=gpd.points_from_xy(xs, ys),
        crs=_RD,
    )
    in_gezicht: pd.DataFrame = gpd.sjoin(
        punten, beschermde_gezichten_df, how="inner", predicate="within"
    )
    return in_gezicht[["identificatie", "rijksbeschermd_gezicht_naam"]].reset_index(
        drop=True
    )


@cached_stampede(ttl=60 * 60 * 24 * 7, noself=True)  # Cache resultaat voor 7 dagen
async def _get_beschermde_gezichten(
    session: aiohttp.ClientSession,
) -> gpd.GeoDataFrame:
    """Haal beschermde gezichten op."""
    beschermde_gezichten = await _query_beschermde_gezichten(session)
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
) -> pd.DataFrame:
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
        pd.DataFrame: De gevonden monumenten (kolommen ``identificatie`` en
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

    delen: List[pd.DataFrame] = []
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


def _voeg_namen_samen(df: pd.DataFrame, kolom: str) -> pd.DataFrame:
    """Voeg per identificatie de unieke waarden van ``kolom`` samen met ", ".

    Args:
        df (pd.DataFrame): Rijen met kolommen ``identificatie`` en ``kolom``; een
            lege waarde telt niet mee
        kolom (str): Naam van de kolom die wordt samengevoegd

    Returns:
        pd.DataFrame: Eén rij per identificatie met een waarde, met de kolommen
            ``identificatie`` en ``kolom``
    """
    return (
        df.dropna(subset=[kolom])
        .drop_duplicates(subset=["identificatie", kolom])
        .groupby("identificatie", sort=False)[kolom]
        .agg(", ".join)
        .reset_index()
    )


def _voeg_frames_samen(frames: List[pd.DataFrame], leeg: pd.DataFrame) -> pd.DataFrame:
    """Voeg de frames van alle batches samen; ``leeg`` als ze allemaal leeg zijn.

    Lege frames doen niet mee: pandas waarschuwt bij het samenvoegen daarvan.
    """
    gevuld = [frame for frame in frames if not frame.empty]
    return pd.concat(gevuld, ignore_index=True) if gevuld else leeg


def _in_batches(verblijfsobject_ids: List[str]) -> List[List[str]]:
    return _in_groepen(verblijfsobject_ids, _QUERY_BATCH_GROOTTE)


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


async def _haal_akte_nummers(
    session: aiohttp.ClientSession,
    rce_df: pd.DataFrame,
    beperkingen_df: pd.DataFrame,
    uitval: Bronuitval,
) -> pd.DataFrame:
    """Haal bij de RCE de nummers op bij de aktes van rijksmonumenten die alleen het
    Kadaster vindt.

    Na alle batches en de herkansing, in één keer: dan is bekend welke
    verblijfsobjecten de RCE zelf een nummer gaf. Heeft de RCE nog een storing, dan
    wordt zij niet opnieuw belast; die verblijfsobjecten houden een leeg nummer.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        rce_df (pd.DataFrame): De rijksmonumenten van de RCE
        beperkingen_df (pd.DataFrame): De beperkingen van de BRK-PB
        uitval (Bronuitval): De uitval van de aanroep, om een storing van de RCE te zien

    Returns:
        pd.DataFrame: Per akte de rijksmonumentnummers die de RCE bij die akte vond
    """
    met_rce_nummer = rce_df.loc[rce_df["rijksmonument_nummer"].notna(), "identificatie"]
    aktes = sorted(
        set(
            beperkingen_df.loc[
                beperkingen_df["grondslagcode"].isin(_RIJKSMONUMENT_GRONDSLAGCODES)
                & ~beperkingen_df["identificatie"].isin(met_rce_nummer)
                & beperkingen_df["akte"].notna(),
                "akte",
            ]
        )
    )
    if not aktes:
        return _akte_nummers_frame([])
    if uitval.in_storing(RCE):
        logger.warning(
            "De RCE heeft een storing: de rijksmonumentnummers bij %d aktes worden "
            "niet opgehaald; rijksmonumenten die alleen het Kadaster vindt, houden "
            "daar een leeg nummer",
            len(aktes),
        )
        return _akte_nummers_frame([])
    return _akte_nummers_frame(await _query_rijksmonumenten_via_aktes(session, aktes))


class _Aanroep:
    """Eén aanroep van _query: de bronnen, de adrespunten en de verwerking per batch.

    Args:
        session (aiohttp.ClientSession): De sessie voor HTTP requests
        verblijfsobject_ids (List[str]): De verblijfsobject ID's van de aanroep
    """

    def __init__(
        self, session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
    ) -> None:
        self.session = session
        self.verblijfsobject_ids = verblijfsobject_ids
        self.uitval = Bronuitval()
        self.adrespunten: _Adrespunten = {}
        self._batch_semaphore = asyncio.Semaphore(_BATCH_CONCURRENCY)
        # De traceback van een onverwachte fout één keer per fouttype loggen, niet
        # voor elke batch opnieuw
        self._gelogde_fouttypes: Set[type] = set()

    async def haal_adrespunten(self) -> None:
        """Haal de adrespunten van alle verblijfsobjecten op bij de BAG.

        De BRK-PB heeft ze nodig, en de batches worden erop gesorteerd.
        """
        with tqdm_asyncio(
            total=len(self.verblijfsobject_ids),
            desc="Adrespunten (BAG)",
            disable=len(self.verblijfsobject_ids) < _BAG_VOORTGANG_VANAF,
        ) as voortgang:
            self.adrespunten.update(
                await _query_adrespunten(
                    self.session,
                    self.verblijfsobject_ids,
                    self.uitval,
                    voortgang=voortgang.update,
                )
            )

    async def verwerk(
        self,
        batches: List[List[str]],
        fase: str,
        rce: Optional[Set[str]] = None,
        brk_pb: Optional[Set[str]] = None,
    ) -> List[_BatchAntwoorden]:
        """Verwerk de batches parallel, met een voortgangsbalk.

        Args:
            batches (List[List[str]]): De batches verblijfsobject ID's
            fase (str): De naam van de fase, voor de voortgangsbalk
            rce (Optional[Set[str]]): Zo nodig de ID's die naar de RCE gaan; anders
                alle
            brk_pb (Optional[Set[str]]): Zo nodig de ID's die naar de BRK-PB gaan;
                anders alle met een adrespunt

        Returns:
            List[_BatchAntwoorden]: De antwoorden van de batches die zijn verwerkt
        """
        antwoorden: List[_BatchAntwoorden] = []
        onvolledig_bepaald = 0

        async def verwerk_batch(batch: List[str]) -> None:
            nonlocal onvolledig_bepaald
            async with self._batch_semaphore:
                antwoord = await self._verwerk_batch(batch, rce, brk_pb)
            if antwoord is not None:
                antwoorden.append(antwoord)
            # Ook een mislukte batch is afgehandeld: de balk loopt vol, en ernaast
            # staat hoeveel verblijfsobjecten onvolledig bepaald zijn
            onvolledig_bepaald += self.uitval.aantal_onvolledig_bepaald(batch)
            voortgang.update(len(batch))
            voortgang.set_postfix(onvolledig_bepaald=onvolledig_bepaald, refresh=False)

        with tqdm_asyncio(
            total=sum(len(batch) for batch in batches),
            desc=fase,
            disable=len(batches) <= 1,
        ) as voortgang:
            # _verwerk_batch vangt elke gewone fout op; alleen bijvoorbeeld een
            # KeyboardInterrupt breekt de TaskGroup af, en die geeft zij onverpakt door.
            async with asyncio.TaskGroup() as tg:
                for batch in batches:
                    tg.create_task(verwerk_batch(batch))
        return antwoorden

    async def _verwerk_batch(
        self, batch: List[str], rce: Optional[Set[str]], brk_pb: Optional[Set[str]]
    ) -> Optional[_BatchAntwoorden]:
        """Bevraag de RCE en de BRK-PB voor één batch; None als de verwerking mislukt."""
        rce_ids = batch if rce is None else [i for i in batch if i in rce]
        brk_pb_adrespunten = {
            i: self.adrespunten[i]
            for i in batch
            if i in self.adrespunten and (brk_pb is None or i in brk_pb)
        }
        try:
            antwoorden = await _process_batch(
                self.session, rce_ids, brk_pb_adrespunten, self.uitval
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
                exc_info=type(fout) not in self._gelogde_fouttypes,
            )
            self._gelogde_fouttypes.add(type(fout))
            self.uitval.registreer_fout(VERWERKING, batch, fout)
            return None
        self.uitval.registreer_antwoord(VERWERKING, len(batch))
        return antwoorden

    async def proefrequest(self, bron: str, verblijfsobject_id: str) -> None:
        """Doe één request voor één verblijfsobject aan een bron met een storing."""
        # Buiten de try: een bron zonder proefrequest is een programmeerfout, en die
        # mag niet als mislukte proefrequest in de log verdwijnen
        proef: Awaitable[object]
        if bron == RCE:
            proef = _query_rijksmonumenten(
                self.session, [verblijfsobject_id], self.uitval
            )
        elif bron == BAG:
            proef = _query_adrespunten(self.session, [verblijfsobject_id], self.uitval)
        elif bron == BRK_PB:
            proef = _query_beperkingen(
                self.session,
                {verblijfsobject_id: self.adrespunten[verblijfsobject_id]},
                self.uitval,
            )
        else:
            raise AssertionError(f"Geen proefrequest voor de bron {bron}")
        try:
            await proef
        except Exception as fout:
            # Bijvoorbeeld een fout bij het koppelen van de beperkingen. De bron gaf
            # dan wel antwoord; het verblijfsobject blijft onbeantwoord en krijgt de
            # herkansing.
            logger.warning(
                "Proefrequest voor %s mislukt (%s: %s)", bron, type(fout).__name__, fout
            )

    async def herkansing(self) -> List[_BatchAntwoorden]:
        """Bevraag voor de verblijfsobjecten zonder antwoord de bron opnieuw.

        Een bron die halverwege haperde geeft aan het einde vaak weer antwoord. Wie
        geen antwoord kreeg, wordt daarom één keer opnieuw bevraagd, en alleen bij de
        bron die geen antwoord gaf: wat de andere bronnen al antwoordden, blijft
        staan. Een mislukte verwerking krijgt beide bronnen opnieuw. Een bron die nog
        een storing heeft krijgt eerst één proefrequest voor één verblijfsobject;
        geeft zij geen antwoord, dan heeft opnieuw bevragen geen zin.

        Returns:
            List[_BatchAntwoorden]: De antwoorden van de herkansing
        """
        uitval = self.uitval
        if not uitval.mislukt:
            return []
        await uitval.wacht_op_proefrequest()
        for bron in uitval.bronnen_in_storing():
            await self.proefrequest(bron, next(iter(uitval.mislukt[bron])))
        herkansbaar = uitval.herkansbaar()
        verwerking = herkansbaar.get(VERWERKING, set())
        bag = herkansbaar.get(BAG, set())
        if bag:
            uitval.vergeet(BAG, bag)
            self.adrespunten.update(
                await _query_adrespunten(
                    self.session,
                    [i for i in self.verblijfsobject_ids if i in bag],
                    uitval,
                )
            )
        rce = herkansbaar.get(RCE, set()) | verwerking
        # Een adrespunt dat de BAG pas nu gaf, gaat ook naar de BRK-PB
        brk_pb = {
            i
            for i in herkansbaar.get(BRK_PB, set()) | verwerking | bag
            if i in self.adrespunten
        }
        # Alleen wissen wat opnieuw wordt bevraagd: de rest zou anders stil False
        # krijgen
        uitval.vergeet(VERWERKING, verwerking)
        uitval.vergeet(RCE, rce)
        uitval.vergeet(BRK_PB, brk_pb)
        opnieuw = [i for i in self.verblijfsobject_ids if i in rce or i in brk_pb]
        if not opnieuw:
            return []
        logger.info(
            "Herkansing voor %d verblijfsobjecten waarvoor een bron geen antwoord gaf",
            len(opnieuw),
        )
        return await self.verwerk(
            _in_batches(_sorteer_ruimtelijk(opnieuw, self.adrespunten)),
            "Herkansing",
            rce=rce,
            brk_pb=brk_pb,
        )


async def _query(
    session: aiohttp.ClientSession, verblijfsobject_ids: List[str]
) -> Tuple[pd.DataFrame, Bronuitval]:
    """Voer queries uit voor een lijst verblijfsobjecten.

    Eerst worden alle adrespunten opgehaald bij de BAG. Daarna worden de batches,
    ruimtelijk gesorteerd, parallel verwerkt: retry, splitsen bij falen en overslaan
    bij een storing gebeuren per bron (zie Bronuitval). Mislukt de verwerking van
    een batch onverwacht, dan worden zijn statussen onbepaald, zodat één fout niet
    het werk van een hele aanroep weggooit. Verblijfsobjecten waarvoor een bron geen
    antwoord gaf krijgen aan het einde één herkansing, alleen bij die bron. Daarna
    worden de statussen in één keer afgeleid uit de antwoorden van alle batches, en
    de beschermde gezichten en provinciale monumenten uit alle adrespunten.

    Gaf een bron, ook na de herkansing, voor geen enkel bevraagd verblijfsobject
    antwoord, dan volgt een BronFoutError. Lukte de verwerking van geen enkele batch,
    dan bereikt de oorspronkelijke fout de aanroeper.

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
    aanroep = _Aanroep(session, verblijfsobject_ids)
    await aanroep.haal_adrespunten()
    antwoorden = await aanroep.verwerk(
        _in_batches(_sorteer_ruimtelijk(verblijfsobject_ids, aanroep.adrespunten)),
        "Monumentstatus",
    )
    antwoorden += await aanroep.herkansing()
    uitval, adrespunten = aanroep.uitval, aanroep.adrespunten
    uitval.controleer()

    # Na de herkansing: pas dan is bekend welke verblijfsobjecten echt niet in de
    # BAG staan. Voor wie de BAG geen antwoord gaf volgt pd.NA en de waarschuwing;
    # wie de BAG niet kent, noemt de client in een GeenAdrespuntWarning.
    bag_mislukt = uitval.mislukt.get(BAG, set())
    uitval.zonder_adrespunt = {
        i for i in verblijfsobject_ids if i not in adrespunten and i not in bag_mislukt
    }
    if uitval.zonder_adrespunt:
        # Ook in de log, voor wie warnings niet opvangt (zoals een pipeline die
        # alleen de log bewaart)
        logger.warning(
            "Voor %d van %d verblijfsobjecten heeft de BAG van PDOK geen adrespunt "
            "teruggegeven; zie GeenAdrespuntWarning",
            len(uitval.zonder_adrespunt),
            len(verblijfsobject_ids),
        )

    rce_df = _voeg_frames_samen([a[0] for a in antwoorden], _rce_frame([]))
    beperkingen_df = _voeg_frames_samen(
        [a[1] for a in antwoorden], _beperkingen_frame([])
    )
    rijksmonumenten, gemeentelijke_monumenten = (
        _bepaal_rijks_en_gemeentelijke_monumenten(
            rce_df,
            beperkingen_df,
            await _haal_akte_nummers(session, rce_df, beperkingen_df, uitval),
        )
    )
    # Uit alle adrespunten en niet per batch, na de herkansing: dan zijn ook de
    # adrespunten bekend die de BAG pas in de herkansing gaf
    beschermde_gezichten = _bepaal_beschermde_gezichten(
        adrespunten, beschermde_gezichten_df
    )
    provinciale_monumenten = await _bepaal_provinciale_monumenten(
        session, adrespunten, uitval
    )
    result = (
        rijksmonumenten.drop_duplicates()
        .merge(
            _voeg_namen_samen(beschermde_gezichten, "rijksbeschermd_gezicht_naam"),
            on="identificatie",
            how="outer",
        )
        .merge(
            gemeentelijke_monumenten.drop_duplicates(), on="identificatie", how="outer"
        )
        .merge(
            _voeg_namen_samen(
                provinciale_monumenten, "provinciaal_monument_omschrijving"
            ),
            on="identificatie",
            how="outer",
        )
    )
    return result[_RESULTAAT_KOLOMMEN], uitval
