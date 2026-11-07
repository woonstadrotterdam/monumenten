"""Client voor monumenten package."""

from __future__ import annotations

import functools
import operator
import warnings
from typing import Any, Dict, FrozenSet, List, Optional, Protocol, Union, cast

import aiohttp
import numpy as np
import pandas as pd

from monumenten._api._provincies import PROVINCIES
from monumenten._bronfouten import BAG, BRK_PB, RCE, VERWERKING
from monumenten._processing import _query

# Per status de bronnen waar zij van afhangt. Is een status niet gevonden en gaf een
# van die bronnen voor het verblijfsobject geen antwoord, dan is zij pd.NA in plaats
# van False. Het adrespunt uit de BAG is nodig voor de BRK-PB, het beschermd gezicht
# en het provinciaal monument. Het beschermd gezicht en het provinciaal monument
# worden na alle batches uit de adrespunten bepaald en hangen dus niet van de
# verwerking van een batch af. De tabel
# "Hangt af van" in de README beschrijft hetzelfde; een test houdt ze gelijk.
_AFHANKELIJKHEDEN: Dict[str, FrozenSet[str]] = {
    "rijksmonument": frozenset({RCE, BAG, BRK_PB, VERWERKING}),
    "rijksmonument_voorbescherming": frozenset({BAG, BRK_PB, VERWERKING}),
    "rijksbeschermd_gezicht": frozenset({BAG}),
    "gemeentelijk_monument": frozenset({BAG, BRK_PB, VERWERKING}),
    "provinciaal_monument": frozenset({BAG, *(p.naam for p in PROVINCIES)}),
}


def _status(aanwezig: pd.Series[bool], onbepaald: pd.Series[bool]) -> pd.Series[bool]:
    """Bepaal een monumentstatus als nullable boolean.

    Args:
        aanwezig (pd.Series[bool]): Of de status bij een bron is gevonden
        onbepaald (pd.Series[bool]): Of een bron waar de status van afhangt geen
            antwoord gaf

    Returns:
        pd.Series[bool]: ``True`` als de status is gevonden, ``pd.NA`` als zij niet is
            gevonden maar een benodigde bron geen antwoord gaf, anders ``False``
    """
    return aanwezig.astype("boolean").mask(onbepaald & ~aanwezig)


class _StatusRij(Protocol):
    """De velden van een resultaatrij die de VERA-omzetting leest.

    Een rij uit ``itertuples`` voldoet hieraan, net als een ``pd.Series``. Een
    ontbrekende of verkeerd gespelde kolom vangt mypy hiermee in
    ``_naar_referentiedata``.
    """

    rijksmonument: Optional[bool]
    rijksmonument_bron: Optional[List[str]]
    rijksbeschermd_gezicht: Optional[bool]
    gemeentelijk_monument: Optional[bool]
    provinciaal_monument: Optional[bool]


class MonumentenClient:
    """Client voor het ophalen van monumentgegevens van verschillende Nederlandse overheids-API's.

    Args:
        session (Optional[aiohttp.ClientSession]): Optionele aiohttp.ClientSession. Indien niet opgegeven wordt
                een nieuwe sessie aangemaakt en beheerd door de client.
    """

    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> "MonumentenClient":
        if self._owns_session:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        if self._owns_session and self._session:
            await self._session.close()

    def _naar_referentiedata(
        self, row: _StatusRij
    ) -> Optional[List[Dict[str, object]]]:
        # Met een niet-bepaalde status zou de lijst onvolledig zijn
        if None in (
            row.rijksmonument,
            row.rijksbeschermd_gezicht,
            row.gemeentelijk_monument,
            row.provinciaal_monument,
        ):
            return None
        statuses: List[Dict[str, object]] = []
        if row.rijksmonument:
            statuses.append(
                {
                    "code": "RIJ",
                    "naam": "Rijksmonument",
                    "bron": row.rijksmonument_bron,
                }
            )
        if row.rijksbeschermd_gezicht:
            statuses.append({"code": "SGR", "naam": "Rijksbeschermd stadsgezicht"})
        if row.gemeentelijk_monument:
            statuses.append({"code": "GEM", "naam": "Gemeentelijk monument"})
        if row.provinciaal_monument:
            statuses.append({"code": "PRO", "naam": "Provinciaal monument"})
        return statuses

    async def process_from_df(
        self,
        df: pd.DataFrame,
        verblijfsobject_id_col: str,
    ) -> pd.DataFrame:
        """Verwerk een DataFrame met verblijfsobject ID's.

        Args:
            df (pd.DataFrame): Input DataFrame met verblijfsobject ID's
            verblijfsobject_id_col (str): Naam van de kolom met de verblijfsobject ID's

        Returns:
            pd.DataFrame: DataFrame met toegevoegde monumentinformatie. De kolommen
                ``rijksmonument``, ``rijksmonument_voorbescherming``,
                ``rijksbeschermd_gezicht``, ``gemeentelijk_monument`` en
                ``provinciaal_monument`` zijn nullable booleans: ``pd.NA`` betekent
                dat de status niet bepaald kon worden omdat een bron geen antwoord
                gaf, de verwerking van een batch onverwacht mislukte of de monumenten
                van de provincie niet opgehaald konden worden. In dat geval volgt ook
                één ``MonumentStatusOnbepaaldWarning`` met per bron de betrokken ID's.
                Een verblijfsobject dat niet in de BAG van PDOK staat (meestal
                "Niet gerealiseerd verblijfsobject") krijgt ``False`` voor de
                statussen die het adrespunt nodig hebben; die ID's noemt één
                ``GeenAdrespuntWarning``.

        Raises:
            RuntimeError: Als de client niet als context manager wordt gebruikt
            ValueError: Als er geen enkel geldig verblijfsobject ID is
            BronFoutError: Als een bron voor geen enkel verblijfsobject antwoord gaf
        """

        if not self._session:
            raise RuntimeError("Client must be used as a context manager")

        # verblijfsobject_id's moeten 16 cijfers lang zijn, en cijfers 5 en 6 moeten '01', '02' of '03' zijn
        ids = df[verblijfsobject_id_col]
        invalid_verblijf_object_ids = (
            (ids.str.len() != 16)
            | (~ids.str.isdigit())
            | (~ids.str.slice(4, 6).isin(["01", "02", "03"]))
        )
        if invalid_verblijf_object_ids.any():
            invalid_ids = ids[invalid_verblijf_object_ids].drop_duplicates().tolist()
            warnings.warn(
                f"{len(invalid_ids)} onjuiste verblijfsobject ID's gevonden: {invalid_ids}"
            )

        valid_id_df = df.loc[~invalid_verblijf_object_ids]
        if valid_id_df.empty:
            raise ValueError("Geen enkel geldig verblijfsobject ID gevonden")
        results, uitval = await _query(
            self._session,
            valid_id_df.loc[:, verblijfsobject_id_col].drop_duplicates().tolist(),
        )
        for waarschuwing in (
            uitval.waarschuwing(),
            uitval.waarschuwing_zonder_adrespunt(),
        ):
            if waarschuwing is not None:
                warnings.warn(waarschuwing, stacklevel=2)
        merged = pd.merge(
            valid_id_df,
            results,
            left_on=verblijfsobject_id_col,
            right_on="identificatie",
            how="left",
        )

        if "identificatie" not in df.columns:
            merged = merged.drop(columns=["identificatie"])

        # Eén masker per bron, en niet per status een nieuwe unie: bij een uitval
        # voor miljoenen verblijfsobjecten telt dat
        zonder_antwoord = {
            bron: merged[verblijfsobject_id_col].isin(ids)
            for bron, ids in uitval.mislukt.items()
        }
        geen_uitval = pd.Series(False, index=merged.index)

        def onbepaald(status: str) -> pd.Series[bool]:
            """Of een bron waar de status van afhangt geen antwoord gaf."""
            maskers = [
                zonder_antwoord[bron]
                for bron in _AFHANKELIJKHEDEN[status]
                if bron in zonder_antwoord
            ]
            return functools.reduce(operator.or_, maskers, geen_uitval)

        rijksmonument_nummer_position = merged.columns.get_loc("rijksmonument_nummer")

        if not isinstance(rijksmonument_nummer_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'rijksmonument_nummer' niet bepalen"
            )

        merged.insert(
            rijksmonument_nummer_position + 1,
            "rijksmonument_url",
            (
                "https://monumentenregister.cultureelerfgoed.nl/monumenten/"
                + merged["rijksmonument_nummer"].fillna("").astype(str)
            ).where(
                merged["rijksmonument_nummer"].notna(),
                np.nan,
            ),
        )

        merged.insert(
            rijksmonument_nummer_position,
            "rijksmonument",
            _status(merged["rijksmonument_bron"].notna(), onbepaald("rijksmonument")),
        )

        # verplaats rijksmonument_bron naar rijksmonument_nummer_position + 1
        columns = merged.columns.tolist()
        rijksmonument_bron_index = columns.index("rijksmonument_bron")
        columns.pop(rijksmonument_bron_index)
        columns.insert(rijksmonument_nummer_position + 1, "rijksmonument_bron")
        merged = merged[columns]

        # voorbescherming (Kadaster EWD): geen rijksmonument, wel apart zichtbaar
        merged["rijksmonument_voorbescherming"] = _status(
            merged["rijksmonument_voorbescherming"].eq(True),
            onbepaald("rijksmonument_voorbescherming"),
        )

        rijksbeschermd_gezicht_naam_position = merged.columns.get_loc(
            "rijksbeschermd_gezicht_naam"
        )

        if not isinstance(rijksbeschermd_gezicht_naam_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'rijksbeschermd_gezicht_naam' niet bepalen"
            )

        merged.insert(
            rijksbeschermd_gezicht_naam_position,
            "rijksbeschermd_gezicht",
            _status(
                merged["rijksbeschermd_gezicht_naam"].notna(),
                onbepaald("rijksbeschermd_gezicht"),
            ),
        )

        gemeentelijk_monument_position = merged.columns.get_loc(
            "grondslag_gemeentelijk_monument",
        )

        if not isinstance(gemeentelijk_monument_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'grondslag_gemeentelijk_monument' niet bepalen"
            )

        merged.insert(
            gemeentelijk_monument_position,
            "gemeentelijk_monument",
            _status(
                merged["grondslag_gemeentelijk_monument"].notna(),
                onbepaald("gemeentelijk_monument"),
            ),
        )

        provinciaal_monument_position = merged.columns.get_loc(
            "provinciaal_monument_omschrijving",
        )

        if not isinstance(provinciaal_monument_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'provinciaal_monument_omschrijving' niet bepalen"
            )

        merged.insert(
            provinciaal_monument_position,
            "provinciaal_monument",
            _status(
                merged["provinciaal_monument_omschrijving"].notna(),
                onbepaald("provinciaal_monument"),
            ),
        )

        merged = merged.replace([np.nan, ""], pd.NA)
        merged = merged.replace({None: pd.NA})
        return merged

    async def process_from_list(
        self, verblijfsobject_ids: List[str], to_vera: bool = False
    ) -> Union[Dict[str, Optional[List[Dict[str, str]]]], Dict[str, Dict[str, Any]]]:
        """Verwerk een lijst met verblijfsobject ID's.

        Args:
            verblijfsobject_ids (List[str]): Lijst met te verwerken ID's
            to_vera (bool): Of de output in VERA-referentiedataformaat moet zijn. Standaard is False.

        Returns:
            Union[Dict[str, Optional[List[Dict[str, str]]]], Dict[str, Dict[str, Any]]]: Dictionary
                met verblijfsobject ID's als keys en lijst van monumentstatussen als values. Een
                status die niet bepaald kon worden omdat een bron geen antwoord gaf is ``None``;
                in VERA-referentiedataformaat is dan de hele value ``None`` in plaats van een lijst.
        """
        df = pd.DataFrame(
            {"bag_verblijfsobject_id": verblijfsobject_ids}
        ).drop_duplicates()

        result = await self.process_from_df(df, "bag_verblijfsobject_id")

        result = result.replace({pd.NA: None, pd.NaT: None, np.nan: None})

        if "rijksmonument_bron" in result.columns:
            result["rijksmonument_bron"] = result["rijksmonument_bron"].apply(
                lambda x: x.split(", ") if pd.notna(x) else None
            )
        # Ensure unique index before converting to dictionary
        result_indexed = result.set_index("bag_verblijfsobject_id")
        if result_indexed.index.duplicated().any():
            # If there are duplicates, keep the first occurrence
            result_indexed = result_indexed[
                ~result_indexed.index.duplicated(keep="first")
            ]

        if not to_vera:
            return cast(
                Dict[str, Dict[str, Any]],
                result_indexed.to_dict(orient="index"),
            )

        # itertuples maakt per rij een namedtuple in plaats van een pd.Series,
        # wat bij miljoenen rijen vele malen sneller is. _naar_referentiedata
        # leest de rij via attributen, dus een namedtuple werkt hetzelfde.
        return cast(
            Dict[str, Optional[List[Dict[str, str]]]],
            {
                row.Index: self._naar_referentiedata(cast(_StatusRij, row))
                for row in result_indexed.itertuples(name="Rij")
            },
        )
