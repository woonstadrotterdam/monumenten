"""Client voor monumenten package."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, cast

import aiohttp
import numpy as np
import pandas as pd

from monumenten._processing import _query


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

    def _naar_referentiedata(self, row: pd.Series[bool]) -> List[Dict[str, object]]:
        statuses = []
        if row.is_rijksmonument:
            statuses.append(
                {
                    "code": "RIJ",
                    "naam": "Rijksmonument",
                    "bron": row.rijksmonument_bron,
                }
            )
        if row.is_beschermd_gezicht:
            statuses.append({"code": "SGR", "naam": "Rijksbeschermd stadsgezicht"})
        if row.is_gemeentelijk_monument:
            statuses.append({"code": "GEM", "naam": "Gemeentelijk monument"})
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
            pd.DataFrame: DataFrame met toegevoegde monumentinformatie

        Raises:
            RuntimeError: Als de client niet als context manager wordt gebruikt
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
            raise ValueError(
                f"Onjuiste verblijfsobject ID's gevonden, bijvoorbeeld: '{df[invalid_verblijf_object_ids].iloc[0, 0]}'"
            )

        results = await _query(
            self._session, df[verblijfsobject_id_col].drop_duplicates().tolist()
        )
        merged = pd.merge(
            df,
            results,
            left_on=verblijfsobject_id_col,
            right_on="identificatie",
            how="left",
        )

        if "identificatie" not in df.columns:
            merged = merged.drop(columns=["identificatie"])

        rijksmonument_nummer_position = merged.columns.get_loc("rijksmonument_nummer")

        if not isinstance(rijksmonument_nummer_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'rijksmonument_nummer' niet bepalen"
            )

        merged.insert(
            rijksmonument_nummer_position + 1,
            "rijksmonument_url",
            "https://monumentenregister.cultureelerfgoed.nl/monumenten/"
            + merged["rijksmonument_nummer"]
            .fillna("")
            .astype(str)
            .where(
                merged["rijksmonument_nummer"].notna(),
                pd.NA,
            ),
        )

        merged.insert(
            rijksmonument_nummer_position,
            "is_rijksmonument",
            merged["rijksmonument_bron"].notna(),
        )

        # verplaats rijksmonument_bron naar rijksmonument_nummer_position + 1
        columns = merged.columns.tolist()
        rijksmonument_bron_index = columns.index("rijksmonument_bron")
        columns.pop(rijksmonument_bron_index)
        columns.insert(rijksmonument_nummer_position + 1, "rijksmonument_bron")
        merged = merged[columns]

        beschermd_gezicht_naam_position = merged.columns.get_loc(
            "beschermd_gezicht_naam"
        )

        if not isinstance(beschermd_gezicht_naam_position, int):
            raise RuntimeError(
                "Interne fout: Kan kolomnummer voor 'beschermd_gezicht_naam' niet bepalen"
            )

        merged.insert(
            beschermd_gezicht_naam_position,
            "is_beschermd_gezicht",
            merged["beschermd_gezicht_naam"].notna(),
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
            "is_gemeentelijk_monument",
            merged["grondslag_gemeentelijk_monument"].notna(),
        )

        merged = merged.replace([np.nan, ""], pd.NA)
        merged = merged.replace({None: pd.NA})
        return merged

    async def process_from_list(
        self, verblijfsobject_ids: List[str], to_vera: bool = False
    ) -> Union[Dict[str, List[Dict[str, str]]], Dict[str, Dict[str, Any]]]:
        """Verwerk een lijst met verblijfsobject ID's.

        Args:
            verblijfsobject_ids (List[str]): Lijst met te verwerken ID's
            to_vera (bool): Of de output in VERA-referentiedataformaat moet zijn. Standaard is False.

        Returns:
            Union[Dict[str, List[Dict[str, str]]], Dict[str, Dict[str, Any]]]: Dictionary met verblijfsobject ID's als keys en lijst van monumentstatussen als values
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

        if not to_vera:
            return cast(
                Dict[str, Dict[str, Any]],
                result.set_index("bag_verblijfsobject_id").to_dict(orient="index"),
            )

        return (
            result.set_index("bag_verblijfsobject_id")
            .apply(self._naar_referentiedata, axis=1)
            .to_dict()
        )
