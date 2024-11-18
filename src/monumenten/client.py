"""Client voor monumenten package."""

from typing import Any, Dict, List, Optional, cast

import aiohttp
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

        results = await _query(
            self._session, df[verblijfsobject_id_col].drop_duplicates()
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
        merged.insert(
            rijksmonument_nummer_position + 1,
            "rijksmonument_url",
            "https://monumentenregister.cultureelerfgoed.nl/monumenten/"
            + merged["rijksmonument_nummer"]
            .fillna("")
            .astype(str)
            .where(merged["rijksmonument_nummer"].notna(), None),
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

        return merged

    async def process_from_list(
        self, verblijfsobject_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Verwerk een lijst met verblijfsobject ID's.

        Args:
            verblijfsobject_ids (List[str]): Lijst met te verwerken ID's

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary met verblijfsobject ID's als keys en dictionaries met monumentinformatie als values
        """
        df = pd.DataFrame({"bag_verblijfsobject_id": verblijfsobject_ids})
        result = await self.process_from_df(df, "bag_verblijfsobject_id")
        result = result.replace({pd.NA: None, pd.NaT: None})
        return cast(
            Dict[str, Dict[str, Any]],
            result.set_index("bag_verblijfsobject_id").to_dict(orient="index"),
        )
