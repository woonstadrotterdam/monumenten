"""Melden voor welke verblijfsobjecten een bron geen antwoord gaf."""

from __future__ import annotations

from typing import Dict, List, Optional


class MonumentStatusOnbepaaldWarning(UserWarning):
    """Een bron gaf voor een deel van de verblijfsobjecten geen antwoord.

    De statussen die van die bron afhangen zijn voor deze verblijfsobjecten
    ``pd.NA`` in plaats van ``False``.

    Args:
        melding (str): Omschrijving van de waarschuwing
        verblijfsobject_ids (Optional[Dict[str, List[str]]]): Per bron (zoals
            "Noord-Holland" of "Drenthe") de verblijfsobject ID's zonder antwoord
    """

    def __init__(
        self,
        melding: str,
        verblijfsobject_ids: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        super().__init__(melding)
        self.verblijfsobject_ids = verblijfsobject_ids or {}
