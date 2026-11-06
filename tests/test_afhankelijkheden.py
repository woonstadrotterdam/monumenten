"""De tabel "Hangt af van" in de README volgt _AFHANKELIJKHEDEN in de client."""

from pathlib import Path

from monumenten._api._provincies import PROVINCIES
from monumenten._bronfouten import BAG, BATCHBRONNEN, BRK_PB, RCE, VERWERKING
from monumenten.client import _AFHANKELIJKHEDEN

_README = Path(__file__).parent.parent / "README.md"

# Eerste woord van een bron in de tabel -> de bronnen die het beschrijft
_BRONNEN_PER_WOORD = {
    "RCE": {RCE},
    "BAG": {BAG},
    "BRK-PB": {BRK_PB},
    "verwerking": {VERWERKING},
    "provincie": {provincie.naam for provincie in PROVINCIES},
}


def _cellen(regel: str) -> list[str]:
    return [cel.strip() for cel in regel.strip().strip("|").split("|")]


def _readme_tabel() -> dict[str, set[str]]:
    """Geef per status in de tabel de bronnen waar zij van afhangt."""
    regels = _README.read_text().splitlines()
    start = next(
        i
        for i, regel in enumerate(regels)
        if regel.startswith("| Status") and "Hangt af van" in regel
    )
    tabel = {}
    for regel in regels[start + 2 :]:
        if not regel.startswith("|"):
            break
        status, bronnen = _cellen(regel)
        tabel[status.strip("`")] = set().union(
            *(_BRONNEN_PER_WOORD[bron.split()[0]] for bron in bronnen.split(", "))
        )
    return tabel


def test_readme_tabel_volgt_afhankelijkheden():
    """Elke status in de tabel hangt af van precies de bronnen in
    _AFHANKELIJKHEDEN."""
    assert _readme_tabel() == _AFHANKELIJKHEDEN


def test_elke_bron_in_afhankelijkheden_is_bekend():
    """Een typfout in een bronnaam zou een status stil nooit onbepaald maken."""
    bekend = BATCHBRONNEN | {provincie.naam for provincie in PROVINCIES}
    for status, bronnen in _AFHANKELIJKHEDEN.items():
        assert bronnen <= bekend, status
