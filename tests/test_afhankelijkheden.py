"""De tabel "Geen antwoord van" in de README volgt _AFHANKELIJKHEDEN in de client."""

from pathlib import Path

from monumenten._api._provincies import PROVINCIES
from monumenten._bronfouten import BAG, BATCHBRONNEN, BRK_PB, RCE, VERWERKING
from monumenten.client import _AFHANKELIJKHEDEN

_README = Path(__file__).parent.parent / "README.md"

# Eerste woord van een rij in de tabel -> de bronnen die de rij beschrijft
_BRONNEN_PER_RIJ = {
    "RCE": {RCE},
    "BAG": {BAG},
    "BRK-PB": {BRK_PB},
    "verwerking": {VERWERKING},
    "provincie": {provincie.naam for provincie in PROVINCIES},
}


def _cellen(regel: str) -> list[str]:
    return [cel.strip() for cel in regel.strip().strip("|").split("|")]


def _readme_tabel() -> dict[str, dict[str, str]]:
    """Geef per rij van de tabel de cel per status."""
    regels = _README.read_text().splitlines()
    start = next(
        i for i, regel in enumerate(regels) if regel.startswith("| Geen antwoord van")
    )
    statussen = [kop.strip("`") for kop in _cellen(regels[start])[1:]]
    tabel = {}
    for regel in regels[start + 2 :]:
        if not regel.startswith("|"):
            break
        rij, *cellen = _cellen(regel)
        tabel[rij.split()[0]] = dict(zip(statussen, cellen, strict=True))
    return tabel


def test_readme_tabel_volgt_afhankelijkheden():
    """Elke bron in de tabel maakt precies de statussen onbepaald die volgens
    _AFHANKELIJKHEDEN van haar afhangen."""
    tabel = _readme_tabel()

    assert set(tabel) == set(_BRONNEN_PER_RIJ)
    for rij, cellen in tabel.items():
        assert set(cellen) == set(_AFHANKELIJKHEDEN), rij
        for status, cel in cellen.items():
            hangt_af = bool(_BRONNEN_PER_RIJ[rij] & _AFHANKELIJKHEDEN[status])
            assert (cel != "bepaald") == hangt_af, (rij, status)


def test_elke_bron_in_afhankelijkheden_is_bekend():
    """Een typfout in een bronnaam zou een status stil nooit onbepaald maken."""
    bekend = BATCHBRONNEN | {provincie.naam for provincie in PROVINCIES}
    for status, bronnen in _AFHANKELIJKHEDEN.items():
        assert bronnen <= bekend, status
