"""Vergelijk de snelheid van twee versies van de package.

Beide versies krijgen een eigen willekeurige steekproef uit dezelfde pools in
benchmarks/pool, bij elke run opnieuw. De Kadaster-API cachet antwoorden op exact
dezelfde query en versnelt adressen die kort daarvoor zijn opgevraagd; met eigen
steekproeven profiteert geen van beide versies van de andere, en raken eerdere runs
beide versies even hard.

Elke ronde draait in een eigen proces, in de omgeving van de betreffende versie
(meetproces.py). Dit script gebruikt alleen de standaardbibliotheek, plus git en uv.

Gebruik:
    python benchmarks/snelheid.py --basis origin/main --kandidaat HEAD
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import statistics
import subprocess
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, List, Tuple

HIER = Path(__file__).resolve().parent
REPO = HIER.parent
MARKER = "<!-- monumenten-snelheid -->"

# set -> (naam in de tabel, naam in een zin)
SETS = {
    "willekeurig": ("Willekeurig", "willekeurige adressen"),
    "monumentvlag": ("Monumenten", "monumenten"),
}
KKG_HOST = "data.kkg.kadaster.nl"
RCE_HOST = "api.linkeddata.cultureelerfgoed.nl"
# host -> (naam in de tabel, naam in een zin)
BRONNEN = {
    KKG_HOST: ("Kadaster: ligging en beperkingen", "Kadaster"),
    "api.labs.kadaster.nl": ("BAG: adressen", "BAG"),
    RCE_HOST: ("RCE: rijksmonumenten", "RCE"),
}

KKG_PER_MINUUT = 50  # onder de limiet van het Kadaster (60) en van de package (55)
BATCH_GROOTTE = 500  # alleen om het aantal Kadaster-verzoeken per ronde te schatten
KKG_TREFFER_MS = 80  # servertijd waaronder een antwoord vermoedelijk uit de cache komt
DREMPEL = 0.20  # verschil vanaf waar een onderdeel trager of sneller heet
BOOTSTRAP_TREKKINGEN = 2000


@dataclass
class Versie:
    """Een versie van de package, uitgecheckt en geïnstalleerd in een eigen omgeving."""

    naam: str
    ref: str
    sha: str
    python: Path


def _omgeving() -> Dict[str, str]:
    """Omgeving voor subprocessen, zonder verwijzing naar de actieve virtualenv."""
    return {
        k: v for k, v in os.environ.items() if k not in ("VIRTUAL_ENV", "PYTHONPATH")
    }


def _voer_uit(cmd: List[str], cwd: Path = REPO) -> str:
    resultaat = subprocess.run(
        cmd, cwd=cwd, env=_omgeving(), capture_output=True, text=True, check=False
    )
    if resultaat.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} mislukt:\n{resultaat.stdout}\n{resultaat.stderr}"
        )
    return resultaat.stdout.strip()


def _installeer(naam: str, ref: str, map_: Path, python_versie: str) -> Versie:
    sha = _voer_uit(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"])
    _voer_uit(["git", "worktree", "add", "--detach", str(map_), sha])
    _voer_uit(
        ["uv", "sync", "--frozen", "--no-dev", "--python", python_versie],
        cwd=map_,
    )
    bin_map = "Scripts" if sys.platform == "win32" else "bin"
    return Versie(naam, ref, sha, map_ / ".venv" / bin_map / "python")


def _trek_steekproeven(
    adressen: int, rng: random.Random
) -> Dict[str, Tuple[List[str], List[str]]]:
    """Trek per set twee disjuncte steekproeven: één voor de basis, één voor de kandidaat."""
    gebruikt: set[str] = set()
    steekproeven = {}
    # monumentvlag eerst, zodat willekeurig geen adressen van die set hergebruikt
    for set_ in sorted(SETS, key=lambda s: s != "monumentvlag"):
        with gzip.open(HIER / "pool" / f"{set_}.txt.gz", "rt") as f:
            pool = [i for i in f.read().split() if i not in gebruikt]
        if len(pool) < 2 * adressen:
            raise SystemExit(f"Pool {set_} heeft maar {len(pool)} ID's")
        keuze = rng.sample(pool, 2 * adressen)
        gebruikt.update(keuze)
        steekproeven[set_] = (keuze[:adressen], keuze[adressen:])
    return steekproeven


def _wacht_op_kadaster(kkg_starts: Deque[float], verwacht: int) -> None:
    """Wacht tot de volgende ronde past binnen het aantal Kadaster-verzoeken per minuut."""
    verwacht = min(verwacht, KKG_PER_MINUUT)
    while True:
        nu = time.time()
        while kkg_starts and kkg_starts[0] <= nu - 60:
            kkg_starts.popleft()
        if len(kkg_starts) + verwacht <= KKG_PER_MINUUT:
            return
        time.sleep(kkg_starts[0] + 60 - nu + 0.1)


def _meet_ronde(versie: Versie, ids: List[str], tmp: Path) -> Dict[str, Any]:
    ids_pad = tmp / "ids.txt"
    uit_pad = tmp / "ronde.json"
    ids_pad.write_text("\n".join(ids))
    uit_pad.unlink(missing_ok=True)
    proces = subprocess.run(
        [
            str(versie.python),
            str(HIER / "meetproces.py"),
            "--ids",
            str(ids_pad),
            "--uit",
            str(uit_pad),
        ],
        cwd=tmp,
        env=_omgeving(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proces.returncode != 0 or not uit_pad.exists():
        staart = "\n".join(proces.stderr.strip().splitlines()[-5:])
        return {
            "totaal_s": None,
            "fout": f"meetproces stopte met code {proces.returncode}: {staart}",
            "verzoeken": [],
        }
    resultaat: Dict[str, Any] = json.loads(uit_pad.read_text())
    return resultaat


def _meetverzoeken(ronde: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Geef de verzoeken van de batches, zonder de verzoeken bij het opstarten.

    Voordat de batches starten haalt de package de beschermde gezichten op bij RCE.
    Die verzoeken zijn klaar voordat het eerste verzoek naar het Kadaster begint.
    """
    verzoeken: List[Dict[str, Any]] = ronde["verzoeken"]
    eerste_batch = min(
        (v["start_epoch"] for v in verzoeken if v["host"] != RCE_HOST),
        default=float("inf"),
    )
    return [
        v
        for v in verzoeken
        if v["host"] != RCE_HOST
        or (
            v["duur_ms"] is not None
            and v["start_epoch"] + v["duur_ms"] / 1000 > eerste_batch
        )
    ]


def _waarden(rondes: List[Dict[str, Any]], host: str) -> List[float]:
    waarden = []
    for ronde in rondes:
        for v in _meetverzoeken(ronde):
            if v["host"] != host or v["status"] != 200 or v["fout"]:
                continue
            waarde = v["server_ms"] if host == KKG_HOST else v["duur_ms"]
            if waarde is not None:
                waarden.append(waarde)
    return waarden


def _verhouding(
    basis: List[float], kandidaat: List[float], rng: random.Random
) -> Tuple[float, float, float]:
    """Verhouding van de medianen (kandidaat / basis) met een bootstrap-interval van 90%."""
    trekkingen = sorted(
        statistics.median(rng.choices(kandidaat, k=len(kandidaat)))
        / statistics.median(rng.choices(basis, k=len(basis)))
        for _ in range(BOOTSTRAP_TREKKINGEN)
    )
    return (
        statistics.median(kandidaat) / statistics.median(basis),
        trekkingen[int(0.05 * BOOTSTRAP_TREKKINGEN)],
        trekkingen[int(0.95 * BOOTSTRAP_TREKKINGEN) - 1],
    )


def _getal(x: float, decimalen: int = 0) -> str:
    return f"{x:,.{decimalen}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _procent(verhouding: float) -> str:
    """Schrijf een verhouding als procentueel verschil, bijvoorbeeld 1,26 als +26%."""
    verschil = round((verhouding - 1) * 100)
    if verschil > 0:
        return f"+{_getal(verschil)}%"
    if verschil < 0:
        return f"−{_getal(-verschil)}%"
    return "0%"


def _oordeel(verhouding: float, laag: float, hoog: float) -> str:
    """Vat een verschil samen, rekening houdend met de marge."""
    if verhouding >= 1 + DREMPEL:
        return "⚠️ trager" if laag > 1 else "❔ mogelijk trager"
    if verhouding <= 1 - DREMPEL and hoog < 1:
        return "🚀 sneller"
    return "✅ geen duidelijk verschil"


def _rapport(
    versies: Tuple[Versie, Versie],
    resultaten: List[Dict[str, Any]],
    args: argparse.Namespace,
    seed: int,
    duur_s: float,
) -> str:
    basis, kandidaat = versies
    rng = random.Random(seed)
    drempel = f"{_getal(DREMPEL * 100)}%"

    def rondes(set_: str, versie: Versie) -> List[Dict[str, Any]]:
        return [
            r for r in resultaten if r["set"] == set_ and r["versie"] == versie.naam
        ]

    regels = []
    per_oordeel: Dict[str, List[str]] = {}
    aantallen = set()
    for set_, (set_kolom, set_zin) in SETS.items():
        for host, (bron_kolom, bron_zin) in BRONNEN.items():
            b = _waarden(rondes(set_, basis), host)
            k = _waarden(rondes(set_, kandidaat), host)
            aantallen.update([len(b), len(k)])
            if not b or not k:
                regels.append(
                    f"| {set_kolom} | {bron_kolom} | – | – | – | te weinig metingen |"
                )
                continue
            verhouding, laag, hoog = _verhouding(b, k, rng)
            oordeel = _oordeel(verhouding, laag, hoog)
            per_oordeel.setdefault(oordeel, []).append(
                f"{bron_zin} bij {set_zin} ({_procent(verhouding)})"
            )
            regels.append(
                f"| {set_kolom} | {bron_kolom} "
                f"| {_getal(statistics.median(b))} ms "
                f"| {_getal(statistics.median(k))} ms "
                f"| {_procent(verhouding)}<br><sub>{_procent(laag)} tot {_procent(hoog)}</sub> "
                f"| {oordeel} |"
            )

    samenvatting = []
    if "⚠️ trager" in per_oordeel:
        samenvatting.append(
            f"⚠️ **Trager dan {basis.naam}:** "
            + "; ".join(per_oordeel["⚠️ trager"])
            + "."
        )
    if "❔ mogelijk trager" in per_oordeel:
        samenvatting.append(
            f"❔ **Mogelijk trager dan {basis.naam}:** "
            + "; ".join(per_oordeel["❔ mogelijk trager"])
            + ". Dit kan toeval zijn; start de benchmark opnieuw om het te controleren."
        )
    if not samenvatting:
        samenvatting.append(f"✅ **Niet trager dan {basis.naam}.**")
    if "🚀 sneller" in per_oordeel:
        samenvatting.append(
            f"🚀 **Sneller dan {basis.naam}:** "
            + "; ".join(per_oordeel["🚀 sneller"])
            + "."
        )

    def per_versie(tel: Dict[str, int]) -> str:
        return f"{basis.naam} {tel[basis.naam]}, {kandidaat.naam} {tel[kandidaat.naam]}"

    cache: Dict[str, int] = {}
    kkg_snel: Dict[str, int] = {}
    mislukt: Dict[str, int] = {}
    for versie in versies:
        eigen = [r for r in resultaten if r["versie"] == versie.naam]
        meet = [v for r in eigen for v in _meetverzoeken(r)]
        cache[versie.naam] = sum(v["cache"] == "HIT" for v in meet)
        kkg_snel[versie.naam] = sum(
            v["host"] == KKG_HOST
            and v["server_ms"] is not None
            and v["server_ms"] <= KKG_TREFFER_MS
            for v in meet
        )
        mislukt[versie.naam] = sum(
            bool(v["fout"]) or v["status"] != 200 for r in eigen for v in r["verzoeken"]
        )
    problemen = []
    if sum(cache.values()):
        problemen.append(
            f"{sum(cache.values())} antwoorden van BAG of RCE kwamen uit de cache "
            f"({per_versie(cache)}). Zo'n antwoord is sneller dan normaal."
        )
    if sum(kkg_snel.values()):
        problemen.append(
            f"{sum(kkg_snel.values())} antwoorden van het Kadaster waren zo snel "
            f"(hooguit {KKG_TREFFER_MS} ms) dat ze waarschijnlijk uit de cache kwamen "
            f"({per_versie(kkg_snel)})."
        )
    if sum(mislukt.values()):
        problemen.append(
            f"{sum(mislukt.values())} verzoeken mislukten ({per_versie(mislukt)}). "
            "Details staan in de ruwe metingen (in CI: het artifact `benchmark`)."
        )
    problemen += [
        f"Ronde {r['ronde'] + 1} ({SETS[r['set']][1]}, {r['versie']}) mislukte: {r['fout']}"
        for r in resultaten
        if r["fout"]
    ]
    if problemen:
        betrouwbaarheid = [
            "⚠️ **De meting is mogelijk niet betrouwbaar:**",
            "",
            *(f"- {p}" for p in problemen),
        ]
    else:
        betrouwbaarheid = [
            "✅ De meting is betrouwbaar: geen antwoorden uit de cache en geen "
            "mislukte verzoeken."
        ]

    n = (
        f"{min(aantallen)}"
        if len(aantallen) == 1
        else f"{min(aantallen)} tot {max(aantallen)}"
    )
    verwerking = ", ".join(
        f"{v.naam} "
        + _getal(sum(r["totaal_s"] or 0 for r in resultaten if r["versie"] == v.naam))
        + " s"
        for v in versies
    )
    uit = [
        MARKER,
        f"## Benchmark: is {kandidaat.naam} trager dan {basis.naam}?",
        "",
        *(regel for zin in samenvatting for regel in (zin, "")),
        f"| Adressen | Onderdeel | {basis.naam} | {kandidaat.naam} | Verschil | Oordeel |",
        "|---|---|--:|--:|--:|---|",
        *regels,
        "",
        *betrouwbaarheid,
        "",
        "<details><summary>Hoe lees je dit?</summary>",
        "",
        f"- **{basis.naam} en {kandidaat.naam}:** hoe lang één verzoek aan de API typisch "
        f"duurt. Per versie en per onderdeel zijn {n} verzoeken gemeten; de tabel toont "
        "de middelste waarde, zodat losse uitschieters niet meetellen. Bij het Kadaster "
        "is dat de rekentijd op hun server, zonder internetvertraging.",
        f"- **Verschil:** hoeveel langer (+) of korter (−) {kandidaat.naam} er typisch "
        "over doet. De API's zijn niet op elk moment even snel, dus een verschil kan "
        "toeval zijn. Klein eronder staat tussen welke waarden het echte verschil met "
        "90% zekerheid ligt.",
        "- **Oordeel:**",
        f"  - ⚠️ trager: minstens {drempel} trager, en ook in het gunstigste geval nog "
        "trager.",
        f"  - ❔ mogelijk trager: minstens {drempel} trager gemeten, maar het kan toeval "
        "zijn. Start de benchmark opnieuw om het te controleren.",
        "  - ✅ geen duidelijk verschil.",
        f"  - 🚀 sneller: minstens {drempel} sneller, en ook in het ongunstigste geval "
        "nog sneller.",
        f"- **Adressen:** per versie {_getal(args.adressen)} willekeurige adressen uit heel "
        f"Nederland en {_getal(args.adressen)} adressen van monumenten. Bij monumenten "
        "doet het Kadaster het meeste werk. Elke versie krijgt eigen adressen, zodat "
        "geen van beide sneller lijkt doordat de API een antwoord nog in de cache had.",
        "- **Onderdelen:** BAG zoekt bij elk verblijfsobject het adres. Het Kadaster "
        "zoekt waar het adres ligt en welke beperkingen, zoals een monumentstatus, "
        "erop rusten. RCE zoekt de rijksmonumenten.",
        "",
        "</details>",
        "",
        f"<sub>{basis.naam} `{basis.sha[:7]}` · {kandidaat.naam} `{kandidaat.sha[:7]}` · "
        f"totale verwerkingstijd: {verwerking} · meting duurde {int(duur_s // 60)} min "
        f"{int(duur_s % 60)} s · herhalen met `--seed {seed}`</sub>",
    ]
    return "\n".join(uit) + "\n"


def main() -> None:
    """Installeer beide versies, meet ze om en om en schrijf het rapport."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--basis", default="origin/main", help="git-ref van de basis")
    parser.add_argument("--kandidaat", default="HEAD", help="git-ref van de kandidaat")
    parser.add_argument(
        "--namen", nargs=2, default=["main", "deze PR"], metavar=("BASIS", "KANDIDAAT")
    )
    parser.add_argument(
        "--adressen", type=int, default=25_000, help="per set per versie"
    )
    parser.add_argument("--rondes", type=int, default=5)
    parser.add_argument("--python", default="3.13")
    parser.add_argument(
        "--seed",
        type=int,
        help="alleen om een meting te herhalen; standaard willekeurig",
    )
    parser.add_argument("--uitvoer", type=Path, help="schrijf het rapport (markdown)")
    parser.add_argument("--ruwe-data", type=Path, help="schrijf alle metingen (JSON)")
    args = parser.parse_args()

    # Nooit afleiden uit commit of run: een herhaalde run zou dan exact dezelfde
    # queries sturen en antwoorden uit de cache krijgen.
    seed = args.seed if args.seed is not None else int.from_bytes(os.urandom(8), "big")
    rng = random.Random(seed)
    steekproeven = _trek_steekproeven(args.adressen, rng)
    ronde_grootte = -(-args.adressen // args.rondes)

    start = time.time()
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        mappen = [tmp / "basis", tmp / "kandidaat"]
        try:
            versies = (
                _installeer(args.namen[0], args.basis, mappen[0], args.python),
                _installeer(args.namen[1], args.kandidaat, mappen[1], args.python),
            )
            resultaten = []
            kkg_starts: Deque[float] = deque()
            for ronde in range(args.rondes):
                for i, set_ in enumerate(SETS):
                    volgorde = [0, 1] if (ronde + i) % 2 == 0 else [1, 0]
                    for v in volgorde:
                        ids = steekproeven[set_][v][
                            ronde * ronde_grootte : (ronde + 1) * ronde_grootte
                        ]
                        _wacht_op_kadaster(kkg_starts, -(-len(ids) // BATCH_GROOTTE))
                        resultaat = _meet_ronde(versies[v], ids, tmp)
                        kkg_starts.extend(
                            r["start_epoch"]
                            for r in resultaat["verzoeken"]
                            if r["host"] == KKG_HOST
                        )
                        resultaten.append(
                            {
                                "set": set_,
                                "ronde": ronde,
                                "versie": versies[v].naam,
                                **resultaat,
                            }
                        )
                        totaal = resultaat["totaal_s"]
                        print(
                            f"ronde {ronde + 1}/{args.rondes} · {set_} · "
                            f"{versies[v].naam}: {len(ids)} adressen in "
                            + (f"{totaal:.1f} s" if totaal is not None else "mislukt"),
                            file=sys.stderr,
                        )
        finally:
            for map_ in mappen:
                if map_.exists():
                    _voer_uit(["git", "worktree", "remove", "--force", str(map_)])

    rapport = _rapport(versies, resultaten, args, seed, time.time() - start)
    print(rapport)
    if args.uitvoer:
        args.uitvoer.write_text(rapport)
    if args.ruwe_data:
        args.ruwe_data.write_text(
            json.dumps(
                {
                    "seed": seed,
                    "versies": [vars(v) | {"python": str(v.python)} for v in versies],
                    "rondes": resultaten,
                },
                indent=1,
            )
        )


if __name__ == "__main__":
    main()
