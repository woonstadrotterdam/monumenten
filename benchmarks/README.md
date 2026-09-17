# Snelheidsmeting

Dit script meet of een versie van de package trager is dan een andere. Bij een PR naar `main` die code, dependencies of de meting zelf raakt, draait het automatisch (workflow `Benchmark`). De uitkomst verschijnt in een PR-comment.

## Lokaal draaien

```bash
python benchmarks/snelheid.py --basis origin/main --kandidaat HEAD
```

Daarvoor zijn git en [uv](https://docs.astral.sh/uv/) nodig. Het script zet beide refs in een tijdelijke worktree, installeert ze met `uv sync --frozen` en meet ze om en om. Alleen gecommitte wijzigingen tellen mee.

Een volledige meting duurt ongeveer 5 minuten. Met `--adressen 1000 --rondes 1` is het in ruim een minuut klaar.

## Wat er gemeten wordt

- **Adressen:** twee soorten, uit heel Nederland.

  - **Willekeurig:** willekeurige adressen.
  - **Monumenten:** adressen die in de dataset als monument staan. Daar doet het Kadaster het meeste werk.

  Elke versie verwerkt van elke soort 25.000 adressen, verdeeld over 5 rondes.

- **Hoe:** elke ronde draait in een eigen proces, via `MonumentenClient.process_from_list`. Het proces gebruikt een aiohttp-sessie die elk verzoek vastlegt.
- **Tijd per verzoek:** per onderdeel telt de middelste waarde (mediaan), zodat losse uitschieters niet meetellen.

  - **Kadaster:** de rekentijd op hun server, uit de header `server-timing`, dus zonder internetvertraging.
  - **BAG en RCE:** de tijd tot het hele antwoord binnen is.

  Het ophalen van de beschermde gezichten bij het opstarten telt niet mee.

- **Verschil:** het verschil staat als percentage. De API's zijn niet op elk moment even snel, dus een verschil kan toeval zijn. Daarom berekent het script ook tussen welke waarden het echte verschil met 90% zekerheid ligt (met bootstrap).
- **Oordeel per onderdeel:**

  - ⚠️ trager: minstens 20% trager, en ook in het gunstigste geval nog trager.
  - ❔ mogelijk trager: minstens 20% trager gemeten, maar het kan toeval zijn. Start de benchmark opnieuw om het te controleren.
  - ✅ geen duidelijk verschil.
  - 🚀 sneller: minstens 20% sneller, en ook in het ongunstigste geval nog sneller.

- **Tempo:** het script houdt alle rondes samen onder 50 Kadaster-verzoeken per minuut.

## Cache

De API's cachen antwoorden op exact dezelfde query, minstens 15 minuten. Ook een licht gewijzigde query op dezelfde adressen, bijvoorbeeld met een andere volgorde, is daarna nog sneller. Daarom:

- **Eigen steekproef:** elke versie krijgt een eigen steekproef, bij elke run opnieuw getrokken.
- **Willekeurige seed:** de seed komt nooit uit een commit of run-ID. Een herhaalde run zou anders dezelfde queries sturen.
- **Controle:** het rapport telt verdachte treffers.
  - Bij BAG en RCE: antwoorden met `x-t-cache: HIT`.
  - Bij het Kadaster: verzoeken met een servertijd van hooguit 80 ms.

Eerdere runs kunnen adressen in de pool opwarmen, maar dat raakt beide versies even hard.

## Pool vernieuwen

De map `pool/` bevat twee pools van elk 200.000 verblijfsobject-ID's: willekeurige en adressen met een monumentvlag. Ze zijn getrokken uit een vastgepinde versie van de dataset [woonstadrotterdam/monumenten](https://huggingface.co/datasets/woonstadrotterdam/monumenten). De vlaggen dienen alleen om adressen te kiezen, niet als waarheid.

Voor een nieuwe pool pas je eerst `HF_REVISIE` aan in `maak_pool.py`, en draai je daarna:

```bash
uv run benchmarks/maak_pool.py
```
