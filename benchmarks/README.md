# Snelheidsmeting

Dit script meet of een versie van de package trager is dan een andere. Bij een PR naar `main` die code, dependencies of de meting zelf raakt, draait het automatisch (workflow `Benchmark`). De uitkomst verschijnt in een PR-comment.

## Lokaal draaien

```bash
python benchmarks/snelheid.py --basis origin/main --kandidaat HEAD
```

Daarvoor zijn git en [uv](https://docs.astral.sh/uv/) nodig. Het script zet beide refs in een tijdelijke worktree, installeert ze met `uv sync --frozen` en meet ze om en om. Alleen gecommitte wijzigingen tellen mee.

Een volledige meting duurt ongeveer 5 minuten, of ongeveer 8 als er doorgemeten wordt (zie hieronder). Met `--adressen 1000 --rondes 1` is het in ruim een minuut klaar.

## Wat er gemeten wordt

- **Adressen:** twee soorten, uit heel Nederland.

  - **Willekeurig:** willekeurige adressen.
  - **Monumenten:** adressen die in de dataset als monument staan. Daar doet het Kadaster het meeste werk.

  Elke versie verwerkt van elke soort 25.000 adressen, verdeeld over 5 rondes.

- **Hoe:** elke ronde draait in een eigen proces, via `MonumentenClient.process_from_list`. Het proces gebruikt een aiohttp-sessie die elk verzoek vastlegt.
- **Tijd per verzoek:** per onderdeel telt de middelste waarde (mediaan), zodat losse uitschieters niet meetellen.

  - **Kadaster:** de rekentijd op hun server, uit de header `server-timing`, dus zonder internetvertraging.
  - **Alle andere onderdelen:** de tijd tot het hele antwoord binnen is.

  Het ophalen van de beschermde gezichten bij het opstarten telt niet mee.

- **Onderdelen:** Kadaster, BAG en RCE staan altijd in de tabel. Roept een versie nog een andere host aan, dan krijgt die ook een regel. Een leesbare naam en uitleg voor zo'n host staan in `BRONNEN` in `snelheid.py`; zonder die naam staat de hostnaam in de tabel.
- **Verschil:** het verschil staat als percentage. De API's zijn niet op elk moment even snel, dus een verschil kan toeval zijn. Daarom berekent het script ook tussen welke waarden het echte verschil met 90% zekerheid ligt. Verzoeken uit dezelfde ronde lijken op elkaar, omdat de API op dat moment even snel of traag is. De berekening (een bootstrap) trekt daarom eerst rondes en pas daarbinnen verzoeken; anders wordt de marge te smal en volgen er vaker onterechte waarschuwingen.
- **Oordeel per onderdeel:**

  - ⚠️ trager: minstens 20% trager, en ook in het gunstigste geval nog trager.
  - ❔ mogelijk trager: minstens 20% trager gemeten, maar het kan toeval zijn. Dan meet het script voor die soort adressen automatisch nog eens zoveel, met nieuwe adressen, en beoordeelt het beide metingen samen. Blijft het ❔, start de benchmark dan opnieuw.
  - ✅ geen duidelijk verschil.
  - 🚀 sneller: minstens 20% sneller, en ook in het ongunstigste geval nog sneller.
  - 🆕 nieuw onderdeel: alleen de nieuwe versie roept deze host aan, dus er is niets om mee te vergelijken. De tabel toont wel hoe lang de verzoeken duren.
  - ➖ niet meer gebruikt: alleen de oude versie roept deze host aan.

- **Tempo:** het script houdt alle rondes samen onder 50 Kadaster-verzoeken per minuut.

## Cache

De API's cachen antwoorden op exact dezelfde query, minstens 15 minuten. Ook een licht gewijzigde query op dezelfde adressen, bijvoorbeeld met een andere volgorde, is daarna nog sneller. Daarom:

- **Eigen steekproef:** elke versie krijgt een eigen steekproef, bij elke run opnieuw getrokken.
- **Willekeurige seed:** de seed komt nooit uit een commit of run-ID. Een herhaalde run zou anders dezelfde queries sturen.
- **Controle:** het rapport telt verdachte treffers.
  - Bij BAG en RCE: antwoorden met `x-t-cache: HIT`.
  - Bij het Kadaster: verzoeken met een servertijd van hooguit 80 ms.

Eerdere runs kunnen adressen in de pool opwarmen, maar dat raakt beide versies even hard.

Verzoeken die niet van de adressen afhangen, zoals het downloaden van een volledige lijst, zijn in elke ronde en voor beide versies gelijk. Daar helpt een eigen steekproef niet. Hooguit de allereerste meting treft zo'n verzoek met een koude cache; daarna is het voor beide versies even warm.

## Pool vernieuwen

De map `pool/` bevat twee pools van elk 200.000 verblijfsobject-ID's: willekeurige en adressen met een monumentvlag. Ze zijn getrokken uit een vastgepinde versie van de dataset [woonstadrotterdam/monumenten](https://huggingface.co/datasets/woonstadrotterdam/monumenten). De vlaggen dienen alleen om adressen te kiezen, niet als waarheid.

Voor een nieuwe pool pas je eerst `HF_REVISIE` aan in `maak_pool.py`, en draai je daarna:

```bash
uv run benchmarks/maak_pool.py
```
