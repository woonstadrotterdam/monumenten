<!-- snippet-start -->

# Monumenten

Een Python package voor het ophalen van monumentgegevens van Nederlandse overheids-API's. Momenteel is het mogelijk om de status van rijksmonumenten, gemeentelijke monumenten en rijksbeschermde gezichten op te halen. Eventueel in [VERA-referentiedataformaat](https://www.coraveraonline.nl/index.php/Referentiedata:EENHEIDMONUMENT).

Door middel van de package is het mogelijk om, indienst gewenst, voor tienduizenden verblijfsobjecten per seconde monumentgegevens op te halen. Er zijn geen API-keys nodig.

> [!NOTE]
> In VERA-referentiedataformaat wordt geen onderscheid gemaakt tussen rijksbeschermde stads- en dorpsgezichten. Alle rijksbeschermde gezichten worden teruggegeven als rijksbeschermd stadsgezicht.

> [!WARNING]
> Het is mogelijk dat een verblijfsobject ten onrechte wel of geen monumentstatus heeft. Dit hangt af van hoe het verblijfsobject staat geregistreerd bij het Kadaster en de Rijksdienst voor het Cultureel Erfgoed. Neem contact met hen op als u denkt u een verkeerde monumentale status terugkrijgt.

> [!TIP]
> Op [deze website](https://huggingface.co/spaces/woonstadrotterdam/monumenten-space) kun je via een gebruikersinterface gebruik maken van de package.

## Installatie

```bash
pip install monumenten
```

## Voorbeeldoutput

| bag_verblijfsobject_id | rijksmonument | rijksmonument_bron | rijksmonument_nummer | rijksmonument_url                                 | rijksbeschermd_gezicht | rijksbeschermd_gezicht_naam | gemeentelijk_monument | grondslag_gemeentelijk_monument                                                        |
| ---------------------- | ------------- | ------------------ | -------------------- | ------------------------------------------------- | ---------------------- | --------------------------- | --------------------- | -------------------------------------------------------------------------------------- |
| 0599010000360091       | True          | RCE, Kadaster      | 524327               | https://monumentenregister.cultureelerfgoed.nl... | False                  | <NA>                        | False                 | <NA>                                                                                   |
| 0599010000486642       | False         | <NA>               | <NA>                 | <NA>                                              | False                  | <NA>                        | False                 | <NA>                                                                                   |
| 0599010000281115       | False         | <NA>               | <NA>                 | <NA>                                              | True                   | Kralingen - Midden          | False                 | <NA>                                                                                   |
| 0599010000076715       | False         | <NA>               | <NA>                 | <NA>                                              | False                  | <NA>                        | True                  | Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift) |
| 0599010000146141       | False         | <NA>               | <NA>                 | <NA>                                              | True                   | Rotterdam - Waterproject    | True                  | Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift) |
| 0232010000002251       | False         | <NA>               | <NA>                 | <NA>                                              | False                  | <NA>                        | True                  | Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift) |
| 0599010000341377       | True          | Kadaster           | <NA>                 | <NA>                                              | False                  | <NA>                        | False                 | <NA>                                                                                   |

## Architectuur

De package combineert drie databronnen om monumentstatussen te bepalen:

```mermaid
flowchart TB

    %% ========================
    %% INPUT
    %% ========================
    subgraph Input["Input"]
        A["Verblijfsobject IDs<br>bijv. 0599010000360091"]
    end

    %% ========================
    %% RCE
    %% ========================
    subgraph RCE["RCE – Rijksdienst Cultureel Erfgoed"]

        subgraph RCE_RM["Rijksmonumenten Query"]
            R1["Monument"]
            R2["BasisregistratieRelatie"]
            R3["BAGRelatie"]
            R4["verblijfsobjectIdentificatie"]
            R5["rijksmonumentnummer"]
        end

        subgraph RCE_BG["Beschermde Gezichten Query"]
            G1["Gezicht"]
            G2["GezichtGeometrie"]
            G3["gezichtWKT"]
            G4["rijksbeschermd_gezicht_naam"]
        end

    end

    %% ========================
    %% KADASTER
    %% ========================
    subgraph Kadaster["Kadaster (via PDOK)"]

        subgraph BAG["BAG – Stage 1"]
            B1["Verblijfsobject"]
            B2["Adrespunt"]
        end

        subgraph BRKPB["BRK-PB – Stage 2"]
            K5["Beperking"]
            K6["grondslagCode"]
            K7["grondslagOmschrijving"]
            K8["Beperkingsgebied"]
        end

    end

    %% ========================
    %% PROCESSING
    %% ========================
    subgraph Processing["Verwerking"]
        P0["Spatial join<br>adrespunt ∈ beperkingsgebied"]
        P1["Merge rijksmonumenten<br>RCE nummer + Kadaster EWE/EWD"]
        P2["Spatial join<br>adrespunt ∈ gezicht WKT"]
        P3["Filter gemeentelijke<br>grondslagcode GG/GWA"]
    end

    %% ========================
    %% OUTPUT
    %% ========================
    subgraph Output["Monumentstatussen"]
        O1["Rijksmonument<br>bron: RCE en/of Kadaster"]
        O2["Beschermd Gezicht<br>bron: RCE"]
        O3["Gemeentelijk Monument<br>bron: Kadaster"]
    end

    %% ========================
    %% FLOWS
    %% ========================

    %% RCE Rijksmonumenten
    A -->|"VALUES ?identificatie"| R1
    R1 -->|"ceo:heeftJuridischeStatus"| R1
    R1 -->|"ceo:rijksmonumentnummer"| R5
    R1 -->|"ceo:heeftBasisregistratieRelatie"| R2
    R2 -->|"ceo:heeftBAGRelatie"| R3
    R3 -->|"ceo:verblijfsobjectIdentificatie"| R4

    %% RCE Beschermde Gezichten
    G1 -->|"ceo:heeftGeometrie"| G2
    G1 -->|"ceo:heeftGezichtsstatus"| G1
    G1 -->|"ceo:heeftNaam/ceo:naam"| G4
    G2 -->|"geo:asWKT"| G3

    %% Kadaster BAG
    A -->|"identificatie"| B1
    B1 -->|"geom"| B2

    %% Kadaster BRK-PB
    B2 -->|"Intersects(geom, adrespunten)"| K5
    K5 -->|"grondslagCode"| K6
    K5 -->|"grondslagOmschrijving"| K7
    K5 -->|"geom"| K8

    %% Processing
    B2 --> P0
    K6 --> P0
    K7 --> P0
    K8 --> P0
    R4 --> P1
    R5 --> P1
    P0 -->|"EWE/EWD"| P1
    B2 --> P2
    G3 --> P2
    G4 --> P2
    P0 -->|"GG/GWA"| P3

    %% Output
    P1 --> O1
    P2 --> O2
    P3 --> O3
```

### Bronlogica per Monumenttype

| Monumenttype               | Primaire Bron     | Secundaire Bron    | Logica                                                                                                                                                                                             |
| -------------------------- | ----------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Rijksmonument**          | RCE               | Kadaster (EWE/EWD) | `rijksmonument_bron` = "RCE, Kadaster" als beide, "RCE" of "Kadaster" als één. Kadaster telt alleen als het adrespunt binnen het beperkingsgebied ligt (zie [Beperkingsgebied](#beperkingsgebied)) |
| **Rijksbeschermd Gezicht** | RCE               | -                  | Spatial join: verblijfsobject geometrie ∈ gezicht geometrie                                                                                                                                        |
| **Gemeentelijk Monument**  | Kadaster (GG/GWA) | -                  | Kadaster beperking met grondslagcode GG of GWA, alleen als het adrespunt binnen het beperkingsgebied ligt (zie [Beperkingsgebied](#beperkingsgebied))                                              |

### Afkortingen

| Afkorting  | Betekenis                                                                                                                             |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **BAG**    | Basisregistratie Adressen en Gebouwen: alle adressen, verblijfsobjecten en panden in Nederland                                        |
| **BGT**    | Basisregistratie Grootschalige Topografie: gedetailleerde kaart van Nederland met onder meer gebouwen, wegen, water, muren en bruggen |
| **BRK**    | Basisregistratie Kadaster: kadastrale percelen, met hun eigenaren, rechten en beperkingen                                             |
| **BRK-PB** | BRK Publiekrechtelijke Beperkingen: de beperkingen uit de BRK, zoals een monumentstatus                                               |
| **PDOK**   | Publieke Dienstverlening Op de Kaart: platform waarop de overheid geodata beschikbaar stelt                                           |
| **RCE**    | Rijksdienst voor het Cultureel Erfgoed                                                                                                |
| **VERA**   | Vastgoed Referentie Architectuur                                                                                                      |
| **WFS**    | Web Feature Service (standaard voor het opvragen van geodata)                                                                         |
| **WKT**    | Well-Known Text (geometrie formaat)                                                                                                   |

### Grondslagcodes (Kadaster)

| Code    | Wet                                                        | Monumenttype          |
| ------- | ---------------------------------------------------------- | --------------------- |
| **EWE** | Erfgoedwet: Afschrift inschrijving rijksmonumentenregister | Rijksmonument         |
| **EWD** | Erfgoedwet: Ontwerpbesluit aanwijzing (voorbescherming)    | Rijksmonument         |
| **GG**  | Gemeentewet: Besluit monument                              | Gemeentelijk monument |
| **GWA** | Gemeentewet: Aanwijzing gemeentelijk monument              | Gemeentelijk monument |

### Beperkingsgebied

Monumentstatus staat in het Kadaster geregistreerd als een _publiekrechtelijke beperking_: een aantekening dat de overheid regels stelt aan wat je met een stuk grond of gebouw mag doen. Zo'n beperking is gekoppeld aan een _perceel_, een stuk grond met een eigen kadastraal nummer. Op één perceel kunnen meerdere gebouwen staan, en een gebouw kan op meerdere percelen staan.

Daarnaast legt de overheid die de beperking registreert (bijvoorbeeld de gemeente of de RCE) vast voor welk gebied de beperking precies geldt: het _beperkingsgebied_. Zonder extra controle zou elk adres in een gebouw dat het perceel raakt de status krijgen, ook als het monument een ander gebouw op dat perceel is, of als het gebouw maar met een strookje op het perceel staat. Daarom telt een beperking alleen als het _adrespunt_ van het verblijfsobject (de locatie van het adres in de BAG, een punt binnen het pand) binnen het beperkingsgebied ligt. De BRK-PB WFS geeft de beperkingen terug waarvan het gebied minstens één van de opgevraagde adrespunten raakt; de package bepaalt daarna zelf per adrespunt binnen welke gebieden het ligt.

Hoe nauwkeurig dat werkt, hangt af van hoe het beperkingsgebied is vastgelegd. Het Kadaster kent vier manieren (in de data het veld `typeBeperkingsgebied`):

| Type in de data                   | Het beperkingsgebied is…                                                            | Welke adressen krijgen de status                                                                                  |
| --------------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `BAG`                             | de omtrek van één pand, overgenomen uit de BAG                                      | alleen adressen in dat pand                                                                                       |
| `Handmatig ingetekende geometrie` | een vlak dat de overheid zelf op de kaart heeft getekend                            | alleen adressen in dat vlak                                                                                       |
| `BGT`                             | één object van de BGT-kaart, bijvoorbeeld een gebouw, muur of brug                  | alleen adressen in dat object                                                                                     |
| `BRK`                             | het hele perceel (of meerdere percelen), zonder aan te geven welk gebouw bedoeld is | alle adressen waarvan het adrespunt op het perceel ligt; gebouwen op hetzelfde perceel zijn niet te onderscheiden |

Alle rijksmonumenten (grondslagcodes EWE/EWD) zijn door de RCE met type `BRK` vastgelegd. Bij een rijksmonument met bron "Kadaster" kan het daarom zo zijn dat niet dit gebouw, maar een ander gebouw op hetzelfde perceel het eigenlijke monument is.

### Endpoints

| Bron                | Type       | Endpoint                                                                            |
| ------------------- | ---------- | ----------------------------------------------------------------------------------- |
| **Kadaster BAG**    | WFS (PDOK) | `https://service.pdok.nl/kadaster/bag/wfs/v2_0`                                     |
| **Kadaster BRK-PB** | WFS (PDOK) | `https://service.pdok.nl/kadaster/brk-publiekrechtelijke-beperkingen-wkpb/wfs/v1_0` |
| **RCE**             | SPARQL     | `https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/sparql`                |

### Groeperen van verzoeken

De BRK-PB WFS doorzoekt per verzoek de omhullende rechthoek van alle opgevraagde adrespunten, en doet daar langer over naarmate die rechthoek groter is. De package houdt die rechthoek daarom klein:

1. Eerst haalt de package voor alle verblijfsobjecten het adrespunt op bij de BAG WFS, met maximaal 100 verblijfsobjecten per verzoek.
2. Daarna sorteert de package de verblijfsobjecten langs een Hilbert-curve, zodat een batch adressen bevat die bij elkaar liggen.
3. Per batch gaan maximaal 500 adrespunten in één verzoek naar de BRK-PB WFS. Is de rechthoek te groot voor het aantal adrespunten, dan splitst de package het verzoek op de grootste sprong tussen twee opeenvolgende adrespunten.

Een bezit dat in één regio ligt gaat daardoor het snelst. Adressen verspreid over heel Nederland kosten meer en tragere verzoeken.

### Retry-gedrag

Aanroepen naar Kadaster (BAG, BRK-PB) en RCE gebruiken een gedeelde retry-logica: maximaal 2 pogingen per request, bij mislukking 3 seconden wachten. Alleen tijdelijke fouten worden herhaald (HTTP 429, 500, 502, 503, 504 en netwerk-/verbindingsfouten); permanente 4xx worden direct doorgegeven. Faalt een aanroep na retries, dan wordt de set IDs/adrespunten in tweeën gedeeld en elk deel opnieuw geprobeerd (recursief, per API, tot min. 1 ID of max. splitdiepte 10); definitief falende aanroepen worden overgeslagen met een waarschuwing.

## Tutorial

<!-- snippet-end -->

Zie [readthedocs](https://monumenten.readthedocs.io/nl/latest/tutorial/).
