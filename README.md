# Monumenten

Een Python package voor het ophalen van monumentgegevens van Nederlandse overheids-API's. Momenteel is het mogelijk om de status van rijksmonumenten, gemeentelijke monumenten en beschermde gezichten op te halen. Eventueel in VERA-referentiedataformaat.

Door middel van de package is het mogelijk om, indienst gewenst, voor tienduizenden verblijfsobjecten per seconde monumentgegevens op te halen. Er zijn geen API-keys nodig.

> In VERA-referentiedataformaat wordt geen onderscheid gemaakt tussen beschermde stads- en dorpsgezichten. Alle beschermde gezichten worden teruggegeven als beschermd stadsgezicht.

## Installatie

`pip install monumenten`

## Gebruik

### Python-native 🐍

```python
import asyncio
import json

from monumenten import MonumentenClient

async def main():
    bag_verblijfsobject_ids = [
        "0599010000360091",
        "0599010000486642",
        "0599010000281115",
        "0599010000146141",
    ]

    async with MonumentenClient() as client:
        result = await client.process_from_list(bag_verblijfsobject_ids)
        print(json.dumps(result, indent=2))

# in een .py file"
if __name__ == "__main__":
    asyncio.run(main())

# in een .ipynb file (notebook):
await main()
```

```python
# OUTPUT
{
  "0599010000360091": {
    "is_rijksmonument": true,
    "rijksmonument_nummer": "524327",
    "rijksmonument_url": "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327",
    "is_beschermd_gezicht": false,
    "beschermd_gezicht_naam": null,
    "is_gemeentelijk_monument": false,
    "grondslag_gemeentelijk_monument": null
  },
  "0599010000486642": {
    "is_rijksmonument": false,
    "rijksmonument_nummer": null,
    "rijksmonument_url": null,
    "is_beschermd_gezicht": false,
    "beschermd_gezicht_naam": null,
    "is_gemeentelijk_monument": false,
    "grondslag_gemeentelijk_monument": null
  },
  "0599010000281115": {
    "is_rijksmonument": false,
    "rijksmonument_nummer": null,
    "rijksmonument_url": null,
    "is_beschermd_gezicht": true,
    "beschermd_gezicht_naam": "Kralingen - Midden",
    "is_gemeentelijk_monument": false,
    "grondslag_gemeentelijk_monument": null
  },
  "0599010000146141": {
    "is_rijksmonument": false,
    "rijksmonument_nummer": null,
    "rijksmonument_url": null,
    "is_beschermd_gezicht": true,
    "beschermd_gezicht_naam": "Rotterdam - Waterproject",
    "is_gemeentelijk_monument": true,
    "grondslag_gemeentelijk_monument": "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
  }
}
```

### In [VERA-referentiedata](https://www.coraveraonline.nl/index.php/Referentiedata:EENHEIDMONUMENT)-formaat

```python

import asyncio

from monumenten import MonumentenClient

async def main():
    bag_verblijfsobject_ids = [
        "0599010000360091",
        "0599010000486642",
        "0599010000281115",
        "0599010000146141",
    ]

    async with MonumentenClient() as client:
        result = await client.process_from_list(
            bag_verblijfsobject_ids,
            to_vera=True # zet to_vera=True
        )
        print(result)

# in een .py file"
if __name__ == "__main__":
    asyncio.run(main())

# in een .ipynb file (notebook):
await main()
```

```python
# OUTPUT
{
    '0599010000360091': [{'code': 'RIJ', 'naam': 'Rijksmonument'}],
    '0599010000486642': [],
    '0599010000281115': [{'code': 'STA', 'naam': 'Beschermd stadsgezicht'}],
    '0599010000146141': [
        {'code': 'STA', 'naam': 'Beschermd stadsgezicht'},
        {'code': 'GEM', 'naam': 'Gemeentelijk monument'}
    ]
}
```

### Met pandas 🐼

```python
import asyncio

import pandas as pd
from monumenten import MonumentenClient


async def main():

    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "0599010000360091",
                "0599010000486642",
                "0599010000360022",
                "0599010000360096",
                "0599010000183527",
                "0599010400025880",
                "0599010000281115",
                "0599010000146141",
            ]
        }
    ) # of lees van een csv of een ander bestand in

    async with MonumentenClient() as client:
        result = await client.process_from_df(
            df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
        )
        result.to_csv("monumenten.csv", index=False)

# in een .py file"
if __name__ == "__main__":
    asyncio.run(main())

# in een .ipynb file (notebook):
await main()
```

| bag_verblijfsobject_id | is_rijksmonument | rijksmonument_nummer | rijksmonument_url                                                | is_beschermd_gezicht | beschermd_gezicht_naam          | is_gemeentelijk_monument | grondslag                                                                              |
| ---------------------- | ---------------- | -------------------- | ---------------------------------------------------------------- | -------------------- | ------------------------------- | ------------------------ | -------------------------------------------------------------------------------------- |
| 0599010000360091       | True             | 524327               | https://monumentenregister.cultureelerfgoed.nl/monumenten/524327 | False                |                                 | False                    |                                                                                        |
| 0599010000486642       | False            |                      |                                                                  | False                |                                 | False                    |                                                                                        |
| 0599010000360022       | True             | 524327               | https://monumentenregister.cultureelerfgoed.nl/monumenten/524327 | False                |                                 | False                    |                                                                                        |
| 0599010000360096       | True             | 524327               | https://monumentenregister.cultureelerfgoed.nl/monumenten/524327 | False                |                                 | False                    |                                                                                        |
| 0599010000183527       | True             | 32807                | https://monumentenregister.cultureelerfgoed.nl/monumenten/32807  | True                 | Rotterdam - Scheepvaartkwartier | False                    |                                                                                        |
| 0599010400025880       | False            |                      |                                                                  | False                |                                 | False                    |                                                                                        |
| 0599010000281115       | False            |                      |                                                                  | True                 | Kralingen - Midden              | False                    |                                                                                        |
| 0599010000146141       | False            |                      |                                                                  | True                 | Rotterdam - Waterproject        | True                     | Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift) |
