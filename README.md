# Monumenten

Een Python package voor het ophalen van monumentgegevens van Nederlandse overheids-API's. Momenteel is het mogelijk om de status van rijksmonumenten en beschermde gezichten op te halen.

Door middel van de package is het mogelijk om, indienst gewenst, voor tienduizenden verblijfsobjecten per seconde monumentgegevens op te halen. Er zijn geen API-keys nodig.

## Installatie

`pip install monumenten`

## Gebruik

### Python-native 🐍

```python
import asyncio

from monumenten import MonumentenClient

async def main():
    bag_verblijfsobject_ids = [
        "0599010000360091",
        "0599010000486642",
        "0599010000281115",
    ]

    async with MonumentenClient() as client:
        result = await client.process_from_list(bag_verblijfsobject_ids)
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
  '0599010000360091': {
    'is_rijksmonument': True,
    'rijksmonument_nummer': '524327',
    'rijksmonument_url': 'https://monumenten.nl/monument/524327',
    'is_beschermd_gezicht': False,
    'beschermd_gezicht_naam': None
  },
  '0599010000486642': {
    'is_rijksmonument': False,
    'rijksmonument_nummer': None,
    'rijksmonument_url': None,
    'is_beschermd_gezicht': False,
    'beschermd_gezicht_naam': None
  },
  '0599010000281115': {
    'is_rijksmonument': False,
    'rijksmonument_nummer': None,
    'rijksmonument_url': None,
    'is_beschermd_gezicht': True,
    'beschermd_gezicht_naam': 'Kralingen - Midden'
  }
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

| bag_verblijfsobject_id | is_rijksmonument | rijksmonument_nummer | rijksmonument_url                     | is_beschermd_gezicht | beschermd_gezicht_naam          |
| ---------------------- | ---------------- | -------------------- | ------------------------------------- | -------------------- | ------------------------------- |
| 0599010000360091       | True             | 524327               | https://monumenten.nl/monument/524327 | False                |                                 |
| 0599010000486642       | False            |                      |                                       | False                |                                 |
| 0599010000360022       | True             | 524327               | https://monumenten.nl/monument/524327 | False                |                                 |
| 0599010000360096       | True             | 524327               | https://monumenten.nl/monument/524327 | False                |                                 |
| 0599010000183527       | True             | 32807                | https://monumenten.nl/monument/32807  | True                 | Rotterdam - Scheepvaartkwartier |
| 0599010400025880       | False            |                      |                                       | False                |                                 |
| 0599010000281115       | False            |                      |                                       | True                 | Kralingen - Midden              |
