"""Live tests voor provinciale monumenten (Noord-Holland en Drenthe)."""

import aiohttp
import pandas as pd
import pytest
import pytest_asyncio
from shapely.geometry import Point

from monumenten import MonumentenClient
from monumenten._api._provincies import DRENTHE, NOORD_HOLLAND, PROVINCIES
from monumenten._processing import _get_provinciegrenzen


@pytest_asyncio.fixture(scope="function")
async def client():
    async with MonumentenClient() as client:
        yield client


@pytest.mark.asyncio
async def test_process_from_list_provinciaal_monument(client: MonumentenClient):
    """Provinciale monumenten komen uit de services van Noord-Holland en Drenthe."""
    bag_verblijfsobject_ids = [
        "1680010000004810",  # Kerkbrink 7, Rolde: provinciaal monument Drenthe PM1-0001
        "0370010000004609",  # Zuiddijk 12, Zuidoostbeemster: Stelling van Amsterdam (NH)
        "0412010000582900",  # Heerenweg 186, Barsingerhorn: provinciaal beschermd dorpsgezicht, geen monument
        "0852010000008443",  # Zeedijk 2303, Uitdam: vakantiehuisje op monumentale dijk, geen monument
        "0599010000486642",  # Rotterdam: non-monument
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)
    assert len(result) == len(bag_verblijfsobject_ids)

    assert result["1680010000004810"]["provinciaal_monument"] is True
    assert (
        result["1680010000004810"]["provinciaal_monument_omschrijving"]
        == "PM1-0001 Dwarshuisboerderij Rolde"
    )
    assert result["1680010000004810"]["rijksmonument"] is False
    assert result["1680010000004810"]["gemeentelijk_monument"] is False

    assert result["0370010000004609"]["provinciaal_monument"] is True
    assert (
        result["0370010000004609"]["provinciaal_monument_omschrijving"]
        == "Stelling van Amsterdam"
    )

    for vo_id in ("0412010000582900", "0852010000008443", "0599010000486642"):
        assert result[vo_id]["provinciaal_monument"] is False
        assert result[vo_id]["provinciaal_monument_omschrijving"] is None

    vera = await client.process_from_list(bag_verblijfsobject_ids, to_vera=True)
    assert vera["1680010000004810"] == [{"code": "PRO", "naam": "Provinciaal monument"}]
    assert vera["0370010000004609"] == [{"code": "PRO", "naam": "Provinciaal monument"}]
    assert vera["0412010000582900"] == []


@pytest.mark.asyncio
async def test_process_from_df_provinciaal_monument(client: MonumentenClient):
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "1680010000004810",  # provinciaal monument Drenthe
                "0599010000360091",  # rijksmonument Rotterdam
            ]
        }
    )

    result = await client.process_from_df(
        df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
    )

    assert len(result) == 2
    kolommen = list(result.columns)
    assert kolommen.index("provinciaal_monument") + 1 == kolommen.index(
        "provinciaal_monument_omschrijving"
    )
    assert bool(result.iloc[0]["provinciaal_monument"]) is True
    assert (
        result.iloc[0]["provinciaal_monument_omschrijving"]
        == "PM1-0001 Dwarshuisboerderij Rolde"
    )
    assert bool(result.iloc[1]["provinciaal_monument"]) is False
    assert pd.isna(result.iloc[1]["provinciaal_monument_omschrijving"])
    assert bool(result.iloc[1]["rijksmonument"]) is True


@pytest.mark.asyncio
async def test_provinciegrenzen_pdok():
    """De grenzen van PDOK leggen adrespunten (RD) in de juiste provincie."""
    async with aiohttp.ClientSession() as session:
        gebieden = await _get_provinciegrenzen(session)

    assert set(gebieden) == {provincie.code for provincie in PROVINCIES}
    noord_holland = gebieden[NOORD_HOLLAND.code]
    drenthe = gebieden[DRENTHE.code]
    # Met de marge van 100 m kunnen losse delen samenvallen tot één polygoon
    assert {noord_holland.geom_type, drenthe.geom_type} <= {"Polygon", "MultiPolygon"}
    # Haarlemmerstraat 73: de straat hoort bij Hillegom (Zuid-Holland), het adres
    # ligt in Noord-Holland
    assert noord_holland.contains(Point(100433.584, 481351.582))
    assert drenthe.contains(Point(239495.0, 556475.0))  # Kerkbrink 7, Rolde
    # Leiden ligt in de bbox van Noord-Holland, maar niet in de provincie
    assert not noord_holland.contains(Point(93911.705, 464260.065))
