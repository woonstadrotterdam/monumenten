import pandas as pd
import pytest
import pytest_asyncio
from monumenten import MonumentenClient


@pytest_asyncio.fixture(scope="function")
async def client():
    async with MonumentenClient() as client:
        yield client


@pytest.mark.asyncio
async def test_process_from_list(client):
    bag_verblijfsobject_ids = [
        "0599010000360091",
        "0599010000486642",
        "0599010000281115",
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)

    assert len(result) == 3

    # Test rijksmonument
    assert "0599010000360091" in result
    assert result["0599010000360091"]["is_rijksmonument"] is True
    assert result["0599010000360091"]["rijksmonument_nummer"] == "524327"
    assert (
        result["0599010000360091"]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert result["0599010000360091"]["is_beschermd_gezicht"] is False

    # Test non-monument
    assert "0599010000486642" in result
    assert result["0599010000486642"]["is_rijksmonument"] is False
    assert result["0599010000486642"]["rijksmonument_nummer"] is None
    assert result["0599010000486642"]["is_beschermd_gezicht"] is False

    # Test beschermd gezicht
    assert "0599010000281115" in result
    assert result["0599010000281115"]["is_rijksmonument"] is False
    assert result["0599010000281115"]["is_beschermd_gezicht"] is True
    assert result["0599010000281115"]["beschermd_gezicht_naam"] == "Kralingen - Midden"


@pytest.mark.asyncio
async def test_process_from_df(client):
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "0599010000360091",  # rijksmonument
                "0599010000486642",  # non-monument
                "0599010000183527",  # both rijksmonument and beschermd gezicht
                "0599010000281115",  # beschermd gezicht only
            ]
        }
    )

    result = await client.process_from_df(
        df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4

    # Test rijksmonument
    assert result.iloc[0]["bag_verblijfsobject_id"] == "0599010000360091"
    assert bool(result.iloc[0]["is_rijksmonument"]) is True
    assert result.iloc[0]["rijksmonument_nummer"] == "524327"
    assert (
        result.iloc[0]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert bool(result.iloc[0]["is_beschermd_gezicht"]) is False

    # Test non-monument
    assert result.iloc[1]["bag_verblijfsobject_id"] == "0599010000486642"
    assert bool(result.iloc[1]["is_rijksmonument"]) is False
    assert pd.isna(result.iloc[1]["rijksmonument_nummer"])
    assert bool(result.iloc[1]["is_beschermd_gezicht"]) is False

    # Test combined rijksmonument and beschermd gezicht
    assert result.iloc[2]["bag_verblijfsobject_id"] == "0599010000183527"
    assert bool(result.iloc[2]["is_rijksmonument"]) is True
    assert result.iloc[2]["rijksmonument_nummer"] == "32807"
    assert (
        result.iloc[2]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/32807"
    )
    assert bool(result.iloc[2]["is_beschermd_gezicht"]) is True
    assert result.iloc[2]["beschermd_gezicht_naam"] == "Rotterdam - Scheepvaartkwartier"

    # Test beschermd gezicht only
    assert result.iloc[3]["bag_verblijfsobject_id"] == "0599010000281115"
    assert bool(result.iloc[3]["is_rijksmonument"]) is False
    assert bool(result.iloc[3]["is_beschermd_gezicht"]) is True
    assert result.iloc[3]["beschermd_gezicht_naam"] == "Kralingen - Midden"
