import pandas as pd
import pytest
import pytest_asyncio
from monumenten import MonumentenClient


@pytest_asyncio.fixture(scope="function")
async def client():
    async with MonumentenClient() as client:
        yield client


@pytest.mark.asyncio
async def test_process_from_list(client: MonumentenClient):
    bag_verblijfsobject_ids = [
        "0599010000360091",  # rijksmonument
        "0599010000486642",  # non-monument
        "0599010000281115",  # beschermd gezicht
        "0599010000076715",  # gemeentelijk monument
        "0599010000146141",  # beschermd stads gezicht en gemeentelijk monument
        "0232010000002251",  # gebouw ligt volgens kadaster op meerdere percelen
        "0599010000341377",  # rijksmonument volgens kadaster maar niet RCE
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)

    assert len(result) == len(bag_verblijfsobject_ids), (
        "Niet voor elk verblijfsobject een resultaat"
    )

    # Test rijksmonument
    assert "0599010000360091" in result
    assert isinstance(result["0599010000360091"], dict)
    assert result["0599010000360091"]["is_rijksmonument"] is True
    assert result["0599010000360091"]["rijksmonument_bron"] == ["RCE", "Kadaster"]
    assert result["0599010000360091"]["rijksmonument_nummer"] == "524327"
    assert (
        result["0599010000360091"]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert result["0599010000360091"]["is_beschermd_gezicht"] is False

    # Test non-monument
    assert "0599010000486642" in result
    assert isinstance(result["0599010000486642"], dict)
    assert result["0599010000486642"]["is_rijksmonument"] is False
    assert result["0599010000486642"]["rijksmonument_bron"] is None
    assert result["0599010000486642"]["rijksmonument_nummer"] is None
    assert result["0599010000486642"]["is_beschermd_gezicht"] is False

    # Test beschermd gezicht
    assert "0599010000281115" in result
    assert isinstance(result["0599010000281115"], dict)
    assert result["0599010000281115"]["is_rijksmonument"] is False
    assert result["0599010000281115"]["rijksmonument_bron"] is None
    assert result["0599010000281115"]["is_beschermd_gezicht"] is True
    assert result["0599010000281115"]["beschermd_gezicht_naam"] == "Kralingen - Midden"

    # Test gemeentelijk monument
    assert "0599010000076715" in result
    assert isinstance(result["0599010000076715"], dict)
    assert result["0599010000076715"]["is_rijksmonument"] is False
    assert result["0599010000076715"]["rijksmonument_bron"] is None
    assert result["0599010000076715"]["is_beschermd_gezicht"] is False
    assert result["0599010000076715"]["is_gemeentelijk_monument"] is True
    assert (
        result["0599010000076715"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test gemeentelijk monument op meerdere percelen
    assert "0232010000002251" in result
    assert isinstance(result["0232010000002251"], dict)
    assert result["0232010000002251"]["is_rijksmonument"] is False
    assert result["0232010000002251"]["rijksmonument_bron"] is None
    assert result["0232010000002251"]["is_beschermd_gezicht"] is False
    assert result["0232010000002251"]["is_gemeentelijk_monument"] is True
    assert (
        result["0232010000002251"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # test beschermd stads gezicht en gemeentelijk monument
    assert "0599010000146141" in result
    assert isinstance(result["0599010000146141"], dict)
    assert result["0599010000146141"]["is_rijksmonument"] is False
    assert result["0599010000146141"]["rijksmonument_bron"] is None
    assert result["0599010000146141"]["is_beschermd_gezicht"] is True
    assert result["0599010000146141"]["is_gemeentelijk_monument"] is True
    assert (
        result["0599010000146141"]["beschermd_gezicht_naam"]
        == "Rotterdam - Waterproject"
    )
    assert (
        result["0599010000146141"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test rijksmonument volgens kadaster maar niet RCE
    assert "0599010000341377" in result
    assert isinstance(result["0599010000341377"], dict)
    assert result["0599010000341377"]["is_rijksmonument"] is True
    assert result["0599010000341377"]["rijksmonument_bron"] == ["Kadaster"]
    assert result["0599010000341377"]["rijksmonument_nummer"] is None
    assert result["0599010000341377"]["is_beschermd_gezicht"] is False
    assert result["0599010000341377"]["beschermd_gezicht_naam"] is None
    assert result["0599010000341377"]["is_gemeentelijk_monument"] is False
    assert result["0599010000341377"]["grondslag_gemeentelijk_monument"] is None


@pytest.mark.asyncio
async def test_process_from_list_vera(client: MonumentenClient):
    bag_verblijfsobject_ids = [
        "0599010000360091",  # rijksmonument
        "0599010000486642",  # non-monument
        "0599010000281115",  # beschermd gezicht
        "0599010000076715",  # gemeentelijk monument
        "0599010000146141",  # beschermd stads gezicht en gemeentelijk monument
        "0599010000341377",  # rijksmonument volgens kadaster maar niet RCE
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids, to_vera=True)
    assert len(result) == len(bag_verblijfsobject_ids), (
        "Niet voor elk verblijfsobject een resultaat"
    )
    # Test rijksmonument
    assert "0599010000360091" in result
    assert isinstance(result["0599010000360091"], list)
    assert len(result["0599010000360091"]) == 1

    assert result["0599010000360091"][0]["code"] == "RIJ"
    assert result["0599010000360091"][0]["naam"] == "Rijksmonument"
    assert result["0599010000360091"][0]["bron"] == ["RCE", "Kadaster"]
    # Test non-monument
    assert "0599010000486642" in result
    assert len(result["0599010000486642"]) == 0

    # Test beschermd gezicht
    assert "0599010000281115" in result
    assert isinstance(result["0599010000281115"], list)
    assert len(result["0599010000281115"]) == 1
    assert result["0599010000281115"][0]["code"] == "SGR"
    assert result["0599010000281115"][0]["naam"] == "Rijksbeschermd stadsgezicht"

    # Test gemeentelijk monument
    assert "0599010000076715" in result
    assert isinstance(result["0599010000076715"], list)
    assert len(result["0599010000076715"]) == 1
    assert result["0599010000076715"][0]["code"] == "GEM"
    assert result["0599010000076715"][0]["naam"] == "Gemeentelijk monument"

    # test beschermd stads gezicht en gemeentelijk monument
    assert "0599010000146141" in result
    assert isinstance(result["0599010000146141"], list)
    assert len(result["0599010000146141"]) == 2
    assert result["0599010000146141"][0]["code"] == "SGR"
    assert result["0599010000146141"][0]["naam"] == "Rijksbeschermd stadsgezicht"
    assert result["0599010000146141"][1]["code"] == "GEM"
    assert result["0599010000146141"][1]["naam"] == "Gemeentelijk monument"

    # Test rijksmonument volgens kadaster maar niet RCE
    assert "0599010000341377" in result
    assert isinstance(result["0599010000341377"], list)
    assert len(result["0599010000341377"]) == 1
    assert result["0599010000341377"][0]["code"] == "RIJ"
    assert result["0599010000341377"][0]["naam"] == "Rijksmonument"
    assert result["0599010000341377"][0]["bron"] == ["Kadaster"]


@pytest.mark.asyncio
async def test_process_from_df(client: MonumentenClient):
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "0599010000360091",  # rijksmonument
                "0599010000486642",  # non-monument
                "0599010000183527",  # both rijksmonument and beschermd gezicht
                "0599010000281115",  # beschermd gezicht only
                "0599010000146141",  # gemeentelijk monument
                "0599010000341377",  # rijksmonument volgens kadaster maar niet RCE
            ]
        }
    )

    result = await client.process_from_df(
        df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(input_df), "Niet voor elk verblijfsobject een resultaat"

    # Test rijksmonument
    assert result.iloc[0]["bag_verblijfsobject_id"] == "0599010000360091"
    assert bool(result.iloc[0]["is_rijksmonument"]) is True
    assert result.iloc[0]["rijksmonument_bron"] == "RCE, Kadaster"
    assert result.iloc[0]["rijksmonument_nummer"] == "524327"
    assert (
        result.iloc[0]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert bool(result.iloc[0]["is_beschermd_gezicht"]) is False

    # Test non-monument
    assert result.iloc[1]["bag_verblijfsobject_id"] == "0599010000486642"
    assert bool(result.iloc[1]["is_rijksmonument"]) is False
    assert pd.isna(result.iloc[1]["rijksmonument_bron"])
    assert pd.isna(result.iloc[1]["rijksmonument_nummer"])
    assert bool(result.iloc[1]["is_beschermd_gezicht"]) is False

    # Test combined rijksmonument and beschermd gezicht
    assert result.iloc[2]["bag_verblijfsobject_id"] == "0599010000183527"
    assert bool(result.iloc[2]["is_rijksmonument"]) is True
    assert result.iloc[2]["rijksmonument_nummer"] == "32807"
    assert result.iloc[2]["rijksmonument_bron"] == "RCE, Kadaster"
    assert (
        result.iloc[2]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/32807"
    )
    assert bool(result.iloc[2]["is_beschermd_gezicht"]) is True
    assert result.iloc[2]["beschermd_gezicht_naam"] == "Rotterdam - Scheepvaartkwartier"

    # Test beschermd gezicht only
    assert result.iloc[3]["bag_verblijfsobject_id"] == "0599010000281115"
    assert bool(result.iloc[3]["is_rijksmonument"]) is False
    assert pd.isna(result.iloc[3]["rijksmonument_bron"])
    assert bool(result.iloc[3]["is_beschermd_gezicht"]) is True
    assert result.iloc[3]["beschermd_gezicht_naam"] == "Kralingen - Midden"

    # Test beschermd stads gezicht en gemeentelijk monument
    assert result.iloc[4]["bag_verblijfsobject_id"] == "0599010000146141"
    assert bool(result.iloc[4]["is_rijksmonument"]) is False
    assert pd.isna(result.iloc[4]["rijksmonument_bron"])
    assert bool(result.iloc[4]["is_beschermd_gezicht"]) is True
    assert result.iloc[4]["beschermd_gezicht_naam"] == "Rotterdam - Waterproject"
    assert bool(result.iloc[4]["is_gemeentelijk_monument"]) is True
    assert (
        result.iloc[4]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test rijksmonument volgens kadaster maar niet RCE
    assert result.iloc[5]["bag_verblijfsobject_id"] == "0599010000341377"
    assert bool(result.iloc[5]["is_rijksmonument"]) is True
    assert result.iloc[5]["rijksmonument_bron"] == "Kadaster"
    assert pd.isna(result.iloc[5]["rijksmonument_nummer"])
    assert bool(result.iloc[5]["is_beschermd_gezicht"]) is False
    assert pd.isna(result.iloc[5]["beschermd_gezicht_naam"])
    assert bool(result.iloc[5]["is_gemeentelijk_monument"]) is False
    assert pd.isna(result.iloc[5]["grondslag_gemeentelijk_monument"])


@pytest.mark.asyncio
async def test_process_from_list_invalid_ids(client: MonumentenClient):
    bag_verblijfsobject_ids = ["123"]

    # test both from_list and from_df

    # from_list
    with pytest.raises(ValueError):
        await client.process_from_list(bag_verblijfsobject_ids)

    # from_df
    with pytest.raises(ValueError):
        await client.process_from_df(
            df=pd.DataFrame({"bag_verblijfsobject_id": ["123"]}),
            verblijfsobject_id_col="bag_verblijfsobject_id",
        )
