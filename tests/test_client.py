import itertools
import warnings
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import AsyncMock, patch

import geopandas as gpd
import pandas as pd
import pytest
import pytest_asyncio
from shapely.geometry import box

from monumenten import MonumentenClient, MonumentStatusOnbepaaldWarning
from monumenten._api._provincies import ProvincialeMonumentenError


@pytest_asyncio.fixture(scope="function")
async def client():
    async with MonumentenClient() as client:
        yield client


@pytest.mark.asyncio
async def test_rijksmonument_nummer_is_numeriek_laagste(client: MonumentenClient):
    """#70: de RCE noemt Dorpsstraat 5 in Aarle-Rixtel bij rijksmonument 6936, de
    kerk, en 520165, het beeld ervoor. Het laagste nummer is het gebouw zelf."""
    result = await client.process_from_list(["1659010000003181"])

    assert result["1659010000003181"]["rijksmonument_nummer"] == "6936"


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
    assert result["0599010000360091"]["rijksmonument"] is True
    assert result["0599010000360091"]["rijksmonument_bron"] == ["RCE", "Kadaster"]
    assert result["0599010000360091"]["rijksmonument_nummer"] == "524327"
    assert (
        result["0599010000360091"]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert result["0599010000360091"]["rijksbeschermd_gezicht"] is False
    assert result["0599010000360091"]["rijksmonument_voorbescherming"] is False

    # Test non-monument
    assert "0599010000486642" in result
    assert isinstance(result["0599010000486642"], dict)
    assert result["0599010000486642"]["rijksmonument_voorbescherming"] is False
    assert result["0599010000486642"]["rijksmonument"] is False
    assert result["0599010000486642"]["rijksmonument_bron"] is None
    assert result["0599010000486642"]["rijksmonument_nummer"] is None
    assert result["0599010000486642"]["rijksbeschermd_gezicht"] is False

    # Test beschermd gezicht
    assert "0599010000281115" in result
    assert isinstance(result["0599010000281115"], dict)
    assert result["0599010000281115"]["rijksmonument"] is False
    assert result["0599010000281115"]["rijksmonument_bron"] is None
    assert result["0599010000281115"]["rijksbeschermd_gezicht"] is True
    assert (
        result["0599010000281115"]["rijksbeschermd_gezicht_naam"]
        == "Kralingen - Midden"
    )

    # Test gemeentelijk monument
    assert "0599010000076715" in result
    assert isinstance(result["0599010000076715"], dict)
    assert result["0599010000076715"]["rijksmonument"] is False
    assert result["0599010000076715"]["rijksmonument_bron"] is None
    assert result["0599010000076715"]["rijksbeschermd_gezicht"] is False
    assert result["0599010000076715"]["gemeentelijk_monument"] is True
    assert (
        result["0599010000076715"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test gemeentelijk monument op meerdere percelen
    assert "0232010000002251" in result
    assert isinstance(result["0232010000002251"], dict)
    assert result["0232010000002251"]["rijksmonument"] is False
    assert result["0232010000002251"]["rijksmonument_bron"] is None
    assert result["0232010000002251"]["rijksbeschermd_gezicht"] is False
    assert result["0232010000002251"]["gemeentelijk_monument"] is True
    assert (
        result["0232010000002251"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # test beschermd stads gezicht en gemeentelijk monument
    assert "0599010000146141" in result
    assert isinstance(result["0599010000146141"], dict)
    assert result["0599010000146141"]["rijksmonument"] is False
    assert result["0599010000146141"]["rijksmonument_bron"] is None
    assert result["0599010000146141"]["rijksbeschermd_gezicht"] is True
    assert result["0599010000146141"]["gemeentelijk_monument"] is True
    assert (
        result["0599010000146141"]["rijksbeschermd_gezicht_naam"]
        == "Rotterdam - Waterproject"
    )
    assert (
        result["0599010000146141"]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test rijksmonument volgens kadaster maar niet RCE: het nummer komt via de akte
    assert "0599010000341377" in result
    assert isinstance(result["0599010000341377"], dict)
    assert result["0599010000341377"]["rijksmonument"] is True
    assert result["0599010000341377"]["rijksmonument_bron"] == ["Kadaster"]
    assert result["0599010000341377"]["rijksmonument_nummer"] == "513884"
    assert result["0599010000341377"]["rijksbeschermd_gezicht"] is False
    assert result["0599010000341377"]["rijksbeschermd_gezicht_naam"] is None
    assert result["0599010000341377"]["gemeentelijk_monument"] is False
    assert result["0599010000341377"]["grondslag_gemeentelijk_monument"] is None
    assert result["0599010000341377"]["rijksmonument_voorbescherming"] is False


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
    assert bool(result.iloc[0]["rijksmonument"]) is True
    assert result.iloc[0]["rijksmonument_bron"] == "RCE, Kadaster"
    assert result.iloc[0]["rijksmonument_nummer"] == "524327"
    assert (
        result.iloc[0]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/524327"
    )
    assert bool(result.iloc[0]["rijksbeschermd_gezicht"]) is False

    # Test non-monument
    assert result.iloc[1]["bag_verblijfsobject_id"] == "0599010000486642"
    assert bool(result.iloc[1]["rijksmonument"]) is False
    assert pd.isna(result.iloc[1]["rijksmonument_bron"])
    assert pd.isna(result.iloc[1]["rijksmonument_nummer"])
    assert bool(result.iloc[1]["rijksbeschermd_gezicht"]) is False

    # Test combined rijksmonument and beschermd gezicht
    assert result.iloc[2]["bag_verblijfsobject_id"] == "0599010000183527"
    assert bool(result.iloc[2]["rijksmonument"]) is True
    # Twee pakhuizen in één blok (32741 en 32807): het laagste nummer telt
    assert result.iloc[2]["rijksmonument_nummer"] == "32741"
    assert result.iloc[2]["rijksmonument_bron"] == "RCE, Kadaster"
    assert (
        result.iloc[2]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/32741"
    )
    assert bool(result.iloc[2]["rijksbeschermd_gezicht"]) is True
    assert (
        result.iloc[2]["rijksbeschermd_gezicht_naam"]
        == "Rotterdam - Scheepvaartkwartier"
    )

    # Test beschermd gezicht only
    assert result.iloc[3]["bag_verblijfsobject_id"] == "0599010000281115"
    assert bool(result.iloc[3]["rijksmonument"]) is False
    assert pd.isna(result.iloc[3]["rijksmonument_bron"])
    assert bool(result.iloc[3]["rijksbeschermd_gezicht"]) is True
    assert result.iloc[3]["rijksbeschermd_gezicht_naam"] == "Kralingen - Midden"

    # Test beschermd stads gezicht en gemeentelijk monument
    assert result.iloc[4]["bag_verblijfsobject_id"] == "0599010000146141"
    assert bool(result.iloc[4]["rijksmonument"]) is False
    assert pd.isna(result.iloc[4]["rijksmonument_bron"])
    assert bool(result.iloc[4]["rijksbeschermd_gezicht"]) is True
    assert result.iloc[4]["rijksbeschermd_gezicht_naam"] == "Rotterdam - Waterproject"
    assert bool(result.iloc[4]["gemeentelijk_monument"]) is True
    assert (
        result.iloc[4]["grondslag_gemeentelijk_monument"]
        == "Gemeentewet: Aanwijzing gemeentelijk monument (voorbescherming, aanwijzing, afschrift)"
    )

    # Test rijksmonument volgens kadaster maar niet RCE: het nummer komt via de akte
    assert result.iloc[5]["bag_verblijfsobject_id"] == "0599010000341377"
    assert bool(result.iloc[5]["rijksmonument"]) is True
    assert result.iloc[5]["rijksmonument_bron"] == "Kadaster"
    assert result.iloc[5]["rijksmonument_nummer"] == "513884"
    assert bool(result.iloc[5]["rijksbeschermd_gezicht"]) is False
    assert pd.isna(result.iloc[5]["rijksbeschermd_gezicht_naam"])
    assert bool(result.iloc[5]["gemeentelijk_monument"]) is False
    assert pd.isna(result.iloc[5]["grondslag_gemeentelijk_monument"])


@pytest.mark.asyncio
async def test_process_from_list_invalid_ids(client: MonumentenClient):
    """Only invalid IDs trigger a warning and then ValueError (no valid rows to process)."""
    bag_verblijfsobject_ids = ["123"]

    # from_list: expect warning then ValueError
    with pytest.warns(Warning, match="onjuiste verblijfsobject"):
        with pytest.raises(ValueError, match="Geen enkel geldig"):
            await client.process_from_list(bag_verblijfsobject_ids)

    # from_df: expect warning then ValueError
    with pytest.warns(Warning, match="onjuiste verblijfsobject"):
        with pytest.raises(ValueError, match="Geen enkel geldig"):
            await client.process_from_df(
                df=pd.DataFrame({"bag_verblijfsobject_id": ["123"]}),
                verblijfsobject_id_col="bag_verblijfsobject_id",
            )


@pytest.mark.asyncio
async def test_process_from_df_invalid_ids_warning_and_filtering(
    client: MonumentenClient,
):
    """Mixed valid and invalid IDs: warning is emitted and only valid rows are returned."""
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "0599010000486642",  # valid (non-monument)
                "123",  # invalid: wrong length
                "tooshort",  # invalid: non-digit
            ]
        }
    )

    with pytest.warns(Warning, match="onjuiste verblijfsobject") as record:
        result = await client.process_from_df(
            df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
        )

    assert len(record) == 1
    msg = str(record[0].message)
    assert "2 onjuiste verblijfsobject ID's" in msg or "2" in msg
    assert "123" in msg
    assert "tooshort" in msg

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["bag_verblijfsobject_id"] == "0599010000486642"


@pytest.mark.asyncio
async def test_process_from_list_duplicate_handling(client: MonumentenClient):
    bag_verblijfsobject_ids = [
        "1916010000074542"
    ]  # verblijfsobject met zowel EWE als GWA grondslagcode

    result = await client.process_from_list(bag_verblijfsobject_ids)

    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"
    assert "1916010000074542" in result

    # Test verblijfsobject met meerdere monumenttypes
    assert isinstance(result["1916010000074542"], dict)
    assert result["1916010000074542"]["rijksmonument"] is True
    assert result["1916010000074542"]["gemeentelijk_monument"] is True
    assert result["1916010000074542"]["rijksmonument_bron"] == ["Kadaster"]
    # Sluisplein 16; de RCE noemt alleen nummer 14, het Kadaster het hele perceel
    assert result["1916010000074542"]["rijksmonument_nummer"] == "25718"
    assert (
        result["1916010000074542"]["rijksmonument_url"]
        == "https://monumentenregister.cultureelerfgoed.nl/monumenten/25718"
    )
    assert result["1916010000074542"]["grondslag_gemeentelijk_monument"] is not None
    assert (
        "Gemeentewet" in result["1916010000074542"]["grondslag_gemeentelijk_monument"]
    )
    assert result["1916010000074542"]["rijksbeschermd_gezicht"] is True
    assert result["1916010000074542"]["rijksbeschermd_gezicht_naam"] is not None
    assert "Leidschendam" in result["1916010000074542"]["rijksbeschermd_gezicht_naam"]


@pytest.mark.asyncio
async def test_process_from_list_duplicate_handling_vera(client: MonumentenClient):
    bag_verblijfsobject_ids = [
        "1916010000074542"
    ]  # verblijfsobject met zowel EWE als GWA grondslagcode

    result = await client.process_from_list(bag_verblijfsobject_ids, to_vera=True)

    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"
    assert "1916010000074542" in result

    # Test VERA output voor verblijfsobject met meerdere monumenttypes
    assert isinstance(result["1916010000074542"], list)
    assert len(result["1916010000074542"]) >= 2  # minstens 2 monumenttypes

    # Controleer voor rijksmonument
    rijksmonument_found = any(
        item["code"] == "RIJ" for item in result["1916010000074542"]
    )
    assert rijksmonument_found

    # Controleer voor gemeentelijk monument
    gemeentelijk_monument_found = any(
        item["code"] == "GEM" for item in result["1916010000074542"]
    )
    assert gemeentelijk_monument_found

    # Controleer voor beschermd gezicht
    beschermd_gezicht_found = any(
        item["code"] == "SGR" for item in result["1916010000074542"]
    )
    assert beschermd_gezicht_found


@pytest.mark.asyncio
async def test_process_from_df_duplicate_handling(client: MonumentenClient):
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "1916010000074542"
            ]  # verblijfsobject met zowel EWE als GWA grondslagcode
        }
    )

    result = await client.process_from_df(
        df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"

    # Test verblijfsobject met meerdere monumenttypes
    row = result.iloc[0]
    assert bool(row["rijksmonument"]) is True
    assert bool(row["gemeentelijk_monument"]) is True
    assert row["rijksmonument_bron"] == "Kadaster"
    assert row["rijksmonument_nummer"] == "25718"
    assert not pd.isna(row["grondslag_gemeentelijk_monument"])
    assert "Gemeentewet" in row["grondslag_gemeentelijk_monument"]
    assert bool(row["rijksbeschermd_gezicht"]) is True
    assert not pd.isna(row["rijksbeschermd_gezicht_naam"])
    assert "Leidschendam" in row["rijksbeschermd_gezicht_naam"]


@pytest.mark.asyncio
async def test_process_from_list_multiple_beschermd_gezichten(client: MonumentenClient):
    """Test verblijfsobject met meerdere beschermde gezichten en gemeentelijk monument."""
    bag_verblijfsobject_ids = [
        "0014010011011647"  # verblijfsobject met meerdere beschermde gezichten en gemeentelijk monument
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)

    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"
    assert "0014010011011647" in result

    # Test verblijfsobject met meerdere beschermde gezichten
    assert isinstance(result["0014010011011647"], dict)
    assert result["0014010011011647"]["rijksmonument"] is False
    assert result["0014010011011647"]["gemeentelijk_monument"] is True
    assert result["0014010011011647"]["rijksbeschermd_gezicht"] is True

    # Controleer dat beschermd gezicht naam concatenated is
    rijksbeschermd_gezicht_naam = result["0014010011011647"][
        "rijksbeschermd_gezicht_naam"
    ]
    assert rijksbeschermd_gezicht_naam is not None
    assert "Groningen" in rijksbeschermd_gezicht_naam
    assert "Schildersbuurt" in rijksbeschermd_gezicht_naam
    assert ", " in rijksbeschermd_gezicht_naam  # Controleer concatenation

    # Controleer gemeentelijk monument details
    assert result["0014010011011647"]["grondslag_gemeentelijk_monument"] is not None
    assert (
        "Gemeentewet" in result["0014010011011647"]["grondslag_gemeentelijk_monument"]
    )


@pytest.mark.asyncio
async def test_process_from_list_multiple_beschermd_gezichten_vera(
    client: MonumentenClient,
):
    """Test VERA output voor verblijfsobject met meerdere beschermde gezichten."""
    bag_verblijfsobject_ids = [
        "0014010011011647"  # verblijfsobject met meerdere beschermde gezichten en gemeentelijk monument
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids, to_vera=True)

    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"
    assert "0014010011011647" in result

    # Test VERA output voor verblijfsobject met meerdere beschermde gezichten
    assert isinstance(result["0014010011011647"], list)
    assert (
        len(result["0014010011011647"]) == 2
    )  # beschermd gezicht + gemeentelijk monument

    # Controleer voor beschermd gezicht (SGR)
    beschermd_gezicht_found = any(
        item["code"] == "SGR" for item in result["0014010011011647"]
    )
    assert beschermd_gezicht_found

    # Controleer voor gemeentelijk monument (GEM)
    gemeentelijk_monument_found = any(
        item["code"] == "GEM" for item in result["0014010011011647"]
    )
    assert gemeentelijk_monument_found


@pytest.mark.asyncio
async def test_process_from_df_multiple_beschermd_gezichten(client: MonumentenClient):
    """Test DataFrame output voor verblijfsobject met meerdere beschermde gezichten."""
    input_df = pd.DataFrame(
        {
            "bag_verblijfsobject_id": [
                "0014010011011647"  # verblijfsobject met meerdere beschermde gezichten en gemeentelijk monument
            ]
        }
    )

    result = await client.process_from_df(
        df=input_df, verblijfsobject_id_col="bag_verblijfsobject_id"
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1, "Niet voor elk verblijfsobject een resultaat"

    # Test verblijfsobject met meerdere beschermde gezichten
    row = result.iloc[0]
    assert bool(row["rijksmonument"]) is False
    assert bool(row["gemeentelijk_monument"]) is True
    assert bool(row["rijksbeschermd_gezicht"]) is True

    # Controleer dat beschermd gezicht naam concatenated is
    rijksbeschermd_gezicht_naam = row["rijksbeschermd_gezicht_naam"]
    assert not pd.isna(rijksbeschermd_gezicht_naam)
    assert "Groningen" in rijksbeschermd_gezicht_naam
    assert "Schildersbuurt" in rijksbeschermd_gezicht_naam
    assert ", " in rijksbeschermd_gezicht_naam  # Controleer concatenation

    # Controleer gemeentelijk monument details
    assert not pd.isna(row["grondslag_gemeentelijk_monument"])
    assert "Gemeentewet" in row["grondslag_gemeentelijk_monument"]


# ---------------------------------------------------------------------------
# Voorbescherming rijksmonument (Kadaster grondslagcode EWD)
# ---------------------------------------------------------------------------


_VO_ALLEEN_EWD = "0599010000000001"
_VO_ALLEEN_EWE = "0599010000000002"
_VO_RCE_EN_EWD = "0599010000000003"


# Adrespunten in RD, zoals de BAG van PDOK ze levert
_ROTTERDAM_RD = (92500.0, 437500.0)
_HAARLEM_RD = (103800.0, 488600.0)
_LEIDEN_RD = (93911.705, 464260.065)


def _beperking(identificatie: str, grondslagcode: str | None) -> dict:
    """Een rij zoals _query_beperkingen die teruggeeft."""
    return {
        "identificatie": identificatie,
        "grondslagcode": grondslagcode,
        "grondslag_gemeentelijk_monument": None,
    }


@contextmanager
def _mock_api_met_voorbescherming() -> Iterator[None]:
    """Mockt alleen de API-laag, zodat _process_batch en _query echt draaien.

    Daardoor heeft de data dezelfde vorm als in productie: een adres met alleen
    EWD komt pas via de merge met de voorbescherming in het rijksmonumentenframe.
    """
    rijksmonumenten = [
        {"identificatie": _VO_RCE_EN_EWD, "rijksmonument_nummer": "12345"}
    ]
    beperkingen = [
        _beperking(_VO_ALLEEN_EWD, "EWD"),
        _beperking(_VO_ALLEEN_EWE, "EWE"),
        _beperking(_VO_RCE_EN_EWD, "EWD"),
    ]
    # Rotterdam ligt buiten de bbox van Noord-Holland en Drenthe, dus er wordt
    # geen provincie bevraagd
    adrespunten = {
        vo: _ROTTERDAM_RD for vo in (_VO_ALLEEN_EWD, _VO_ALLEEN_EWE, _VO_RCE_EN_EWD)
    }
    geen_gezichten = gpd.GeoDataFrame(
        columns=["rijksbeschermd_gezicht_naam", "geometry"],
        geometry="geometry",
        crs="EPSG:28992",
    )
    with (
        patch(
            "monumenten._processing._query_rijksmonumenten",
            AsyncMock(return_value=rijksmonumenten),
        ),
        patch(
            "monumenten._processing._query_adrespunten",
            AsyncMock(return_value=adrespunten),
        ),
        patch(
            "monumenten._processing._query_beperkingen",
            AsyncMock(return_value=beperkingen),
        ),
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            AsyncMock(return_value=geen_gezichten),
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_voorbescherming_gemockt_df(client: MonumentenClient):
    """Voorbescherming: rijksmonument False, aparte vlag True, juiste kolomvolgorde."""
    with _mock_api_met_voorbescherming():
        result = await client.process_from_df(
            pd.DataFrame(
                {
                    "bag_verblijfsobject_id": [
                        _VO_ALLEEN_EWD,
                        _VO_ALLEEN_EWE,
                        _VO_RCE_EN_EWD,
                    ]
                }
            ),
            "bag_verblijfsobject_id",
        )

    assert list(result.columns) == [
        "bag_verblijfsobject_id",
        "rijksmonument",
        "rijksmonument_bron",
        "rijksmonument_nummer",
        "rijksmonument_url",
        "rijksmonument_voorbescherming",
        "rijksbeschermd_gezicht",
        "rijksbeschermd_gezicht_naam",
        "gemeentelijk_monument",
        "grondslag_gemeentelijk_monument",
        "provinciaal_monument",
        "provinciaal_monument_omschrijving",
    ]
    rijen = result.set_index("bag_verblijfsobject_id")

    # Alleen EWD: geen rijksmonument, wel voorbescherming
    row = rijen.loc[_VO_ALLEEN_EWD]
    assert bool(row["rijksmonument"]) is False
    assert pd.isna(row["rijksmonument_bron"])
    assert pd.isna(row["rijksmonument_nummer"])
    assert pd.isna(row["rijksmonument_url"])
    assert bool(row["rijksmonument_voorbescherming"]) is True
    assert bool(row["gemeentelijk_monument"]) is False
    assert bool(row["provinciaal_monument"]) is False

    # Alleen EWE: rijksmonument via Kadaster, geen voorbescherming
    row = rijen.loc[_VO_ALLEEN_EWE]
    assert bool(row["rijksmonument"]) is True
    assert row["rijksmonument_bron"] == "Kadaster"
    assert bool(row["rijksmonument_voorbescherming"]) is False

    # RCE en EWD: rijksmonument via RCE, EWD telt niet als Kadaster-bron
    row = rijen.loc[_VO_RCE_EN_EWD]
    assert bool(row["rijksmonument"]) is True
    assert row["rijksmonument_bron"] == "RCE"
    assert row["rijksmonument_nummer"] == "12345"
    assert bool(row["rijksmonument_voorbescherming"]) is True


@pytest.mark.asyncio
async def test_voorbescherming_gemockt_vera(client: MonumentenClient):
    """Voorbescherming heeft geen VERA-code (EENHEIDMONUMENT kent geen voorbescherming)."""
    with _mock_api_met_voorbescherming():
        result = await client.process_from_list([_VO_ALLEEN_EWD], to_vera=True)

    assert result[_VO_ALLEEN_EWD] == []


_VO_HAARLEM = "0392010000000001"  # Noord-Holland, kandidaat
_VO_LEIDEN = "0546010000000001"  # in de bbox van Noord-Holland, buiten de grens


@contextmanager
def _mock_api_noord_holland_plat() -> Iterator[None]:
    """Mockt de API-laag met Noord-Holland onbereikbaar.

    Haarlem heeft ook een gemeentelijk monument en een rijksmonument; die
    statussen blijven bepaald.
    """
    beperkingen = [
        {
            "identificatie": _VO_HAARLEM,
            "grondslagcode": "GG",
            "grondslag_gemeentelijk_monument": "Gemeentewet",
        },
    ]
    adrespunten = {_VO_HAARLEM: _HAARLEM_RD, _VO_LEIDEN: _LEIDEN_RD}
    geen_gezichten = gpd.GeoDataFrame(
        columns=["rijksbeschermd_gezicht_naam", "geometry"],
        geometry="geometry",
        crs="EPSG:28992",
    )
    with (
        patch(
            "monumenten._processing._query_rijksmonumenten",
            AsyncMock(
                return_value=[
                    {"identificatie": _VO_HAARLEM, "rijksmonument_nummer": "12345"}
                ]
            ),
        ),
        patch(
            "monumenten._processing._query_adrespunten",
            AsyncMock(return_value=adrespunten),
        ),
        patch(
            "monumenten._processing._query_beperkingen",
            AsyncMock(return_value=beperkingen),
        ),
        patch(
            "monumenten._processing._get_beschermde_gezichten",
            AsyncMock(return_value=geen_gezichten),
        ),
        patch(
            "monumenten._processing._get_provinciegrenzen",
            AsyncMock(
                return_value={
                    "27": box(100000, 470000, 150000, 580000),
                    "22": box(200000, 520000, 270000, 580000),
                }
            ),
        ),
        patch(
            "monumenten._processing._get_provinciale_monumenten",
            AsyncMock(side_effect=ProvincialeMonumentenError("Noord-Holland plat")),
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_provincie_plat_gemockt_df(client: MonumentenClient):
    """Een uitgevallen provincie geeft pd.NA en een waarschuwing, alleen voor haar
    kandidaten; de andere statussen blijven bepaald."""
    with _mock_api_noord_holland_plat():
        with pytest.warns(MonumentStatusOnbepaaldWarning) as meldingen:
            result = await client.process_from_df(
                pd.DataFrame({"bag_verblijfsobject_id": [_VO_HAARLEM, _VO_LEIDEN]}),
                "bag_verblijfsobject_id",
            )

    assert meldingen[0].message.verblijfsobject_ids == {"Noord-Holland": [_VO_HAARLEM]}
    # De oorzaak staat in de melding, zodat een kapotte service te onderscheiden
    # is van een filter dat niets meer herkent
    assert (
        "Noord-Holland (1 verblijfsobject(en), ProvincialeMonumentenError: "
        "Noord-Holland plat)"
    ) in str(meldingen[0].message)
    assert result["provinciaal_monument"].dtype == "boolean"
    rijen = result.set_index("bag_verblijfsobject_id")
    assert rijen.loc[_VO_HAARLEM, "provinciaal_monument"] is pd.NA
    assert bool(rijen.loc[_VO_HAARLEM, "rijksmonument"]) is True
    assert bool(rijen.loc[_VO_HAARLEM, "gemeentelijk_monument"]) is True
    assert bool(rijen.loc[_VO_LEIDEN, "provinciaal_monument"]) is False


@pytest.mark.asyncio
async def test_provincie_plat_gemockt_list_en_vera(client: MonumentenClient):
    """Onbepaald is None; in VERA is de hele value None, zodat PRO niet stil
    wegvalt."""
    with _mock_api_noord_holland_plat():
        with pytest.warns(MonumentStatusOnbepaaldWarning):
            result = await client.process_from_list([_VO_HAARLEM, _VO_LEIDEN])
        with pytest.warns(MonumentStatusOnbepaaldWarning):
            vera = await client.process_from_list(
                [_VO_HAARLEM, _VO_LEIDEN], to_vera=True
            )

    assert result[_VO_HAARLEM]["provinciaal_monument"] is None
    assert result[_VO_HAARLEM]["gemeentelijk_monument"] is True
    assert result[_VO_LEIDEN]["provinciaal_monument"] is False
    assert vera[_VO_HAARLEM] is None
    assert vera[_VO_LEIDEN] == []


@pytest.mark.asyncio
async def test_provincie_bereikbaar_geen_waarschuwing(client: MonumentenClient):
    with _mock_api_met_voorbescherming():
        with warnings.catch_warnings():
            warnings.simplefilter("error", MonumentStatusOnbepaaldWarning)
            result = await client.process_from_df(
                pd.DataFrame({"bag_verblijfsobject_id": [_VO_ALLEEN_EWD]}),
                "bag_verblijfsobject_id",
            )

    assert result["provinciaal_monument"].dtype == "boolean"
    assert bool(result.iloc[0]["provinciaal_monument"]) is False


@pytest.mark.asyncio
async def test_process_from_list_beperkingsgebied_bevat_adrespunt(
    client: MonumentenClient,
):
    """Een beperking telt alleen als het adrespunt binnen het beperkingsgebied ligt.

    Een beperking hangt aan een perceel; zonder deze controle krijgt elk adres in
    een gebouw dat het perceel raakt de status, ook als het monument elders staat.
    """
    bag_verblijfsobject_ids = [
        "0106010000003924",  # Brinkstraat 5 Assen (Brinkflat): strookje op perceel met EWE van buren en een klein handmatig GWA-vlak
        "0119010000024961",  # Prinses Marijkestraat 50 Meppel: GWA type BRK, adrespunt buiten perceel
        "0301010000015563",  # Dennendijk 13 Warnsveld: GWA type BAG, gebied ligt elders op het perceel
        "0193010000008403",  # Diezerstraat 12 Zwolle: GWA handmatig, buurpand grenst aan het gebied
        "0119010000024715",  # Zuideinde 76G Meppel: GWA type BRK, adrespunt op perceel
        "0363010001026929",  # Laurierstraat 107A Amsterdam: GWA type BAG, gebied is het pand zelf
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)

    assert len(result) == len(bag_verblijfsobject_ids), (
        "Niet voor elk verblijfsobject een resultaat"
    )

    # Adrespunt buiten het beperkingsgebied: geen status
    assert result["0106010000003924"]["rijksmonument"] is False
    assert result["0106010000003924"]["rijksmonument_bron"] is None
    for vbo_id in [
        "0106010000003924",
        "0119010000024961",
        "0301010000015563",
        "0193010000008403",
    ]:
        assert result[vbo_id]["gemeentelijk_monument"] is False, vbo_id
        assert result[vbo_id]["grondslag_gemeentelijk_monument"] is None, vbo_id

    # Adrespunt binnen het beperkingsgebied: status blijft
    for vbo_id in ["0119010000024715", "0363010001026929"]:
        assert result[vbo_id]["gemeentelijk_monument"] is True, vbo_id


@pytest.mark.asyncio
async def test_process_from_list_adrespunten_op_een_lijn(client: MonumentenClient):
    """Adrespunten die exact op één verticale lijn liggen krijgen hun beperking.

    De BRK-PB WFS vindt niets als de omhullende rechthoek van de adrespunten geen
    breedte heeft. Beide adressen hebben x = 177087.49 en verschillen 5 cm in y.
    """
    bag_verblijfsobject_ids = [
        "0935010000036807",  # Lyonnetstraat 13A Maastricht: GWA
        "0935010000036808",  # Lyonnetstraat 13B Maastricht: GWA
    ]

    result = await client.process_from_list(bag_verblijfsobject_ids)

    for vbo_id in bag_verblijfsobject_ids:
        assert result[vbo_id]["gemeentelijk_monument"] is True, vbo_id


def _gevarieerd_resultaat() -> pd.DataFrame:
    """Bouw een resultaat zoals ``process_from_df`` het geeft, met alle combinaties.

    Elke combinatie van bron, beschermd gezicht, gemeentelijk monument, provinciaal
    monument (ook ``pd.NA``) en voorbescherming komt voor. Het verblijfsobject met
    alle statussen staat er twee keer in, de tweede keer zonder beschermd gezicht,
    zoals bij meerdere beschermde gezichten.

    Returns:
        pd.DataFrame: Resultaat met dezelfde kolommen en dtypes als in productie
    """
    rijen = []
    for i, (bron, gezicht, gemeentelijk, provinciaal, voorbescherming) in enumerate(
        itertools.product(
            [pd.NA, "RCE", "Kadaster", "RCE, Kadaster"],
            [False, True],
            [False, True],
            [False, True, pd.NA],
            [False, True],
        )
    ):
        rijen.append(
            {
                "bag_verblijfsobject_id": f"0599010000{i:06d}",
                "rijksmonument": bron is not pd.NA,
                "rijksmonument_bron": bron,
                "rijksmonument_nummer": "12345" if bron is not pd.NA else pd.NA,
                "rijksmonument_url": pd.NA,
                "rijksmonument_voorbescherming": voorbescherming,
                "rijksbeschermd_gezicht": gezicht,
                "rijksbeschermd_gezicht_naam": "Gezicht" if gezicht else pd.NA,
                "gemeentelijk_monument": gemeentelijk,
                "grondslag_gemeentelijk_monument": (
                    "Gemeentewet" if gemeentelijk else pd.NA
                ),
                "provinciaal_monument": provinciaal,
                "provinciaal_monument_omschrijving": (
                    "Provinciaal" if provinciaal is True else pd.NA
                ),
            }
        )
    alles = next(
        rij
        for rij in rijen
        if rij["rijksmonument_bron"] is not pd.NA
        and rij["rijksmonument_bron"] == "RCE, Kadaster"
        and rij["rijksbeschermd_gezicht"]
        and rij["gemeentelijk_monument"]
        and rij["provinciaal_monument"] is True
    )
    dubbel = {**alles, "rijksbeschermd_gezicht": False}
    df = pd.DataFrame(rijen + [dubbel])
    df["provinciaal_monument"] = df["provinciaal_monument"].astype("boolean")
    return df


_BRON_ALS_LIJST = {
    "RCE": ["RCE"],
    "Kadaster": ["Kadaster"],
    "RCE, Kadaster": ["RCE", "Kadaster"],
}


def _verwachte_vera(rij: dict) -> list | None:
    """De VERA-referentiedata voor één rij, rechtstreeks uit de regels afgeleid.

    Args:
        rij (dict): Eén rij uit ``_gevarieerd_resultaat``

    Returns:
        list | None: De items in de volgorde RIJ, SGR, GEM, PRO, of ``None`` als het
            provinciaal monument onbepaald is
    """
    if pd.isna(rij["provinciaal_monument"]):
        return None
    verwacht = []
    if rij["rijksmonument"]:
        verwacht.append(
            {
                "code": "RIJ",
                "naam": "Rijksmonument",
                "bron": _BRON_ALS_LIJST[rij["rijksmonument_bron"]],
            }
        )
    if rij["rijksbeschermd_gezicht"]:
        verwacht.append({"code": "SGR", "naam": "Rijksbeschermd stadsgezicht"})
    if rij["gemeentelijk_monument"]:
        verwacht.append({"code": "GEM", "naam": "Gemeentelijk monument"})
    if rij["provinciaal_monument"]:
        verwacht.append({"code": "PRO", "naam": "Provinciaal monument"})
    return verwacht


@contextmanager
def _mock_process_from_df(result: pd.DataFrame) -> Iterator[None]:
    """Laat ``process_from_df`` een vast resultaat geven, zonder netwerk."""

    async def nep_process_from_df(self, df, verblijfsobject_id_col):
        return result.copy()

    with patch.object(MonumentenClient, "process_from_df", nep_process_from_df):
        yield


@pytest.mark.asyncio
async def test_process_from_list_vera_alle_combinaties(client: MonumentenClient):
    """Elke combinatie van statussen geeft de VERA-items volgens de regels.

    Voorbescherming levert geen item op; bij een dubbel verblijfsobject telt de
    eerste rij, en de volgorde van de keys is die van de invoer.
    """
    result = _gevarieerd_resultaat()
    with _mock_process_from_df(result):
        vera = await client.process_from_list(
            result["bag_verblijfsobject_id"].tolist(), to_vera=True
        )

    eerste_rijen = result.drop_duplicates("bag_verblijfsobject_id").to_dict("records")
    assert list(vera) == [rij["bag_verblijfsobject_id"] for rij in eerste_rijen]
    for rij in eerste_rijen:
        vbo_id = rij["bag_verblijfsobject_id"]
        assert vera[vbo_id] == _verwachte_vera(rij), vbo_id
    # Er zitten lijsten en None tussen, anders test de vergelijking weinig
    assert any(v is None for v in vera.values())
    assert any(v == [] for v in vera.values())


@pytest.mark.asyncio
async def test_process_from_list_vera_volgorde_en_bron(client: MonumentenClient):
    """Volgorde van de items is RIJ, SGR, GEM, PRO; bron is een lijst.

    Het verblijfsobject met alle statussen komt twee keer voor; de eerste rij telt,
    dus SGR zit erin.
    """
    result = _gevarieerd_resultaat()
    rijen = result.set_index("bag_verblijfsobject_id")
    alles = rijen.index[
        (rijen["rijksmonument_bron"] == "RCE, Kadaster").fillna(False)
        & rijen["rijksbeschermd_gezicht"]
        & rijen["gemeentelijk_monument"]
        & rijen["provinciaal_monument"].fillna(False)
    ][0]
    onbepaald = rijen.index[rijen["provinciaal_monument"].isna()][0]
    with _mock_process_from_df(result):
        vera = await client.process_from_list(
            result["bag_verblijfsobject_id"].tolist(), to_vera=True
        )

    assert vera[alles] == [
        {"code": "RIJ", "naam": "Rijksmonument", "bron": ["RCE", "Kadaster"]},
        {"code": "SGR", "naam": "Rijksbeschermd stadsgezicht"},
        {"code": "GEM", "naam": "Gemeentelijk monument"},
        {"code": "PRO", "naam": "Provinciaal monument"},
    ]
    assert vera[onbepaald] is None
    assert len(vera) == len(rijen.index.unique())
