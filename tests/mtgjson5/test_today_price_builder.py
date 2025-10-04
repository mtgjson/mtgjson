import json
import pathlib
from typing import List, TextIO
from unittest.mock import patch

from mtgjson5.classes import (
    MtgjsonPricesObject,
    MtgjsonPricesRecordV2,
    MtgjsonPricesV2Container,
)
from mtgjson5.price_builder import PriceBuilder
from mtgjson5.providers import (
    CardKingdomProvider,
    CardHoarderProvider,
    TCGPlayerProvider,
    MultiverseBridgeProvider,
)
from mtgjson5.providers.abstract import AbstractProvider
from mtgjson5.providers.cardmarket.monolith import CardMarketProvider


def get_slim_all_printings_path() -> pathlib.Path:
    return (
        pathlib.Path(__file__)
        .resolve()
        .parent.joinpath("resources")
        .joinpath("today_price_builder")
        .joinpath("slim_all_printings.json")
    )


def get_resource_file_buffer(file_name: str) -> TextIO:
    return (
        pathlib.Path(__file__)
        .resolve()
        .parent.joinpath("resources")
        .joinpath("today_price_builder")
        .joinpath(file_name)
        .open()
    )


def assert_build_today_prices(
    provider: AbstractProvider, expected_results: List[MtgjsonPricesObject]
) -> None:
    builder = PriceBuilder(provider, all_printings_path=get_slim_all_printings_path())
    today_prices = builder.build_today_prices()

    actual_results = today_prices.values()
    assert len(expected_results) == len(actual_results)
    for expected, actual in zip(expected_results, actual_results):
        assert expected.to_json() == actual


def test_card_kingdom_build_today_prices():
    provider = CardKingdomProvider()
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("card_kingdom_api_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardkingdom",
            provider.today_date,
            "USD",
            111.02,
            222.02,
            None,
            111.01,
            222.01,
            None,
        ),
        MtgjsonPricesObject(
            "paper",
            "cardkingdom",
            provider.today_date,
            "USD",
            None,
            None,
            333.02,
            None,
            None,
            333.01,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_card_market_build_today_prices():
    provider = CardMarketProvider(init_map=False)
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("card_market_api_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardmarket",
            provider.today_date,
            "EUR",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        ),
        MtgjsonPricesObject(
            "paper",
            "cardmarket",
            provider.today_date,
            "EUR",
            None,
            None,
            None,
            None,
            None,
            333.01,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_card_hoarder_build_today_prices():
    provider = CardHoarderProvider()
    patch.object(
        provider,
        "download",
        side_effect=[
            get_resource_file_buffer("card_hoarder_non_foil_api_response.txt").read(),
            get_resource_file_buffer("card_hoarder_foil_api_response.txt").read(),
        ],
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "mtgo",
            "cardhoarder",
            provider.today_date,
            "USD",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        )
    ]

    assert_build_today_prices(provider, expected_results)


def test_tcgplayer_build_today_prices():
    provider = TCGPlayerProvider()
    patch.object(
        provider,
        "get_tcgplayer_magic_set_ids",
        return_value=json.load(
            get_resource_file_buffer("tcgplayer_magic_set_ids.json")
        ),
    ).start()
    patch.object(
        provider,
        "get_api_results",
        side_effect=[
            json.load(
                get_resource_file_buffer("tcgplayer_buylist_group_response.json")
            ),
            json.load(
                get_resource_file_buffer("tcgplayer_pricing_group_response.json")
            ),
        ],
    ).start()
    patch.object(
        provider,
        "get_tcgplayer_sku_data",
        return_value=json.load(
            get_resource_file_buffer("tcgplayer_sku_data_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "tcgplayer",
            provider.today_date,
            "USD",
            111.02,
            222.02,
            None,
            111.01,
            222.01,
            None,
        ),
        MtgjsonPricesObject(
            "paper",
            "tcgplayer",
            provider.today_date,
            "USD",
            None,
            None,
            333.02,
            None,
            None,
            333.01,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_multiverse_bridge_cardsphere_build_today_prices():
    provider = MultiverseBridgeProvider()
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("multiverse_bridge_prices_responses.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardsphere",
            provider.today_date,
            "USD",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        )
    ]

    assert_build_today_prices(provider, expected_results)


def test_convert_legacy_to_v2():
    """Test conversion from legacy price format to v2 format."""
    legacy_prices = {
        "00000000-0000-0000-0000-000000000001": {
            "paper": {
                "cardkingdom": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2024-01-01": 10.5, "2024-01-02": 11.0},
                        "foil": {"2024-01-01": 20.5},
                    },
                    "buylist": {
                        "normal": {"2024-01-01": 8.0},
                    },
                }
            }
        },
        "00000000-0000-0000-0000-000000000002": {
            "mtgo": {
                "cardhoarder": {
                    "currency": "USD",
                    "retail": {
                        "normal": {"2024-01-01": 5.5},
                    },
                }
            }
        },
    }

    v2_container = PriceBuilder.convert_legacy_to_v2(legacy_prices)

    # Verify container has correct providers
    assert set(v2_container.get_providers()) == {"cardkingdom", "cardhoarder"}

    # Verify total record count
    # cardkingdom: 2 retail normal + 1 retail foil + 1 buylist normal = 4
    # cardhoarder: 1 retail normal = 1
    # Total = 5
    assert v2_container.get_record_count() == 5

    # Verify JSON serialization structure
    v2_json = v2_container.to_json()
    assert "cardkingdom" in v2_json
    assert "cardhoarder" in v2_json
    assert len(v2_json["cardkingdom"]) == 4
    assert len(v2_json["cardhoarder"]) == 1

    # Verify a specific record
    ck_records = v2_json["cardkingdom"]
    normal_retail_records = [
        r
        for r in ck_records
        if r["treatment"] == "normal"
        and r["priceType"] == "retail"
        and r["date"] == "2024-01-01"
    ]
    assert len(normal_retail_records) == 1
    assert normal_retail_records[0]["priceValue"] == 10.5
    assert normal_retail_records[0]["uuid"] == "00000000-0000-0000-0000-000000000001"
    assert normal_retail_records[0]["platform"] == "paper"
    assert normal_retail_records[0]["currency"] == "USD"


def test_v2_record_to_json():
    """Test MtgjsonPricesRecordV2 serialization."""
    record = MtgjsonPricesRecordV2(
        provider="testprovider",
        treatment="foil",
        currency="EUR",
        price_value=15.75,
        price_variant="retail",
        uuid="test-uuid-123",
        platform="paper",
        price_type="retail",
        date="2024-01-15",
        subtype="premium",
    )

    result = record.to_json()

    assert result["provider"] == "testprovider"
    assert result["treatment"] == "foil"
    assert result["currency"] == "EUR"
    assert result["priceValue"] == 15.75
    assert result["priceVariant"] == "retail"
    assert result["uuid"] == "test-uuid-123"
    assert result["platform"] == "paper"
    assert result["priceType"] == "retail"
    assert result["date"] == "2024-01-15"
    assert result["subtype"] == "premium"


def test_v2_record_to_json_without_subtype():
    """Test MtgjsonPricesRecordV2 serialization without optional subtype."""
    record = MtgjsonPricesRecordV2(
        provider="testprovider",
        treatment="normal",
        currency="USD",
        price_value=5.0,
        price_variant="buylist",
        uuid="test-uuid-456",
        platform="mtgo",
        price_type="buy_list",
        date="2024-02-01",
    )

    result = record.to_json()

    assert "subtype" not in result
    assert result["provider"] == "testprovider"
    assert result["priceValue"] == 5.0
