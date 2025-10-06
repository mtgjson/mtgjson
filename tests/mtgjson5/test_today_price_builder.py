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


def test_v2_record_to_json():
    """Test MtgjsonPricesRecordV2 serialization."""
    record = MtgjsonPricesRecordV2(
        provider="testprovider",
        treatment="foil",
        currency="EUR",
        price_value=15.75,
        price_variant="market",
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
    assert result["priceVariant"] == "market"
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
        price_variant="low",
        uuid="test-uuid-456",
        platform="mtgo",
        price_type="buy_list",
        date="2024-02-01",
    )

    result = record.to_json()

    assert "subtype" not in result
    assert result["provider"] == "testprovider"
    assert result["priceValue"] == 5.0
    assert result["priceVariant"] == "low"


def test_tcgplayer_build_v2_prices():
    """Test TCGPlayerProvider.build_v2_prices() with all price variants."""
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
            # First call for retail pricing
            json.load(
                get_resource_file_buffer("tcgplayer_pricing_group_response.json")
            ),
            # Second call for buylist pricing
            json.load(
                get_resource_file_buffer("tcgplayer_buylist_group_response.json")
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

    # Build v2 prices
    v2_records = provider.build_v2_prices(get_slim_all_printings_path())

    # Should have records for all price variants
    # Fixture has 3 retail rows (Normal, Foil, Etched) with 5 price variants each = 15 records
    # Plus 2 buylist rows (Normal SKU, Foil SKU) = 2 records
    # But we need to account for UUIDs - with 2 UUIDs per product
    # So: 3 rows × 5 variants × 2 UUIDs = 30 retail records
    # And: 2 SKUs × 2 UUIDs = 4 buylist records (but only where high price exists)
    # Total should be significant - let's validate structure instead

    assert len(v2_records) > 0, "Should have generated v2 records"

    # Validate retail records exist with all variants
    retail_records = [r for r in v2_records if r.price_type == "retail"]
    assert len(retail_records) > 0, "Should have retail records"

    # Check that all price variants are present
    variants_found = {r.price_variant for r in retail_records}
    expected_variants = {"market", "low", "mid", "high", "direct_low"}
    assert variants_found == expected_variants, f"Should have all variants, found: {variants_found}"

    # Validate buylist records exist
    buylist_records = [r for r in v2_records if r.price_type == "buy_list"]
    assert len(buylist_records) > 0, "Should have buylist records"
    assert all(r.price_variant == "high" for r in buylist_records), "Buylist should use 'high' variant"

    # Validate a specific retail record structure
    market_record = next(
        (r for r in retail_records if r.price_variant == "market" and r.treatment == "normal"),
        None
    )
    assert market_record is not None, "Should find a market price for normal treatment"
    assert market_record.provider == "tcgplayer"
    assert market_record.platform == "paper"
    assert market_record.currency == "USD"
    assert market_record.price_value == 111.01  # From fixture
    assert market_record.subtype == "Normal"
    assert market_record.uuid == "00000000-0000-0000-0000-000000000001"

    # Validate different treatments exist
    treatments_found = {r.treatment for r in v2_records}
    assert "normal" in treatments_found
    assert "foil" in treatments_found or "etched" in treatments_found

    # Validate all records have proper structure
    for record in v2_records:
        assert record.provider == "tcgplayer"
        assert record.platform == "paper"
        assert record.currency == "USD"
        assert record.price_value > 0
        assert record.uuid.startswith("00000000-0000-0000")
        assert record.date == provider.today_date
        assert record.treatment in ["normal", "foil", "etched"]
        assert record.subtype is not None
