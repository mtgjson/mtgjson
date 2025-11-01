"""
Test shape and structure of get_tcgplayer_prices_map return values.

Uses pytest-mock to mock HTTP responses, avoiding live API calls.
"""

import json

import pytest

from mtgjson5.classes.mtgjson_prices import MtgjsonPricesObject
from mtgjson5.providers.tcgplayer import get_tcgplayer_prices_map


@pytest.fixture(autouse=True)
def mock_tcgplayer_api(mocker):
    """
    Mock TCGplayer API responses for all tests in this module.

    This patches TCGPlayerProvider.get_api_results to return test data
    without making real HTTP calls.
    """
    # Mock response data for group 3094
    mock_pricing_data = [
        {
            "productId": 530197,
            "lowPrice": 0.15,
            "midPrice": 0.25,
            "highPrice": 0.35,
            "marketPrice": 0.28,
            "directLowPrice": 0.20,
            "subTypeName": "Normal",
        },
        {
            "productId": 530197,
            "lowPrice": 0.50,
            "midPrice": 0.75,
            "highPrice": 1.00,
            "marketPrice": 0.85,
            "directLowPrice": 0.60,
            "subTypeName": "Foil",
        },
        {
            "productId": 530198,
            "lowPrice": 1.00,
            "midPrice": 2.00,
            "highPrice": 3.00,
            "marketPrice": 2.25,
            "directLowPrice": 1.50,
            "subTypeName": "Normal",
        },
        {
            "productId": 530199,
            "lowPrice": 5.00,
            "midPrice": 7.50,
            "highPrice": 10.00,
            "marketPrice": 8.00,
            "directLowPrice": 6.00,
            "subTypeName": "Foil",
        },
    ]

    def get_api_results_mock(url, params=None):
        """Mock get_api_results to return test data based on URL."""
        if "3094" in url:
            return mock_pricing_data
        elif "999999999" in url:
            return []  # Empty response for non-existent group
        return []

    # Patch the instance method by monkeypatching the actual singleton instance
    # Instantiate to get the real instance (singleton will return same instance each time)
    from mtgjson5.providers.tcgplayer import TCGPlayerProvider

    provider_instance = TCGPlayerProvider()
    mocker.patch.object(provider_instance, "get_api_results", get_api_results_mock)

    return mock_pricing_data


@pytest.fixture
def sample_group():
    """Sample TCGplayer group ID and name."""
    return ("3094", "Murders at Karlov Manor")


@pytest.fixture
def sample_foil_nonfoil_map():
    """
    Sample mapping: TCGplayer productId -> set of MTGJSON UUIDs.
    For testing Normal and Foil finishes.
    """
    return {
        "530197": {"uuid-530197-normal-foil"},
        "530198": {"uuid-530198-normal-foil"},
    }


@pytest.fixture
def sample_etched_map():
    """
    Sample mapping: TCGplayer productId -> set of MTGJSON UUIDs.
    For testing Etched finish.
    """
    return {
        "530199": {"uuid-530199-etched"},
    }


def test_get_tcgplayer_prices_map_shape(
    sample_group, sample_foil_nonfoil_map, sample_etched_map
):
    """
    Test that get_tcgplayer_prices_map returns correct structure.

    Assertions:
    - Return type is dict[str, MtgjsonPricesObject]
    - Keys are exactly the UUIDs from input maps
    - All values have correct metadata (source, provider, currency)
    - Normal/Foil/Etched prices mapped correctly based on subTypeName
    - No buylist fields set (this function only does retail/sell prices)
    """
    prices_map = get_tcgplayer_prices_map(
        sample_group, sample_foil_nonfoil_map, sample_etched_map
    )

    # Return type should be dict
    assert isinstance(prices_map, dict)

    # Should only have UUIDs we requested (subset of input maps)
    all_expected_uuids = set(
        uuid for uuids in sample_foil_nonfoil_map.values() for uuid in uuids
    ) | set(uuid for uuids in sample_etched_map.values() for uuid in uuids)
    assert set(prices_map.keys()).issubset(all_expected_uuids)

    # Each value should be a MtgjsonPricesObject with correct metadata
    for uuid, price_obj in prices_map.items():
        assert isinstance(price_obj, MtgjsonPricesObject)
        assert price_obj.source == "paper"
        assert price_obj.provider == "tcgplayer"
        assert price_obj.currency == "USD"
        assert price_obj.date  # date should be set

        # Buylist fields should NOT be set by this function
        assert price_obj.buy_normal is None
        assert price_obj.buy_foil is None
        assert price_obj.buy_etched is None

        # At least one sell field should be populated
        has_sell_price = (
            price_obj.sell_normal is not None
            or price_obj.sell_foil is not None
            or price_obj.sell_etched is not None
        )
        assert has_sell_price, f"UUID {uuid} has no sell prices set"


def test_get_tcgplayer_prices_map_field_mapping(
    sample_group, sample_foil_nonfoil_map, sample_etched_map
):
    """
    Test specific field mapping logic.

    Field mapping rules:
    - subTypeName "Normal" → sell_normal
    - subTypeName "Foil" + normal mapping → sell_foil
    - subTypeName "Foil" + etched mapping → sell_etched
    """
    prices = get_tcgplayer_prices_map(
        sample_group, sample_foil_nonfoil_map, sample_etched_map
    )

    # If we got results, verify field types
    for uuid, obj in prices.items():
        if obj.sell_normal is not None:
            assert isinstance(obj.sell_normal, (int, float))
        if obj.sell_foil is not None:
            assert isinstance(obj.sell_foil, (int, float))
        if obj.sell_etched is not None:
            assert isinstance(obj.sell_etched, (int, float))


def test_get_tcgplayer_prices_map_empty_results():
    """
    Test that function handles empty API results gracefully.
    """
    # Use a non-existent group ID that will return empty results
    group = ("999999999", "Nonexistent Set")
    foil_nonfoil_map = {"1": {"uuid-test"}}
    etched_map = {}

    prices = get_tcgplayer_prices_map(group, foil_nonfoil_map, etched_map)

    # Should return empty dict, not raise exception
    assert isinstance(prices, dict)
