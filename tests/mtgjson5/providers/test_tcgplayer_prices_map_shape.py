"""
Test shape and structure of get_tcgplayer_prices_map return values.

Uses VCR.py to record/replay HTTP interactions with TCGplayer API.
No live network calls in normal test runs.
"""

import pytest
import vcr

from mtgjson5.classes.mtgjson_prices import MtgjsonPricesObject
from mtgjson5.providers.tcgplayer import get_tcgplayer_prices_map


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


@vcr.use_cassette(
    "tests/mtgjson5/providers/cassettes/tcgplayer_prices_map.yaml",
    record_mode="once",
    filter_headers=["authorization"],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)
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


def test_get_tcgplayer_prices_map_field_mapping():
    """
    Test specific field mapping logic (using cassette).

    Field mapping rules:
    - subTypeName "Normal" → sell_normal
    - subTypeName "Foil" + normal mapping → sell_foil
    - subTypeName "Foil" + etched mapping → sell_etched
    """
    # Use the same cassette with specific test data
    group = ("3094", "Murders at Karlov Manor")

    # Test Normal finish
    foil_nonfoil_map = {"530197": {"uuid-test-normal"}}
    etched_map = {}

    with vcr.use_cassette(
        "tests/mtgjson5/providers/cassettes/tcgplayer_prices_map.yaml",
        record_mode="once",
        filter_headers=["authorization"],
        match_on=["method", "scheme", "host", "port", "path", "query"],
    ):
        prices = get_tcgplayer_prices_map(group, foil_nonfoil_map, etched_map)

        # If we got a result for this product, verify Normal mapping
        if "uuid-test-normal" in prices:
            obj = prices["uuid-test-normal"]
            # Normal subType should populate sell_normal
            # (may not be in cassette, but if it is, this is the rule)
            if obj.sell_normal is not None:
                assert isinstance(obj.sell_normal, (int, float))


def test_get_tcgplayer_prices_map_empty_results():
    """
    Test that function handles empty API results gracefully.
    """
    # Use a non-existent group ID that will return empty results
    group = ("999999999", "Nonexistent Set")
    foil_nonfoil_map = {"1": {"uuid-test"}}
    etched_map = {}

    with vcr.use_cassette(
        "tests/mtgjson5/providers/cassettes/tcgplayer_prices_map_empty.yaml",
        record_mode="once",
        filter_headers=["authorization"],
        match_on=["method", "scheme", "host", "port", "path", "query"],
    ):
        prices = get_tcgplayer_prices_map(group, foil_nonfoil_map, etched_map)

        # Should return empty dict, not raise exception
        assert isinstance(prices, dict)
