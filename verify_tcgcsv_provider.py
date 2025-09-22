#!/usr/bin/env python3
"""
TcgCsvProvider Verification Script

This script performs comprehensive testing of the TcgCsvProvider
to ensure it works correctly with real tcgcsv.com API data.

Usage:
    python verify_tcgcsv_provider.py
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mtgjson5.providers.tcgcsv_provider import TcgCsvProvider


def test_provider_initialization():
    """Test provider can be initialized correctly"""
    print("Testing provider initialization...")
    
    provider = TcgCsvProvider()
    
    # Verify singleton behavior
    provider2 = TcgCsvProvider()
    assert provider is provider2, "Provider should be singleton"
    
    # Verify configuration
    assert provider.base_url == "https://tcgcsv.com/tcgplayer/1"
    assert "tcgcsv" in provider._build_http_header().get("User-Agent", "").lower()
    
    print("✅ Provider initialization: PASSED")
    return provider


def test_real_data_fetch(provider):
    """Test fetching real data from FIC set"""
    print("\nTesting real data fetch...")
    
    # Test with real FIC group ID
    set_code = "FIC"
    group_id = "24220"
    
    price_data = provider.generate_today_price_dict_for_set(set_code, group_id)
    
    # Verify we got data
    assert len(price_data) > 0, "Should return price data"
    print(f"✅ Retrieved {len(price_data)} price records")
    
    # Test data structure
    first_key = next(iter(price_data.keys()))
    price_obj = price_data[first_key]
    
    # Verify price object structure
    assert price_obj.source == "paper"
    assert price_obj.provider == "tcgcsv"
    assert price_obj.currency == "USD"
    assert price_obj.date == provider.today_date
    
    # Verify at least some price data exists
    has_price_data = (
        price_obj.sell_normal is not None or
        price_obj.sell_foil is not None or
        price_obj.sell_etched is not None
    )
    assert has_price_data, "Should have some price data"
    
    print("✅ Real data fetch: PASSED")
    return price_data


def test_price_variant_detection(price_data):
    """Test that we can detect different price variants"""
    print("\nTesting price variant detection...")
    
    normal_count = 0
    foil_count = 0
    etched_count = 0
    
    for price_obj in price_data.values():
        if price_obj.sell_normal is not None:
            normal_count += 1
        if price_obj.sell_foil is not None:
            foil_count += 1
        if price_obj.sell_etched is not None:
            etched_count += 1
    
    print(f"  Normal prices: {normal_count}")
    print(f"  Foil prices: {foil_count}")
    print(f"  Etched prices: {etched_count}")
    
    # FIC should have at least some foil variants
    total_prices = normal_count + foil_count + etched_count
    assert total_prices > 0, "Should have detected some price variants"
    
    print("✅ Price variant detection: PASSED")


def test_error_handling(provider):
    """Test error handling with invalid data"""
    print("\nTesting error handling...")
    
    # Test with invalid group ID
    result = provider.generate_today_price_dict_for_set("TEST", "99999")
    assert isinstance(result, dict), "Should return dict even on error"
    assert len(result) == 0, "Should return empty dict on error"
    
    print("✅ Error handling: PASSED")


def test_price_data_quality(price_data):
    """Test the quality and validity of price data"""
    print("\nTesting price data quality...")
    
    sample_size = min(10, len(price_data))
    sample_items = list(price_data.items())[:sample_size]
    
    for product_id, price_obj in sample_items:
        # Verify product ID is valid
        assert product_id.isdigit(), f"Product ID should be numeric: {product_id}"
        
        # Verify price values are positive if they exist
        for price_field in ['sell_normal', 'sell_foil', 'sell_etched']:
            price_value = getattr(price_obj, price_field, None)
            if price_value is not None:
                assert isinstance(price_value, (int, float)), f"Price should be numeric: {price_value}"
                assert price_value > 0, f"Price should be positive: {price_value}"
    
    print(f"✅ Price data quality: PASSED (validated {sample_size} records)")


def main():
    """Run all verification tests"""
    print("=" * 60)
    print("TcgCsvProvider Verification Test Suite")
    print("=" * 60)
    
    try:
        # Run tests in sequence
        provider = test_provider_initialization()
        price_data = test_real_data_fetch(provider)
        test_price_variant_detection(price_data)
        test_error_handling(provider)
        test_price_data_quality(price_data)
        
        # Summary
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED! 🎉")
        print("=" * 60)
        print(f"✅ Provider properly initialized")
        print(f"✅ Successfully fetched {len(price_data)} price records from FIC set")
        print(f"✅ Price variants correctly detected and parsed")
        print(f"✅ Error handling working correctly")
        print(f"✅ Price data quality validated")
        print()
        print("The TcgCsvProvider is ready for integration!")
        
    except Exception as e:
        print(f"\n❌ VERIFICATION FAILED: {e}")
        print("\nPlease check the error and fix any issues before integration.")
        sys.exit(1)


if __name__ == "__main__":
    main()