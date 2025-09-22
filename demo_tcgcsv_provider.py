#!/usr/bin/env python3
"""
Demonstration script for TcgCsvProvider

This script shows how to use the TcgCsvProvider to fetch pricing data
for the FIC (Commander Final Fantasy) set from tcgcsv.com.

Usage:
    python demo_tcgcsv_provider.py
"""

import json
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mtgjson5.providers.tcgcsv_provider import TcgCsvProvider


def main():
    """Demonstrate TcgCsvProvider functionality"""
    
    print("=" * 60)
    print("TcgCsvProvider Demonstration")
    print("=" * 60)
    print()
    
    # Initialize the provider
    print("Initializing TcgCsvProvider...")
    provider = TcgCsvProvider()
    print(f"✓ Provider initialized with base URL: {provider.base_url}")
    print()
    
    # For demonstration, we'll use a test group ID
    # In real usage, this would come from MTGJSON set data
    set_code = "FIC"  # Commander Final Fantasy
    
    # Note: We need a real group ID for FIC from MTGJSON or tcgcsv-etl
    # For demonstration purposes, let's use a placeholder
    # In a real implementation, this would be retrieved from the set data
    test_group_id = "12345"  # This would need to be the actual FIC group ID
    
    print(f"Attempting to fetch prices for set {set_code}...")
    print(f"Using test group ID: {test_group_id}")
    print()
    
    try:
        # Fetch price data
        price_data = provider.generate_today_price_dict_for_set(set_code, test_group_id)
        
        if price_data:
            print(f"✓ Successfully fetched {len(price_data)} price records")
            print()
            
            # Display sample price data
            print("Sample price data:")
            print("-" * 40)
            
            for i, (product_id, price_obj) in enumerate(price_data.items()):
                if i >= 3:  # Show only first 3 records
                    print(f"... and {len(price_data) - 3} more records")
                    break
                    
                print(f"Product ID: {product_id}")
                print(f"  Source: {price_obj.source}")
                print(f"  Provider: {price_obj.provider}")
                print(f"  Currency: {price_obj.currency}")
                print(f"  Date: {price_obj.date}")
                
                if price_obj.sell_normal:
                    print(f"  Normal price: ${price_obj.sell_normal:.2f}")
                if price_obj.sell_foil:
                    print(f"  Foil price: ${price_obj.sell_foil:.2f}")
                if price_obj.sell_etched:
                    print(f"  Etched price: ${price_obj.sell_etched:.2f}")
                print()
        else:
            print("⚠ No price data retrieved")
            print("This could be due to:")
            print("  - Invalid group ID (expected for demo)")
            print("  - Network issues")
            print("  - API changes")
            
    except Exception as e:
        print(f"⚠ Error fetching price data: {e}")
        print()
        print("This is expected in the demo since we're using a placeholder group ID.")
        print("In real usage, the group ID would come from MTGJSON set metadata.")
    
    print()
    print("=" * 60)
    print("Provider Information")
    print("=" * 60)
    print(f"Base URL: {provider.base_url}")
    print(f"User Agent: {provider._build_http_header().get('User-Agent', 'N/A')}")
    print(f"Provider Class: {provider.__class__.__name__}")
    
    print()
    print("Next Steps:")
    print("1. Integrate with MTGJSON set data to get real group IDs")
    print("2. Add collector number mapping between TCGCSV and MTGJSON")
    print("3. Register provider in price_builder.py")
    print("4. Test with real FIC set data")


if __name__ == "__main__":
    main()