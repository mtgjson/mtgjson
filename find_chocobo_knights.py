#!/usr/bin/env python3
"""
Quick script to find Chocobo Knights variants in TCGCSV enrichment data
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mtgjson5.providers.tcgcsv_provider import TcgCsvProvider


def main():
    """Find Chocobo Knights variants"""
    
    print("🐓 Searching for Chocobo Knights variants...")
    print("=" * 50)
    
    # Initialize provider
    provider = TcgCsvProvider()
    
    # Fetch enrichment data for FIC
    enrichment_data = provider.fetch_set_enrichment_data("FIC", "24220")
    
    # Search for Chocobo Knights variants
    chocobo_variants = []
    
    for product_id, data in enrichment_data.items():
        display_name = data.get("tcgplayer_display_name", "").lower()
        if "chocobo" in display_name and "knight" in display_name:
            chocobo_variants.append((product_id, data))
    
    if chocobo_variants:
        print(f"✅ Found {len(chocobo_variants)} Chocobo Knights variant(s)!\n")
        
        for product_id, data in chocobo_variants:
            print(f"Product ID: {product_id}")
            print(f"Display Name: {data.get('tcgplayer_display_name', 'Unknown')}")
            if "collector_number" in data:
                print(f"Collector #: {data['collector_number']}")
            if "rarity" in data:
                print(f"Rarity: {data['rarity']}")
            if "clean_name" in data:
                print(f"Clean Name: {data['clean_name']}")
            if "tcgplayer_url" in data:
                print(f"TCGPlayer URL: {data['tcgplayer_url']}")
            if "prices" in data:
                prices = data['prices']
                price_strs = [f"{finish}: ${price:.2f}" for finish, price in prices.items()]
                print(f"Prices: {', '.join(price_strs)}")
            print("-" * 40)
    else:
        print("❌ No Chocobo Knights variants found.")
        print("\nLet's check for any cards with 'chocobo' in the name:")
        
        chocobo_any = []
        for product_id, data in enrichment_data.items():
            display_name = data.get("tcgplayer_display_name", "").lower()
            if "chocobo" in display_name:
                chocobo_any.append((product_id, data))
        
        if chocobo_any:
            print(f"Found {len(chocobo_any)} cards with 'chocobo' in the name:")
            for product_id, data in chocobo_any[:10]:  # Show first 10
                print(f"  {product_id}: {data.get('tcgplayer_display_name', 'Unknown')}")
        else:
            print("No cards with 'chocobo' found at all.")


if __name__ == "__main__":
    main()