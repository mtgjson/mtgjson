#!/usr/bin/env python
"""
Generate VCR cassette from Scryfall requests-cache database.

This script exports cached Scryfall responses to a VCR-compatible cassette
that can be used for offline testing.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from requests_cache import CachedSession
from tests.utils.vcr_export import to_vcr_cassette

# Path to Scryfall cache
cache_path = project_root / ".mtgjson5_cache" / "ScryfallProvider"
cassette_path = project_root / "tests" / "cassettes" / "test_catalog_keyword_abilities.yaml"

# Open the cache and export to VCR cassette
session = CachedSession(str(cache_path))
print(f"Cache has {len(session.cache.responses)} responses")

# Export to cassette
to_vcr_cassette(session.cache, str(cassette_path))
print(f"Exported cassette to {cassette_path}")
