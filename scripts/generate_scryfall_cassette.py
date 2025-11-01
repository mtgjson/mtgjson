#!/usr/bin/env python
"""
Generate VCR cassettes from provider requests-cache databases.

This script exports cached provider responses to VCR-compatible cassettes
that can be used for offline testing.

By default, exports all provider caches found in .mtgjson5_cache/.
Pass provider name as argument to export specific provider only.

Cassettes are organized by host (e.g., api.scryfall.com.yml) so multiple
tests can share the same cassette file.

Usage:
    python scripts/generate_scryfall_cassette.py [provider_name]

Examples:
    # Export all providers to host-based cassettes
    python scripts/generate_scryfall_cassette.py

    # Export only Scryfall provider
    python scripts/generate_scryfall_cassette.py ScryfallProvider
"""
import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from requests_cache import CachedSession
from tests.utils.vcr_export import to_vcr_cassettes_by_host

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Paths
cache_dir = project_root / ".mtgjson5_cache"
cassette_dir = project_root / "tests" / "cassettes"
cassette_dir.mkdir(exist_ok=True)

# Determine which providers to export
if len(sys.argv) > 1:
    # Export specific provider
    provider_name = sys.argv[1]
    cache_files = list(cache_dir.glob(f"{provider_name}.sqlite"))
    if not cache_files:
        logger.error(f"No cache found for provider: {provider_name}")
        sys.exit(1)
else:
    # Export all providers
    cache_files = list(cache_dir.glob("*Provider.sqlite"))

if not cache_files:
    logger.warning("No provider caches found in .mtgjson5_cache/")
    sys.exit(0)

# Export each provider cache to separate cassette files by host
for cache_file in cache_files:
    provider_name = cache_file.stem
    logger.info(f"Exporting {provider_name}...")

    session = CachedSession(str(cache_file))
    response_count = len(session.cache.responses)

    if response_count == 0:
        logger.warning(f"  {provider_name} cache is empty, skipping")
        continue

    logger.info(f"  Found {response_count} cached responses")

    # Export to separate cassette files by host (e.g., api.scryfall.com.yml)
    # This allows multiple tests to share cassettes by host
    to_vcr_cassettes_by_host(session.cache, str(cassette_dir))
    logger.info(f"  Exported to {cassette_dir}/<host>.yml")

logger.info("Done!")
