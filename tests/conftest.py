"""Pytest configuration and fixtures for MTGJSON tests."""

import json
import os
from pathlib import Path
from typing import Any, Dict, Generator

import pytest
import requests_cache
import responses

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "scryfall"


def load_fixture(name: str) -> Dict[str, Any]:
    """Load a JSON fixture file and return parsed data."""
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


@pytest.fixture
def reset_scryfall_singleton():
    """Reset the ScryfallProvider singleton between tests."""
    from mtgjson5.providers.scryfall.monolith import ScryfallProvider

    # Clear the singleton instance if it exists
    if hasattr(ScryfallProvider, "_instance"):
        ScryfallProvider._instance = None
    yield
    # Clean up after test
    if hasattr(ScryfallProvider, "_instance"):
        ScryfallProvider._instance = None


@pytest.fixture
def mock_scryfall_catalog(reset_scryfall_singleton):
    """Mock Scryfall catalog endpoints with static fixture data."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        # Mock the cards_without_limits endpoint (called during ScryfallProvider init)
        cards_without_limits = load_fixture("cards_without_limits")
        rsps.add(
            responses.GET,
            "https://api.scryfall.com/cards/search",
            json=cards_without_limits,
            status=200,
        )

        # Mock the catalog endpoint
        catalog_data = load_fixture("catalog_keyword_abilities")
        rsps.add(
            responses.GET,
            "https://api.scryfall.com/catalog/keyword-abilities",
            json=catalog_data,
            status=200,
        )
        yield rsps


@pytest.fixture
def disable_cache() -> Generator[None, None, None]:
    """
    Disable requests-cache for VCR tests.

    Uses requests_cache.disabled() context manager (the official API) rather than
    monkey-patching CachedSession. This approach:
    - Is documented in requests-cache compatibility guide
    - Properly handles global cache state
    - Won't break if library internals change
    - Makes the intent explicit

    Alternative (NOT used): unittest.mock.patch('requests_cache.CachedSession', requests.Session)
    - More brittle, depends on internal implementation details
    - Doesn't handle all caching mechanisms (e.g., globally installed cache)

    While requests-cache and VCR can coexist, disabling the cache during VCR tests
    ensures deterministic playback from cassettes without cache interference.

    See: https://requests-cache.readthedocs.io/en/stable/user_guide/compatibility.html
    """
    with requests_cache.disabled():
        yield


# VCR configuration
@pytest.fixture(scope="module")
def vcr_config() -> Dict[str, Any]:
    """
    Configure VCR for deterministic HTTP testing.

    Filters volatile headers and sets decode mode for reproducible cassettes.
    """
    return {
        # Remove headers that change between requests to ensure cassette stability
        # Without this, cassettes would be invalidated on every API change
        "filter_headers": [
            "authorization",    # API keys/tokens (security + stability)
            "date",             # Server timestamp (changes every request)
            "server",           # Server version info (changes with deployments)
            "cf-cache-status",  # Cloudflare cache status (non-deterministic)
            "expires",          # Cache expiry time (time-dependent)
            "etag",             # Resource version identifier (changes with updates)
            "last-modified",    # Resource modification time (time-dependent)
        ],
        # Automatically decode gzip/deflate responses for human-readable cassettes
        # Without this, cassette YAML would contain binary compressed data
        "decode_compressed_response": True,
        # Default record mode: "once" for local dev, "none" for offline/CI testing
        # Can be overridden with --record-mode flag
        # See README "Testing with VCR Cassettes" for mode explanations
        "record_mode": "none" if os.environ.get("MTGJSON_OFFLINE_MODE") else "once",
    }


@pytest.fixture(scope="session")
def vcr_cassette_dir(pytestconfig: pytest.Config) -> str:
    """Set cassette directory for VCR."""
    return "tests/cassettes"
