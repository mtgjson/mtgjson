"""Pytest configuration and fixtures for MTGJSON tests."""

import os
from typing import Any, Dict, Generator

import pytest
import requests_cache


@pytest.fixture
def disable_cache() -> Generator[None, None, None]:
    """
    Disable requests-cache for VCR tests.

    From requests-cache docs (compatibility.md):
    "If you have an application that uses requests-cache and you just want to use
    [another mocking library] in your tests, the easiest thing to do is to disable
    requests-cache."

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
            "authorization",     # API keys/tokens (security + stability)
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
