"""Pytest configuration and fixtures for MTGJSON tests."""

import os
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture
def disable_cache() -> Generator[None, None, None]:
    """
    Disable requests-cache for VCR tests.

    Patches MtgjsonConfig.use_cache to return False, causing retryable_session()
    to use a regular requests.Session instead of CachedSession. This avoids
    incompatibility between requests-cache and vcrpy (VCRHTTPResponse doesn't
    have `_request_url` attribute that CachedSession expects).

    This approach completely bypasses the caching mechanism during tests,
    ensuring VCR cassettes work correctly for deterministic playback.
    """
    with patch("mtgjson5.retryable_session.MtgjsonConfig") as mock_config:
        mock_config.return_value.use_cache = False
        yield


# VCR configuration
@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """
    Configure VCR for deterministic HTTP testing.

    Filters volatile headers and sets decode mode for reproducible cassettes.
    """
    return {
        # Remove headers that change between requests to ensure cassette stability
        # Without this, cassettes would be invalidated on every API change
        "filter_headers": [
            "authorization",  # API keys/tokens (security + stability)
            "date",  # Server timestamp (changes every request)
            "server",  # Server version info (changes with deployments)
            "cf-cache-status",  # Cloudflare cache status (non-deterministic)
            "expires",  # Cache expiry time (time-dependent)
            "etag",  # Resource version identifier (changes with updates)
            "last-modified",  # Resource modification time (time-dependent)
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
