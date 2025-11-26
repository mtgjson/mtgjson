"""Pytest configuration and fixtures for MTGJSON tests."""

import os
from typing import Any, Dict, Generator, Optional

import pytest
import requests_cache

from mtgjson5.mtgjson_config import MtgjsonConfig


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
    TCGplayer-specific: masks OAuth tokens and credentials in cassettes.
    """
    import json

    def scrub_tcgplayer_oauth_response(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scrub sensitive fields from TCGplayer OAuth responses.

        Redacts:
        - access_token: Bearer token for API access
        - userName: User identifier that may contain sensitive info

        This ensures recorded cassettes don't contain actual credentials.
        """
        try:
            body = response["body"]["string"]
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            data = json.loads(body)
        except Exception:
            # If we can't parse JSON, return response unchanged
            return response

        if isinstance(data, dict):
            if "access_token" in data:
                data["access_token"] = "REDACTED"
            if "userName" in data:
                data["userName"] = "REDACTED"
            response["body"]["string"] = json.dumps(data).encode("utf-8")

        return response

    return {
        # Remove headers that change between requests to ensure cassette stability
        # Without this, cassettes would be invalidated on every API change
        "filter_headers": [
            "authorization",  # API keys/tokens (security + stability)
            ("Authorization", "Bearer REDACTED"),  # TCGplayer: redact bearer tokens
            "date",  # Server timestamp (changes every request)
            "server",  # Server version info (changes with deployments)
            "cf-cache-status",  # Cloudflare cache status (non-deterministic)
            "expires",  # Cache expiry time (time-dependent)
            "etag",  # Resource version identifier (changes with updates)
            "last-modified",  # Resource modification time (time-dependent)
        ],
        # TCGplayer OAuth: filter sensitive POST form data
        "filter_post_data_parameters": [
            "grant_type",
            "client_id",
            "client_secret",
        ],
        # Automatically decode gzip/deflate responses for human-readable cassettes
        # Without this, cassette YAML would contain binary compressed data
        "decode_compressed_response": True,
        # TCGplayer OAuth: scrub access_token and userName from response bodies
        "before_record_response": scrub_tcgplayer_oauth_response,
        # Match requests by method, scheme, host, port, path, and query
        # This ensures stable cassette matching for TCGplayer token requests
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        # Default record mode: "once" for local dev, "none" for offline/CI testing
        # Can be overridden with --record-mode flag
        # See README "Testing with VCR Cassettes" for mode explanations
        "record_mode": "none" if os.environ.get("MTGJSON_OFFLINE_MODE") else "once",
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request: pytest.FixtureRequest) -> str:
    """Set cassette directory for VCR using absolute path.

    Overrides pytest-recording's default behavior which places cassettes
    relative to each test module. Instead, all cassettes go in tests/cassettes/
    to match the project structure where cassettes are organized by provider
    and test name (e.g., providers/<provider>/<test_name>.yml).
    """
    # Get project root from pytest config
    root = request.config.rootpath
    return os.path.join(str(root), "tests", "cassettes")


@pytest.fixture
def tcgplayer_config() -> Generator[Any, None, None]:
    """
    Fixture to temporarily modify TCGPlayer config for testing.

    Provides a helper function to set/remove [TCGPlayer] section and options
    in the MtgjsonConfig singleton without cross-test leakage.

    Usage:
        def test_something(tcgplayer_config):
            tcgplayer_config(client_id="test_id", client_secret="test_secret")
            # Test code that uses MtgjsonConfig

        def test_missing_config(tcgplayer_config):
            tcgplayer_config(present=False)  # Remove entire [TCGPlayer] section
            # Test code that expects missing config
    """
    cfg = MtgjsonConfig()
    parser = cfg.config_parser

    # Snapshot current state for restoration
    snapshot_defaults = dict(parser.defaults())
    snapshot = {s: dict(parser.items(s)) for s in parser.sections()}

    def set_tcgplayer_config(
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        api_version: Optional[str] = None,
        present: bool = True,
    ) -> None:
        """
        Set or remove TCGPlayer configuration.

        SECURITY: Clears ALL config sections first to prevent other API keys
        from being captured in VCR cassettes during recording.

        Args:
            client_id: OAuth client ID (if provided)
            client_secret: OAuth client secret (if provided)
            api_version: API version string (if provided)
            present: If False, remove entire [TCGPlayer] section
        """
        # IMPORTANT: Clear ALL sections to prevent key leakage in cassettes
        # This ensures only test credentials are present during recording
        for section in list(parser.sections()):
            parser.remove_section(section)

        section = "TCGPlayer"

        if not present:
            # Already cleared above, nothing more to do
            return

        parser.add_section(section)

        # Only set keys that are explicitly provided
        # Omission simulates missing keys in config
        if client_id is not None:
            parser.set(section, "client_id", client_id)

        if client_secret is not None:
            parser.set(section, "client_secret", client_secret)

        if api_version is not None:
            parser.set(section, "api_version", api_version)

    try:
        yield set_tcgplayer_config
    finally:
        # Restore original state
        parser.clear()
        if snapshot_defaults:
            parser["DEFAULT"] = snapshot_defaults
        for sec, opts in snapshot.items():
            if not parser.has_section(sec):
                parser.add_section(sec)
            for k, v in opts.items():
                parser.set(sec, k, v)
