"""Pytest configuration and fixtures for MTGJSON tests."""

import os
from typing import Generator

import pytest
import requests_cache


@pytest.fixture(autouse=True)
def disable_cache() -> Generator[None, None, None]:
    """
    Disable requests-cache for all tests.

    VCR cassettes should be the single source of truth for HTTP responses in tests.
    Disabling the cache ensures that all HTTP requests go through VCR for recording
    or playback, providing deterministic offline testing.
    """
    from mtgjson5.mtgjson_config import MtgjsonConfig

    # Save original cache setting
    config = MtgjsonConfig()
    original_use_cache = config.use_cache

    # Disable caching at config level so providers don't create cached sessions
    config.use_cache = False

    try:
        with requests_cache.disabled():
            yield
    finally:
        # Restore original setting
        config.use_cache = original_use_cache


@pytest.fixture(autouse=False)
def reset_scryfall_singleton():
    """
    Reset ScryfallProvider singleton before tests.

    The ScryfallProvider uses @singleton decorator which persists across tests.
    This fixture clears the singleton instance to ensure proper test isolation
    and allow VCR to record all HTTP requests including those in __init__.

    Use this fixture in tests that need a fresh ScryfallProvider instance.
    """
    from mtgjson5.providers.scryfall.monolith import ScryfallProvider

    # Clear the singleton instance (ScryfallProvider is a _SingletonWrapper)
    ScryfallProvider._instance = None

    yield

    # Clean up after test (optional, but good practice)
    ScryfallProvider._instance = None


# VCR configuration
@pytest.fixture(scope="module")
def vcr_config():
    """
    Configure VCR for deterministic HTTP testing.

    Filters volatile headers and sets decode mode for reproducible cassettes.
    """
    return {
        "filter_headers": [
            "authorization",
            "date",
            "server",
            "cf-cache-status",
            "expires",
            "etag",
            "last-modified",
        ],
        "decode_compressed_response": True,
        "record_mode": "none" if os.environ.get("CI") else "once",
    }


@pytest.fixture(scope="session")
def vcr_cassette_dir(pytestconfig):
    """Set cassette directory for VCR."""
    return "tests/cassettes"


@pytest.fixture(scope="session")
def cached_session():
    """
    Provide a requests-cache session for tests.

    Used during recording to populate HTTP cache that can be exported to VCR cassettes.
    Cache stored at tests/.http_cache.sqlite with no expiration.
    """
    session = requests_cache.CachedSession(
        "tests/.http_cache.sqlite",
        expire_after=None,
        stale_if_error=True,
    )
    # Store in pytest config for access in sessionfinish
    return session


def pytest_addoption(parser):
    """Add custom pytest options."""
    parser.addoption(
        "--export-cassettes",
        action="store_true",
        default=False,
        help="Export requests-cache to VCR cassettes after recording",
    )


def pytest_sessionfinish(session, exitstatus):
    """
    Export cached responses to VCR cassettes if --export-cassettes is set.

    Only exports when in record mode (not CI).
    """
    if not session.config.getoption("--export-cassettes"):
        return

    # Skip export in CI or when in 'none' record mode
    if os.environ.get("CI"):
        return

    try:
        from .utils.vcr_export import to_vcr_cassettes_by_host

        # Load the cache from the cached_session
        cache_path = "tests/.http_cache.sqlite"
        if os.path.exists(cache_path):
            temp_session = requests_cache.CachedSession(cache_path)
            to_vcr_cassettes_by_host(temp_session.cache, "tests/cassettes")
            print("\nExported requests-cache to VCR cassettes in tests/cassettes/")
    except Exception as e:
        print(f"\nWarning: Failed to export cassettes: {e}")
