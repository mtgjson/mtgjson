"""Pytest configuration and fixtures for MTGJSON tests."""

import importlib
import os
import pkgutil
from typing import Any, Dict, Generator, List

import pytest
import requests
import requests_cache
from mtgjson5.providers import abstract


def _get_all_singleton_providers() -> List[Any]:
    """
    Discover all provider classes that use @singleton decorator.

    Returns:
        List of singleton wrapper instances (providers)
    """
    import mtgjson5.providers

    singletons = []

    # Walk through all modules in mtgjson5.providers
    for importer, modname, ispkg in pkgutil.walk_packages(
        mtgjson5.providers.__path__, prefix="mtgjson5.providers."
    ):
        try:
            module = importlib.import_module(modname)
            # Find all attributes that have _instance (singleton wrappers)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if hasattr(attr, "_instance") and hasattr(attr, "__wrapped__"):
                    singletons.append(attr)
        except Exception:
            # Skip modules that fail to import
            pass

    return singletons


@pytest.fixture(autouse=False)
def with_test_session(
    request: pytest.FixtureRequest, cached_session: requests_cache.CachedSession
) -> Generator[None, None, None]:
    """
    Reset all provider singletons and inject appropriate test session.

    Many providers use @singleton decorator and persist across tests.
    This fixture:
    1. Clears all singleton provider instances for test isolation
    2. Monkey-patches retryable_session() to return:
       - cached_session during recording (no VCR) for cache population
       - plain Session during playback (VCR active) to avoid conflicts

    Use this fixture in provider tests that need fresh instances.

    Aliases: reset_scryfall_singleton (for backward compatibility)
    """
    # Discover and clear all singleton provider instances
    singleton_providers = _get_all_singleton_providers()
    for provider in singleton_providers:
        provider._instance = None

    # Check if VCR is active for this test
    vcr_markers = [mark for mark in request.node.iter_markers(name="vcr")]
    using_vcr = len(vcr_markers) > 0

    # Monkey-patch retryable_session where it's imported and used
    original_retryable_session = abstract.retryable_session

    if using_vcr:
        # VCR playback mode: use plain session to avoid requests-cache/VCR conflict
        abstract.retryable_session = lambda *args, **kwargs: requests.Session()
    else:
        # Recording mode: use cached_session to populate cache for later export
        abstract.retryable_session = lambda *args, **kwargs: cached_session

    yield

    # Restore original retryable_session
    abstract.retryable_session = original_retryable_session

    # Clean up all singleton instances
    for provider in singleton_providers:
        provider._instance = None


# Backward compatibility alias
@pytest.fixture(autouse=False)
def reset_scryfall_singleton(
    request: pytest.FixtureRequest, cached_session: requests_cache.CachedSession
) -> Generator[None, None, None]:
    """
    Backward compatibility alias for with_test_session.

    Deprecated: Use with_test_session instead. This fixture has the same
    implementation but with a more specific name that is now misleading
    since it resets ALL provider singletons, not just Scryfall.
    """
    # Discover and clear all singleton provider instances
    singleton_providers = _get_all_singleton_providers()
    for provider in singleton_providers:
        provider._instance = None

    # Check if VCR is active for this test
    vcr_markers = [mark for mark in request.node.iter_markers(name="vcr")]
    using_vcr = len(vcr_markers) > 0

    # Monkey-patch retryable_session where it's imported and used
    original_retryable_session = abstract.retryable_session

    if using_vcr:
        # VCR playback mode: use plain session to avoid requests-cache/VCR conflict
        abstract.retryable_session = lambda *args, **kwargs: requests.Session()
    else:
        # Recording mode: use cached_session to populate cache for later export
        abstract.retryable_session = lambda *args, **kwargs: cached_session

    yield

    # Restore original retryable_session
    abstract.retryable_session = original_retryable_session

    # Clean up all singleton instances
    for provider in singleton_providers:
        provider._instance = None


# VCR configuration
@pytest.fixture(scope="module")
def vcr_config() -> Dict[str, Any]:
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
def vcr_cassette_dir(pytestconfig: pytest.Config) -> str:
    """Set cassette directory for VCR."""
    return "tests/cassettes"


@pytest.fixture(scope="session")
def cached_session() -> requests_cache.CachedSession:
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


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest options."""
    parser.addoption(
        "--export-cassettes",
        action="store_true",
        default=False,
        help="Export requests-cache to VCR cassettes after recording",
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
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
        from tests.utils.vcr_export import to_vcr_cassettes_by_host

        # Load the cache from the cached_session
        cache_path = "tests/.http_cache.sqlite"
        if os.path.exists(cache_path):
            temp_session = requests_cache.CachedSession(cache_path)
            to_vcr_cassettes_by_host(temp_session.cache, "tests/cassettes")
            print("\nExported requests-cache to VCR cassettes in tests/cassettes/")
    except Exception as e:
        print(f"\nWarning: Failed to export cassettes: {e}")
