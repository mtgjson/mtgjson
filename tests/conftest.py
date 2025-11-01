"""Pytest configuration and fixtures for MTGJSON tests."""

import os
import pytest
import requests_cache


@pytest.fixture
def disable_cache():
    """
    Disable requests-cache for VCR tests.

    From requests-cache docs (compatibility.md):
    "If you have an application that uses requests-cache and you just want to use
    [another mocking library] in your tests, the easiest thing to do is to disable
    requests-cache."

    VCR and requests-cache cannot be used together - VCR intercepts at the httplib
    level while requests-cache wraps responses, causing conflicts.

    See: https://requests-cache.readthedocs.io/en/stable/user_guide/compatibility.html#vcr
    """
    with requests_cache.disabled():
        yield


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
