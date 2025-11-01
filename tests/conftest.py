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

    Filters volatile data-sensitive headers and sets decode mode for reproducible cassettes.
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
