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
def vcr_cassette_dir(pytestconfig: pytest.Config) -> str:
    """Set cassette directory for VCR."""
    return "tests/cassettes"
