"""
Pytest configuration for MTGJSON tests.
"""

import os

import pytest
import vcr


def filter_request(request):
    """Filter sensitive data from recorded requests."""
    if request.body:
        # Filter client_id and client_secret from POST bodies
        body_str = (
            request.body.decode("utf-8")
            if isinstance(request.body, bytes)
            else request.body
        )
        if "client_id" in body_str or "client_secret" in body_str:
            # Replace values but keep structure for replay
            import re

            body_str = re.sub(r"client_id=[^&]+", "client_id=FILTERED", body_str)
            body_str = re.sub(
                r"client_secret=[^&]+", "client_secret=FILTERED", body_str
            )
            request.body = body_str
    return request


@pytest.fixture(scope="module")
def vcr_config():
    """
    VCR configuration for recording HTTP interactions.

    - record_mode: 'once' by default (records if cassette missing, replays otherwise)
    - Use 'all' to force re-recording (set via --record-mode=all pytest flag or env var)
    - Use 'none' in CI to ensure no live calls
    """
    record_mode = os.environ.get("VCR_RECORD_MODE", "once")

    return {
        "record_mode": record_mode,
        "filter_headers": ["authorization", "Authorization"],
        "before_record_request": filter_request,
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        "cassette_library_dir": "tests/mtgjson5/providers/cassettes",
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    """Return cassette directory path."""
    return "tests/mtgjson5/providers/cassettes"
