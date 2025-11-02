"""Tests for TCGplayer provider authentication and header building."""

import logging

import pytest

from mtgjson5.providers import TCGPlayerProvider


# Clear singleton cache before each test to ensure isolation
@pytest.fixture(autouse=True)
def reset_tcgplayer_singleton():
    """Reset TCGPlayerProvider singleton between tests."""
    # Clear the singleton cache if it exists
    if hasattr(TCGPlayerProvider, "_instances"):
        TCGPlayerProvider._instances = {}
    yield
    # Clean up after test
    if hasattr(TCGPlayerProvider, "_instances"):
        TCGPlayerProvider._instances = {}


@pytest.mark.vcr()
def test_token_success_builds_header_and_sets_api_version(
    tcgplayer_config, disable_cache
):
    """
    Test successful token retrieval builds correct header and sets API version.

    Uses VCR cassette to replay recorded OAuth token exchange.
    
    RECORDING: Pass real credentials via TCGPLAYER_CLIENT_ID and TCGPLAYER_CLIENT_SECRET env vars
    PLAYBACK: Uses cassette with dummy credentials
    """
    import os
    
    # For recording: use real credentials from environment
    # For playback: use dummy credentials (cassette provides response)
    client_id = os.environ.get("TCGPLAYER_CLIENT_ID", "dummy_id")
    client_secret = os.environ.get("TCGPLAYER_CLIENT_SECRET", "dummy_secret")
    
    tcgplayer_config(
        client_id=client_id,
        client_secret=client_secret,
        api_version="v1.39.0"
    )

    # Act
    provider = TCGPlayerProvider()
    header = provider._build_http_header()

    # Assert
    assert header == {"Authorization": "Bearer REDACTED"}
    assert provider.api_version == "v1.39.0"


def test_default_api_version_when_missing(tcgplayer_config, monkeypatch):
    """
    Test that default API version is used when not specified in config.

    References: mtgjson5/providers/tcgplayer.py lines 166-171
    """

    # Mock the token request to avoid HTTP call
    def fake_post(*args, **kwargs):
        class FakeResp:
            ok = True
            text = '{"access_token": "fake_token"}'

        return FakeResp()

    monkeypatch.setattr("mtgjson5.providers.tcgplayer.requests.post", fake_post)

    # Arrange: omit api_version, provide creds
    tcgplayer_config(client_id="id", client_secret="secret")

    # Act
    provider = TCGPlayerProvider()
    header = provider._build_http_header()

    # Assert: default version set during token retrieval
    assert provider.api_version == "v1.39.0"
    assert header == {"Authorization": "Bearer fake_token"}


def test_missing_section_logs_and_returns_empty_bearer(tcgplayer_config, caplog):
    """
    Test that missing [TCGPlayer] section logs warning and returns empty token.

    References: mtgjson5/providers/tcgplayer.py lines 139-143
    """
    # Arrange: remove entire section
    tcgplayer_config(present=False)

    # Act
    provider = TCGPlayerProvider()
    with caplog.at_level(logging.WARNING):
        token = provider._request_tcgplayer_bearer()

    # Assert
    assert token == ""
    assert any(
        "TCGPlayer config section not established. Skipping requests"
        in record.getMessage()
        for record in caplog.records
    )

    header = provider._build_http_header()
    assert header == {"Authorization": "Bearer "}


def test_missing_options_logs_and_returns_empty_bearer(tcgplayer_config, caplog):
    """
    Test that missing client_id/client_secret logs warning and returns empty token.

    References: mtgjson5/providers/tcgplayer.py lines 145-150
    """
    # Arrange: section exists but missing required keys
    tcgplayer_config()  # creates section with no options

    # Act
    provider = TCGPlayerProvider()
    with caplog.at_level(logging.WARNING):
        token = provider._request_tcgplayer_bearer()

    # Assert
    assert token == ""
    assert any(
        "TCGPlayer keys not established. Skipping requests" in record.getMessage()
        for record in caplog.records
    )


def test_token_post_failure_logs_error_and_returns_empty(
    tcgplayer_config, caplog, monkeypatch
):
    """
    Test that failed token POST request logs error and returns empty token.

    References: mtgjson5/providers/tcgplayer.py lines 162-164
    """
    # Arrange: valid config but mocked failed HTTP response
    tcgplayer_config(client_id="id", client_secret="secret")

    class FakeResp:
        ok = False
        reason = "Unauthorized"
        status_code = 401
        text = ""

    def fake_post(*args, **kwargs):
        return FakeResp()

    monkeypatch.setattr("mtgjson5.providers.tcgplayer.requests.post", fake_post)

    # Act
    provider = TCGPlayerProvider()
    with caplog.at_level(logging.ERROR):
        token = provider._request_tcgplayer_bearer()

    # Assert
    assert token == ""
    assert any(
        "Unable to contact TCGPlayer. Reason: Unauthorized" in record.getMessage()
        for record in caplog.records
    )
