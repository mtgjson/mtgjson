"""Tests for TCGplayer provider API download and parsing methods."""

import json
import logging

import pytest

from mtgjson5.providers.tcgplayer import TCGPlayerProvider


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


class TestDownload:
    """Tests for the download method."""

    @pytest.mark.vcr("providers/tcgplayer/test_download_replaces_api_version_in_url.yml")
    def test_download_replaces_api_version_in_url(self, disable_cache):
        """
        Test download replaces [API_VERSION] placeholder with actual version.

        References: mtgjson5/providers/tcgplayer.py lines 176-189
        The download method replaces [API_VERSION] in URLs with the configured
        version (default v1.39.0).

        To record/update this test's cassette:
            pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestDownload::test_download_replaces_api_version_in_url --record-mode=all

        RECORDING: Uses real credentials from mtgjson.properties
        PLAYBACK: Uses cassette (credentials already scrubbed)
        """
        # Arrange
        provider = TCGPlayerProvider()
        url_with_placeholder = (
            "https://api.tcgplayer.com/[API_VERSION]/catalog/categories"
        )

        # Act
        response = provider.download(url_with_placeholder)

        # Assert
        # Should receive valid JSON response (not an error about invalid URL)
        assert response is not None
        assert len(response) > 0
        # Parse to verify it's valid JSON
        data = json.loads(response)
        assert "results" in data or "errors" in data or "success" in data


class TestGetApiResults:
    """Tests for the get_api_results method."""

    @pytest.mark.vcr("providers/tcgplayer/test_get_api_results_success_parses_json.yml")
    def test_get_api_results_success_parses_json(self, disable_cache):
        """
        Test get_api_results successfully parses JSON response.

        References: mtgjson5/providers/tcgplayer.py lines 386-405
        Should extract the "results" array from TCGPlayer API response.

        To record/update this test's cassette:
            pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestGetApiResults::test_get_api_results_success_parses_json --record-mode=all

        RECORDING: Uses real credentials from mtgjson.properties
        PLAYBACK: Uses cassette (credentials already scrubbed)
        """
        # Arrange
        provider = TCGPlayerProvider()
        # Use categories endpoint which should always return results
        url = "https://api.tcgplayer.com/[API_VERSION]/catalog/categories"

        # Act
        results = provider.get_api_results(url)

        # Assert
        assert isinstance(results, list)
        # Categories endpoint should return at least one category (Magic: The Gathering)
        assert len(results) > 0

    def test_get_api_results_empty_response_returns_empty_list(self, monkeypatch):
        """
        Test get_api_results returns empty list when download returns empty string.

        References: mtgjson5/providers/tcgplayer.py lines 395-397
        """
        # Arrange
        provider = TCGPlayerProvider()

        def fake_download(*args, **kwargs):
            return ""

        monkeypatch.setattr(provider, "download", fake_download)

        # Act
        results = provider.get_api_results("http://fake.url")

        # Assert
        assert results == []

    def test_get_api_results_invalid_json_logs_error_returns_empty_list(
        self, monkeypatch, caplog
    ):
        """
        Test get_api_results logs error and returns empty list on invalid JSON.

        References: mtgjson5/providers/tcgplayer.py lines 399-403
        """
        # Arrange
        provider = TCGPlayerProvider()
        invalid_json = "{ this is not valid json }"

        def fake_download(*args, **kwargs):
            return invalid_json

        monkeypatch.setattr(provider, "download", fake_download)

        # Act
        with caplog.at_level(logging.ERROR):
            results = provider.get_api_results("http://fake.url")

        # Assert
        assert results == []
        assert any(
            "Unable to decode TCGPlayer API Response" in record.getMessage()
            for record in caplog.records
        )


class TestGetTcgplayerMagicSetIds:
    """Tests for the get_tcgplayer_magic_set_ids method."""

    @pytest.mark.vcr(
        "providers/tcgplayer/test_get_tcgplayer_magic_set_ids_single_page.yml"
    )
    def test_get_tcgplayer_magic_set_ids_single_page(self, disable_cache):
        """
        Test get_tcgplayer_magic_set_ids retrieves Magic set IDs and names.

        References: mtgjson5/providers/tcgplayer.py lines 191-214
        This method paginates through TCGPlayer's groups endpoint to get all
        Magic: The Gathering sets. For this test, we'll verify it handles
        at least one page of results correctly.

        To record/update this test's cassette:
            pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestGetTcgplayerMagicSetIds::test_get_tcgplayer_magic_set_ids_single_page --record-mode=all

        RECORDING: Uses real credentials from mtgjson.properties
        PLAYBACK: Uses cassette (credentials already scrubbed)
        """
        # Arrange
        provider = TCGPlayerProvider()

        # Act
        result = provider.get_tcgplayer_magic_set_ids()

        # Assert
        assert isinstance(result, list)
        # Should have multiple sets
        assert len(result) > 0
        # Each entry should be tuple of (groupId, name)
        for entry in result:
            assert isinstance(entry, tuple)
            assert len(entry) == 2
            group_id, name = entry
            assert isinstance(group_id, (int, str))
            assert isinstance(name, str)
            assert len(name) > 0
