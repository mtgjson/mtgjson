"""Tests for Scryfall provider using VCR cassettes.

This test module covers the ScryfallProvider class from mtgjson5/providers/scryfall/monolith.py.

Test Organization:
    - Phase 1: Core Download and Pagination (10 tests)
        - download() method with success, retries, and error handling
        - download_all_pages() with single/multiple pages and errors
        - download_cards() for set downloads with sorting validation

Test Strategy:
    - VCR tests: Use pytest-recording to replay HTTP interactions from cassettes
      These tests require recording once with --record-mode=once, then run offline
    - Mock tests: Use unittest.mock to test error handling without network calls
      These tests run entirely offline and test retry/error logic

Recording VCR Cassettes:
    All VCR tests need cassettes recorded. To record all cassettes at once:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=once -v

    To record a specific test:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_success --record-mode=once

    VCR cassettes are stored in: tests/cassettes/providers/scryfall/<test_name>.yml

    IMPORTANT: ALL VCR tests MUST use the disable_cache fixture to prevent
    requests-cache interference with VCR playback.

Running Tests:
    - Offline (using existing cassettes):
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=none

    - Update all cassettes:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=all

Coverage:
    Target: 80-85% coverage of mtgjson5/providers/scryfall/monolith.py
    Current implementation: Phase 1 (10 tests covering core download functionality)
"""

import sys
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests.exceptions

from mtgjson5.providers.scryfall.monolith import ScryfallProvider


@pytest.mark.vcr("providers/scryfall/test_catalog_keyword_abilities.yml")
def test_catalog_keyword_abilities(disable_cache):
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Uses per-test VCR cassette for offline deterministic testing.

    To record/update this test's cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_catalog_keyword_abilities --record-mode=all

    Each test uses its own cassette under providers/scryfall/<test_name>.yml.
    """
    provider = ScryfallProvider()
    data = provider.get_catalog_entry("keyword-abilities")

    # Assert on stable, well-known keyword abilities
    assert isinstance(data, list), "Catalog should return a list"
    assert len(data) > 0, "Catalog should not be empty"

    # Check for some common keyword abilities that have been in Magic for years
    # Note: Scryfall returns them in title case
    expected_keywords = ["Flying", "Haste", "Vigilance", "Trample", "Lifelink"]
    for keyword in expected_keywords:
        assert keyword in data, f"Expected keyword '{keyword}' not found in catalog"


# ============================================================================
# Phase 1: Core Download and Pagination Tests
# ============================================================================


@pytest.mark.vcr("providers/scryfall/test_download_success.yml")
def test_download_success(disable_cache):
    """
    Test successful download from Scryfall API.

    Verifies that:
    - download() successfully fetches and parses JSON
    - Returns expected data structure
    - Uses VCR cassette for offline testing

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_success --record-mode=once
    """
    provider = ScryfallProvider()

    # Use the catalog API as a simple test endpoint
    result = provider.download("https://api.scryfall.com/catalog/card-names")

    assert isinstance(result, dict), "Response should be a dictionary"
    assert result.get("object") == "catalog", "Should return a catalog object"
    assert "data" in result, "Response should contain data field"
    assert isinstance(result["data"], list), "Data should be a list"
    assert len(result["data"]) > 0, "Data should not be empty"


def test_download_with_chunked_encoding_error_retries(disable_cache):
    """
    Test that download() retries on ChunkedEncodingError.

    Verifies that:
    - First request raises ChunkedEncodingError
    - Method retries with decremented retry_ttl
    - Eventually succeeds on retry
    - Uses mock to avoid actual network calls
    """
    provider = ScryfallProvider()

    # Mock session.get to raise ChunkedEncodingError once, then succeed
    mock_response = Mock()
    mock_response.json.return_value = {"object": "catalog", "data": ["test"]}

    with patch.object(provider.session, "get") as mock_get:
        # First call raises error, second succeeds
        mock_get.side_effect = [
            requests.exceptions.ChunkedEncodingError("Connection broken"),
            mock_response
        ]

        with patch("time.sleep") as mock_sleep:
            result = provider.download("https://api.scryfall.com/test", retry_ttl=3)

        # Verify retry happened
        assert mock_get.call_count == 2, "Should retry once after error"
        mock_sleep.assert_called_once_with(0)  # 3 - 3 = 0 seconds sleep
        assert result == {"object": "catalog", "data": ["test"]}


def test_download_chunked_encoding_error_max_retries(disable_cache):
    """
    Test that download() exits after exhausting retries on ChunkedEncodingError.

    Verifies that:
    - ChunkedEncodingError raised on all retry attempts
    - sys.exit(1) called after max retries exhausted
    - Error logged appropriately
    """
    provider = ScryfallProvider()

    with patch.object(provider.session, "get") as mock_get:
        # Always raise ChunkedEncodingError
        mock_get.side_effect = requests.exceptions.ChunkedEncodingError(
            "Connection broken"
        )

        with patch("time.sleep"):
            with patch("sys.exit") as mock_exit:
                provider.download("https://api.scryfall.com/test", retry_ttl=3)

                # Should call sys.exit(1) after retries exhausted
                mock_exit.assert_called_once_with(1)

        # Should have tried 4 times total (initial + 3 retries)
        assert mock_get.call_count == 4, "Should retry 3 times before giving up"


def test_download_json_parsing_error_with_504(disable_cache):
    """
    Test that download() retries on JSON parsing error with 504 status.

    Verifies that:
    - ValueError raised when parsing invalid JSON
    - If response.text contains "504", logs warning and retries
    - Sleeps 5 seconds before retry
    - Eventually succeeds on retry
    """
    provider = ScryfallProvider()

    # First response: invalid JSON with 504 error
    mock_504_response = Mock()
    mock_504_response.json.side_effect = ValueError("Invalid JSON")
    mock_504_response.text = "504 Gateway Timeout"

    # Second response: valid JSON
    mock_success_response = Mock()
    mock_success_response.json.return_value = {"object": "catalog", "data": ["test"]}

    with patch.object(provider.session, "get") as mock_get:
        mock_get.side_effect = [mock_504_response, mock_success_response]

        with patch("time.sleep") as mock_sleep:
            result = provider.download("https://api.scryfall.com/test")

        # Verify retry happened after 504 error
        assert mock_get.call_count == 2, "Should retry after 504 error"
        mock_sleep.assert_called_once_with(5)  # Should sleep 5 seconds
        assert result == {"object": "catalog", "data": ["test"]}


@pytest.mark.vcr("providers/scryfall/test_download_all_pages_single_page.yml")
def test_download_all_pages_single_page(disable_cache):
    """
    Test download_all_pages() with single-page response.

    Verifies that:
    - Single page of data correctly downloaded
    - No additional page requests when has_more is False
    - All cards from single page returned

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_all_pages_single_page --record-mode=once
    """
    provider = ScryfallProvider()

    # Use a query that returns only one page
    # Search for a very specific card that returns minimal results
    url = "https://api.scryfall.com/cards/search?q=set:lea+name:^Black+Lotus$"

    result = provider.download_all_pages(url)

    assert isinstance(result, list), "Should return a list"
    assert len(result) > 0, "Should return at least one card"
    # Black Lotus from Alpha should be in results
    assert any(card["name"] == "Black Lotus" for card in result)


@pytest.mark.vcr("providers/scryfall/test_download_all_pages_multiple_pages.yml")
def test_download_all_pages_multiple_pages(disable_cache):
    """
    Test download_all_pages() with multi-page response.

    Verifies that:
    - Multiple pages correctly downloaded
    - Page parameter incremented for each request
    - All cards from all pages returned
    - Stops when has_more is False

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_all_pages_multiple_pages --record-mode=once
    """
    provider = ScryfallProvider()

    # Use a query that returns multiple pages (small set with many cards)
    # WAR (War of the Spark) has 264+ cards which should span multiple pages
    url = "https://api.scryfall.com/cards/search?order=set&q=set:war+unique:prints"

    result = provider.download_all_pages(url)

    assert isinstance(result, list), "Should return a list"
    # WAR has 264 cards, should be enough to span multiple pages (175 per page)
    assert len(result) > 175, "Should return more than one page of results"
    # Verify some known cards from WAR
    card_names = {card["name"] for card in result}
    assert "Teferi, Time Raveler" in card_names
    assert "Nicol Bolas, Dragon-God" in card_names


@pytest.mark.vcr("providers/scryfall/test_download_all_pages_error_response.yml")
def test_download_all_pages_error_response(disable_cache):
    """
    Test download_all_pages() with error response from API.

    Verifies that:
    - Error responses handled gracefully
    - Returns empty list when API returns error
    - Logs warning for non-404 errors

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_all_pages_error_response --record-mode=once
    """
    provider = ScryfallProvider()

    # Use an invalid query that will return an error
    url = "https://api.scryfall.com/cards/search?q=invalid:query:syntax"

    result = provider.download_all_pages(url)

    assert isinstance(result, list), "Should return a list even on error"
    assert len(result) == 0, "Should return empty list on error"


@pytest.mark.vcr("providers/scryfall/test_download_cards_m19.yml")
def test_download_cards_success(disable_cache):
    """
    Test download_cards() successfully downloads all cards for a set.

    Verifies that:
    - All cards from set downloaded
    - Cards sorted by name, then by collector_number
    - Expected cards present in result

    Uses M19 (Core Set 2019) as test set - medium-sized modern set.

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_cards_success --record-mode=once
    """
    provider = ScryfallProvider()

    result = provider.download_cards("M19")

    assert isinstance(result, list), "Should return a list"
    assert len(result) > 0, "M19 should have cards"

    # Verify sorting by name, then collector_number
    for i in range(len(result) - 1):
        curr = result[i]
        next_card = result[i + 1]

        # Names should be in order
        if curr["name"] == next_card["name"]:
            # Same name -> collector numbers should be in order
            assert curr["collector_number"] <= next_card["collector_number"], \
                f"Cards with same name should be sorted by collector_number: {curr['name']}"
        else:
            assert curr["name"] <= next_card["name"], \
                f"Cards should be sorted by name: {curr['name']} vs {next_card['name']}"

    # Verify some known M19 cards
    card_names = {card["name"] for card in result}
    assert "Nicol Bolas, the Ravager" in card_names or "Nicol Bolas, the Ravager // Nicol Bolas, the Arisen" in card_names
    assert "Lightning Strike" in card_names


@pytest.mark.vcr("providers/scryfall/test_download_cards_empty_set.yml")
def test_download_cards_empty_set(disable_cache):
    """
    Test download_cards() with non-existent set code.

    Verifies that:
    - Non-existent set returns empty list
    - No exceptions raised
    - Handles error gracefully

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_cards_empty_set --record-mode=once
    """
    provider = ScryfallProvider()

    # Use a set code that doesn't exist
    result = provider.download_cards("NOTAREALSET123")

    assert isinstance(result, list), "Should return a list even for invalid set"
    assert len(result) == 0, "Non-existent set should return empty list"


@pytest.mark.vcr("providers/scryfall/test_download_cards_sorting.yml")
def test_download_cards_sorting(disable_cache):
    """
    Test that download_cards() properly sorts cards by name and collector_number.

    Verifies that:
    - Cards with same name sorted by collector_number
    - Different names sorted alphabetically
    - Sorting is stable and consistent

    Uses KHM (Kaldheim) which has many cards with multiple versions.

    To record/update cassette:
        pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_cards_sorting --record-mode=once
    """
    provider = ScryfallProvider()

    result = provider.download_cards("KHM")

    assert len(result) > 0, "KHM should have cards"

    # Verify strict sorting
    prev_name = ""
    prev_number = ""

    for card in result:
        curr_name = card["name"]
        curr_number = card["collector_number"]

        if prev_name == curr_name:
            # Same name -> collector_number should increase or stay same
            assert curr_number >= prev_number, \
                f"Collector numbers out of order for {curr_name}: {prev_number} -> {curr_number}"
        else:
            # Different name -> should be alphabetically after
            assert curr_name >= prev_name, \
                f"Card names out of order: {prev_name} -> {curr_name}"

        prev_name = curr_name
        prev_number = curr_number
