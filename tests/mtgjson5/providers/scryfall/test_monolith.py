"""Tests for Scryfall provider using VCR cassettes."""

import pytest
import requests


@pytest.mark.vcr()
def test_catalog_keyword_abilities():
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Uses VCR cassette for offline deterministic testing.
    VCR will intercept and record/replay HTTP calls automatically.
    """
    from mtgjson5.providers.scryfall.monolith import ScryfallProvider

    provider = ScryfallProvider()

    # Replace cached session with plain session to avoid VCR/requests-cache conflict
    provider.set_session(requests.Session())

    # Fetch keyword abilities catalog
    # VCR will intercept this call and record/replay it
    data = provider.get_catalog_entry("keyword-abilities")

    # Assert on stable, well-known keyword abilities
    assert isinstance(data, list), "Catalog should return a list"
    assert len(data) > 0, "Catalog should not be empty"

    # Check for some common keyword abilities that have been in Magic for years
    # Note: Scryfall returns them in title case
    expected_keywords = ["Flying", "Haste", "Vigilance", "Trample", "Lifelink"]
    for keyword in expected_keywords:
        assert keyword in data, f"Expected keyword '{keyword}' not found in catalog"
