"""Tests for Scryfall provider using VCR cassettes."""

import pytest


@pytest.mark.vcr("api.scryfall.com.yml")
def test_catalog_keyword_abilities(disable_cache):
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Uses shared host-based VCR cassette for offline deterministic testing.
    VCR cassettes are generated from requests-cache exports using
    scripts/generate_scryfall_cassette.py

    Multiple Scryfall tests can share the same api.scryfall.com.yml cassette.
    """
    from mtgjson5.providers.scryfall.monolith import ScryfallProvider

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
