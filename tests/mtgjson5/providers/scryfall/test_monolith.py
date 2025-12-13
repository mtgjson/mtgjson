"""Tests for Scryfall provider using Pydantic-validated fixtures."""

from mtgjson5.providers.scryfall.monolith import ScryfallProvider


def test_catalog_keyword_abilities(mock_scryfall_catalog, disable_cache):
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Uses static JSON fixtures validated against Pydantic schemas for
    deterministic, offline testing without VCR cassettes.
    """
    provider = ScryfallProvider()
    data = provider.get_catalog_entry("keyword-abilities")

    assert isinstance(data, list), "Catalog should return a list"
    assert len(data) > 0, "Catalog should not be empty"

    expected_keywords = ["Flying", "Haste", "Vigilance", "Trample", "Lifelink"]
    for keyword in expected_keywords:
        assert keyword in data, f"Expected keyword '{keyword}' not found in catalog"
