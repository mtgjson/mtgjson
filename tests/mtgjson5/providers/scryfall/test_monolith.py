"""Tests for Scryfall provider using VCR cassettes."""

import pytest


@pytest.mark.vcr()
def test_catalog_keyword_abilities(reset_scryfall_singleton):
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Uses VCR cassette for offline deterministic testing.
    VCR will intercept and record/replay HTTP calls automatically.

    Resets the ScryfallProvider singleton to ensure test isolation and
    allow VCR to record all HTTP requests including those in __init__.
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
