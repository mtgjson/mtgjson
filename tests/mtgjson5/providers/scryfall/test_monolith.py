"""Tests for Scryfall provider using VCR cassettes."""

import pytest

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
