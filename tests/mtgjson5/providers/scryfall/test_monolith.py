"""Tests for Scryfall provider using VCR cassettes."""

import pytest


@pytest.mark.vcr("api.scryfall.com.yml")
def test_catalog_keyword_abilities(reset_scryfall_singleton):
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Recording workflow (when cassette needs updating):
    1. Remove @pytest.mark.vcr() decorator temporarily
    2. pytest tests/mtgjson5/providers/scryfall/
       → Caches HTTP responses via reset_scryfall_singleton fixture
    3. pytest tests/mtgjson5/providers/scryfall/ --export-cassettes
       → Exports cache to tests/cassettes/api.scryfall.com.yml
    4. Re-add @pytest.mark.vcr() decorator
    5. Commit cassette

    Playback (normal use, CI):
    - VCR replays from api.scryfall.com.yml cassette (no network)
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
