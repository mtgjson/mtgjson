"""Tests for Scryfall provider using VCR cassettes.

Example of using with_test_session fixture for VCR-based provider testing.
The fixture automatically:
- Resets all singleton providers for test isolation
- Injects cached_session during recording OR plain session during VCR playback
- Works with any provider (not just Scryfall)

Usage in other provider tests:
    @pytest.mark.vcr("api.provider-host.com.yml")
    def test_my_provider(with_test_session):
        provider = MyProvider()
        result = provider.some_method()
        assert result is not None
"""

from typing import Any

import pytest
from mtgjson5.providers.scryfall.monolith import ScryfallProvider


@pytest.mark.vcr("api.scryfall.com.yml")
def test_catalog_keyword_abilities(with_test_session: Any) -> None:
    """
    Test that we can fetch keyword abilities catalog from Scryfall.

    Recording workflow (when cassette needs updating):
    1. Remove @pytest.mark.vcr() decorator temporarily
    2. pytest tests/mtgjson5/providers/scryfall/
       → Caches HTTP responses via with_test_session fixture
       → Cache is automatically cleared at session start for fresh recordings
    3. pytest tests/mtgjson5/providers/scryfall/ --export-cassettes
       → Exports cache to tests/cassettes/api.scryfall.com.yml
    4. Re-add @pytest.mark.vcr() decorator
    5. Commit cassette

    Playback (normal use, CI):
    - VCR replays from api.scryfall.com.yml cassette (no network)
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
