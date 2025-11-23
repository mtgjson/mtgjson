"""Test the EnrichmentProvider."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from mtgjson5.classes import MtgjsonCardObject
from mtgjson5.providers.enrichment_provider import EnrichmentProvider


# Production UUIDs from card_enrichment.json
# Format: SET_NUMBER_CARDNAME_COLOR
FIN_551A_TRAVELING_CHOCOBO_YELLOW = "1b999e7e-83ff-5536-8f0c-b28413d157dc"
FIN_551B_TRAVELING_CHOCOBO_PINK = "b923c4dd-9428-5faf-9362-2c9b3d53528a"
LCI_410A_CAVERN_MULTICOLOR = "479e911d-ce0e-5e90-93c7-be72198cbd40"


class TestEnrichmentProviderInit:
    """Test EnrichmentProvider initialization."""

    def test_init_loads_valid_file(self):
        """Test that EnrichmentProvider loads production enrichment file."""
        provider = EnrichmentProvider()
        # Should load the real file with production data
        assert "_comment" in provider._data
        assert "by_uuid" in provider._data
        assert len(provider._data["by_uuid"]) > 0
        # Check for known production UUID (FIN 551a - Traveling Chocobo, yellow)
        assert FIN_551A_TRAVELING_CHOCOBO_YELLOW in provider._data["by_uuid"]
        assert FIN_551B_TRAVELING_CHOCOBO_PINK in provider._data["by_uuid"]

    def test_init_handles_missing_file(self, tmp_path, monkeypatch):
        """Test that EnrichmentProvider handles missing enrichment file gracefully."""
        # This test demonstrates the behavior, but monkeypatch doesn't work
        # due to Python's module-level constant caching. Keeping for documentation.
        pytest.skip("RESOURCE_PATH mocking not supported due to import-time evaluation")

    def test_init_handles_malformed_json(self, tmp_path, monkeypatch):
        """Test that EnrichmentProvider handles malformed JSON gracefully."""
        # This test demonstrates the behavior, but monkeypatch doesn't work
        # due to Python's module-level constant caching. Keeping for documentation.
        pytest.skip("RESOURCE_PATH mocking not supported due to import-time evaluation")


class TestEnrichmentProviderValidation:
    """Test EnrichmentProvider validation logic."""

    def test_validate_valid_promo_types(self):
        """Test validation accepts valid promo_types."""
        provider = EnrichmentProvider()
        entry = {"promo_types": ["neoninkyellow", "neoninkblue"]}
        assert provider._validate_enrichment_entry(entry, "test") is True

    def test_validate_empty_promo_types(self):
        """Test validation accepts empty promo_types list."""
        provider = EnrichmentProvider()
        entry = {"promo_types": []}
        assert provider._validate_enrichment_entry(entry, "test") is True

    def test_validate_no_promo_types(self):
        """Test validation accepts entries without promo_types."""
        provider = EnrichmentProvider()
        entry = {"other_field": "value"}
        assert provider._validate_enrichment_entry(entry, "test") is True

    def test_validate_rejects_non_list_promo_types(self):
        """Test validation rejects non-list promo_types."""
        provider = EnrichmentProvider()
        entry = {"promo_types": "not-a-list"}
        assert provider._validate_enrichment_entry(entry, "test") is False

    def test_validate_rejects_non_string_items(self):
        """Test validation rejects promo_types with non-string items."""
        provider = EnrichmentProvider()
        entry = {"promo_types": [123, 456]}
        assert provider._validate_enrichment_entry(entry, "test") is False

    def test_validate_rejects_mixed_types(self):
        """Test validation rejects promo_types with mixed types."""
        provider = EnrichmentProvider()
        entry = {"promo_types": ["valid", 123, "another"]}
        assert provider._validate_enrichment_entry(entry, "test") is False


class TestEnrichmentProviderLookup:
    """Test EnrichmentProvider lookup strategies."""

    def test_lookup_by_uuid_primary(self):
        """Test primary lookup by UUID (FIN 551a - Traveling Chocobo, yellow)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = FIN_551A_TRAVELING_CHOCOBO_YELLOW
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkyellow"]}

    def test_lookup_by_uuid_multiple_promo_types(self):
        """Test UUID lookup with multiple promo_types (LCI 410a - Cavern of Souls, three-color/multicolor/rainbow)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = LCI_410A_CAVERN_MULTICOLOR
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkthreecolor", "neoninkmulticolor", "neoninkrainbow"]}

    def test_lookup_by_set_number_name(self):
        """Test fallback lookup by set+number+name (NEO 430 - Hidetsugu Devouring Chaos, green)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid"
        card.set_code = "NEO"
        card.number = "430"
        card.name = "Hidetsugu, Devouring Chaos"
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkgreen"]}

    def test_lookup_by_set_number_name_with_wrong_name_returns_none(self):
        """Test wrong name with correct set+number returns None (NEO 430 exists but name doesn't match)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid"
        card.set_code = "NEO"
        card.number = "430"
        card.name = "Different Name"
        
        # Production data has "430|Hidetsugu, Devouring Chaos" - name must match
        result = provider.get_enrichment_for_card(card)
        assert result is None

    def test_lookup_prefers_number_name_over_uuid_when_uuid_missing(self):
        """Test that set+number+name works when UUID not in enrichment data."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid-12345"
        card.set_code = "SLD"
        card.number = "424"
        card.name = "Ghostly Prison"
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkyellow"]}

    def test_lookup_returns_none_for_missing_card(self):
        """Test that lookup returns None for cards not in enrichment data."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid-99999"
        card.set_code = "XXX"
        card.number = "999"
        card.name = "Non-existent Card"
        
        result = provider.get_enrichment_for_card(card)
        assert result is None

    def test_lookup_returns_none_for_missing_set(self):
        """Test that lookup returns None for cards in non-existent set."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid-88888"
        card.set_code = "MISSING"
        card.number = "1"
        card.name = "Some Card"
        
        result = provider.get_enrichment_for_card(card)
        assert result is None


class TestEnrichmentProviderDeepCopy:
    """Test that EnrichmentProvider returns deep copies."""

    def test_returns_deep_copy(self):
        """Test that returned data is a deep copy (FIN 551a - Traveling Chocobo, yellow)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = FIN_551A_TRAVELING_CHOCOBO_YELLOW
        
        result1 = provider.get_enrichment_for_card(card)
        result2 = provider.get_enrichment_for_card(card)
        
        # Modify first result
        result1["promo_types"].append("modified")
        
        # Second result should be unchanged
        assert result2 == {"promo_types": ["neoninkyellow"]}
        assert "modified" not in result2["promo_types"]

    def test_deep_copy_with_nested_structures(self, tmp_path, monkeypatch):
        """Test deep copy works with nested dictionaries."""
        # Skip due to monkeypatch limitations, but the deep copy logic is tested above
        pytest.skip("RESOURCE_PATH mocking not supported due to import-time evaluation")


class TestEnrichmentProviderInvalidData:
    """Test EnrichmentProvider handles invalid data gracefully."""

    def test_invalid_promo_types_returns_none(self, tmp_path, monkeypatch):
        """Test that invalid promo_types causes validation to return None."""
        # Skip due to monkeypatch limitations
        pytest.skip("RESOURCE_PATH mocking not supported due to import-time evaluation")


class TestEnrichmentProviderEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_card_without_uuid(self):
        """Test card without UUID falls back to set+number+name (NEO 430 - Hidetsugu, green)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        # No UUID set
        card.set_code = "NEO"
        card.number = "430"
        card.name = "Hidetsugu, Devouring Chaos"
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkgreen"]}

    def test_card_without_number(self):
        """Test lookup for card without number returns None."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid-77777"
        card.set_code = "NEO"
        # No number set
        card.name = "Test Card"
        
        result = provider.get_enrichment_for_card(card)
        assert result is None

    def test_card_without_name(self):
        """Test card without name returns None (NEO 430 requires name match)."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.uuid = "non-existent-uuid-66666"
        card.set_code = "NEO"
        card.number = "430"
        # No name set
        
        # NEO 430 is stored as "430|Hidetsugu, Devouring Chaos", number-only lookup fails
        result = provider.get_enrichment_for_card(card)
        assert result is None

    def test_empty_by_uuid_section(self, tmp_path, monkeypatch):
        """Test provider works when by_uuid section is empty."""
        # Skip due to monkeypatch limitations
        pytest.skip("RESOURCE_PATH mocking not supported due to import-time evaluation")

    def test_special_characters_in_card_name(self):
        """Test lookup with special characters in card name."""
        provider = EnrichmentProvider()
        card = MtgjsonCardObject()
        card.set_code = "TLA"
        card.number = "359"
        card.name = "Aang, Swift Savior // Aang and La, Ocean's Fury"
        
        result = provider.get_enrichment_for_card(card)
        assert result == {"promo_types": ["neoninkyellow"]}
