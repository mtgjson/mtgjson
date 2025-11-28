"""Tests for TCGplayer provider enum utilities and card finish detection."""

import pytest

from mtgjson5.providers.tcgplayer import CardFinish, TCGPlayerProvider


class TestCardFinish:
    """Tests for CardFinish enum and has_value class method."""

    def test_card_finish_has_value_true(self):
        """
        Test CardFinish.has_value returns True for existing enum value.

        References: mtgjson5/providers/tcgplayer.py lines 35-40
        """
        # Arrange
        value = "Foil Etched"

        # Act
        result = CardFinish.has_value(value)

        # Assert
        assert result is True

    def test_card_finish_has_value_false(self):
        """
        Test CardFinish.has_value returns False for non-existent value.

        References: mtgjson5/providers/tcgplayer.py lines 35-40
        """
        # Arrange
        value = "Nonexistent Finish"

        # Act
        result = CardFinish.has_value(value)

        # Assert
        assert result is False


class TestGetCardFinish:
    """Tests for get_card_finish static method."""

    def test_get_card_finish_finds_foil_etched(self):
        """
        Test get_card_finish extracts 'Foil Etched' finish from card name.

        References: mtgjson5/providers/tcgplayer.py lines 408-427
        TCGPlayer indicates finishes by ending card names with "(Finish)"
        """
        # Arrange
        card_name = "Lightning Bolt (Foil Etched)"

        # Act
        # Access static method through the singleton wrapper's __wrapped__ attribute
        result = TCGPlayerProvider.__wrapped__.get_card_finish(card_name)

        # Assert
        assert result == "FOIL ETCHED"

    def test_get_card_finish_ignores_numbers_in_parentheses(self):
        r"""
        Test get_card_finish ignores parentheses containing numbers.

        References: mtgjson5/providers/tcgplayer.py lines 419
        Regex pattern excludes numbers: r"\(([^)0-9]+)\)"
        This handles card names like "Card Name (123)" or "Card Name (v.2)"
        """
        # Arrange
        card_name = "Serra Angel (012)"

        # Act
        # Access static method through the singleton wrapper's __wrapped__ attribute
        result = TCGPlayerProvider.__wrapped__.get_card_finish(card_name)

        # Assert
        assert result is None

    def test_get_card_finish_returns_none_when_no_finish(self):
        """
        Test get_card_finish returns None when card has no recognized finish.

        References: mtgjson5/providers/tcgplayer.py lines 417-427
        """
        # Arrange
        card_name = "Black Lotus"

        # Act
        # Access static method through the singleton wrapper's __wrapped__ attribute
        result = TCGPlayerProvider.__wrapped__.get_card_finish(card_name)

        # Assert
        assert result is None


class TestConvertSkuDataEnum:
    """Tests for convert_sku_data_enum method."""

    def test_convert_sku_data_enum_converts_ids_to_names(self):
        """
        Test convert_sku_data_enum transforms SKU IDs into human-readable names.

        References: mtgjson5/providers/tcgplayer.py lines 429-455
        Converts enum IDs (languageId, printingId, conditionId) to names.
        """
        # Arrange
        provider = TCGPlayerProvider()
        product = {
            "name": "Lightning Bolt",
            "skus": [
                {
                    "skuId": 12345,
                    "productId": 67890,
                    "languageId": 1,  # ENGLISH
                    "printingId": 2,  # FOIL
                    "conditionId": 1,  # NEAR_MINT
                }
            ],
        }

        # Act
        result = provider.convert_sku_data_enum(product)

        # Assert
        assert len(result) == 1
        sku = result[0]
        assert sku["skuId"] == 12345
        assert sku["productId"] == 67890
        assert sku["language"] == "ENGLISH"
        assert sku["printing"] == "FOIL"
        assert sku["condition"] == "NEAR MINT"
        # No "finish" key since card name has no recognized finish
        assert "finish" not in sku

    def test_convert_sku_data_enum_includes_finish_when_detected(self):
        """
        Test convert_sku_data_enum includes finish field when detected in name.

        References: mtgjson5/providers/tcgplayer.py lines 440, 451-452
        """
        # Arrange
        provider = TCGPlayerProvider()
        product = {
            "name": "Lightning Bolt (Foil Etched)",
            "skus": [
                {
                    "skuId": 12345,
                    "productId": 67890,
                    "languageId": 1,  # ENGLISH
                    "printingId": 2,  # FOIL
                    "conditionId": 1,  # NEAR_MINT
                }
            ],
        }

        # Act
        result = provider.convert_sku_data_enum(product)

        # Assert
        assert len(result) == 1
        sku = result[0]
        assert sku["finish"] == "FOIL ETCHED"
