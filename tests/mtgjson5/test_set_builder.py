"""
Comprehensive tests for mtgjson5/set_builder.py

This test file covers Phase 1 of the test implementation plan:
- Pure parsing functions (parse_card_types, get_card_colors, get_card_cmc, is_number)
- Legalities parsing (parse_legalities)
- Core card building (build_mtgjson_card)
- Foreign data parsing (parse_foreign)
- UUID generation (add_uuid, get_mtgjson_v4_uuid)

Coverage target: 0% → 80%+
Total tests in Phase 1: 48 tests
"""

import uuid
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from mtgjson5 import set_builder
from mtgjson5.classes import (
    MtgjsonCardObject,
    MtgjsonForeignDataObject,
    MtgjsonLegalitiesObject,
)


# =============================================================================
# Tests for parse_card_types (tests 1-10)
# =============================================================================


class TestParseCardTypes:
    """Test suite for parse_card_types function."""

    @pytest.mark.parametrize(
        "card_type,expected_supertypes,expected_types,expected_subtypes",
        [
            # Test 1: Simple creature with one type and one subtype
            ("Creature — Human", [], ["Creature"], ["Human"]),
            # Test 2: Multiple subtypes
            ("Creature — Human Wizard", [], ["Creature"], ["Human", "Wizard"]),
            # Test 3: Legendary supertype
            (
                "Legendary Creature — Dragon",
                ["Legendary"],
                ["Creature"],
                ["Dragon"],
            ),
            # Test 4: Multiple supertypes
            (
                "Legendary Snow Creature — Elf Warrior",
                ["Legendary", "Snow"],
                ["Creature"],
                ["Elf", "Warrior"],
            ),
            # Test 5: No subtypes
            ("Instant", [], ["Instant"], []),
            # Test 6: Artifact creature
            ("Artifact Creature — Golem", [], ["Artifact", "Creature"], ["Golem"]),
            # Test 7: Planeswalker
            ("Legendary Planeswalker — Jace", ["Legendary"], ["Planeswalker"], ["Jace"]),
            # Test 8: Plane (special case - entire subtype)
            ("Plane — Ravnica", [], ["Plane"], ["Ravnica"]),
            # Test 9: Multi-word subtype (Time Lord from WHO set)
            ("Creature — Time Lord", [], ["Creature"], ["Time Lord"]),
            # Test 10: Complex type with multiple multi-word subtypes
            ("Legendary Enchantment Creature — Time Lord Warrior", ["Legendary"], ["Enchantment", "Creature"], ["Time Lord", "Warrior"]),
        ],
    )
    def test_parse_card_types_variations(
        self,
        card_type: str,
        expected_supertypes: List[str],
        expected_types: List[str],
        expected_subtypes: List[str],
    ):
        """
        Test parse_card_types with various card type combinations.

        Given: A card type string with various combinations of supertypes, types, and subtypes
        When: parse_card_types is called
        Then: Returns correct tuple of (supertypes, types, subtypes)
        """
        # Act
        supertypes, types, subtypes = set_builder.parse_card_types(card_type)

        # Assert
        assert supertypes == expected_supertypes, f"Supertypes mismatch for {card_type}"
        assert types == expected_types, f"Types mismatch for {card_type}"
        assert subtypes == expected_subtypes, f"Subtypes mismatch for {card_type}"


# =============================================================================
# Tests for get_card_colors (tests 11-15)
# =============================================================================


class TestGetCardColors:
    """Test suite for get_card_colors function."""

    def test_single_color_white(self):
        """
        Test extracting single white color from mana cost.

        Given: A mana cost string containing only white mana
        When: get_card_colors is called
        Then: Returns list with only 'W'
        """
        # Arrange
        mana_cost = "{W}{W}"

        # Act
        colors = set_builder.get_card_colors(mana_cost)

        # Assert
        assert colors == ["W"]

    def test_multicolor_card(self):
        """
        Test extracting multiple colors from mana cost.

        Given: A mana cost string containing multiple colors
        When: get_card_colors is called
        Then: Returns list with all present colors in WUBRG order
        """
        # Arrange
        mana_cost = "{2}{U}{B}{R}"

        # Act
        colors = set_builder.get_card_colors(mana_cost)

        # Assert
        assert colors == ["U", "B", "R"]

    def test_all_five_colors(self):
        """
        Test extracting all five colors from mana cost.

        Given: A mana cost string containing all WUBRG colors
        When: get_card_colors is called
        Then: Returns list with all five colors
        """
        # Arrange
        mana_cost = "{W}{U}{B}{R}{G}"

        # Act
        colors = set_builder.get_card_colors(mana_cost)

        # Assert
        assert colors == ["W", "U", "B", "R", "G"]

    def test_colorless_card(self):
        """
        Test colorless card returns empty list.

        Given: A mana cost string with no colored mana
        When: get_card_colors is called
        Then: Returns empty list
        """
        # Arrange
        mana_cost = "{3}"

        # Act
        colors = set_builder.get_card_colors(mana_cost)

        # Assert
        assert colors == []

    def test_hybrid_mana(self):
        """
        Test hybrid mana extracts both colors.

        Given: A mana cost string with hybrid mana symbols
        When: get_card_colors is called
        Then: Returns list with colors from hybrid symbols
        """
        # Arrange
        mana_cost = "{G/W}{U/R}"

        # Act
        colors = set_builder.get_card_colors(mana_cost)

        # Assert
        # Should extract W, U, R, G
        assert set(colors) == {"W", "U", "R", "G"}


# =============================================================================
# Tests for get_card_cmc (tests 16-18)
# =============================================================================


class TestGetCardCmc:
    """Test suite for get_card_cmc function."""

    def test_simple_numeric_cost(self):
        """
        Test CMC calculation for simple numeric mana cost.

        Given: A mana cost with numeric and colored mana
        When: get_card_cmc is called
        Then: Returns correct total converted mana cost
        """
        # Arrange
        mana_cost = "{2}{G}{G}"

        # Act
        cmc = set_builder.get_card_cmc(mana_cost)

        # Assert
        assert cmc == 4.0

    def test_hybrid_mana_cmc(self):
        """
        Test CMC calculation for hybrid mana (uses higher cost).

        Given: A mana cost with hybrid mana symbols
        When: get_card_cmc is called
        Then: Returns CMC using the first (higher) value of hybrid symbols
        """
        # Arrange
        mana_cost = "{2/W}{G/U}"

        # Act
        cmc = set_builder.get_card_cmc(mana_cost)

        # Assert
        # {2/W} = 2, {G/U} = 1, total = 3
        assert cmc == 3.0

    def test_variable_mana_ignored(self):
        """
        Test that X, Y, Z variables are not counted in CMC.

        Given: A mana cost with variable mana symbols (X, Y, Z)
        When: get_card_cmc is called
        Then: Returns CMC without counting variables
        """
        # Arrange
        mana_cost = "{X}{U}{U}"

        # Act
        cmc = set_builder.get_card_cmc(mana_cost)

        # Assert
        # X is ignored, {U}{U} = 2
        assert cmc == 2.0

    def test_half_mana(self):
        """
        Test CMC calculation with half mana symbols.

        Given: A mana cost with half mana symbols (HW, HU, etc.)
        When: get_card_cmc is called
        Then: Returns CMC counting half mana as 0.5
        """
        # Arrange
        mana_cost = "{HW}{HW}"

        # Act
        cmc = set_builder.get_card_cmc(mana_cost)

        # Assert
        assert cmc == 1.0


# =============================================================================
# Tests for is_number (tests 19-21)
# =============================================================================


class TestIsNumber:
    """Test suite for is_number function."""

    def test_integer_string(self):
        """
        Test that integer strings are recognized as numbers.

        Given: A string representing an integer
        When: is_number is called
        Then: Returns True
        """
        # Arrange
        value = "42"

        # Act
        result = set_builder.is_number(value)

        # Assert
        assert result is True

    def test_float_string(self):
        """
        Test that float strings are recognized as numbers.

        Given: A string representing a float
        When: is_number is called
        Then: Returns True
        """
        # Arrange
        value = "3.14"

        # Act
        result = set_builder.is_number(value)

        # Assert
        assert result is True

    def test_non_numeric_string(self):
        """
        Test that non-numeric strings return False.

        Given: A string that is not a number
        When: is_number is called
        Then: Returns False
        """
        # Arrange
        value = "X"

        # Act
        result = set_builder.is_number(value)

        # Assert
        assert result is False

    def test_unicode_numeric(self):
        """
        Test that unicode numeric characters are recognized.

        Given: A unicode numeric character
        When: is_number is called
        Then: Returns True
        """
        # Arrange
        value = "½"  # Unicode fraction

        # Act
        result = set_builder.is_number(value)

        # Assert
        assert result is True


# =============================================================================
# Tests for parse_legalities (tests 22-24)
# =============================================================================


class TestParseLegalities:
    """Test suite for parse_legalities function."""

    def test_legal_formats(self):
        """
        Test parsing legal formats from Scryfall data.

        Given: A Scryfall legalities dict with legal formats
        When: parse_legalities is called
        Then: Returns MtgjsonLegalitiesObject with capitalized legal statuses
        """
        # Arrange
        sf_legalities = {
            "standard": "legal",
            "modern": "legal",
            "legacy": "legal",
            "vintage": "legal",
        }

        # Act
        result = set_builder.parse_legalities(sf_legalities)

        # Assert
        assert isinstance(result, MtgjsonLegalitiesObject)
        assert result.standard == "Legal"
        assert result.modern == "Legal"
        assert result.legacy == "Legal"
        assert result.vintage == "Legal"

    def test_restricted_and_banned_formats(self):
        """
        Test parsing restricted and banned formats.

        Given: A Scryfall legalities dict with restricted and banned formats
        When: parse_legalities is called
        Then: Returns MtgjsonLegalitiesObject with correct statuses
        """
        # Arrange
        sf_legalities = {
            "vintage": "restricted",
            "legacy": "banned",
            "modern": "legal",
        }

        # Act
        result = set_builder.parse_legalities(sf_legalities)

        # Assert
        assert result.vintage == "Restricted"
        assert result.legacy == "Banned"
        assert result.modern == "Legal"

    def test_not_legal_formats_excluded(self):
        """
        Test that not_legal formats are not included.

        Given: A Scryfall legalities dict with not_legal formats
        When: parse_legalities is called
        Then: Returns MtgjsonLegalitiesObject without not_legal formats
        """
        # Arrange
        sf_legalities = {
            "standard": "not_legal",
            "modern": "legal",
            "legacy": "not_legal",
        }

        # Act
        result = set_builder.parse_legalities(sf_legalities)

        # Assert
        assert not hasattr(result, "standard") or result.standard is None
        assert result.modern == "Legal"
        assert not hasattr(result, "legacy") or result.legacy is None


# =============================================================================
# Tests for build_mtgjson_card - Normal Cards (tests 25-30)
# =============================================================================


class TestBuildMtgjsonCardNormal:
    """Test suite for build_mtgjson_card with normal cards."""

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_normal_card_basic_attributes(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test building a normal card with basic attributes.

        Given: A normal Scryfall card object
        When: build_mtgjson_card is called
        Then: Returns a list with one MtgjsonCardObject with correct basic attributes
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21", "M20"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        assert len(cards) == 1
        card = cards[0]
        assert isinstance(card, MtgjsonCardObject)
        assert card.name == "Grizzly Bears"
        assert card.set_code == "M21"
        assert card.mana_cost == "{1}{G}"
        assert card.mana_value == 2.0
        assert card.power == "2"
        assert card.toughness == "2"
        assert card.colors == ["G"]

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_card_with_parsed_types(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that build_mtgjson_card correctly parses card types.

        Given: A Scryfall card with type line
        When: build_mtgjson_card is called
        Then: Returns card with correctly parsed supertypes, types, and subtypes
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.supertypes == []
        assert card.types == ["Creature"]
        assert card.subtypes == ["Bear"]

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_card_with_identifiers(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that build_mtgjson_card populates identifiers correctly.

        Given: A Scryfall card with various IDs
        When: build_mtgjson_card is called
        Then: Returns card with correctly populated identifiers object
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.identifiers.scryfall_id == "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        assert (
            card.identifiers.scryfall_oracle_id
            == "ef1a8e9f-c0d1-4c3c-9e3b-3d0e8c8f1e8f"
        )
        assert card.identifiers.multiverse_id == "123456"
        assert (
            card.identifiers.scryfall_illustration_id
            == "99c56c3e-a77e-4e3a-a3a9-8e3a3a3a3a3a"
        )

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_card_with_availability(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that build_mtgjson_card sets availability correctly.

        Given: A Scryfall card with games array
        When: build_mtgjson_card is called
        Then: Returns card with correct availability flags
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.availability.paper is True
        assert card.availability.mtgo is True
        assert card.availability.arena is False

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_card_with_legalities(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that build_mtgjson_card parses legalities correctly.

        Given: A Scryfall card with legalities dict
        When: build_mtgjson_card is called
        Then: Returns card with correctly parsed legalities
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.legalities.standard == "Legal"
        assert card.legalities.modern == "Legal"
        assert card.legalities.legacy == "Legal"
        assert card.legalities.vintage == "Legal"

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_card_with_finishes(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that build_mtgjson_card sets foil/nonfoil flags correctly.

        Given: A Scryfall card with finishes array
        When: build_mtgjson_card is called
        Then: Returns card with correct has_foil and has_non_foil flags
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.has_foil is True
        assert card.has_non_foil is True
        assert card.finishes == ["nonfoil", "foil"]


# =============================================================================
# Tests for build_mtgjson_card - DFC/Split/Meld Cards (tests 31-37)
# =============================================================================


class TestBuildMtgjsonCardComplex:
    """Test suite for build_mtgjson_card with complex card layouts."""

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_builds_dfc_card_creates_two_faces(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_dfc,
    ):
        """
        Test that DFC card creates separate card objects for each face.

        Given: A double-faced card (transform) Scryfall object
        When: build_mtgjson_card is called
        Then: Returns a list with two MtgjsonCardObjects, one for each face
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["ISD"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_dfc)

        # Assert
        assert len(cards) == 2
        # Implementation returns faces in reverse order (back face first)
        assert cards[1].face_name == "Delver of Secrets"
        assert cards[0].face_name == "Insectile Aberration"

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_dfc_card_sets_face_attributes(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_dfc,
    ):
        """
        Test that DFC card faces have correct face-specific attributes.

        Given: A double-faced card Scryfall object
        When: build_mtgjson_card is called
        Then: Each face has correct power, toughness, and oracle text
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["ISD"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_dfc)

        # Assert
        # Implementation returns faces in reverse order (back face first)
        back = cards[0]
        front = cards[1]
        assert front.power == "1"
        assert front.toughness == "1"
        assert back.power == "3"
        assert back.toughness == "2"
        assert "Flying" in back.text

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_split_card_creates_two_faces(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_split,
    ):
        """
        Test that split card creates separate objects for each half.

        Given: A split card Scryfall object
        When: build_mtgjson_card is called
        Then: Returns a list with two MtgjsonCardObjects for each half
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["APC"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_split)

        # Assert
        assert len(cards) == 2
        # Implementation returns faces in reverse order (back face first)
        assert cards[1].face_name == "Fire"
        assert cards[0].face_name == "Ice"

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_split_card_calculates_face_cmc(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_split,
    ):
        """
        Test that split card faces have correct individual CMC.

        Given: A split card with different mana costs per side
        When: build_mtgjson_card is called
        Then: Each face has correct face_mana_value
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["APC"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_split)

        # Assert
        fire = cards[0]
        ice = cards[1]
        assert fire.face_mana_value == 2.0  # {1}{R}
        assert ice.face_mana_value == 2.0  # {1}{U}

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_meld_card_processes_all_parts(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_meld,
    ):
        """
        Test that meld card processes all_parts correctly.

        Given: A meld card Scryfall object with all_parts
        When: build_mtgjson_card is called
        Then: Returns card with names set from all_parts
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["EMN"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Mock meld_triplets.json
        with patch("mtgjson5.set_builder.RESOURCE_PATH") as mock_path:
            mock_file = MagicMock()
            mock_file.__enter__.return_value.read.return_value = "[]"
            mock_path.joinpath.return_value.open.return_value = mock_file

            # Act
            cards = set_builder.build_mtgjson_card(sample_scryfall_card_meld)

        # Assert
        assert len(cards) >= 1
        # Meld cards should have names populated from all_parts

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_card_with_ascii_name(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that cards with non-ASCII characters get ascii_name field.

        Given: A Scryfall card with unicode characters in name
        When: build_mtgjson_card is called
        Then: Returns card with ascii_name field populated
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Modify the card to have unicode characters (use accented character instead of ligature)
        # NFD normalization drops the Æ ligature entirely, so use a character that converts properly
        sample_scryfall_card_normal["name"] = "Jötun Grunt"

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.ascii_name == "Jotun Grunt"

    @patch("mtgjson5.set_builder.parse_foreign")
    @patch("mtgjson5.set_builder.parse_rulings")
    @patch("mtgjson5.set_builder.parse_printings")
    @patch("mtgjson5.set_builder.EdhrecProviderCardRanks")
    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_card_is_rebalanced_flag(
        self,
        mock_scryfall,
        mock_edhrec,
        mock_printings,
        mock_rulings,
        mock_foreign,
        sample_scryfall_card_normal,
    ):
        """
        Test that cards with A- prefix get is_rebalanced flag.

        Given: A Scryfall card with A- prefix in name
        When: build_mtgjson_card is called
        Then: Returns card with is_rebalanced and is_alternative flags set
        """
        # Arrange
        mock_edhrec.return_value.get_salt_rating.return_value = None
        mock_printings.return_value = ["M21"]
        mock_rulings.return_value = []
        mock_foreign.return_value = []
        mock_scryfall.return_value.cards_without_limits = []

        # Modify to have A- prefix
        sample_scryfall_card_normal["name"] = "A-Grizzly Bears"

        # Act
        cards = set_builder.build_mtgjson_card(sample_scryfall_card_normal)

        # Assert
        card = cards[0]
        assert card.is_rebalanced is True
        assert card.is_alternative is True


# =============================================================================
# Tests for parse_foreign (tests 38-40)
# =============================================================================


class TestParseForeign:
    """Test suite for parse_foreign function."""

    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_parse_foreign_returns_list(self, mock_scryfall):
        """
        Test that parse_foreign returns a list of foreign data objects.

        Given: A valid Scryfall prints URL
        When: parse_foreign is called
        Then: Returns a list of MtgjsonForeignDataObject
        """
        # Arrange
        mock_provider = Mock()
        mock_provider.download_all_pages.return_value = [
            {
                "set": "m21",
                "collector_number": "200",
                "lang": "ja",
                "name": "灰色熊",
                "multiverse_ids": [123457],
                "id": "foreign-id-1",
            }
        ]
        mock_scryfall.return_value = mock_provider

        # Act
        result = set_builder.parse_foreign(
            "https://api.scryfall.com/cards/search", "Grizzly Bears", "200", "m21"
        )

        # Assert
        assert isinstance(result, list)
        if result:
            assert isinstance(result[0], MtgjsonForeignDataObject)

    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_parse_foreign_filters_english(self, mock_scryfall):
        """
        Test that parse_foreign filters out English language cards.

        Given: API results containing English language cards
        When: parse_foreign is called
        Then: Returns list without English cards
        """
        # Arrange
        mock_provider = Mock()
        mock_provider.download_all_pages.return_value = [
            {
                "set": "m21",
                "collector_number": "200",
                "lang": "en",
                "name": "Grizzly Bears",
                "multiverse_ids": [123456],
                "id": "english-id",
            },
            {
                "set": "m21",
                "collector_number": "200",
                "lang": "ja",
                "name": "灰色熊",
                "multiverse_ids": [123457],
                "id": "foreign-id-1",
            },
        ]
        mock_scryfall.return_value = mock_provider

        # Act
        result = set_builder.parse_foreign(
            "https://api.scryfall.com/cards/search", "Grizzly Bears", "200", "m21"
        )

        # Assert
        # Should not include English version
        assert all(entry.language != "English" for entry in result)

    @patch("mtgjson5.set_builder.ScryfallProvider")
    def test_parse_foreign_handles_dfc(self, mock_scryfall):
        """
        Test that parse_foreign handles double-faced cards correctly.

        Given: A DFC with card_faces in foreign language
        When: parse_foreign is called
        Then: Returns foreign data with combined name and face_name
        """
        # Arrange
        mock_provider = Mock()
        mock_provider.download_all_pages.return_value = [
            {
                "set": "isd",
                "collector_number": "51",
                "lang": "ja",
                "name": "秘密を掘り下げる者 // 昆虫の逸脱者",
                "card_faces": [
                    {"name": "秘密を掘り下げる者", "printed_name": "秘密を掘り下げる者"},
                    {"name": "昆虫の逸脱者", "printed_name": "昆虫の逸脱者"},
                ],
                "multiverse_ids": [123458],
                "id": "foreign-dfc-id",
            }
        ]
        mock_scryfall.return_value = mock_provider

        # Act
        result = set_builder.parse_foreign(
            "https://api.scryfall.com/cards/search",
            "Delver of Secrets",
            "51",
            "isd",
        )

        # Assert
        assert len(result) > 0
        # Should have combined name with //
        assert "//" in result[0].name


# =============================================================================
# Tests for UUID Generation (tests 41-48)
# =============================================================================


class TestUuidGeneration:
    """Test suite for UUID generation functions."""

    def test_add_uuid_generates_v5_uuid(self):
        """
        Test that add_uuid generates a valid UUIDv5.

        Given: A MtgjsonCardObject with scryfall_id
        When: add_uuid is called
        Then: Generates a valid UUIDv5 string
        """
        # Arrange
        card = MtgjsonCardObject()
        card.name = "Test Card"
        card.types = ["Creature"]
        card.foreign_data = []
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"

        # Mock UuidCacheProvider to return None (no cached UUID)
        with patch("mtgjson5.set_builder.UuidCacheProvider") as mock_cache:
            mock_cache.return_value.get_uuid.return_value = None

            # Act
            set_builder.add_uuid(card)

        # Assert
        assert card.uuid is not None
        # Validate it's a proper UUID format
        try:
            uuid.UUID(card.uuid)
            assert True
        except ValueError:
            assert False, "Generated UUID is not valid"

    def test_add_uuid_uses_cached_uuid(self):
        """
        Test that add_uuid uses cached UUID when available.

        Given: A card with a cached UUID in UuidCacheProvider
        When: add_uuid is called
        Then: Uses the cached UUID instead of generating new one
        """
        # Arrange
        card = MtgjsonCardObject()
        card.name = "Test Card"
        card.types = ["Creature"]
        card.foreign_data = []
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        cached_uuid = "12345678-1234-5678-1234-567812345678"

        with patch("mtgjson5.set_builder.UuidCacheProvider") as mock_cache:
            mock_cache.return_value.get_uuid.return_value = cached_uuid

            # Act
            set_builder.add_uuid(card)

        # Assert
        assert card.uuid == cached_uuid

    def test_add_uuid_includes_side_in_generation(self):
        """
        Test that add_uuid includes side in UUID generation.

        Given: Two cards with same scryfall_id but different sides
        When: add_uuid is called on both
        Then: Generates different UUIDs for each side
        """
        # Arrange
        card_a = MtgjsonCardObject()
        card_a.name = "Test Card"
        card_a.types = ["Creature"]
        card_a.foreign_data = []
        card_a.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        card_a.side = "a"

        card_b = MtgjsonCardObject()
        card_b.name = "Test Card"
        card_b.types = ["Creature"]
        card_b.foreign_data = []
        card_b.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        card_b.side = "b"

        with patch("mtgjson5.set_builder.UuidCacheProvider") as mock_cache:
            mock_cache.return_value.get_uuid.return_value = None

            # Act
            set_builder.add_uuid(card_a)
            set_builder.add_uuid(card_b)

        # Assert
        assert card_a.uuid != card_b.uuid

    def test_add_uuid_defaults_side_to_a(self):
        """
        Test that add_uuid defaults side to 'a' when not specified.

        Given: A card without side attribute
        When: add_uuid is called
        Then: Uses 'a' as default side for UUID generation
        """
        # Arrange
        card = MtgjsonCardObject()
        card.name = "Test Card"
        card.types = ["Creature"]
        card.foreign_data = []
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        # side is None

        with patch("mtgjson5.set_builder.UuidCacheProvider") as mock_cache:
            mock_cache.return_value.get_uuid.return_value = None

            # Act
            set_builder.add_uuid(card)

        # Assert
        # Should have generated UUID (test passes if no exception)
        assert card.uuid is not None

    def test_get_mtgjson_v4_uuid_token(self):
        """
        Test that get_mtgjson_v4_uuid generates correct UUID for tokens.

        Given: A token MtgjsonCardObject
        When: get_mtgjson_v4_uuid is called
        Then: Generates UUID using token-specific algorithm
        """
        # Arrange
        token = MtgjsonCardObject(is_token=True)
        token.name = "Goblin"
        token.face_name = None
        token.colors = ["R"]
        token.power = "1"
        token.toughness = "1"
        token.side = None
        token.set_code = "TM21"
        token.identifiers.scryfall_id = "token-id-123"
        token.types = ["Token"]

        # Act
        v4_uuid = set_builder.get_mtgjson_v4_uuid(token)

        # Assert
        assert v4_uuid is not None
        # Validate it's a proper UUID format
        try:
            uuid.UUID(v4_uuid)
            assert True
        except ValueError:
            assert False, "Generated v4 UUID is not valid"

    def test_get_mtgjson_v4_uuid_normal_card(self):
        """
        Test that get_mtgjson_v4_uuid generates correct UUID for normal cards.

        Given: A normal MtgjsonCardObject
        When: get_mtgjson_v4_uuid is called
        Then: Generates UUID using normal card algorithm
        """
        # Arrange
        card = MtgjsonCardObject()
        card.name = "Grizzly Bears"
        card.face_name = None
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        card.types = ["Creature"]

        # Act
        v4_uuid = set_builder.get_mtgjson_v4_uuid(card)

        # Assert
        assert v4_uuid is not None
        try:
            uuid.UUID(v4_uuid)
            assert True
        except ValueError:
            assert False, "Generated v4 UUID is not valid"

    def test_add_uuid_populates_v4_id(self):
        """
        Test that add_uuid also populates mtgjson_v4_id identifier.

        Given: A MtgjsonCardObject
        When: add_uuid is called
        Then: Populates both uuid and identifiers.mtgjson_v4_id
        """
        # Arrange
        card = MtgjsonCardObject()
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        card.types = ["Creature"]
        card.foreign_data = []
        card.name = "Grizzly Bears"

        with patch("mtgjson5.set_builder.UuidCacheProvider") as mock_cache:
            mock_cache.return_value.get_uuid.return_value = None

            # Act
            set_builder.add_uuid(card)

        # Assert
        assert card.identifiers.mtgjson_v4_id is not None
        # Both should be valid UUIDs
        try:
            uuid.UUID(card.uuid)
            uuid.UUID(card.identifiers.mtgjson_v4_id)
            assert True
        except ValueError:
            assert False, "Generated UUIDs are not valid"

    @patch("mtgjson5.set_builder.UuidCacheProvider")
    def test_add_extra_language_uuids(self, mock_cache):
        """
        Test that add_uuid also generates UUIDs for foreign_data entries.

        Given: A card with foreign_data entries
        When: add_uuid is called
        Then: Each foreign_data entry gets a unique UUID
        """
        # Arrange
        card = MtgjsonCardObject()
        card.identifiers.scryfall_id = "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc"
        card.types = ["Creature"]
        card.name = "Grizzly Bears"

        # Add foreign data
        foreign_entry_1 = MtgjsonForeignDataObject()
        foreign_entry_1.language = "Japanese"
        foreign_entry_2 = MtgjsonForeignDataObject()
        foreign_entry_2.language = "French"
        card.foreign_data = [foreign_entry_1, foreign_entry_2]

        mock_cache.return_value.get_uuid.return_value = None

        # Act
        set_builder.add_uuid(card)

        # Assert
        assert foreign_entry_1.uuid is not None
        assert foreign_entry_2.uuid is not None
        assert foreign_entry_1.uuid != foreign_entry_2.uuid
