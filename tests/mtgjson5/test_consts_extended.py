"""Tests for constants modules: cards, expansions, mappings, languages, outputs, fields."""

from __future__ import annotations

import pytest

from mtgjson5.consts import (
    BASIC_LAND_NAMES,
    CARD_MARKET_BUFFER,
    FUNNY_SETS_WITH_ACORN,
    LANGUAGE_MAP,
    MULTI_WORD_SUB_TYPES,
    SUPER_TYPES,
    SUPPORTED_SET_TYPES,
    TYPEDDICT_FIELD_ALIASES,
)
from mtgjson5.consts.fields import (
    IDENTIFIERS_FIELD_SOURCES,
    OMIT_EMPTY_LIST_FIELDS,
    OMIT_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    ORACLE_IDENTITY_COLS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_CARD_LIST_FIELDS,
    REQUIRED_DECK_LIST_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SCRYFALL_COLUMNS_TO_DROP,
)
from mtgjson5.consts.mappings import ASCII_REPLACEMENTS
from mtgjson5.consts.outputs import COMPILED_OUTPUT_NAMES

# ---------------------------------------------------------------------------
# TestCardConstants
# ---------------------------------------------------------------------------


class TestCardConstants:
    def test_super_types_cardinality(self):
        assert len(SUPER_TYPES) == 6

    @pytest.mark.parametrize("st", ["Basic", "Legendary", "Snow", "World", "Host", "Ongoing"])
    def test_super_types_members(self, st: str):
        assert st in SUPER_TYPES

    def test_basic_land_names_exactly_five(self):
        assert len(BASIC_LAND_NAMES) == 5

    @pytest.mark.parametrize("land", ["Plains", "Island", "Swamp", "Mountain", "Forest"])
    def test_basic_land_names_members(self, land: str):
        assert land in BASIC_LAND_NAMES

    def test_multi_word_sub_types_contains_time_lord(self):
        assert "Time Lord" in MULTI_WORD_SUB_TYPES

    def test_basic_lands_not_supertypes(self):
        assert BASIC_LAND_NAMES.isdisjoint(SUPER_TYPES)


# ---------------------------------------------------------------------------
# TestExpansionConstants
# ---------------------------------------------------------------------------


class TestExpansionConstants:
    def test_funny_sets_contains_unf(self):
        assert "UNF" in FUNNY_SETS_WITH_ACORN

    def test_supported_set_types_expected(self):
        expected = {"expansion", "core", "draft_innovation", "commander", "masters"}
        assert expected == SUPPORTED_SET_TYPES


# ---------------------------------------------------------------------------
# TestASCIIReplacements
# ---------------------------------------------------------------------------


class TestASCIIReplacements:
    def test_all_values_are_ascii(self):
        for char, replacement in ASCII_REPLACEMENTS.items():
            assert replacement.isascii(), f"Replacement for {char!r} is not ASCII: {replacement!r}"

    def test_all_keys_are_non_ascii(self):
        for char in ASCII_REPLACEMENTS:
            assert not char.isascii(), f"Key {char!r} is already ASCII"

    def test_ligatures(self):
        assert ASCII_REPLACEMENTS["Æ"] == "AE"
        assert ASCII_REPLACEMENTS["æ"] == "ae"
        assert ASCII_REPLACEMENTS["Œ"] == "OE"
        assert ASCII_REPLACEMENTS["œ"] == "oe"

    def test_eszett(self):
        assert ASCII_REPLACEMENTS["ß"] == "ss"

    def test_accented_vowels_lowercase(self):
        for char in ("é", "è", "ê", "ë"):
            assert ASCII_REPLACEMENTS[char] == "e"
        for char in ("á", "à", "â", "ä", "ã"):
            assert ASCII_REPLACEMENTS[char] == "a"

    def test_special_characters_removed(self):
        assert ASCII_REPLACEMENTS["®"] == ""
        assert ASCII_REPLACEMENTS["꞉"] == ""

    def test_no_duplicate_keys(self):
        keys = list(ASCII_REPLACEMENTS.keys())
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# TestLanguageMap
# ---------------------------------------------------------------------------


class TestLanguageMap:
    def test_english_present(self):
        assert LANGUAGE_MAP["en"] == "English"

    def test_phyrexian_codes(self):
        assert LANGUAGE_MAP["ph"] == "Phyrexian"
        assert LANGUAGE_MAP["px"] == "Phyrexian"

    @pytest.mark.parametrize(
        ("code", "name"),
        [("ja", "Japanese"), ("fr", "French"), ("de", "German"), ("es", "Spanish")],
    )
    def test_major_languages(self, code: str, name: str):
        assert LANGUAGE_MAP[code] == name

    def test_all_values_non_empty(self):
        for code, name in LANGUAGE_MAP.items():
            assert name, f"Empty language name for code {code!r}"


# ---------------------------------------------------------------------------
# TestOutputConstants
# ---------------------------------------------------------------------------


class TestOutputConstants:
    def test_all_printings_in_compiled(self):
        assert "AllPrintings" in COMPILED_OUTPUT_NAMES

    def test_meta_in_compiled(self):
        assert "Meta" in COMPILED_OUTPUT_NAMES

    @pytest.mark.parametrize("name", ["Standard", "Pioneer", "Modern", "Legacy", "Vintage"])
    def test_format_files_in_compiled(self, name: str):
        assert name in COMPILED_OUTPUT_NAMES

    @pytest.mark.parametrize(
        "name", ["StandardAtomic", "PioneerAtomic", "ModernAtomic", "LegacyAtomic", "VintageAtomic", "PauperAtomic"]
    )
    def test_atomic_format_files_in_compiled(self, name: str):
        assert name in COMPILED_OUTPUT_NAMES


# ---------------------------------------------------------------------------
# TestFieldConstants
# ---------------------------------------------------------------------------


class TestFieldConstants:
    def test_required_list_fields_is_union(self):
        assert REQUIRED_LIST_FIELDS == REQUIRED_CARD_LIST_FIELDS | REQUIRED_DECK_LIST_FIELDS

    def test_omit_fields_is_union(self):
        assert OMIT_FIELDS == OPTIONAL_BOOL_FIELDS | OMIT_EMPTY_LIST_FIELDS | OTHER_OPTIONAL_FIELDS

    def test_optional_bools_all_start_with_is_or_has(self):
        for field in OPTIONAL_BOOL_FIELDS:
            assert field.startswith(("is", "has")), f"{field} has unexpected prefix"

    def test_required_set_bools(self):
        assert "isFoilOnly" in REQUIRED_SET_BOOL_FIELDS
        assert "isOnlineOnly" in REQUIRED_SET_BOOL_FIELDS

    def test_oracle_identity_cols_tuple(self):
        assert isinstance(ORACLE_IDENTITY_COLS, tuple)
        assert "name" in ORACLE_IDENTITY_COLS
        assert "type" in ORACLE_IDENTITY_COLS

    def test_scryfall_columns_to_drop_no_duplicates(self):
        assert len(SCRYFALL_COLUMNS_TO_DROP) == len(set(SCRYFALL_COLUMNS_TO_DROP))

    def test_identifiers_field_sources_has_scryfall_id(self):
        assert "scryfallId" in IDENTIFIERS_FIELD_SOURCES

    def test_card_market_buffer_is_digit_string(self):
        assert CARD_MARKET_BUFFER.isdigit()

    def test_typeddict_field_aliases_rulings(self):
        assert TYPEDDICT_FIELD_ALIASES[("Rulings", "publishedAt")] == "date"
        assert TYPEDDICT_FIELD_ALIASES[("Rulings", "comment")] == "text"
