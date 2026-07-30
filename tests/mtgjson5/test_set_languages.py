"""Tests for documented set language additions."""

from __future__ import annotations

import pytest

from mtgjson5.build.languages import get_set_language_additions, merge_set_languages
from mtgjson5.consts.languages import LANGUAGE_MAP


class TestSetLanguageAdditions:
    def test_resource_loads(self):
        assert get_set_language_additions()

    def test_set_codes_are_upper_case(self):
        for code in get_set_language_additions():
            assert code == code.upper()

    def test_languages_are_known_names(self):
        known = set(LANGUAGE_MAP.values())
        for code, langs in get_set_language_additions().items():
            unknown = set(langs) - known
            assert not unknown, f"{code} lists unknown language(s): {sorted(unknown)}"

    def test_languages_are_sorted_and_unique(self):
        for code, langs in get_set_language_additions().items():
            assert list(langs) == sorted(set(langs)), f"{code} additions are unsorted or duplicated"

    def test_english_is_never_an_addition(self):
        for code, langs in get_set_language_additions().items():
            assert "English" not in langs, f"{code} lists English, which is always implied"

    @pytest.mark.parametrize(
        ("set_code", "expected"),
        [
            # Portal Three Kingdoms was printed in Traditional Chinese
            ("PTK", "Chinese Traditional"),
            # Seventh Edition had an Italian printing
            ("7ED", "Italian"),
            # Fourth Edition had a Korean printing
            ("4ED", "Korean"),
        ],
    )
    def test_known_printings_present(self, set_code, expected):
        assert expected in get_set_language_additions()[set_code]


class TestMergeSetLanguages:
    def test_english_always_present(self):
        assert merge_set_languages("NEO", []) == ["English"]

    def test_missing_set_code(self):
        assert merge_set_languages(None, ["French"]) == ["English", "French"]

    def test_result_is_sorted_and_deduped(self):
        result = merge_set_languages("PTK", ["Japanese", "Japanese", "Chinese Traditional"])
        assert result == ["Chinese Traditional", "English", "Japanese"]

    def test_set_code_is_case_insensitive(self):
        assert merge_set_languages("ptk", []) == merge_set_languages("PTK", [])
