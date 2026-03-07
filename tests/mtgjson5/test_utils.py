"""Tests for mtgjson5.utils pure utility functions."""

from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any
from unittest.mock import patch

import pytest

from mtgjson5.utils import (
    deep_sort_keys,
    generate_output_file_hashes,
    get_all_entities_from_content,
    get_file_hash,
    get_str_or_none,
    parse_magic_rules_subset,
    recursive_sort,
    sort_internal_lists,
    to_camel_case,
    to_snake_case,
    url_keygen,
)


# ---------------------------------------------------------------------------
# url_keygen
# ---------------------------------------------------------------------------


class TestUrlKeygen:
    def test_with_leading(self):
        result = url_keygen("test_seed")
        assert result.startswith("https://mtgjson.com/links/")
        assert len(result) == len("https://mtgjson.com/links/") + 16

    def test_without_leading(self):
        result = url_keygen("test_seed", with_leading=False)
        assert not result.startswith("https://")
        assert len(result) == 16

    def test_deterministic(self):
        assert url_keygen(42) == url_keygen(42)

    def test_different_seeds_differ(self):
        assert url_keygen("a") != url_keygen("b")

    def test_int_seed(self):
        result = url_keygen(12345, with_leading=False)
        expected = hashlib.sha256(b"12345").hexdigest()[:16]
        assert result == expected


# ---------------------------------------------------------------------------
# to_camel_case / to_snake_case
# ---------------------------------------------------------------------------


class TestCaseConversion:
    def test_snake_to_camel(self):
        assert to_camel_case("some_field_name") == "someFieldName"

    def test_single_word_camel(self):
        assert to_camel_case("name") == "name"

    def test_already_camel_is_unchanged(self):
        # No underscores means no transformation
        assert to_camel_case("alreadyCamel") == "alreadyCamel"

    def test_camel_to_snake(self):
        assert to_snake_case("someFieldName") == "some_field_name"

    def test_single_word_snake(self):
        assert to_snake_case("name") == "name"

    def test_leading_uppercase(self):
        assert to_snake_case("Name") == "name"

    def test_consecutive_uppercase(self):
        # "HTTPResponse" -> "h_t_t_p_response"
        result = to_snake_case("HTTPResponse")
        assert result == "h_t_t_p_response"


# ---------------------------------------------------------------------------
# parse_magic_rules_subset
# ---------------------------------------------------------------------------


class TestParseMagicRulesSubset:
    def test_with_headers(self):
        rules = "Intro\nSTART\nContent A\nSTART\nContent B\nEND\nRest"
        result = parse_magic_rules_subset(rules, "START", "END")
        assert result == "\nContent B"

    def test_without_headers(self):
        rules = "Line1\r\nLine2\r\nLine3"
        result = parse_magic_rules_subset(rules)
        assert result == "Line1\nLine2\nLine3"

    def test_windows_line_endings_normalized(self):
        rules = "A\r\nB\r\nC"
        result = parse_magic_rules_subset(rules)
        assert "\r" not in result
        assert result == "A\nB\nC"


# ---------------------------------------------------------------------------
# sort_internal_lists
# ---------------------------------------------------------------------------


class TestSortInternalLists:
    def test_sort_list(self):
        assert sort_internal_lists(["c", "a", "b"]) == ["a", "b", "c"]

    def test_sort_set(self):
        result = sort_internal_lists({"c", "a", "b"})
        assert result == ["a", "b", "c"]

    def test_sort_nested_dict(self):
        data = {"key": ["z", "a", "m"]}
        result = sort_internal_lists(data)
        assert result["key"] == ["a", "m", "z"]

    def test_none_values_filtered(self):
        assert sort_internal_lists([3, None, 1]) == [1, 3]

    def test_dict_passthrough(self):
        data = {"a": 1, "b": 2}
        result = sort_internal_lists(data)
        assert result == {"a": 1, "b": 2}

    def test_scalar_passthrough(self):
        assert sort_internal_lists(42) == 42
        assert sort_internal_lists("hello") == "hello"


# ---------------------------------------------------------------------------
# get_file_hash
# ---------------------------------------------------------------------------


class TestGetFileHash:
    def test_hash_real_file(self, tmp_path: pathlib.Path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        result = get_file_hash(f)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_nonexistent_file(self, tmp_path: pathlib.Path):
        f = tmp_path / "missing.txt"
        result = get_file_hash(f)
        assert result == ""

    def test_deterministic(self, tmp_path: pathlib.Path):
        f = tmp_path / "test.txt"
        f.write_text("deterministic content")
        assert get_file_hash(f) == get_file_hash(f)


# ---------------------------------------------------------------------------
# generate_output_file_hashes
# ---------------------------------------------------------------------------


class TestGenerateOutputFileHashes:
    def test_creates_hash_files(self, tmp_path: pathlib.Path):
        f = tmp_path / "test.json"
        f.write_text('{"key": "value"}')

        from mtgjson5 import constants

        generate_output_file_hashes(tmp_path)

        hash_file = tmp_path / f"test.json.{constants.HASH_TO_GENERATE.name}"
        assert hash_file.exists()
        assert len(hash_file.read_text()) > 0

    def test_skips_excluded_dirs(self, tmp_path: pathlib.Path):
        from mtgjson5 import constants

        dm = tmp_path / "data-models"
        dm.mkdir()
        (dm / "file.json").write_text("data")

        generate_output_file_hashes(tmp_path)

        assert not list(dm.glob(f"*.{constants.HASH_TO_GENERATE.name}"))

    def test_skips_types_dir(self, tmp_path: pathlib.Path):
        from mtgjson5 import constants

        types_dir = tmp_path / "types"
        types_dir.mkdir()
        (types_dir / "file.ts").write_text("type T = string;")

        generate_output_file_hashes(tmp_path)

        assert not list(types_dir.glob(f"*.{constants.HASH_TO_GENERATE.name}"))

    def test_skips_ts_bundle(self, tmp_path: pathlib.Path):
        from mtgjson5 import constants

        (tmp_path / "AllMTGJSONTypes.ts").write_text("export type T = string;")
        generate_output_file_hashes(tmp_path)

        assert not (tmp_path / f"AllMTGJSONTypes.ts.{constants.HASH_TO_GENERATE.name}").exists()


# ---------------------------------------------------------------------------
# get_str_or_none
# ---------------------------------------------------------------------------


class TestGetStrOrNone:
    def test_string_value(self):
        assert get_str_or_none("hello") == "hello"

    def test_int_value(self):
        assert get_str_or_none(42) == "42"

    def test_none_value(self):
        assert get_str_or_none(None) is None

    def test_empty_string(self):
        assert get_str_or_none("") is None

    def test_zero(self):
        assert get_str_or_none(0) is None

    def test_false(self):
        assert get_str_or_none(False) is None


# ---------------------------------------------------------------------------
# get_all_entities_from_content
# ---------------------------------------------------------------------------


class TestGetAllEntitiesFromContent:
    def test_extracts_cards_and_tokens(self):
        content = {
            "SET1": {
                "cards": [{"name": "Card A"}],
                "tokens": [{"name": "Token B"}],
            }
        }
        result = get_all_entities_from_content(content)
        assert len(result) == 2
        names = {e["name"] for e in result}
        assert names == {"Card A", "Token B"}

    def test_includes_sealed_when_requested(self):
        content = {
            "SET1": {
                "cards": [{"name": "Card A"}],
                "tokens": [],
                "sealedProduct": [{"name": "Booster"}],
            }
        }
        result = get_all_entities_from_content(content, include_sealed_product=True)
        assert len(result) == 2
        names = {e["name"] for e in result}
        assert "Booster" in names

    def test_excludes_sealed_by_default(self):
        content = {
            "SET1": {
                "cards": [],
                "tokens": [],
                "sealedProduct": [{"name": "Booster"}],
            }
        }
        result = get_all_entities_from_content(content, include_sealed_product=False)
        assert len(result) == 0

    def test_empty_content(self):
        assert get_all_entities_from_content({}) == []

    def test_multiple_sets(self):
        content = {
            "SET1": {"cards": [{"name": "A"}], "tokens": []},
            "SET2": {"cards": [{"name": "B"}, {"name": "C"}], "tokens": []},
        }
        result = get_all_entities_from_content(content)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# recursive_sort / deep_sort_keys
# ---------------------------------------------------------------------------


class TestRecursiveSort:
    def test_sorts_top_level_keys(self):
        result = recursive_sort({"b": 1, "a": 2})
        assert list(result.keys()) == ["a", "b"]

    def test_sorts_nested_keys(self):
        result = recursive_sort({"z": {"b": 1, "a": 2}, "a": 3})
        assert list(result.keys()) == ["a", "z"]
        assert list(result["z"].keys()) == ["a", "b"]

    def test_non_dict_values_unchanged(self):
        result = recursive_sort({"a": [3, 1, 2]})
        assert result["a"] == [3, 1, 2]  # Lists NOT sorted


class TestDeepSortKeys:
    def test_sorts_dict_keys(self):
        result = deep_sort_keys({"b": 1, "a": 2})
        assert list(result.keys()) == ["a", "b"]

    def test_sorts_nested_dicts(self):
        result = deep_sort_keys({"z": {"b": 1, "a": 2}})
        assert list(result["z"].keys()) == ["a", "b"]

    def test_sorts_dicts_inside_lists(self):
        result = deep_sort_keys([{"b": 1, "a": 2}])
        assert list(result[0].keys()) == ["a", "b"]

    def test_scalar_passthrough(self):
        assert deep_sort_keys(42) == 42
        assert deep_sort_keys("hello") == "hello"

    def test_list_order_preserved(self):
        result = deep_sort_keys([3, 1, 2])
        assert result == [3, 1, 2]
