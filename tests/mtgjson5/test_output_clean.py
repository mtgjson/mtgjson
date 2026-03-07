"""Tests for pipeline output functions: clean_nested and prepare_cards_for_json."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from mtgjson5.pipeline.stages.output import clean_nested, prepare_cards_for_json


# ---------------------------------------------------------------------------
# TestCleanNested
# ---------------------------------------------------------------------------


class TestCleanNested:
    def test_none_returns_none(self):
        assert clean_nested(None) is None

    def test_scalar_passthrough(self):
        assert clean_nested(42) == 42
        assert clean_nested("hello") == "hello"
        assert clean_nested(True) is True

    def test_dict_keys_sorted(self):
        result = clean_nested({"z": 1, "a": 2, "m": 3})
        assert list(result.keys()) == ["a", "m", "z"]

    def test_none_values_omitted(self):
        result = clean_nested({"a": 1, "b": None})
        assert result == {"a": 1}

    def test_none_values_kept_when_omit_false(self):
        result = clean_nested({"a": 1, "b": None}, omit_empty=False)
        assert result == {"a": 1, "b": None}

    def test_empty_dict_returns_none(self):
        result = clean_nested({"a": None})
        assert result is None

    def test_empty_dict_kept_when_omit_false(self):
        result = clean_nested({"a": None}, omit_empty=False)
        assert result == {"a": None}

    def test_required_list_field_kept_as_empty(self):
        result = clean_nested({"colors": None, "name": "Test"})
        assert result["colors"] == []

    def test_legalities_kept_as_empty_dict(self):
        result = clean_nested({"legalities": None, "name": "Test"})
        assert result["legalities"] == {}

    def test_optional_bool_false_omitted(self):
        result = clean_nested({"isReprint": False, "name": "Test"})
        assert "isReprint" not in result

    def test_required_set_bool_false_kept(self):
        # isFoilOnly is in REQUIRED_SET_BOOL_FIELDS
        result = clean_nested({"isFoilOnly": False, "name": "Test"})
        assert result["isFoilOnly"] is False

    def test_empty_list_omitted(self):
        result = clean_nested({"keywords": []})
        assert result is None  # entire dict becomes None since only key is omitted

    def test_empty_list_kept_for_required_fields(self):
        result = clean_nested({"colors": [], "name": "Test"})
        assert result["colors"] == []

    def test_list_none_items_removed(self):
        result = clean_nested({"items": [1, None, 3]})
        assert result["items"] == [1, 3]

    def test_sorted_list_fields(self):
        result = clean_nested({"colorIdentity": ["R", "G", "W"]})
        assert result["colorIdentity"] == ["G", "R", "W"]

    def test_rulings_sorted_by_date(self):
        rulings = [
            {"date": "2024-01-15", "text": "B ruling"},
            {"date": "2024-01-01", "text": "A ruling"},
        ]
        result = clean_nested({"rulings": rulings, "name": "Test"})
        assert result["rulings"][0]["date"] == "2024-01-01"

    def test_tuple_converted_to_list(self):
        result = clean_nested({"items": (1, 2, 3)})
        assert result["items"] == [1, 2, 3]

    def test_set_converted_to_sorted_list(self):
        result = clean_nested({"items": {"c", "a", "b"}})
        assert result["items"] == ["a", "b", "c"]

    def test_nested_dicts_cleaned(self):
        result = clean_nested({"outer": {"inner": 1, "gone": None}})
        assert result == {"outer": {"inner": 1}}

    def test_field_handlers(self):
        handlers = {"name": lambda v: v.upper()}
        result = clean_nested({"name": "test"}, field_handlers=handlers)
        assert result["name"] == "TEST"

    def test_deeply_nested_structure(self):
        data: dict[str, Any] = {
            "name": "Test Card",
            "identifiers": {"scryfallId": "abc", "mtgoId": None},
            "colors": ["R", "G"],
        }
        result = clean_nested(data)
        assert result["identifiers"] == {"scryfallId": "abc"}
        assert result["colors"] == ["G", "R"]  # sorted


# ---------------------------------------------------------------------------
# TestPrepareCardsForJson
# ---------------------------------------------------------------------------


class TestPrepareCardsForJson:
    def test_fills_required_list_nulls(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "colors": [None],
        }, schema={"name": pl.String, "colors": pl.List(pl.String)})
        result = prepare_cards_for_json(df)
        assert result["colors"].to_list() == [[]]

    def test_nullifies_empty_omit_list(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "keywords": [[]],
        }, schema={"name": pl.String, "keywords": pl.List(pl.String)})
        result = prepare_cards_for_json(df)
        assert result["keywords"].to_list() == [None]

    def test_nullifies_false_optional_bool(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "isReprint": [False],
        })
        result = prepare_cards_for_json(df)
        assert result["isReprint"].to_list() == [None]

    def test_keeps_true_optional_bool(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "isReprint": [True],
        })
        result = prepare_cards_for_json(df)
        assert result["isReprint"].to_list() == [True]

    def test_nullifies_empty_string_optional(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "asciiName": [""],
        })
        result = prepare_cards_for_json(df)
        assert result["asciiName"].to_list() == [None]

    def test_nullifies_zero_numeric_optional(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "edhrecSaltiness": [0.0],
        }, schema={"name": pl.String, "edhrecSaltiness": pl.Float64})
        result = prepare_cards_for_json(df)
        assert result["edhrecSaltiness"].to_list() == [None]

    def test_drops_internal_columns(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "_row_id": [0],
            "_temp": ["val"],
        })
        result = prepare_cards_for_json(df)
        assert "_row_id" not in result.columns
        assert "_temp" not in result.columns
        assert "name" in result.columns

    def test_keeps_non_empty_values(self):
        df = pl.DataFrame({
            "name": ["Test"],
            "asciiName": ["Test Card"],
            "edhrecSaltiness": [1.5],
        }, schema={"name": pl.String, "asciiName": pl.String, "edhrecSaltiness": pl.Float64})
        result = prepare_cards_for_json(df)
        assert result["asciiName"].to_list() == ["Test Card"]
        assert result["edhrecSaltiness"].to_list() == [1.5]
