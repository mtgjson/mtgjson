"""Tests for v2 pipeline cleaning functions: clean_nested, prepare_cards_for_json, dataframe_to_cards_list."""

from __future__ import annotations

import polars as pl

from mtgjson5.pipeline.stages.output import (
    clean_nested,
    dataframe_to_cards_list,
    prepare_cards_for_json,
)

# ---------------------------------------------------------------------------
# TestCleanNested
# ---------------------------------------------------------------------------


class TestCleanNested:
    def test_removes_none_values(self):
        result = clean_nested({"a": 1, "b": None})
        assert result == {"a": 1}

    def test_keeps_required_list_fields_as_empty(self):
        """colors should be kept as [] even when None (it's in REQUIRED_LIST_FIELDS)."""
        result = clean_nested({"colors": None, "name": "Test"})
        assert result["colors"] == []

    def test_keeps_legalities_as_empty_dict(self):
        result = clean_nested({"legalities": None, "name": "Test"})
        assert result["legalities"] == {}

    def test_omits_false_optional_bool(self):
        """isFunny=False should be omitted (it's in OPTIONAL_BOOL_FIELDS)."""
        result = clean_nested({"isFunny": False, "name": "Test"})
        assert "isFunny" not in result

    def test_sorts_dict_keys(self):
        result = clean_nested({"z": 1, "a": 2, "m": 3})
        assert list(result.keys()) == ["a", "m", "z"]

    def test_sorts_sorted_list_fields(self):
        """Items in SORTED_LIST_FIELDS should be sorted."""
        result = clean_nested({"colorIdentity": ["G", "W", "U"]})
        assert result["colorIdentity"] == ["G", "U", "W"]

    def test_sorts_rulings(self):
        rulings = [
            {"date": "2022-01-01", "text": "B ruling"},
            {"date": "2021-06-15", "text": "A ruling"},
            {"date": "2022-01-01", "text": "A ruling"},
        ]
        result = clean_nested({"rulings": rulings})
        dates = [r["date"] for r in result["rulings"]]
        texts = [r["text"] for r in result["rulings"]]
        assert dates == ["2021-06-15", "2022-01-01", "2022-01-01"]
        assert texts[1] == "A ruling"
        assert texts[2] == "B ruling"

    def test_recursive_nested_dict(self):
        result = clean_nested({"outer": {"inner": 1, "gone": None}})
        assert result == {"outer": {"inner": 1}}

    def test_omits_variations_empty_list(self):
        """variations: [] should be omitted (it's in OMIT_EMPTY_LIST_FIELDS)."""
        result = clean_nested({"variations": [], "name": "Test"})
        assert "variations" not in result

    def test_tuple_to_list(self):
        result = clean_nested({"items": (1, 2, 3)})
        assert result["items"] == [1, 2, 3]

    def test_set_to_sorted_list(self):
        result = clean_nested({"items": {"c", "a", "b"}})
        assert result["items"] == ["a", "b", "c"]

    def test_fully_empty_nested_dict_returns_none(self):
        result = clean_nested({"outer": {"a": None, "b": None}})
        # The nested dict is empty after removing Nones, then outer
        # has only empty value, so it should also be None
        assert result is None

    def test_nested_dict_all_values_none(self):
        """A dict where all values are None should return None."""
        result = clean_nested({"a": None, "b": None, "c": None})
        assert result is None

    def test_list_of_all_nones(self):
        """A list of all Nones should have them stripped; the resulting empty list
        causes the parent dict to become None (empty after cleanup)."""
        result = clean_nested({"items": [None, None, None]})
        assert result is None

    def test_deeply_nested_three_levels(self):
        """3+ levels of nesting should be cleaned recursively."""
        result = clean_nested({"a": {"b": {"c": {"d": 1, "e": None}}}})
        assert result == {"a": {"b": {"c": {"d": 1}}}}


# ---------------------------------------------------------------------------
# TestPrepareCardsForJson
# ---------------------------------------------------------------------------


class TestPrepareCardsForJson:
    def test_null_colors_filled_with_empty_list(self):
        df = pl.DataFrame(
            {
                "colors": [None, ["W"]],
                "name": ["A", "B"],
            },
            schema={"colors": pl.List(pl.String), "name": pl.String},
        )
        result = prepare_cards_for_json(df)
        assert result["colors"][0].to_list() == []
        assert result["colors"][1].to_list() == ["W"]

    def test_empty_otherFaceIds_becomes_null(self):
        df = pl.DataFrame(
            {
                "otherFaceIds": [[], ["uuid-1"]],
                "name": ["A", "B"],
            },
            schema={"otherFaceIds": pl.List(pl.String), "name": pl.String},
        )
        result = prepare_cards_for_json(df)
        assert result["otherFaceIds"][0] is None
        assert result["otherFaceIds"][1].to_list() == ["uuid-1"]

    def test_false_isFunny_becomes_null(self):
        df = pl.DataFrame(
            {
                "isFunny": [False, True],
                "name": ["A", "B"],
            }
        )
        result = prepare_cards_for_json(df)
        assert result["isFunny"][0] is None
        assert result["isFunny"][1] is True


# ---------------------------------------------------------------------------
# TestDataframeToCardsList
# ---------------------------------------------------------------------------


class TestDataframeToCardsList:
    def test_sorted_by_number_then_side(self):
        df = pl.DataFrame(
            {
                "number": ["10", "2", "1"],
                "side": [None, None, None],
                "name": ["C", "B", "A"],
            }
        )
        result = dataframe_to_cards_list(df)
        names = [c["name"] for c in result]
        assert names == ["A", "B", "C"]

    def test_empty_dataframe(self):
        df = pl.DataFrame(
            {"number": [], "side": [], "name": []}, schema={"number": pl.String, "side": pl.String, "name": pl.String}
        )
        result = dataframe_to_cards_list(df)
        assert result == []

    def test_clean_nested_applied(self):
        df = pl.DataFrame(
            {
                "number": ["1"],
                "side": [None],
                "name": ["Test"],
                "isFunny": [False],
            }
        )
        result = dataframe_to_cards_list(df)
        assert len(result) == 1
        assert "isFunny" not in result[0]
