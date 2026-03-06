"""Tests for v2 typing utilities."""

from __future__ import annotations

from typing import Optional

from mtgjson5.models._typing import TypedDictUtils, is_union_type, unwrap_optional
from mtgjson5.models.submodels import Rulings

# ---------------------------------------------------------------------------
# TestTypedDictUtils
# ---------------------------------------------------------------------------


class TestTypedDictUtils:
    def test_is_typeddict_true(self):
        assert TypedDictUtils.is_typeddict(Rulings) is True

    def test_is_typeddict_false_for_dict(self):
        assert TypedDictUtils.is_typeddict(dict) is False

    def test_is_typeddict_false_for_non_dict(self):
        assert TypedDictUtils.is_typeddict(str) is False

    def test_filter_none(self):
        result = TypedDictUtils.filter_none({"a": 1, "b": None, "c": 3})
        assert result == {"a": 1, "c": 3}

    def test_filter_none_empty(self):
        result = TypedDictUtils.filter_none({})
        assert result == {}

    def test_apply_aliases_rulings(self):
        from mtgjson5.consts.mappings import TYPEDDICT_FIELD_ALIASES

        d = {"publishedAt": "2024-01-01", "comment": "Test", "source": "scryfall"}
        result = TypedDictUtils.apply_aliases(Rulings, d, TYPEDDICT_FIELD_ALIASES)
        assert "date" in result
        assert "text" in result
        assert result["date"] == "2024-01-01"
        assert result["text"] == "Test"


# ---------------------------------------------------------------------------
# TestUnwrapOptional
# ---------------------------------------------------------------------------


class TestUnwrapOptional:
    def test_optional_str(self):
        inner, is_opt = unwrap_optional(Optional[str])
        assert inner is str
        assert is_opt is True

    def test_plain_str(self):
        inner, is_opt = unwrap_optional(str)
        assert inner is str
        assert is_opt is False

    def test_union_syntax(self):
        """str | None should unwrap to (str, True)."""
        inner, is_opt = unwrap_optional(str | None)
        assert inner is str
        assert is_opt is True


# ---------------------------------------------------------------------------
# TestIsUnionType
# ---------------------------------------------------------------------------


class TestIsUnionType:
    def test_optional_is_union(self):
        assert is_union_type(Optional[str]) is True

    def test_plain_not_union(self):
        assert is_union_type(str) is False

    def test_union_syntax(self):
        assert is_union_type(str | None) is True
