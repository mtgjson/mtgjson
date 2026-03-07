"""Extended tests for mtgjson5.models._typing: apply_aliases, _clean_nested, _clean_list, is_field_required."""

from __future__ import annotations

from typing import Optional, TypedDict

from typing_extensions import Required

from mtgjson5.models._typing import TypedDictUtils, unwrap_optional


# ---------------------------------------------------------------------------
# Sample TypedDicts for testing
# ---------------------------------------------------------------------------


class SampleTD(TypedDict, total=False):
    name: Required[str]
    value: int
    optional_field: str


# ---------------------------------------------------------------------------
# TestApplyAliases
# ---------------------------------------------------------------------------


class TestApplyAliases:
    def test_basic_aliasing(self):
        aliases = {("SampleTD", "value"): "renamed_value"}
        d = {"name": "test", "value": 42}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert "renamed_value" in result
        assert result["renamed_value"] == 42
        assert "value" not in result

    def test_none_values_removed(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "value": None}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert "value" not in result
        assert result["name"] == "test"

    def test_nested_dict_cleaned(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "nested": {"a": 1, "b": None}}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert result["nested"] == {"a": 1}

    def test_nested_list_cleaned(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "items": [1, None, 3]}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert result["items"] == [1, 3]

    def test_empty_nested_dict_removed(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "nested": {"only": None}}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert "nested" not in result

    def test_empty_nested_list_removed(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "items": [None, None]}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert "items" not in result

    def test_no_aliases_passthrough(self):
        aliases: dict[tuple[str, str], str] = {}
        d = {"name": "test", "value": 42}
        result = TypedDictUtils.apply_aliases(SampleTD, d, aliases)
        assert result == {"name": "test", "value": 42}


# ---------------------------------------------------------------------------
# TestCleanNested
# ---------------------------------------------------------------------------


class TestCleanNested:
    def test_removes_none_values(self):
        d = {"a": 1, "b": None, "c": "hello"}
        result = TypedDictUtils._clean_nested(d, {})
        assert result == {"a": 1, "c": "hello"}

    def test_recursive_dict(self):
        d = {"outer": {"inner": 1, "gone": None}}
        result = TypedDictUtils._clean_nested(d, {})
        assert result == {"outer": {"inner": 1}}

    def test_recursive_list(self):
        d = {"items": [1, None, {"a": None}]}
        result = TypedDictUtils._clean_nested(d, {})
        assert result == {"items": [1]}

    def test_deeply_nested(self):
        d = {"a": {"b": {"c": {"d": None, "e": 5}}}}
        result = TypedDictUtils._clean_nested(d, {})
        assert result == {"a": {"b": {"c": {"e": 5}}}}

    def test_empty_dict_after_cleaning(self):
        d = {"a": None, "b": None}
        result = TypedDictUtils._clean_nested(d, {})
        assert result == {}


# ---------------------------------------------------------------------------
# TestCleanList
# ---------------------------------------------------------------------------


class TestCleanList:
    def test_removes_none_items(self):
        result = TypedDictUtils._clean_list([1, None, 3], {})
        assert result == [1, 3]

    def test_cleans_dict_items(self):
        result = TypedDictUtils._clean_list([{"a": 1, "b": None}], {})
        assert result == [{"a": 1}]

    def test_removes_empty_dict_items(self):
        result = TypedDictUtils._clean_list([{"a": None}], {})
        assert result == []

    def test_cleans_nested_lists(self):
        result = TypedDictUtils._clean_list([[1, None, 3]], {})
        assert result == [[1, 3]]

    def test_removes_empty_nested_lists(self):
        result = TypedDictUtils._clean_list([[None]], {})
        assert result == []

    def test_primitives_preserved(self):
        result = TypedDictUtils._clean_list(["a", 1, True], {})
        assert result == ["a", 1, True]


# ---------------------------------------------------------------------------
# TestIsFieldRequired
# ---------------------------------------------------------------------------


class TestIsFieldRequired:
    def test_required_field(self):
        assert TypedDictUtils.is_field_required(SampleTD, "name") is True

    def test_optional_field(self):
        assert TypedDictUtils.is_field_required(SampleTD, "value") is False

    def test_missing_field(self):
        assert TypedDictUtils.is_field_required(SampleTD, "nonexistent") is False


# ---------------------------------------------------------------------------
# TestGetFields
# ---------------------------------------------------------------------------


class TestGetFields:
    def test_gets_field_types(self):
        fields = TypedDictUtils.get_fields(SampleTD)
        assert "name" in fields
        assert fields["name"] is str

    def test_inherited_fields(self):

        class ParentTD(TypedDict):
            parent_field: str

        class ChildTD(ParentTD):
            child_field: int

        fields = TypedDictUtils.get_fields(ChildTD)
        assert "parent_field" in fields
        assert "child_field" in fields


# ---------------------------------------------------------------------------
# TestUnwrapOptionalExtended
# ---------------------------------------------------------------------------


class TestUnwrapOptionalExtended:
    def test_multi_type_union(self):
        """str | int | None should return (str | int | None, True)."""
        tp = str | int | None
        inner, is_opt = unwrap_optional(tp)
        assert is_opt is True

    def test_non_optional_union(self):
        """str | int (no None) should not be optional."""
        tp = str | int
        inner, is_opt = unwrap_optional(tp)
        assert is_opt is False
