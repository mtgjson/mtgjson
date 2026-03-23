"""Tests for mtgjson5.build.serializers — escape functions, serialize_complex_types, batched."""

from __future__ import annotations

import json

import polars as pl
import pytest

from mtgjson5.build.serializers import (
    _drop_nulls,
    batched,
    escape_mysql,
    escape_postgres,
    escape_sqlite,
    serialize_complex_types,
)

# =============================================================================
# escape_sqlite
# =============================================================================


class TestEscapeSqlite:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "NULL"),
            (True, "1"),
            (False, "0"),
            (42, "42"),
            (3.14, "3.14"),
            ("hello", "'hello'"),
            ("it's", "'it''s'"),
            (["a", "b"], '\'["a", "b"]\''),
        ],
        ids=["null", "true", "false", "int", "float", "string", "single-quote", "list-json"],
    )
    def test_escape_sqlite(self, value, expected):
        assert escape_sqlite(value) == expected


# =============================================================================
# escape_mysql
# =============================================================================


class TestEscapeMysql:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "NULL"),
            (True, "TRUE"),
            (False, "FALSE"),
            (42, "42"),
            ("hello", "'hello'"),
            ("it's", "'it\\'s'"),
            ("tab\there", "'tab\\there'"),
            ("new\nline", "'new\\nline'"),
            ("null\0byte", "'null\\0byte'"),
        ],
        ids=["null", "true", "false", "int", "string", "single-quote", "tab", "newline", "null-byte"],
    )
    def test_escape_mysql(self, value, expected):
        assert escape_mysql(value) == expected


# =============================================================================
# escape_postgres
# =============================================================================


class TestEscapePostgres:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "\\N"),
            (True, "t"),
            (False, "f"),
            (42, "42"),
            ("hello", "hello"),
            ("tab\there", "tab\\there"),
            ("back\\slash", "back\\\\slash"),
        ],
        ids=["null", "true", "false", "int", "string", "tab", "backslash"],
    )
    def test_escape_postgres(self, value, expected):
        assert escape_postgres(value) == expected


# =============================================================================
# serialize_complex_types
# =============================================================================


class TestSerializeComplexTypes:
    def test_list_column_to_csv_string(self):
        df = pl.DataFrame({"keywords": [["Defender", "Flying"], ["Trample"]]})
        result = serialize_complex_types(df)
        assert result["keywords"].to_list() == ["Defender, Flying", "Trample"]

    def test_struct_column_to_json_string(self):
        df = pl.DataFrame(
            {"ids": [{"a": 1, "b": None}, {"a": 2, "b": "x"}]},
            schema={"ids": pl.Struct({"a": pl.Int64, "b": pl.String})},
        )
        result = serialize_complex_types(df)
        vals = result["ids"].to_list()
        assert '"a":1' in vals[0]
        # null b should be dropped in the first row
        assert "b" not in vals[0]
        assert '"b":"x"' in vals[1]

    def test_no_complex_columns_passthrough(self):
        df = pl.DataFrame({"name": ["Alpha"], "count": [5]})
        result = serialize_complex_types(df)
        assert result.equals(df)

    def test_null_list_preserved(self):
        df = pl.DataFrame(
            {"keywords": [["Flying"], None]},
            schema={"keywords": pl.List(pl.String)},
        )
        result = serialize_complex_types(df)
        vals = result["keywords"].to_list()
        assert vals[0] == "Flying"
        assert vals[1] is None

    def test_null_struct_preserved(self):
        df = pl.DataFrame(
            {"ids": [{"a": 1}, None]},
            schema={"ids": pl.Struct({"a": pl.Int64})},
        )
        result = serialize_complex_types(df)
        vals = result["ids"].to_list()
        assert vals[0] is not None
        assert vals[1] is None


# =============================================================================
# _drop_nulls
# =============================================================================


class TestDropNulls:
    def test_nested_dict_null_removal(self):
        obj = {"a": 1, "b": None, "c": {"d": 2, "e": None}}
        assert _drop_nulls(obj) == {"a": 1, "c": {"d": 2}}

    def test_list_items_preserved(self):
        obj = {"items": [1, 2, None]}
        result = _drop_nulls(obj)
        # Lists keep their items (including None-valued items) per the implementation
        assert result == {"items": [1, 2, None]}


# =============================================================================
# batched
# =============================================================================


class TestBatched:
    def test_exact_divisor(self):
        assert list(batched([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]

    def test_remainder(self):
        assert list(batched([1, 2, 3], 2)) == [[1, 2], [3]]

    def test_empty(self):
        assert not list(batched([], 5))


# =============================================================================
# TestCrossDialectConsistency
# =============================================================================


class TestCrossDialectConsistency:
    @pytest.mark.parametrize(
        ("value", "sqlite_expected", "pg_expected", "mysql_expected"),
        [
            (None, "NULL", "\\N", "NULL"),
        ],
        ids=["none"],
    )
    def test_none_all_dialects(self, value, sqlite_expected, pg_expected, mysql_expected):
        assert escape_sqlite(value) == sqlite_expected
        assert escape_postgres(value) == pg_expected
        assert escape_mysql(value) == mysql_expected

    @pytest.mark.parametrize(
        ("value", "sqlite_expected", "pg_expected", "mysql_expected"),
        [
            (True, "1", "t", "TRUE"),
            (False, "0", "f", "FALSE"),
        ],
        ids=["true", "false"],
    )
    def test_bool_all_dialects(self, value, sqlite_expected, pg_expected, mysql_expected):
        assert escape_sqlite(value) == sqlite_expected
        assert escape_postgres(value) == pg_expected
        assert escape_mysql(value) == mysql_expected

    def test_int_consistent(self):
        for val in (0, 1, 42, -7):
            expected = str(val)
            assert escape_sqlite(val) == expected
            assert escape_postgres(val) == expected
            assert escape_mysql(val) == expected

    def test_dict_serialized_as_json(self):
        val = {"a": 1}
        sqlite_result = escape_sqlite(val)
        pg_result = escape_postgres(val)
        mysql_result = escape_mysql(val)
        # All should produce parseable JSON (possibly with SQL quoting)
        # SQLite wraps in single quotes
        assert json.loads(sqlite_result.strip("'").replace("''", "'")) == {"a": 1}
        # Postgres escapes backslashes
        assert json.loads(pg_result.replace("\\\\", "\\")) == {"a": 1}
        # MySQL wraps in single quotes with backslash escaping
        inner = mysql_result.strip("'").replace("\\'", "'").replace('\\"', '"')
        assert json.loads(inner) == {"a": 1}

    def test_unicode_apostrophe(self):
        val = "l'esprit"
        # Each dialect should handle the apostrophe properly
        sqlite_result = escape_sqlite(val)
        assert sqlite_result == "'l''esprit'"
        pg_result = escape_postgres(val)
        assert pg_result == "l'esprit"
        mysql_result = escape_mysql(val)
        assert mysql_result == "'l\\'esprit'"


# =============================================================================
# TestListOfStructSerialization
# =============================================================================


class TestListOfStructSerialization:
    def test_list_of_struct_produces_valid_json(self):
        """List[Struct] columns should serialize to JSON arrays, not Python repr."""
        df = pl.DataFrame(
            [{"items": [{"uuid": "abc", "count": 2}]}],
            schema={"items": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64}))},
        )
        result = serialize_complex_types(df)
        assert result.schema["items"] == pl.String
        value = result["items"][0]
        parsed = json.loads(value)
        assert parsed == [{"uuid": "abc", "count": 2}]

    def test_list_of_struct_with_nulls_drops_null_values(self):
        """Null values within structs should be dropped in JSON output."""
        df = pl.DataFrame(
            [{"items": [{"uuid": "abc", "name": None}]}],
            schema={"items": pl.List(pl.Struct({"uuid": pl.String, "name": pl.String}))},
        )
        result = serialize_complex_types(df)
        parsed = json.loads(result["items"][0])
        assert parsed == [{"uuid": "abc"}]

    def test_list_of_struct_null_row(self):
        """Null list values should serialize to null string."""
        df = pl.DataFrame(
            [{"items": None}],
            schema={"items": pl.List(pl.Struct({"uuid": pl.String}))},
        )
        result = serialize_complex_types(df)
        assert result["items"][0] is None

    def test_list_of_string_still_uses_csv(self):
        """List[String] columns should still use comma-separated format."""
        df = pl.DataFrame(
            [{"colors": ["R", "G"]}],
            schema={"colors": pl.List(pl.String)},
        )
        result = serialize_complex_types(df)
        assert result["colors"][0] == "R, G"

    def test_struct_still_uses_json(self):
        """Struct columns should still use JSON serialization."""
        df = pl.DataFrame(
            [{"ids": {"scryfallId": "sf-001"}}],
            schema={"ids": pl.Struct({"scryfallId": pl.String})},
        )
        result = serialize_complex_types(df)
        parsed = json.loads(result["ids"][0])
        assert parsed == {"scryfallId": "sf-001"}
