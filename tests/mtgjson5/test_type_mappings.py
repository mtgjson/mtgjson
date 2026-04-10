"""Tests for Polars dtype → SQL type mappings in SQLite and MySQL builders."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.build.formats.mysql import _polars_to_mysql_type
from mtgjson5.build.formats.sqlite import _polars_to_sqlite_type

# =============================================================================
# _polars_to_sqlite_type
# =============================================================================


class TestPolarsToSqliteType:
    @pytest.mark.parametrize(
        ("dtype", "expected"),
        [
            (pl.Int64, "INTEGER"),
            (pl.Float64, "REAL"),
            (pl.Boolean, "BOOLEAN"),
            (pl.Date, "DATE"),
            (pl.Datetime, "DATETIME"),
            (pl.String, "TEXT"),
            (pl.List(pl.String), "TEXT"),
        ],
        ids=["int64", "float64", "boolean", "date", "datetime", "string", "list-string"],
    )
    def test_polars_to_sqlite_type(self, dtype, expected):
        assert _polars_to_sqlite_type(dtype) == expected

    def test_int32_maps_to_integer(self):
        assert _polars_to_sqlite_type(pl.Int32) == "INTEGER"

    def test_float32_maps_to_real(self):
        assert _polars_to_sqlite_type(pl.Float32) == "REAL"

    def test_struct_maps_to_text(self):
        assert _polars_to_sqlite_type(pl.Struct({"a": pl.String})) == "TEXT"


# =============================================================================
# _polars_to_mysql_type
# =============================================================================


class TestPolarsToMysqlType:
    """MySQL type mapper takes (dtype, table_name, col_name) for index-aware overrides."""

    @pytest.mark.parametrize(
        ("dtype", "expected"),
        [
            (pl.Int64, "INTEGER"),
            (pl.Float64, "FLOAT"),
            (pl.Boolean, "BOOLEAN"),
            (pl.Date, "DATE"),
            (pl.Datetime, "DATETIME"),
            (pl.String, "TEXT"),
            (pl.Struct({"a": pl.String}), "TEXT"),
        ],
        ids=["int64", "float64", "boolean", "date", "datetime", "string", "struct"],
    )
    def test_polars_to_mysql_type(self, dtype, expected):
        assert _polars_to_mysql_type(dtype, "_test", "_col") == expected

    def test_int32_maps_to_integer(self):
        assert _polars_to_mysql_type(pl.Int32, "_test", "_col") == "INTEGER"

    def test_list_maps_to_text(self):
        assert _polars_to_mysql_type(pl.List(pl.String), "_test", "_col") == "TEXT"

    def test_sealed_products_contents_maps_to_longtext(self):
        assert _polars_to_mysql_type(pl.String, "sealedProducts", "contents") == "LONGTEXT"
        assert _polars_to_mysql_type(pl.List(pl.String), "sealedProducts", "contents") == "LONGTEXT"

    def test_contents_in_other_tables_stays_text(self):
        assert _polars_to_mysql_type(pl.String, "cards", "contents") == "TEXT"


# =============================================================================
# Dialect differences
# =============================================================================


class TestDialectDifferences:
    def test_boolean_sqlite_vs_mysql(self):
        assert _polars_to_sqlite_type(pl.Boolean) == "BOOLEAN"
        assert _polars_to_mysql_type(pl.Boolean, "_test", "_col") == "BOOLEAN"

    def test_float_sqlite_vs_mysql(self):
        assert _polars_to_sqlite_type(pl.Float64) == "REAL"
        assert _polars_to_mysql_type(pl.Float64, "_test", "_col") == "FLOAT"


# =============================================================================
# TestUnsignedAndNestedTypes
# =============================================================================


class TestUnsignedAndNestedTypes:
    def test_uint8_maps_to_integer_sqlite(self):
        assert _polars_to_sqlite_type(pl.UInt8) == "INTEGER"

    def test_uint8_maps_to_integer_mysql(self):
        assert _polars_to_mysql_type(pl.UInt8, "_test", "_col") == "INTEGER"

    def test_uint32_maps_to_integer_sqlite(self):
        assert _polars_to_sqlite_type(pl.UInt32) == "INTEGER"

    def test_uint32_maps_to_integer_mysql(self):
        assert _polars_to_mysql_type(pl.UInt32, "_test", "_col") == "INTEGER"

    def test_nested_struct_maps_to_text_sqlite(self):
        dtype = pl.Struct({"inner": pl.Struct({"a": pl.String})})
        assert _polars_to_sqlite_type(dtype) == "TEXT"

    def test_nested_struct_maps_to_text_mysql(self):
        dtype = pl.Struct({"inner": pl.Struct({"a": pl.String})})
        assert _polars_to_mysql_type(dtype, "_test", "_col") == "TEXT"

    def test_binary_defaults_to_text_sqlite(self):
        assert _polars_to_sqlite_type(pl.Binary) == "TEXT"

    def test_binary_defaults_to_text_mysql(self):
        assert _polars_to_mysql_type(pl.Binary, "_test", "_col") == "TEXT"
