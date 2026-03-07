"""Tests for type survival across all output formats (SQLite, CSV, Parquet, Postgres, MySQL)."""

from __future__ import annotations

import json
import sqlite3

import polars as pl
import pytest

from mtgjson5.build.formats.csv import _flatten_for_csv
from mtgjson5.build.formats.sqlite import _polars_to_sqlite_type
from mtgjson5.build.serializers import (
    escape_mysql,
    escape_postgres,
    escape_sqlite,
    serialize_complex_types,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_full_card_df() -> pl.DataFrame:
    """Return a DataFrame with representative column types for cross-format tests."""
    return pl.DataFrame(
        {
            "uuid": ["card-001", "card-002", None],
            "name": ["Alpha Card", "Beta Card", None],
            "cmc": [3, 0, None],
            "price": [1.99, 0.0, None],
            "hasFoil": [True, False, None],
            "colors": [["W", "U"], [], None],
            "identifiers": [
                {"scryfallId": "sf-001", "multiverseId": "123"},
                {"scryfallId": "sf-002", "multiverseId": None},
                None,
            ],
        },
        schema={
            "uuid": pl.String,
            "name": pl.String,
            "cmc": pl.Int64,
            "price": pl.Float64,
            "hasFoil": pl.Boolean,
            "colors": pl.List(pl.String),
            "identifiers": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String}),
        },
    )


def _write_table_direct(
    cursor: sqlite3.Cursor,
    table_name: str,
    df: pl.DataFrame,
) -> int:
    """Replicate SQLiteBuilder._write_table without needing an AssemblyContext."""
    if df is None or len(df) == 0:
        return 0

    serialized = serialize_complex_types(df)
    schema = serialized.schema
    cols = ", ".join([f'"{c}" {_polars_to_sqlite_type(schema[c])}' for c in serialized.columns])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols})')

    placeholders = ", ".join(["?" for _ in serialized.columns])
    col_names = ", ".join([f'"{c}"' for c in serialized.columns])

    rows = serialized.rows()
    cursor.executemany(
        f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})',
        rows,
    )
    return len(serialized)


# =============================================================================
# TestSerializeComplexTypesListToCsv
# =============================================================================


class TestSerializeComplexTypesListToCsv:
    def test_list_to_csv_string(self):
        df = pl.DataFrame({"colors": [["W", "U"]]}, schema={"colors": pl.List(pl.String)})
        result = serialize_complex_types(df)
        assert result["colors"].dtype == pl.String
        assert result["colors"][0] == "W, U"

    def test_struct_to_json(self):
        df = pl.DataFrame(
            {"ids": [{"scryfallId": "sf-001", "multiverseId": None}]},
            schema={"ids": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String})},
        )
        result = serialize_complex_types(df)
        parsed = json.loads(result["ids"][0])
        assert parsed["scryfallId"] == "sf-001"
        assert "multiverseId" not in parsed  # null stripped


# =============================================================================
# TestSqliteTypeStorage
# =============================================================================


class TestSqliteTypeStorage:
    @pytest.fixture
    def db(self, tmp_path):
        path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(str(path))
        yield conn
        conn.close()

    def test_bool_to_int(self, db):
        df = pl.DataFrame({"hasFoil": [True, False]})
        _write_table_direct(db.cursor(), "test", df)
        db.commit()
        rows = db.execute("SELECT hasFoil FROM test ORDER BY rowid").fetchall()
        assert rows[0][0] == 1
        assert rows[1][0] == 0

    def test_list_to_text(self, db):
        df = pl.DataFrame(
            {"colors": [["W", "U"]]},
            schema={"colors": pl.List(pl.String)},
        )
        _write_table_direct(db.cursor(), "test", df)
        db.commit()
        val = db.execute("SELECT colors FROM test").fetchone()[0]
        assert val == "W, U"

    def test_struct_to_json_text(self, db):
        df = pl.DataFrame(
            {"ids": [{"scryfallId": "sf-001", "multiverseId": "123"}]},
            schema={"ids": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String})},
        )
        _write_table_direct(db.cursor(), "test", df)
        db.commit()
        val = db.execute("SELECT ids FROM test").fetchone()[0]
        parsed = json.loads(val)
        assert parsed["scryfallId"] == "sf-001"
        assert parsed["multiverseId"] == "123"


# =============================================================================
# TestCsvFlattenPreservesScalarTypes
# =============================================================================


class TestCsvFlattenPreservesScalarTypes:
    def test_scalar_types_survive(self, tmp_path):
        df = pl.DataFrame(
            {
                "name": ["Alpha"],
                "count": [42],
                "price": [3.14],
                "flag": [True],
            }
        )
        flat = _flatten_for_csv(df)
        path = tmp_path / "test.csv"
        flat.write_csv(path)
        read_back = pl.read_csv(path)
        assert read_back["count"][0] == 42
        assert abs(read_back["price"][0] - 3.14) < 0.001
        assert read_back["flag"][0] is True


# =============================================================================
# TestParquetPreservesAllNativeTypes
# =============================================================================


class TestParquetPreservesAllNativeTypes:
    def test_all_types_survive(self, tmp_path):
        df = _make_full_card_df()
        path = tmp_path / "test.parquet"
        df.write_parquet(path, compression="zstd", compression_level=9)
        read_back = pl.read_parquet(path)
        for col in df.columns:
            assert read_back.schema[col] == df.schema[col], f"Type mismatch for {col}"
        assert read_back.equals(df)


# =============================================================================
# TestBoolConversionTable
# =============================================================================


class TestBoolConversionTable:
    @pytest.mark.parametrize(
        ("value", "sqlite_expected", "pg_expected", "mysql_expected"),
        [
            (True, "1", "t", "TRUE"),
            (False, "0", "f", "FALSE"),
            (None, "NULL", "\\N", "NULL"),
        ],
        ids=["true", "false", "none"],
    )
    def test_bool_across_dialects(self, value, sqlite_expected, pg_expected, mysql_expected):
        assert escape_sqlite(value) == sqlite_expected
        assert escape_postgres(value) == pg_expected
        assert escape_mysql(value) == mysql_expected


# =============================================================================
# TestIntAcrossEscapeFunctions
# =============================================================================


class TestIntAcrossEscapeFunctions:
    def test_int_consistent(self):
        assert escape_sqlite(42) == "42"
        assert escape_postgres(42) == "42"
        assert escape_mysql(42) == "42"

    def test_zero_consistent(self):
        assert escape_sqlite(0) == "0"
        assert escape_postgres(0) == "0"
        assert escape_mysql(0) == "0"


# =============================================================================
# TestFloatPrecisionAcrossEscapes
# =============================================================================


class TestFloatPrecisionAcrossEscapes:
    def test_float_precision(self):
        val = 3.14159
        assert escape_sqlite(val) == "3.14159"
        assert escape_postgres(val) == "3.14159"
        assert escape_mysql(val) == "3.14159"
