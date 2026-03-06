"""Tests for SQLite output builder — _write_table, text dump, indexes."""

from __future__ import annotations

import sqlite3

import polars as pl
import pytest

from mtgjson5.build.formats.sqlite import (
    TABLE_INDEXES,
    _polars_to_sqlite_type,
)
from mtgjson5.build.serializers import serialize_complex_types

# =============================================================================
# Helpers
# =============================================================================


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

    if table_name in TABLE_INDEXES:
        for idx_name, col in TABLE_INDEXES[table_name]:
            cursor.execute(f'CREATE INDEX "idx_{table_name}_{idx_name}" ON "{table_name}" ("{col}")')

    return len(serialized)


# =============================================================================
# TestSqliteWriteTable
# =============================================================================


class TestSqliteWriteTable:
    @pytest.fixture
    def db(self, tmp_path):
        path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(str(path))
        yield conn
        conn.close()

    def test_table_created_with_correct_columns(self, db):
        df = pl.DataFrame({"uuid": ["a", "b"], "name": ["Alpha", "Beta"], "cmc": [1, 2]})
        _write_table_direct(db.cursor(), "cards", df)
        db.commit()
        cursor = db.execute("PRAGMA table_info(cards)")
        col_names = [row[1] for row in cursor.fetchall()]
        assert set(col_names) == {"uuid", "name", "cmc"}

    def test_all_rows_inserted(self, db):
        df = pl.DataFrame({"uuid": ["a", "b", "c"], "val": [1, 2, 3]})
        _write_table_direct(db.cursor(), "test_table", df)
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
        assert count == 3

    def test_indexes_created(self, db):
        df = pl.DataFrame({"uuid": ["a"], "name": ["X"], "setCode": ["TST"]})
        _write_table_direct(db.cursor(), "cards", df)
        db.commit()
        indexes = db.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        idx_names = {row[0] for row in indexes}
        assert "idx_cards_uuid" in idx_names
        assert "idx_cards_name" in idx_names
        assert "idx_cards_setCode" in idx_names

    def test_boolean_stored_as_integer(self, db):
        df = pl.DataFrame({"uuid": ["a"], "flag": [True]})
        _write_table_direct(db.cursor(), "test_bool", df)
        db.commit()
        val = db.execute("SELECT flag FROM test_bool").fetchone()[0]
        assert val == 1

    def test_null_values_preserved(self, db):
        df = pl.DataFrame(
            {"uuid": ["a"], "name": [None]},
            schema={"uuid": pl.String, "name": pl.String},
        )
        _write_table_direct(db.cursor(), "test_null", df)
        db.commit()
        val = db.execute("SELECT name FROM test_null").fetchone()[0]
        assert val is None

    def test_list_stored_as_comma_separated_text(self, db):
        df = pl.DataFrame(
            {"keywords": [["Flying", "Trample"]]},
            schema={"keywords": pl.List(pl.String)},
        )
        _write_table_direct(db.cursor(), "test_list", df)
        db.commit()
        val = db.execute("SELECT keywords FROM test_list").fetchone()[0]
        assert val == "Flying, Trample"

    def test_struct_stored_as_json_text(self, db):
        df = pl.DataFrame(
            {"ids": [{"scryfallId": "sf-001", "multiverseId": "123"}]},
            schema={"ids": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String})},
        )
        _write_table_direct(db.cursor(), "test_struct", df)
        db.commit()
        val = db.execute("SELECT ids FROM test_struct").fetchone()[0]
        import json

        parsed = json.loads(val)
        assert parsed["scryfallId"] == "sf-001"

    def test_false_boolean_stored_as_zero(self, db):
        df = pl.DataFrame({"flag": [False]})
        _write_table_direct(db.cursor(), "test_bool_false", df)
        db.commit()
        val = db.execute("SELECT flag FROM test_bool_false").fetchone()[0]
        assert val == 0

    def test_null_boolean_stored_as_null(self, db):
        df = pl.DataFrame({"flag": [None]}, schema={"flag": pl.Boolean})
        _write_table_direct(db.cursor(), "test_bool_null", df)
        db.commit()
        val = db.execute("SELECT flag FROM test_bool_null").fetchone()[0]
        assert val is None

    def test_float_precision_preserved(self, db):
        df = pl.DataFrame({"price": [3.14159]})
        _write_table_direct(db.cursor(), "test_float", df)
        db.commit()
        val = db.execute("SELECT price FROM test_float").fetchone()[0]
        assert abs(val - 3.14159) < 1e-10


# =============================================================================
# TestTableIndexes
# =============================================================================


class TestTableIndexes:
    def test_expected_tables_present(self):
        expected = {
            "cards",
            "tokens",
            "sets",
            "cardIdentifiers",
            "cardLegalities",
            "cardForeignData",
            "cardRulings",
            "cardPurchaseUrls",
            "tokenIdentifiers",
            "setTranslations",
            "setBoosterSheets",
            "setBoosterSheetCards",
            "setBoosterContents",
            "setBoosterContentWeights",
        }
        assert set(TABLE_INDEXES.keys()) == expected

    def test_index_naming_convention(self, tmp_path):
        db_path = tmp_path / "idx_test.sqlite"
        conn = sqlite3.connect(str(db_path))
        df = pl.DataFrame({"uuid": ["a"], "name": ["X"], "setCode": ["TST"]})
        _write_table_direct(conn.cursor(), "cards", df)
        conn.commit()
        indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        for (idx_name,) in indexes:
            assert idx_name.startswith("idx_cards_")
        conn.close()
