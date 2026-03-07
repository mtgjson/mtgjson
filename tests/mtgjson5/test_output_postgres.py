"""Tests for PostgreSQL output builder — _flatten_for_sql and dump syntax."""

from __future__ import annotations

import polars as pl

from mtgjson5.build.serializers import escape_postgres, serialize_complex_types

# =============================================================================
# Helpers
# =============================================================================


def _flatten_for_sql(df: pl.DataFrame) -> pl.DataFrame:
    """Replicate PostgresBuilder._flatten_for_sql without needing an AssemblyContext."""
    exclude = {"identifiers", "legalities", "rulings", "foreignData"}
    return df.select([c for c in df.columns if c not in exclude])


def _generate_postgres_dump(df: pl.DataFrame) -> str:
    """Generate PostgreSQL dump text, mirroring PostgresBuilder.write() logic."""
    serialized = serialize_complex_types(df)
    lines = []
    lines.append("BEGIN;")

    cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
    lines.append(f'CREATE TABLE IF NOT EXISTS "cards" (\n    {cols}\n);')

    col_names = ", ".join([f'"{c}"' for c in serialized.columns])
    lines.append(f'COPY "cards" ({col_names}) FROM stdin;')

    for row in serialized.rows():
        escaped = [escape_postgres(v) for v in row]
        lines.append("\t".join(escaped))

    lines.append("\\.")
    lines.append("COMMIT;")
    return "\n".join(lines)


# =============================================================================
# TestFlattenForSql
# =============================================================================


class TestFlattenForSql:
    def test_excludes_nested_columns(self):
        df = pl.DataFrame(
            {
                "uuid": ["a"],
                "name": ["Alpha"],
                "identifiers": ["should-drop"],
                "legalities": ["should-drop"],
                "rulings": ["should-drop"],
                "foreignData": ["should-drop"],
            }
        )
        result = _flatten_for_sql(df)
        assert "identifiers" not in result.columns
        assert "legalities" not in result.columns
        assert "rulings" not in result.columns
        assert "foreignData" not in result.columns

    def test_preserves_non_excluded_columns(self):
        df = pl.DataFrame(
            {
                "uuid": ["a"],
                "name": ["Alpha"],
                "setCode": ["TST"],
                "identifiers": ["drop"],
            }
        )
        result = _flatten_for_sql(df)
        assert set(result.columns) == {"uuid", "name", "setCode"}


# =============================================================================
# TestPostgresDataCorrectness
# =============================================================================


class TestPostgresDataCorrectness:
    def test_data_lines_between_copy_and_terminator(self):
        df = pl.DataFrame(
            {
                "uuid": ["a", "b"],
                "name": ["Alpha", "Beta"],
            }
        )
        dump = _generate_postgres_dump(df)
        lines = dump.split("\n")
        copy_idx = next(i for i, line in enumerate(lines) if line.startswith("COPY"))
        term_idx = next(i for i, line in enumerate(lines) if line == "\\.")
        data_lines = lines[copy_idx + 1 : term_idx]
        assert len(data_lines) == 2

    def test_tab_separated_values(self):
        df = pl.DataFrame(
            {
                "uuid": ["a"],
                "name": ["Alpha"],
                "count": [42],
            }
        )
        dump = _generate_postgres_dump(df)
        lines = dump.split("\n")
        copy_idx = next(i for i, line in enumerate(lines) if line.startswith("COPY"))
        data_line = lines[copy_idx + 1]
        parts = data_line.split("\t")
        assert len(parts) == 3
        assert parts[0] == "a"
        assert parts[1] == "Alpha"
        assert parts[2] == "42"

    def test_null_renders_as_backslash_n(self):
        df = pl.DataFrame(
            {"uuid": ["a"], "name": [None]},
            schema={"uuid": pl.String, "name": pl.String},
        )
        dump = _generate_postgres_dump(df)
        lines = dump.split("\n")
        copy_idx = next(i for i, line in enumerate(lines) if line.startswith("COPY"))
        data_line = lines[copy_idx + 1]
        parts = data_line.split("\t")
        assert parts[1] == "\\N"


# =============================================================================
# TestPostgresBoolAndEscaping
# =============================================================================


class TestPostgresBoolAndEscaping:
    def test_boolean_renders_as_t_f(self):
        assert escape_postgres(True) == "t"
        assert escape_postgres(False) == "f"

    def test_null_renders_as_backslash_n_in_data_line(self):
        df = pl.DataFrame(
            {"uuid": ["a"], "val": [None]},
            schema={"uuid": pl.String, "val": pl.String},
        )
        dump = _generate_postgres_dump(df)
        lines = dump.split("\n")
        copy_idx = next(i for i, line in enumerate(lines) if line.startswith("COPY"))
        data_line = lines[copy_idx + 1]
        assert "\\N" in data_line

    def test_special_chars_escaped_in_data(self):
        df = pl.DataFrame({"text": ["tab\there\nnewline\\backslash"]})
        dump = _generate_postgres_dump(df)
        lines = dump.split("\n")
        copy_idx = next(i for i, line in enumerate(lines) if line.startswith("COPY"))
        data_line = lines[copy_idx + 1]
        # Tab in data should be escaped (not a field separator)
        assert "\\t" in data_line
        assert "\\n" in data_line
        assert "\\\\" in data_line
