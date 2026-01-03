"""SQLite output builder for MTGJSON."""

from __future__ import annotations

import contextlib
import pathlib
import sqlite3
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.utils import LOGGER

from ..serializers import serialize_complex_types


if TYPE_CHECKING:
    from ..context import AssemblyContext


class SQLiteBuilder:
    """Builds SQLite database exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def _load_cards(self) -> pl.DataFrame | None:
        """Load cards from parquet cache."""
        parquet_dir = self.ctx.parquet_dir
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found. Run build_cards() first.")
            return None
        return pl.read_parquet(parquet_dir / "**/*.parquet")

    def _flatten_for_sql(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten cards for SQL table (exclude nested columns)."""
        exclude = {"identifiers", "legalities", "rulings", "foreignData"}
        return df.select([c for c in df.columns if c not in exclude])

    def write(self, output_path: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write SQLite database using native sqlite3."""
        cards_df = self._load_cards()
        if cards_df is None:
            return None

        if output_path is None:
            output_path = self.ctx.output_path / "AllPrintings.sqlite"

        if output_path.exists():
            output_path.unlink()

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()

        # Create table with TEXT columns
        cols = ", ".join([f'"{c}" TEXT' for c in serialized.columns])
        cursor.execute(f'CREATE TABLE "cards" ({cols})')

        # Batch insert rows
        placeholders = ", ".join(["?" for _ in serialized.columns])
        col_names = ", ".join([f'"{c}"' for c in serialized.columns])

        batch_size = 10000
        rows = serialized.rows()
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(
                f'INSERT INTO "cards" ({col_names}) VALUES ({placeholders})', batch
            )

        # Create indexes
        for idx, col in [("uuid", "uuid"), ("name", "name"), ("setCode", "setCode")]:
            with contextlib.suppress(Exception):
                cursor.execute(f'CREATE INDEX "idx_cards_{idx}" ON "cards" ("{col}")')

        conn.commit()
        conn.close()

        LOGGER.info(f"Wrote AllPrintings.sqlite ({len(serialized):,} rows)")
        return output_path

    def write_text_dump(
        self, output_path: pathlib.Path | None = None
    ) -> pathlib.Path | None:
        """Write SQLite text file (.sql)."""
        from datetime import datetime

        from ..serializers import escape_sqlite

        cards_df = self._load_cards()
        if cards_df is None:
            return None

        if output_path is None:
            output_path = self.ctx.output_path / "AllPrintings.sql"

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON SQLite Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
            )
            f.write("BEGIN TRANSACTION;\n\n")

            cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
            f.write(f'CREATE TABLE IF NOT EXISTS "cards" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in serialized.columns])
            for row in serialized.rows():
                values = ", ".join(escape_sqlite(v) for v in row)
                f.write(f'INSERT INTO "cards" ({col_names}) VALUES ({values});\n')

            f.write(
                '\nCREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards" ("uuid");\n'
            )
            f.write(
                'CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards" ("name");\n'
            )
            f.write(
                'CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards" ("setCode");\n'
            )
            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote AllPrintings.sql ({len(serialized):,} rows)")
        return output_path
