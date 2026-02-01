"""SQLite output builder for MTGJSON."""

from __future__ import annotations

import contextlib
import pathlib
import sqlite3
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.utils import LOGGER

from ..assemble import TableAssembler
from ..serializers import serialize_complex_types


if TYPE_CHECKING:
    from polars.datatypes import DataTypeClass
    from ..context import AssemblyContext


# Table-specific indexes for query optimization
TABLE_INDEXES = {
    "cards": [("uuid", "uuid"), ("name", "name"), ("setCode", "setCode")],
    "tokens": [("uuid", "uuid"), ("name", "name"), ("setCode", "setCode")],
    "sets": [("code", "code"), ("name", "name")],
    "cardIdentifiers": [("uuid", "uuid")],
    "cardLegalities": [("uuid", "uuid")],
    "cardForeignData": [("uuid", "uuid"), ("language", "language")],
    "cardRulings": [("uuid", "uuid")],
    "cardPurchaseUrls": [("uuid", "uuid")],
    "tokenIdentifiers": [("uuid", "uuid")],
    "setTranslations": [("code", "code")],
    "setBoosterSheets": [("setCode", "setCode")],
    "setBoosterSheetCards": [("setCode", "setCode"), ("cardUuid", "cardUuid")],
    "setBoosterContents": [("setCode", "setCode")],
    "setBoosterContentWeights": [("setCode", "setCode")],
}


def _polars_to_sqlite_type(dtype: DataTypeClass) -> str:
    """Map Polars dtype to SQLite type."""
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return "INTEGER"
    if dtype in (pl.Float32, pl.Float64):
        return "REAL"
    if dtype == pl.Boolean:
        return "INTEGER"
    # String, Date, Datetime, List, Struct -> TEXT
    return "TEXT"


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
        return pl.read_parquet(parquet_dir / "*/*.parquet")

    def _load_tokens(self) -> pl.DataFrame | None:
        """Load tokens from parquet cache (separate tokens_dir)."""
        tokens_dir = self.ctx.tokens_dir
        if not tokens_dir.exists():
            return None
        return pl.read_parquet(tokens_dir / "*/*.parquet")

    def _load_sets(self) -> pl.DataFrame | None:
        """Load sets metadata as DataFrame.

        Filters out traditional token sets (type='token' AND code starts with 'T')
        to match CDN reference. Keeps special token sets like L14, SBRO, WMOM.
        """
        if self.ctx.set_meta:
            df = pl.DataFrame(list(self.ctx.set_meta.values()))
            if "type" in df.columns:
                is_traditional_token = (
                    (pl.col("type") == "token") & pl.col("code").str.starts_with("T")
                )
                df = df.filter(~is_traditional_token)
            return df
        return None

    def _write_table(
        self,
        cursor: sqlite3.Cursor,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """Write a single table to SQLite.

        Returns the number of rows written.
        """
        if df is None or len(df) == 0:
            return 0

        serialized = serialize_complex_types(df)

        schema = serialized.schema
        cols = ", ".join([
            f'"{c}" {_polars_to_sqlite_type(schema[c])}'
            for c in serialized.columns
        ])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols})')

        # Batch insert rows
        placeholders = ", ".join(["?" for _ in serialized.columns])
        col_names = ", ".join([f'"{c}"' for c in serialized.columns])

        batch_size = 10000
        rows = serialized.rows()
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(
                f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})',
                batch,
            )

        if table_name in TABLE_INDEXES:
            for idx_name, col in TABLE_INDEXES[table_name]:
                with contextlib.suppress(Exception):
                    cursor.execute(
                        f'CREATE INDEX "idx_{table_name}_{idx_name}" '
                        f'ON "{table_name}" ("{col}")'
                    )

        return len(serialized)

    def write(self, output_path: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write SQLite database using native sqlite3.

        Creates all normalized tables matching the CDN reference:
        - cards, cardIdentifiers, cardLegalities, cardForeignData, cardRulings, cardPurchaseUrls
        - tokens, tokenIdentifiers
        - sets, setTranslations
        - setBoosterSheets, setBoosterSheetCards, setBoosterContents, setBoosterContentWeights
        - meta
        """
        cards_df = self._load_cards()
        if cards_df is None:
            return None

        if output_path is None:
            output_path = self.ctx.output_path / "AllPrintings.sqlite"

        if output_path.exists():
            output_path.unlink()

        tokens_df = self._load_tokens()
        sets_df = self._load_sets()

        tables = TableAssembler.build_all(cards_df, tokens_df, sets_df)

        if self.ctx.booster_configs:
            booster_tables = TableAssembler.build_boosters(self.ctx.booster_configs)
            tables.update(booster_tables)

        meta = MtgjsonMetaObject()
        tables["meta"] = pl.DataFrame({"date": [meta.date], "version": [meta.version]})

        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()

        total_rows = 0
        table_count = 0

        for table_name, df in tables.items():
            if df is not None and len(df) > 0:
                rows = self._write_table(cursor, table_name, df)
                if rows > 0:
                    LOGGER.info(f"  {table_name}: {rows:,} rows")
                    total_rows += rows
                    table_count += 1

        conn.commit()
        conn.close()

        LOGGER.info(
            f"Wrote AllPrintings.sqlite ({table_count} tables, {total_rows:,} total rows)"
        )
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

        tokens_df = self._load_tokens()
        sets_df = self._load_sets()

        tables = TableAssembler.build_all(cards_df, tokens_df, sets_df)

        if self.ctx.booster_configs:
            booster_tables = TableAssembler.build_boosters(self.ctx.booster_configs)
            tables.update(booster_tables)

        meta = MtgjsonMetaObject()
        tables["meta"] = pl.DataFrame({"date": [meta.date], "version": [meta.version]})

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON SQLite Dump\n"
                f"-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
            )
            f.write("BEGIN TRANSACTION;\n\n")

            for table_name, df in tables.items():
                if df is None or len(df) == 0:
                    continue

                serialized = serialize_complex_types(df)

                schema = serialized.schema
                cols = ",\n    ".join([
                    f'"{c}" {_polars_to_sqlite_type(schema[c])}'
                    for c in serialized.columns
                ])
                f.write(f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n    {cols}\n);\n\n')

                col_names = ", ".join([f'"{c}"' for c in serialized.columns])
                for row in serialized.rows():
                    values = ", ".join(escape_sqlite(v) for v in row)
                    f.write(f'INSERT INTO "{table_name}" ({col_names}) VALUES ({values});\n')

                if table_name in TABLE_INDEXES:
                    for idx_name, col in TABLE_INDEXES[table_name]:
                        f.write(
                            f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{idx_name}" '
                            f'ON "{table_name}" ("{col}");\n'
                        )
                f.write("\n")
            f.write("COMMIT;\n")

        cards_count = len(tables.get("cards", pl.DataFrame()))
        LOGGER.info(f"Wrote AllPrintings.sql ({cards_count:,} cards)")
        return output_path
