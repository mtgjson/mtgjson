"""PostgreSQL output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from datetime import datetime
from typing import IO, TYPE_CHECKING

import polars as pl

from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.utils import LOGGER

from ..serializers import escape_postgres, serialize_complex_types
from .sqlite import TABLE_INDEXES

if TYPE_CHECKING:
    from ..context import AssemblyContext


def _polars_to_postgres_type(dtype: pl.DataType) -> str:
    """Map Polars dtype to PostgreSQL type."""
    if dtype.is_integer():
        return "INTEGER"
    if dtype.is_float():
        return "FLOAT"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Datetime:
        return "TIMESTAMP"
    # String, List, Struct -> TEXT
    return "TEXT"


class PostgresBuilder:
    """Builds PostgreSQL exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    @staticmethod
    def _write_table(f: IO[str], table_name: str, df: pl.DataFrame) -> int:
        """Write a single table to the PostgreSQL dump file.

        Returns the number of rows written.
        """
        if df is None or len(df) == 0:
            return 0

        serialized = serialize_complex_types(df)

        schema = serialized.schema
        col_defs = ",\n    ".join([f'"{c}" {_polars_to_postgres_type(schema[c])}' for c in serialized.columns])
        f.write(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;\n')
        f.write(f'CREATE TABLE "{table_name}" (\n    {col_defs}\n);\n\n')

        col_names = ", ".join([f'"{c}"' for c in serialized.columns])
        f.write(f'COPY "{table_name}" ({col_names}) FROM stdin;\n')

        for row in serialized.rows():
            escaped = [escape_postgres(v) for v in row]
            f.write("\t".join(escaped) + "\n")

        f.write("\\.\n\n")

        if table_name in TABLE_INDEXES:
            for idx_name, col in TABLE_INDEXES[table_name]:
                f.write(f'CREATE INDEX "idx_{table_name}_{idx_name}" ON "{table_name}" ("{col}");\n')
            f.write("\n")

        return len(serialized)

    def write(
        self,
        output_path: pathlib.Path | None = None,
        postgres_uri: str | None = None,
    ) -> pathlib.Path | None:
        """Write PostgreSQL - direct ADBC or dump file.

        Creates all normalized tables matching the SQLite/MySQL exports:
        - cards, cardIdentifiers, cardLegalities, cardForeignData, cardRulings, cardPurchaseUrls
        - tokens, tokenIdentifiers
        - sets, setTranslations
        - setBoosterSheets, setBoosterSheetCards, setBoosterContents, setBoosterContentWeights
        - meta
        """
        tables = self.ctx.normalized_tables
        if not tables:
            return None

        tables = dict(tables)  # shallow copy for adding meta/boosters
        tables.update(self.ctx.normalized_boosters)

        meta = MtgjsonMeta()
        tables["meta"] = pl.DataFrame({"date": [meta.date], "version": [meta.version]})

        if postgres_uri:
            # Direct database write
            total_rows = 0
            for table_name, df in tables.items():
                if df is None or len(df) == 0:
                    continue
                serialized = serialize_complex_types(df)
                serialized.write_database(
                    table_name=table_name,
                    connection=postgres_uri,
                    if_table_exists="replace",
                    engine="adbc",
                )
                total_rows += len(serialized)
            LOGGER.info(f"Wrote to PostgreSQL ({total_rows:,} rows)")
            return None

        # Dump file
        if output_path is None:
            output_path = self.ctx.output_path / "AllPrintings.psql"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write("BEGIN;\n\n")

            total_rows = 0
            table_count = 0

            for table_name, df in tables.items():
                if df is not None and len(df) > 0:
                    rows = self._write_table(f, table_name, df)
                    if rows > 0:
                        LOGGER.info(f"  {table_name}: {rows:,} rows")
                        total_rows += rows
                        table_count += 1

            f.write("COMMIT;\n")

        LOGGER.info(f"Wrote AllPrintings.psql ({table_count} tables, {total_rows:,} total rows)")
        return output_path
