"""PostgreSQL output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.utils import LOGGER

from ..serializers import escape_postgres, serialize_complex_types

if TYPE_CHECKING:
    from ..context import AssemblyContext


class PostgresBuilder:
    """Builds PostgreSQL exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def _load_cards(self) -> pl.DataFrame | None:
        """Load cards from parquet cache."""
        return self.ctx.all_cards_df

    def _flatten_for_sql(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten cards for SQL table (exclude nested columns)."""
        exclude = {"identifiers", "legalities", "rulings", "foreignData"}
        return df.select([c for c in df.columns if c not in exclude])

    def write(
        self,
        output_path: pathlib.Path | None = None,
        postgres_uri: str | None = None,
    ) -> pathlib.Path | None:
        """Write PostgreSQL - direct ADBC or dump file."""
        cards_df = self._load_cards()
        if cards_df is None:
            return None

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        if postgres_uri:
            # Direct database write
            serialized.write_database(
                table_name="cards",
                connection=postgres_uri,
                if_table_exists="replace",
                engine="adbc",
            )
            LOGGER.info(f"Wrote to PostgreSQL ({len(serialized):,} rows)")
            return None

        # Dump file
        if output_path is None:
            output_path = self.ctx.output_path / "AllPrintings.psql"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write("BEGIN;\n\n")

            cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
            f.write(f'CREATE TABLE IF NOT EXISTS "cards" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in serialized.columns])
            f.write(f'COPY "cards" ({col_names}) FROM stdin;\n')

            for row in serialized.rows():
                escaped = [escape_postgres(v) for v in row]
                f.write("\t".join(escaped) + "\n")

            f.write("\\.\n\n")
            f.write('CREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards" ("uuid");\n')
            f.write('CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards" ("name");\n')
            f.write('CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards" ("setCode");\n')
            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote AllPrintings.psql ({len(serialized):,} rows)")
        return output_path
