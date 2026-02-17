"""MySQL output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.utils import LOGGER

from ..assemble import TableAssembler
from ..serializers import escape_mysql, serialize_complex_types
from .sqlite import TABLE_INDEXES

if TYPE_CHECKING:
    from ..context import AssemblyContext

# Indexed columns that need VARCHAR instead of TEXT for index creation
_MYSQL_VARCHAR_OVERRIDES: dict[str, str] = {
    "uuid": "VARCHAR(36) NOT NULL",
    "cardUuid": "VARCHAR(36) NOT NULL",
    "name": "VARCHAR(255) NOT NULL",
    "setCode": "VARCHAR(6) NOT NULL",
    "code": "VARCHAR(6) NOT NULL",
    "language": "VARCHAR(25) NOT NULL",
}

# Integer columns that exceed MySQL INTEGER range and need BIGINT
_MYSQL_BIGINT_COLUMNS: set[str] = {"sheetTotalWeight", "cardWeight"}


def _polars_to_mysql_type(dtype: pl.DataType, table_name: str, col_name: str) -> str:
    """Map Polars dtype to MySQL type, with compatibility overrides"""

    table_indexes = TABLE_INDEXES.get(table_name, [])
    indexed_cols = {col for _, col in table_indexes}
    if col_name in indexed_cols:
        override = _MYSQL_VARCHAR_OVERRIDES.get(col_name)
        if override:
            return override

    if col_name in _MYSQL_BIGINT_COLUMNS:
        return "BIGINT"

    if dtype.is_integer():
        return "INTEGER"
    if dtype.is_float():
        return "FLOAT"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Datetime:
        return "DATETIME"
    # String, List, Struct -> TEXT
    return "TEXT"


class MySQLBuilder:
    """Builds MySQL exports."""

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
            schema_overrides = {
                "isOnlineOnly": pl.Boolean,
                "isFoilOnly": pl.Boolean,
                "isNonFoilOnly": pl.Boolean,
                "isForeignOnly": pl.Boolean,
                "isPartialPreview": pl.Boolean,
            }
            df = pl.DataFrame(list(self.ctx.set_meta.values()), schema_overrides=schema_overrides)
            if "type" in df.columns:
                is_traditional_token = (pl.col("type") == "token") & pl.col("code").str.starts_with("T")
                df = df.filter(~is_traditional_token)
            return df
        return None

    def write(self, output_path: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write MySQL text file (.sql).

        Creates MySQL-compatible SQL with:
        - Backtick-quoted identifiers
        - ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        - START TRANSACTION / COMMIT
        - MySQL data types (BOOLEAN, FLOAT, etc.)
        """
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

        meta = MtgjsonMeta()
        tables["meta"] = pl.DataFrame({"date": [meta.date], "version": [meta.version]})

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"-- MTGSQLive Output File\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write("SET names 'utf8mb4';\n")
            f.write("START TRANSACTION;\n\n")

            for table_name, df in tables.items():
                if df is None or len(df) == 0:
                    continue

                serialized = serialize_complex_types(df)

                schema = serialized.schema
                col_defs = ["    `id` INTEGER PRIMARY KEY AUTO_INCREMENT"]
                for c in serialized.columns:
                    col_defs.append(f"    `{c}` {_polars_to_mysql_type(schema[c], table_name, c)}")

                cols_str = ",\n".join(col_defs)
                f.write(
                    f"DROP TABLE IF EXISTS `{table_name}`;\n"
                    f"CREATE TABLE `{table_name}` (\n{cols_str}\n) "
                    f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n"
                )

                col_names = ", ".join([f"`{c}`" for c in serialized.columns])
                for row in serialized.rows():
                    values = ", ".join(escape_mysql(v) for v in row)
                    f.write(f"INSERT INTO `{table_name}` ({col_names}) VALUES ({values});\n")

                if table_name in TABLE_INDEXES:
                    for idx_name, col in TABLE_INDEXES[table_name]:
                        f.write(f"CREATE INDEX `idx_{table_name}_{idx_name}` ON `{table_name}` (`{col}`);\n")
                f.write("\n")
            f.write("COMMIT;\n")

        cards_count = len(tables.get("cards", pl.DataFrame()))
        LOGGER.info(f"Wrote AllPrintings.sql (MySQL format, {cards_count:,} cards)")
        return output_path
