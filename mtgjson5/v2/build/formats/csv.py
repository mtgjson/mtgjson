"""CSV output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.utils import LOGGER

from ..assemble import TableAssembler

if TYPE_CHECKING:
    from ..context import AssemblyContext


def _flatten_for_csv(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flatten nested columns for CSV export.

    - Struct columns are serialized to JSON strings
    - List columns are serialized to JSON strings
    """
    schema = df.schema

    # Convert struct and list columns to JSON strings
    complex_cols = [c for c in df.columns if isinstance(schema.get(c), pl.Struct | pl.List)]

    if not complex_cols:
        return df

    return df.with_columns(
        [
            pl.col(c).struct.json_encode().alias(c)
            if isinstance(schema.get(c), pl.Struct)
            else pl.col(c).cast(pl.String).alias(c)
            for c in complex_cols
        ]
    )


class CSVBuilder:
    """Builds CSV file exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def _load_cards(self) -> pl.DataFrame | None:
        """Load cards from parquet cache."""
        parquet_dir = self.ctx.parquet_dir
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found")
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
            # Explicit schema to avoid type inference issues with mixed None/bool values
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

    def write(self, output_dir: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write CSV files to output directory."""
        if output_dir is None:
            output_dir = self.ctx.output_path / "csv"

        output_dir.mkdir(parents=True, exist_ok=True)

        cards_df = self._load_cards()
        if cards_df is None:
            return None

        tokens_df = self._load_tokens()
        sets_df = self._load_sets()

        # Build normalized tables
        tables = TableAssembler.build_all(cards_df, tokens_df, sets_df)

        # Write each table
        for name, df in tables.items():
            if df is not None and len(df) > 0:
                path = output_dir / f"{name}.csv"
                # Flatten nested columns for CSV compatibility
                flat_df = _flatten_for_csv(df)
                flat_df.write_csv(path)
                LOGGER.info(f"  {name}.csv: {flat_df.height:,} rows")

        # Write meta
        meta = MtgjsonMetaObject()
        meta_df = pl.DataFrame({"date": [meta.date], "version": [meta.version]})
        meta_df.write_csv(output_dir / "meta.csv")
        LOGGER.info("  meta.csv: 1 row")

        # Build and write booster tables
        if self.ctx.booster_configs:
            booster_tables = TableAssembler.build_boosters(self.ctx.booster_configs)
            for name, df in booster_tables.items():
                if df is not None and len(df) > 0:
                    path = output_dir / f"{name}.csv"
                    flat_df = _flatten_for_csv(df)
                    flat_df.write_csv(path)
                    LOGGER.info(f"  {name}.csv: {flat_df.height:,} rows")

        LOGGER.info(f"Wrote CSV files to {output_dir}")
        return output_dir
