"""Parquet output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Literal

import polars as pl

from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.utils import LOGGER

if TYPE_CHECKING:
    from ..context import AssemblyContext


class ParquetBuilder:
    """Builds Parquet file exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def write(self, output_dir: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write Parquet files to output directory."""
        if output_dir is None:
            output_dir = self.ctx.output_path / "parquet"

        output_dir.mkdir(parents=True, exist_ok=True)
        compression: Literal["zstd"] = "zstd"

        tables = self.ctx.normalized_tables
        if not tables:
            return None

        # Write each table
        for name, df in tables.items():
            if df is not None and len(df) > 0:
                path = output_dir / f"{name}.parquet"
                df.write_parquet(path, compression=compression, compression_level=9)
                LOGGER.info(f"  {name}.parquet: {df.height:,} rows")

        # Write AllPrintings (full cards with nested structures)
        cards_df = self.ctx.all_cards_df
        if cards_df is not None:
            path = output_dir / "AllPrintings.parquet"
            cards_df.write_parquet(path, compression=compression, compression_level=9)
            LOGGER.info(f"  AllPrintings.parquet: {cards_df.height:,} rows")

        # Write meta
        meta = MtgjsonMeta()
        meta_df = pl.DataFrame({"date": [meta.date], "version": [meta.version]})
        meta_df.write_parquet(output_dir / "meta.parquet", compression=compression, compression_level=9)
        LOGGER.info("  meta.parquet: 1 row")

        # Write booster tables
        for name, df in self.ctx.normalized_boosters.items():
            if df is not None and len(df) > 0:
                path = output_dir / f"{name}.parquet"
                df.write_parquet(path, compression=compression, compression_level=9)
                LOGGER.info(f"  {name}.parquet: {df.height:,} rows")

        LOGGER.info(f"Wrote Parquet files to {output_dir}")
        return output_dir
