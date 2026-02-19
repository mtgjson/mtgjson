"""CSV output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.utils import LOGGER

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

    def write(self, output_dir: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write CSV files to output directory."""
        if output_dir is None:
            output_dir = self.ctx.output_path / "csv"

        output_dir.mkdir(parents=True, exist_ok=True)

        tables = self.ctx.normalized_tables
        if not tables:
            return None

        # Write each table
        for name, df in tables.items():
            if df is not None and len(df) > 0:
                path = output_dir / f"{name}.csv"
                # Flatten nested columns for CSV compatibility
                flat_df = _flatten_for_csv(df)
                flat_df.write_csv(path)
                LOGGER.info(f"  {name}.csv: {flat_df.height:,} rows")

        # Write meta
        meta = MtgjsonMeta()
        meta_df = pl.DataFrame({"date": [meta.date], "version": [meta.version]})
        meta_df.write_csv(output_dir / "meta.csv")
        LOGGER.info("  meta.csv: 1 row")

        # Write booster tables
        for name, df in self.ctx.normalized_boosters.items():
            if df is not None and len(df) > 0:
                path = output_dir / f"{name}.csv"
                flat_df = _flatten_for_csv(df)
                flat_df.write_csv(path)
                LOGGER.info(f"  {name}.csv: {flat_df.height:,} rows")

        LOGGER.info(f"Wrote CSV files to {output_dir}")
        return output_dir
