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
        # Exclude token sets
        card_dirs = [
            d
            for d in parquet_dir.iterdir()
            if d.is_dir() and not d.name.startswith("setCode=T")
        ]
        if not card_dirs:
            LOGGER.error("No card parquet directories found")
            return None
        return pl.read_parquet(parquet_dir / "setCode=[!T]*/*.parquet")

    def _load_tokens(self) -> pl.DataFrame | None:
        """Load tokens from parquet cache."""
        parquet_dir = self.ctx.parquet_dir
        token_dirs = [
            d
            for d in parquet_dir.iterdir()
            if d.is_dir() and d.name.startswith("setCode=T")
        ]
        if token_dirs:
            return pl.read_parquet(parquet_dir / "setCode=T*/*.parquet")
        return None

    def _load_sets(self) -> pl.DataFrame | None:
        """Load sets metadata as DataFrame."""
        if self.ctx.set_meta:
            return pl.DataFrame(list(self.ctx.set_meta.values()))
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
                df.write_csv(path)
                LOGGER.info(f"  {name}.csv: {df.height:,} rows")

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
                    df.write_csv(path)
                    LOGGER.info(f"  {name}.csv: {df.height:,} rows")

        LOGGER.info(f"Wrote CSV files to {output_dir}")
        return output_dir
