"""TCGPlayer data persistence."""

import logging
from pathlib import Path
from typing import Literal

import polars as pl

LOGGER = logging.getLogger(__name__)


class TcgPlayerStorage:
    """
    TCGPlayer data persistence.

    Handles reading/writing product data to parquet.
    """

    DEFAULT_COMPRESSION: Literal["zstd"] = "zstd"
    DEFAULT_COMPRESSION_LEVEL = 9

    @staticmethod
    def write(
        df: pl.DataFrame,
        path: Path | str,
        compression: Literal["zstd"] = DEFAULT_COMPRESSION,
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
    ) -> Path:
        """Write products DataFrame to parquet."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        df.write_parquet(
            path,
            compression=compression,
            compression_level=compression_level,
            statistics=True,
        )

        size_mb = path.stat().st_size / 1024 / 1024
        LOGGER.info(f"Wrote {len(df):,} products to {path} ({size_mb:.2f} MB)")
        return path

    @staticmethod
    def read(path: Path | str) -> pl.DataFrame:
        """Load products from parquet."""
        path = Path(path)
        df = pl.read_parquet(path)
        LOGGER.info(f"Loaded {len(df):,} products from {path}")
        return df

    @staticmethod
    def scan(path: Path | str) -> pl.LazyFrame:
        """Lazy scan products parquet."""
        return pl.scan_parquet(path)

    @staticmethod
    def exists(path: Path | str) -> bool:
        """Check if cache file exists."""
        return Path(path).exists()
