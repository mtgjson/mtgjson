"""Card Kingdom data persistence."""

import logging
from pathlib import Path
from typing import Literal

import polars as pl

LOGGER = logging.getLogger(__name__)


class CardKingdomStorage:
    """
    Handles CK data persistence to/from Parquet.

    Optimized settings for CK data characteristics:
    - zstd compression for good ratio + speed
    - Row groups sized for typical queries
    """

    DEFAULT_COMPRESSION = "zstd"
    DEFAULT_COMPRESSION_LEVEL = 9
    DEFAULT_ROW_GROUP_SIZE = 100_000

    @staticmethod
    def write(
        df: pl.DataFrame,
        path: Path | str,
        compression: Literal["zstd"] | None = "zstd",
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
    ) -> Path:
        """
        Write DataFrame to Parquet with optimized settings.

        Returns path to written file.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        compression_arg: Literal["zstd"] = compression or "zstd"
        df.write_parquet(
            path,
            compression=compression_arg,
            compression_level=compression_level,
            statistics=True,
            row_group_size=CardKingdomStorage.DEFAULT_ROW_GROUP_SIZE,
        )

        size_mb = path.stat().st_size / 1024 / 1024
        LOGGER.info(f"Wrote {len(df):,} records to {path} ({size_mb:.2f} MB)")
        return path

    @staticmethod
    def read(path: Path | str) -> pl.DataFrame:
        """Load CK data from Parquet file."""
        path = Path(path)
        df = pl.read_parquet(path)
        LOGGER.info(f"Loaded {len(df):,} records from {path}")
        return df

    @staticmethod
    def exists(path: Path | str) -> bool:
        """Check if cache file exists."""
        return Path(path).exists()
