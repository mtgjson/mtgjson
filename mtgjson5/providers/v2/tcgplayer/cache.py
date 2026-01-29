"""TCGPlayer data persistence with streaming support."""

import logging
from pathlib import Path
from typing import Literal

import polars as pl

LOGGER = logging.getLogger(__name__)


class PartFileManager:
    """
    Manages incremental part file writes for streaming large datasets.

    Writes data to numbered part files, then combines into final output.
    """

    def __init__(
        self,
        output_path: Path,
        prefix: str = ".tcg_part",
        flush_threshold: int = 50_000,
    ):
        self.output_path = output_path
        self.prefix = prefix
        self.flush_threshold = flush_threshold
        self.part_files: list[Path] = []
        self._buffer: list[dict] = []
        self._part_counter = 0

    @property
    def buffer_size(self) -> int:
        """Return current buffer size."""
        return len(self._buffer)

    @property
    def should_flush(self) -> bool:
        """Return True if buffer should be flushed."""
        return self.buffer_size >= self.flush_threshold

    def add(self, records: list[dict]) -> None:
        """Add records to buffer."""
        self._buffer.extend(records)

    def flush(self) -> Path | None:
        """
        Write buffer to part file if non-empty.

        Returns path to written file, or None if buffer empty.
        """
        if not self._buffer:
            return None

        part_path = (
            self.output_path.parent / f"{self.prefix}_{self._part_counter:04d}.parquet"
        )
        self._part_counter += 1

        pl.DataFrame(
            self._buffer,
            schema={
                "productId": pl.Int64(),
                "name": pl.String(),
                "cleanName": pl.String(),
                "groupId": pl.Int64(),
                "url": pl.String(),
                "skus": pl.List(
                    pl.Struct(
                        {
                            "skuId": pl.Int64(),
                            "languageId": pl.Int64(),
                            "printingId": pl.Int64(),
                            "conditionId": pl.Int64(),
                        }
                    )
                ),
            },
        ).write_parquet(part_path)
        self.part_files.append(part_path)

        count = len(self._buffer)
        self._buffer = []

        LOGGER.debug(f"Flushed {count:,} products to {part_path.name}")
        return part_path

    def flush_if_needed(self) -> Path | None:
        """Flush if buffer exceeds threshold."""
        if self.should_flush:
            return self.flush()
        return None

    def combine(self) -> pl.LazyFrame:
        """
        Combine all part files into final output.

        Cleans up part files after combining.
        """
        # Final flush
        self.flush()

        if not self.part_files:
            # No data - write empty file
            pl.DataFrame(
                schema={
                    "productId": pl.Int64(),
                    "name": pl.String(),
                    "cleanName": pl.String(),
                    "groupId": pl.Int64(),
                    "url": pl.String(),
                    "skus": pl.List(
                        pl.Struct(
                            {
                                "skuId": pl.Int64(),
                                "languageId": pl.Int64(),
                                "printingId": pl.Int64(),
                                "conditionId": pl.Int64(),
                            }
                        )
                    ),
                }
            ).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        try:
            # Scan all parts
            pattern = str(self.output_path.parent / f"{self.prefix}_*.parquet")
            lf = pl.scan_parquet(pattern, glob=True, rechunk=True)

            # Write combined output
            lf.sink_parquet(self.output_path)
            LOGGER.info(f"Combined {len(self.part_files)} parts to {self.output_path}")

        finally:
            self._cleanup_parts()

        return pl.scan_parquet(self.output_path)

    def _cleanup_parts(self) -> None:
        """Delete part files."""
        for part_file in self.part_files:
            try:
                part_file.unlink()
            except OSError as e:
                LOGGER.warning(f"Failed to delete {part_file}: {e}")
        self.part_files = []


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
