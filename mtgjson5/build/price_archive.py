"""
Price archive management: loading, saving, partitioning, and migration.
"""

from __future__ import annotations

import datetime
import json
import logging
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dateutil.relativedelta
import orjson
import polars as pl

from mtgjson5 import constants

LOGGER = logging.getLogger(__name__)

# Partitioned price archive directory
PRICES_PARTITION_DIR = constants.CACHE_PATH / "prices"

# Schema for price data
PRICE_SCHEMA = {
    "uuid": pl.String,
    "date": pl.String,
    "source": pl.String,
    "provider": pl.String,
    "price_type": pl.String,  # "buylist" or "retail"
    "finish": pl.String,  # "normal", "foil", "etched"
    "price": pl.Float64,
    "currency": pl.String,
}


def prune_prices(df: pl.LazyFrame, months: int = 3) -> pl.LazyFrame:
    """
    Filter out price entries older than `months` months.

    Args:
        df: LazyFrame with price data
        months: Number of months to keep (default 3)

    Returns:
        Filtered LazyFrame
    """
    cutoff = (datetime.date.today() + dateutil.relativedelta.relativedelta(months=-months)).strftime("%Y-%m-%d")

    return df.filter(pl.col("date") >= cutoff)


def merge_prices(archive: pl.LazyFrame, today: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merge today's prices into archive, keeping latest price per unique key.

    Uses concat + group_by to handle duplicates, preferring today's values.

    Args:
        archive: Existing price archive
        today: Today's price data

    Returns:
        Merged LazyFrame with deduplication
    """
    key_cols = ["uuid", "date", "source", "provider", "price_type", "finish"]

    combined = pl.concat([archive, today])

    return combined.group_by(key_cols).agg(
        [
            pl.col("price").last(),
            pl.col("currency").last(),
        ]
    )


def load_archive(
    path: Path | None = None,
    json_to_dataframe: Callable[[dict[str, Any]], pl.DataFrame] | None = None,
) -> pl.LazyFrame:
    """
    Load price archive from parquet or JSON.

    Args:
        path: Path to archive file (default: cache dir)
        json_to_dataframe: Callable to convert JSON dict to DataFrame

    Returns:
        LazyFrame with archive data
    """
    cache_dir = constants.CACHE_PATH
    parquet_path = cache_dir / "prices_archive.parquet"
    json_path = cache_dir / "prices.json"

    if path is not None:
        if path.suffix == ".parquet" and path.exists():
            LOGGER.info(f"Loading price archive from {path}")
            return pl.scan_parquet(path)
        elif path.exists():
            return _load_json_archive(path, json_to_dataframe)

    if parquet_path.exists():
        LOGGER.info(f"Loading parquet archive from {parquet_path}")
        return pl.scan_parquet(parquet_path)

    if json_path.exists():
        LOGGER.info(f"Loading JSON archive from {json_path}")
        return _load_json_archive(json_path, json_to_dataframe)

    LOGGER.info("No existing price archive found, starting fresh")
    return pl.DataFrame(schema=PRICE_SCHEMA).lazy()


def _load_json_archive(
    path: Path,
    json_to_dataframe: Callable[[dict[str, Any]], pl.DataFrame] | None = None,
) -> pl.LazyFrame:
    """Load archive from JSON file and convert to LazyFrame."""
    try:
        LOGGER.info(f"Converting JSON archive (orjson): {path}")
        with open(path, "rb") as f:
            data = orjson.loads(f.read())
    except ImportError:
        LOGGER.info(f"Converting JSON archive (json): {path}")
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

    if json_to_dataframe is not None:
        df = json_to_dataframe(data)
    else:
        df = _default_json_to_dataframe(data)
    LOGGER.info(f"Loaded {len(df):,} price records from JSON")
    return df.lazy()


def _default_json_to_dataframe(data: dict[str, Any]) -> pl.DataFrame:
    """Convert nested JSON price dict to flat DataFrame."""
    if "data" in data:
        data = data["data"]

    records = [
        {
            "uuid": uuid,
            "date": date,
            "source": source,
            "provider": provider,
            "price_type": price_type,
            "finish": finish,
            "price": float(price),
            "currency": price_data.get("currency", "USD"),
        }
        for uuid, sources in data.items()
        if isinstance(sources, dict)
        for source, providers in sources.items()
        if isinstance(providers, dict)
        for provider, price_data in providers.items()
        if isinstance(price_data, dict)
        for price_type in ("buylist", "retail")
        for finish, finish_data in price_data.get(price_type, {}).items()
        if isinstance(finish_data, dict)
        for date, price in finish_data.items()
        if price is not None
    ]

    if not records:
        return pl.DataFrame(schema=PRICE_SCHEMA)
    return pl.DataFrame(records, schema=PRICE_SCHEMA)


def save_archive(df: pl.LazyFrame, path: Path | None = None) -> Path:
    """
    Save price archive to parquet with zstd compression.

    Args:
        df: LazyFrame with price data
        path: Output path (default: cache dir)

    Returns:
        Path to saved file
    """
    if path is None:
        cache_dir = constants.CACHE_PATH
        path = cache_dir / "prices_archive.parquet"

    path.parent.mkdir(parents=True, exist_ok=True)

    collected = df.collect()
    collected.write_parquet(path, compression="zstd", compression_level=9)

    LOGGER.info(f"Saved price archive: {len(collected):,} rows, {path.stat().st_size:,} bytes")
    return path


def save_prices_partitioned(df: pl.LazyFrame | pl.DataFrame, today_date: str) -> Path:
    """
    Save today's prices to date-partitioned directory.

    Creates a partition structure like:
        .mtgjson5_cache/prices/date=2024-01-30/data.parquet

    Args:
        df: DataFrame or LazyFrame with today's price data
        today_date: Date string in YYYY-MM-DD format

    Returns:
        Path to the created partition directory
    """
    partition_path = PRICES_PARTITION_DIR / f"date={today_date}"
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / "data.parquet"

    if isinstance(df, pl.LazyFrame):
        df.sink_parquet(output_file, compression="zstd", compression_level=9)
    else:
        df.write_parquet(output_file, compression="zstd", compression_level=9)

    LOGGER.info(f"Saved today's prices to partition: {partition_path}")
    return partition_path


def load_partitioned_archive(days: int = 90) -> pl.LazyFrame:
    """
    Load archive from partitioned directory, lazy streaming.

    Scans all date partitions and returns a LazyFrame.

    Args:
        days: Maximum age of partitions to include (90 default)

    Returns:
        LazyFrame with all price data from partitions
    """
    if not PRICES_PARTITION_DIR.exists():
        LOGGER.info("No partitioned archive found, returning empty LazyFrame")
        return pl.LazyFrame(schema=PRICE_SCHEMA)

    partitions = list(PRICES_PARTITION_DIR.glob("date=*/data.parquet"))

    if not partitions:
        LOGGER.info("No partition files found, returning empty LazyFrame")
        return pl.LazyFrame(schema=PRICE_SCHEMA)

    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    valid_partitions = []
    for p in partitions:
        date_part = p.parent.name
        if date_part.startswith("date="):
            date_str = date_part[5:]
            if date_str >= cutoff:
                valid_partitions.append(p)

    if not valid_partitions:
        LOGGER.info("No valid partitions within retention period")
        return pl.LazyFrame(schema=PRICE_SCHEMA)

    LOGGER.info(f"Loading {len(valid_partitions)} partitions from archive")

    return pl.scan_parquet(valid_partitions)


def prune_partitions(days: int = 90) -> int:
    """
    Delete partition directories older than retention period.

    Args:
        days: Number of days to keep (90 default)

    Returns:
        Number of partitions deleted
    """
    if not PRICES_PARTITION_DIR.exists():
        return 0

    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    deleted_count = 0
    for partition in PRICES_PARTITION_DIR.glob("date=*"):
        if not partition.is_dir():
            continue

        date_str = partition.name.split("=")[1] if "=" in partition.name else ""
        if date_str and date_str < cutoff:
            try:
                shutil.rmtree(partition)
                LOGGER.info(f"Pruned old partition: {partition.name}")
                deleted_count += 1
            except Exception as e:
                LOGGER.warning(f"Failed to prune {partition}: {e}")

    if deleted_count > 0:
        LOGGER.info(f"Pruned {deleted_count} old partitions")

    return deleted_count


def migrate_legacy_archive() -> bool:
    """
    One-time migration from single parquet/JSON to partitioned format.

    Reads legacy archive files and splits them into date-partitioned
    directories. After successful migration, removes the legacy files.

    Returns:
        True if migration occurred, False if no legacy files found
    """
    legacy_parquet = constants.CACHE_PATH / "prices_archive.parquet"
    legacy_s3 = constants.CACHE_PATH / "prices_archive_s3.parquet"

    migrated = False

    for legacy_path in [legacy_parquet, legacy_s3]:
        if not legacy_path.exists():
            continue

        LOGGER.info(f"Migrating legacy archive: {legacy_path}")

        try:
            lf = pl.scan_parquet(legacy_path)
            df = lf.collect()

            if len(df) == 0:
                LOGGER.info(f"Legacy archive is empty, removing: {legacy_path}")
                legacy_path.unlink()
                continue

            dates = df.select("date").unique()["date"].to_list()
            LOGGER.info(f"Migrating {len(df):,} rows across {len(dates)} dates")

            for date_val in dates:
                if not date_val:
                    continue

                date_group = df.filter(pl.col("date") == date_val)
                partition_path = PRICES_PARTITION_DIR / f"date={date_val}"
                partition_path.mkdir(parents=True, exist_ok=True)

                output_file = partition_path / "data.parquet"

                if output_file.exists():
                    existing = pl.read_parquet(output_file)
                    date_group = pl.concat([existing, date_group]).unique(
                        subset=[
                            "uuid",
                            "date",
                            "source",
                            "provider",
                            "price_type",
                            "finish",
                        ]
                    )

                date_group.write_parquet(output_file, compression="zstd", compression_level=9)

            legacy_path.unlink()
            LOGGER.info(f"Migration complete, removed {legacy_path}")
            migrated = True

        except Exception as e:
            LOGGER.error(f"Failed to migrate {legacy_path}: {e}")

    return migrated


def list_local_partitions() -> list[str]:
    """
    List available date partitions locally.

    Returns:
        List of date strings (YYYY-MM-DD) for available partitions
    """
    if not PRICES_PARTITION_DIR.exists():
        return []

    dates = []
    for partition in PRICES_PARTITION_DIR.glob("date=*"):
        if partition.is_dir() and (partition / "data.parquet").exists():
            date_str = partition.name.split("=")[1] if "=" in partition.name else ""
            if date_str:
                dates.append(date_str)

    return sorted(dates)


def load_json_archive_to_parquet(json_path: Path, output_path: Path) -> pl.DataFrame:
    """
    Convert existing JSON price archive to parquet format.

    Utility for migrating from old format to new Polars-based storage.

    Args:
        json_path: Path to AllPrices.json or similar
        output_path: Path for output parquet file

    Returns:
        DataFrame with converted data
    """
    LOGGER.info(f"Converting {json_path} to parquet")

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    if "data" in data:
        data = data["data"]

    records: list[dict[str, Any]] = []

    for uuid, sources in data.items():
        if not isinstance(sources, dict):
            continue
        for source, providers in sources.items():
            if not isinstance(providers, dict):
                continue
            for provider, price_data in providers.items():
                if not isinstance(price_data, dict):
                    continue

                currency = price_data.get("currency", "USD")

                for price_type in ("buylist", "retail"):
                    type_data = price_data.get(price_type, {})
                    if not isinstance(type_data, dict):
                        continue
                    for finish in ("normal", "foil", "etched"):
                        finish_data = type_data.get(finish, {})
                        if not isinstance(finish_data, dict):
                            continue
                        for date, price in finish_data.items():
                            if price is not None:
                                records.append(
                                    {
                                        "uuid": uuid,
                                        "date": date,
                                        "source": source,
                                        "provider": provider,
                                        "price_type": price_type,
                                        "finish": finish,
                                        "price": float(price),
                                        "currency": currency,
                                    }
                                )

    df = pl.DataFrame(records, schema=PRICE_SCHEMA)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path, compression="zstd", compression_level=9)

    LOGGER.info(f"Converted {len(df):,} price records to {output_path}")
    return df
