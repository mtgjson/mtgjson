"""
S3 sync operations for the price archive.
"""

from __future__ import annotations

import datetime
import json
import logging
import lzma
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import polars as pl

from mtgjson5 import constants
from mtgjson5.build.price_archive import PRICES_PARTITION_DIR, list_local_partitions
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler

LOGGER = logging.getLogger(__name__)


def _get_s3_config() -> tuple[str, str] | None:
    """
    Get S3 bucket configuration for prices.

    Returns:
        Tuple of (bucket_name, base_path) or None if not configured
    """
    if not MtgjsonConfig().has_section("Prices"):
        return None

    bucket_name = MtgjsonConfig().get("Prices", "bucket_name")

    return bucket_name, "price_archive"


def sync_partition_to_s3(date: str) -> bool:
    """
    Upload a single date partition to S3.

    Args:
        date: Date string in YYYY-MM-DD format

    Returns:
        True if upload succeeded
    """
    config = _get_s3_config()
    if config is None:
        LOGGER.debug("No S3 config, skipping partition upload")
        return False

    bucket_name, base_path = config
    local_path = PRICES_PARTITION_DIR / f"date={date}" / "data.parquet"

    if not local_path.exists():
        LOGGER.warning(f"Local partition not found: {local_path}")
        return False

    s3_path = f"{base_path}/date={date}/data.parquet"

    return MtgjsonS3Handler().upload_file(str(local_path), bucket_name, s3_path)


def sync_partition_from_s3(date: str) -> bool:
    """
    Download a single date partition from S3.

    Args:
        date: Date string in YYYY-MM-DD format

    Returns:
        True if download succeeded
    """
    config = _get_s3_config()
    if config is None:
        return False

    bucket_name, base_path = config
    local_path = PRICES_PARTITION_DIR / f"date={date}" / "data.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    s3_path = f"{base_path}/date={date}/data.parquet"

    return MtgjsonS3Handler().download_file(bucket_name, s3_path, str(local_path))


def list_s3_partitions() -> list[str]:
    """
    List available date partitions on S3.

    Returns:
        List of date strings (YYYY-MM-DD) for available partitions
    """
    config = _get_s3_config()
    if config is None:
        return []

    bucket_name, base_path = config

    try:
        import boto3

        s3 = boto3.client("s3")
        prefix = f"{base_path}/date="

        dates = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
            # CommonPrefixes contains the "folders" (date=YYYY-MM-DD/)
            for prefix_obj in page.get("CommonPrefixes", []):
                folder = prefix_obj.get("Prefix", "")
                # Extract date from path like "price_archive/date=2024-01-30/"
                if "date=" in folder:
                    date_part = folder.split("date=")[1].rstrip("/")
                    if date_part:
                        dates.append(date_part)

        return sorted(dates)

    except Exception as e:
        LOGGER.warning(f"Failed to list S3 partitions: {e}")
        return []


def sync_missing_partitions_from_s3(days: int = 90) -> int:
    """
    Download partitions from S3 that we don't have locally.

    Only downloads partitions within the retention period.

    Args:
        days: Maximum age of partitions to sync (90 default)

    Returns:
        Number of partitions downloaded
    """
    config = _get_s3_config()
    if config is None:
        LOGGER.info("No S3 config, skipping partition sync")
        return 0

    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    s3_partitions = set(list_s3_partitions())
    local_parts = set(list_local_partitions())

    # Filter to only include partitions within retention period
    s3_partitions = {d for d in s3_partitions if d >= cutoff}

    missing = s3_partitions - local_parts

    if not missing:
        LOGGER.info("All S3 partitions are synced locally")
        return 0

    LOGGER.info(f"Downloading {len(missing)} missing partitions from S3")

    downloaded = 0
    for date in sorted(missing):
        if sync_partition_from_s3(date):
            downloaded += 1
            LOGGER.info(f"Downloaded partition: date={date}")

    return downloaded


def sync_partition_to_s3_with_retry(date: str, max_retries: int = 3, base_delay: float = 1.0) -> bool:
    """
    Upload a single date partition to S3 with retry logic.

    Args:
        date: Date string in YYYY-MM-DD format
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.0)

    Returns:
        True if upload succeeded
    """
    for attempt in range(max_retries + 1):
        if sync_partition_to_s3(date):
            return True

        if attempt < max_retries:
            delay = base_delay * (2**attempt)
            LOGGER.warning(f"Retry {attempt + 1}/{max_retries} for partition {date} after {delay}s delay")
            time.sleep(delay)

    LOGGER.error(f"Failed to upload partition {date} after {max_retries + 1} attempts")
    return False


def sync_local_partitions_to_s3(days: int = 90, max_workers: int = 16, max_retries: int = 3) -> int:
    """
    Upload local partitions to S3 that S3 doesn't have.

    Used after migration to push the converted partitions to S3,
    making S3 the authoritative hive.

    Args:
        days: Maximum age of partitions to sync (default 90)
        max_workers: Maximum number of concurrent uploads (default: 16)
        max_retries: Maximum number of retry attempts per partition (default: 3)

    Returns:
        Number of partitions uploaded

    Raises:
        RuntimeError: If any partitions fail to upload after retries
    """
    config = _get_s3_config()
    if config is None:
        LOGGER.info("No S3 config, skipping partition upload")
        return 0

    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

    s3_partitions = set(list_s3_partitions())
    local_parts = set(list_local_partitions())

    # Filter to only include partitions within retention period
    local_parts = {d for d in local_parts if d >= cutoff}

    missing_on_s3 = local_parts - s3_partitions

    if not missing_on_s3:
        LOGGER.info("All local partitions are synced to S3")
        return 0

    total = len(missing_on_s3)
    LOGGER.info(f"Uploading {total} local partitions to S3 with {max_workers} workers")

    uploaded = 0
    failed_partitions: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(sync_partition_to_s3_with_retry, date, max_retries): date for date in missing_on_s3}

        for future in as_completed(futures):
            date = futures[future]
            try:
                if future.result():
                    uploaded += 1
                else:
                    failed_partitions.append(date)
            except Exception as e:
                LOGGER.error(f"Unexpected error uploading partition {date}: {e}")
                failed_partitions.append(date)

            completed = uploaded + len(failed_partitions)
            if completed % 10 == 0:
                LOGGER.info(f"  Progress: {completed}/{total} partitions processed...")

    if failed_partitions:
        raise RuntimeError(
            f"Upload incomplete: {len(failed_partitions)}/{total} partitions failed "
            f"after retries: {sorted(failed_partitions)}"
        )

    LOGGER.info(f"Uploaded {uploaded} partitions to S3")
    return uploaded


def get_price_archive_from_s3(
    load_archive_fn: Callable[..., pl.LazyFrame] | None = None,
    json_to_dataframe_fn: Callable[[dict[str, Any]], pl.DataFrame] | None = None,
) -> pl.LazyFrame:
    """
    Download price archive from S3 and convert to LazyFrame.

    Tries Parquet format first, falls back to legacy JSON format.
    Falls back to local archive if S3 config is missing or download fails.

    Args:
        load_archive_fn: Callable to load local archive (fallback)
        json_to_dataframe_fn: Callable to convert JSON dict to DataFrame

    Returns:
        LazyFrame with archive data
    """
    from mtgjson5.build.price_archive import load_archive

    _load_archive = load_archive_fn or load_archive

    if not MtgjsonConfig().has_section("Prices"):
        LOGGER.info("No S3 config, using local archive only")
        return _load_archive()

    bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
    bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

    constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)

    parquet_path = bucket_object_path.replace(".json.xz", ".parquet")
    local_parquet = constants.CACHE_PATH / "prices_archive_s3.parquet"

    LOGGER.info(f"Trying Parquet archive from S3: {parquet_path}")
    if MtgjsonS3Handler().download_file(bucket_name, parquet_path, str(local_parquet)):
        try:
            lf = pl.scan_parquet(local_parquet)
            row_count = lf.select(pl.len()).collect().item()
            LOGGER.info(f"Loaded {row_count:,} price records from S3 Parquet archive")
            return lf
        except Exception as e:
            LOGGER.warning(f"Failed to read Parquet archive: {e}")
            if local_parquet.exists():
                local_parquet.unlink()

    LOGGER.info(f"Trying legacy JSON archive from S3: {bucket_object_path}")
    temp_file = constants.CACHE_PATH / "temp_prices.json.xz"

    if not MtgjsonS3Handler().download_file(bucket_name, bucket_object_path, str(temp_file)):
        LOGGER.warning("S3 download failed, using local archive")
        return _load_archive()

    try:
        LOGGER.info("Decompressing JSON archive...")
        with lzma.open(temp_file) as f:
            data = json.load(f)
        temp_file.unlink()

        LOGGER.info("Converting JSON to DataFrame...")
        if json_to_dataframe_fn is not None:
            df = json_to_dataframe_fn(data.get("data", data))
        else:
            from mtgjson5.build.price_archive import _default_json_to_dataframe

            df = _default_json_to_dataframe(data.get("data", data))
        LOGGER.info(f"Loaded {len(df):,} price records from S3 JSON archive")
        return df.lazy()
    except Exception as e:
        LOGGER.error(f"Failed to process S3 archive: {e}")
        if temp_file.exists():
            temp_file.unlink()
        return _load_archive()


def upload_archive_to_s3(
    archive_data: dict[str, Any] | pl.LazyFrame | pl.DataFrame,
    json_to_dataframe_fn: Callable[[dict[str, Any]], pl.DataFrame] | None = None,
) -> None:
    """
    Upload price archive to S3 in Parquet format.

    Args:
        archive_data: Price data as dict, DataFrame, or LazyFrame
        json_to_dataframe_fn: Callable to convert JSON dict to DataFrame
    """
    if not MtgjsonConfig().has_section("Prices"):
        LOGGER.info("No S3 config, skipping upload")
        return

    bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
    bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

    parquet_path = bucket_object_path.replace(".json.xz", ".parquet")
    local_parquet = constants.CACHE_PATH / "prices_archive_upload.parquet"
    local_parquet.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(archive_data, dict):
        LOGGER.info("Converting dict to DataFrame for upload...")
        if json_to_dataframe_fn is not None:
            df = json_to_dataframe_fn(archive_data)
        else:
            from mtgjson5.build.price_archive import _default_json_to_dataframe

            df = _default_json_to_dataframe(archive_data)
    elif isinstance(archive_data, pl.LazyFrame):
        df = archive_data.collect()
    else:
        df = archive_data

    LOGGER.info(f"Writing {len(df):,} rows to Parquet...")
    df.write_parquet(local_parquet, compression="zstd", compression_level=9)
    LOGGER.info(f"Parquet archive: {local_parquet.stat().st_size / 1024 / 1024:.1f} MB")

    LOGGER.info(f"Uploading to S3: {parquet_path}")
    MtgjsonS3Handler().upload_file(str(local_parquet), bucket_name, parquet_path)
    LOGGER.info("S3 upload complete")

    final_path = constants.CACHE_PATH / "prices_archive.parquet"
    if local_parquet != final_path:
        local_parquet.rename(final_path)
