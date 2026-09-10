"""Keep the last good TCGPlayer catalog in S3, where a fresh container can get it.

MTGJSON's published TcgplayerSkus.json is enough to put every SKU back after a
failed fetch, but it carries no product names, so alternative-foil detection -
which matches on parenthesized name suffixes - finds nothing on a night that
falls back to it. Archiving the catalog parquet itself keeps names, group IDs,
and URLs, so a fallback build looks like an ordinary one apart from its age.

The archive is off until ``catalog_bucket_name`` is set under [TCGPlayer];
without it the published file remains the fallback.
"""

from __future__ import annotations

import logging
from pathlib import Path

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler

LOGGER = logging.getLogger(__name__)

CATALOG_OBJECT_PATH = "tcg_catalog/tcg_skus.parquet"


def catalog_archive_location() -> tuple[str, str] | None:
    """Return (bucket, object path) for the catalog archive, or None if unset."""
    config = MtgjsonConfig()
    if not config.has_section("TCGPlayer"):
        return None

    bucket_name = config.get("TCGPlayer", "catalog_bucket_name")
    if not bucket_name:
        return None

    return bucket_name, config.get("TCGPlayer", "catalog_object_path", fallback=CATALOG_OBJECT_PATH)


def upload_catalog(catalog_path: Path) -> bool:
    """Archive a freshly fetched catalog. Failing to archive never fails a build."""
    location = catalog_archive_location()
    if location is None:
        LOGGER.debug("No TCGPlayer catalog archive configured, skipping upload")
        return False

    bucket_name, object_path = location
    try:
        uploaded = MtgjsonS3Handler().upload_file(str(catalog_path), bucket_name, object_path)
    except Exception as e:
        LOGGER.error(f"Could not archive the TCGPlayer catalog: {e}")
        return False

    if uploaded:
        LOGGER.info(f"Archived the TCGPlayer catalog to s3://{bucket_name}/{object_path}")
    return uploaded


def download_catalog(destination: Path) -> bool:
    """Restore the archived catalog to ``destination``."""
    location = catalog_archive_location()
    if location is None:
        return False

    bucket_name, object_path = location
    LOGGER.info(f"Restoring the archived TCGPlayer catalog from s3://{bucket_name}/{object_path}")
    try:
        return MtgjsonS3Handler().download_file(bucket_name, object_path, str(destination))
    except Exception as e:
        LOGGER.error(f"Could not restore the archived TCGPlayer catalog: {e}")
        return False
