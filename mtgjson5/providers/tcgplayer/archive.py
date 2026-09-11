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
from datetime import UTC, datetime
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
    """Archive a freshly fetched catalog. Failing to archive never fails a build.

    Reading the config counts as part of the attempt: this runs after the parts
    have been combined and discarded, so anything raising out of here would turn
    a catalog MTGJSON already has into a failed build.
    """
    try:
        location = catalog_archive_location()
        if location is None:
            LOGGER.debug("No TCGPlayer catalog archive configured, skipping upload")
            return False

        bucket_name, object_path = location
        # Retry: a single blip here costs tomorrow's build its one fallback that
        # still carries product names.
        uploaded = MtgjsonS3Handler().upload_file_with_retry(str(catalog_path), bucket_name, object_path)
    except Exception as e:
        LOGGER.error(f"Could not archive the TCGPlayer catalog: {e}")
        return False

    if uploaded:
        LOGGER.info(f"Archived the TCGPlayer catalog to s3://{bucket_name}/{object_path}")
    return uploaded


def download_catalog(destination: Path, max_age_hours: float | None = None) -> bool:
    """Restore the archived catalog to ``destination``.

    ``max_age_hours`` bounds how old the object may be. Nothing downstream can
    work that out on its own: the restored file is stamped with the time it was
    downloaded, so an archive left behind by a build that stopped uploading
    weeks ago would read as today's catalog and become the regression baseline.
    """
    try:
        location = catalog_archive_location()
        if location is None:
            return False

        bucket_name, object_path = location
        handler = MtgjsonS3Handler()
        if max_age_hours is not None and not _recent_enough(handler, bucket_name, object_path, max_age_hours):
            return False

        LOGGER.info(f"Restoring the archived TCGPlayer catalog from s3://{bucket_name}/{object_path}")
        return handler.download_file(bucket_name, object_path, str(destination))
    except Exception as e:
        LOGGER.error(f"Could not restore the archived TCGPlayer catalog: {e}")
        return False


def _recent_enough(handler: MtgjsonS3Handler, bucket_name: str, object_path: str, max_age_hours: float) -> bool:
    """Return True if the archived object is recent enough to fall back to."""
    last_modified = handler.object_last_modified(bucket_name, object_path)
    if last_modified is None:
        return False

    age_hours = (datetime.now(UTC) - last_modified).total_seconds() / 3600
    if age_hours >= max_age_hours:
        LOGGER.warning(f"The archived TCGPlayer catalog is {age_hours:.0f}h old, too stale to fall back to")
        return False
    return True
