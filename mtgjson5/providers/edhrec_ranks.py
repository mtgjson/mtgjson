"""Hold edhrecRank steady between weekly refreshes.

Scryfall reshuffles edhrec_rank every day, and since rank sits on every card,
that one field touches roughly 80% of set files on a night when nothing else
changed. Consumers who sync by file hash end up re-downloading nearly
everything for rank noise (#1322).

Instead, the ranks are snapshotted by oracle id each Monday and reused through
Sunday. A snapshot older than the latest Monday is replaced by the next build,
so a failed Monday build refreshes on Tuesday. Cards whose oracle id isn't in
the snapshot (new cards) take Scryfall's live rank, so new spoilers aren't
left without one.

Production builds start from a fresh container, so the snapshot only survives
between builds if it's archived in S3: set ``rank_snapshot_bucket_name`` under
[EDHRec]. Without it, the snapshot lives in the local cache directory, which
on a fresh container means a new snapshot every build - the old behavior.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

import polars as pl

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler

LOGGER = logging.getLogger(__name__)

SNAPSHOT_OBJECT_PATH = "edhrec/edhrec_rank_snapshot.parquet"
REFRESH_WEEKDAY = 0  # Monday

_KEY = "_rank_oracle_id"


def rank_snapshot_location() -> tuple[str, str] | None:
    """Return (bucket, object path) for the snapshot archive, or None if unset."""
    config = MtgjsonConfig()
    if not config.has_section("EDHRec"):
        return None

    bucket_name = config.get("EDHRec", "rank_snapshot_bucket_name")
    if not bucket_name:
        return None

    return bucket_name, config.get("EDHRec", "rank_snapshot_object_path", fallback=SNAPSHOT_OBJECT_PATH)


def last_refresh_day(today: datetime.date) -> datetime.date:
    """The most recent refresh weekday on or before ``today``."""
    return today - datetime.timedelta(days=(today.weekday() - REFRESH_WEEKDAY) % 7)


def _oracle_key(schema: pl.Schema) -> pl.Expr:
    """Oracle id for a Scryfall card, reaching into faces for reversible cards."""
    if "card_faces" in schema:
        return pl.coalesce(
            pl.col("oracle_id"),
            pl.col("card_faces").list.first().struct.field("oracle_id"),
        ).alias(_KEY)
    return pl.col("oracle_id").alias(_KEY)


def live_ranks(cards_lf: pl.LazyFrame) -> pl.DataFrame:
    """One rank per oracle id from today's Scryfall cards (null when unranked)."""
    return (
        cards_lf.select(_oracle_key(cards_lf.collect_schema()), pl.col("edhrec_rank"))
        .filter(pl.col(_KEY).is_not_null())
        .group_by(_KEY)
        .agg(pl.col("edhrec_rank").min())
        .sort(_KEY)
        .collect()
    )


def _read_snapshot(path: Path) -> pl.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pl.read_parquet(path)
    except Exception as e:
        LOGGER.warning(f"Unreadable EDHREC rank snapshot at {path}: {e}")
        return None


def _taken_on(snapshot: pl.DataFrame) -> datetime.date | None:
    if snapshot.height == 0 or "takenOn" not in snapshot.columns:
        return None
    taken_on = snapshot["takenOn"][0]
    return taken_on if isinstance(taken_on, datetime.date) else None


def resolve_snapshot(
    live: pl.DataFrame,
    local_path: Path,
    today: datetime.date | None = None,
) -> pl.DataFrame | None:
    """Return the snapshot to build with, taking a new one when it's due.

    Never raises: a build without a usable snapshot falls back to live ranks.
    """
    today = today or datetime.date.today()
    location = rank_snapshot_location()

    try:
        if location is not None:
            bucket_name, object_path = location
            MtgjsonS3Handler().download_file(bucket_name, object_path, str(local_path))

        snapshot = _read_snapshot(local_path)
        taken_on = _taken_on(snapshot) if snapshot is not None else None
        if snapshot is not None and taken_on is not None and last_refresh_day(today) <= taken_on <= today:
            LOGGER.info(f"Reusing EDHREC rank snapshot taken {taken_on} ({snapshot.height:,} oracle ids)")
            return snapshot

        if live.height == 0:
            # Nothing to snapshot (Scryfall gave us no cards); keep what we had.
            return snapshot

        snapshot = live.with_columns(pl.lit(today).alias("takenOn"))
        snapshot.write_parquet(local_path)
        LOGGER.info(f"Took a new EDHREC rank snapshot ({snapshot.height:,} oracle ids)")

        if location is not None:
            bucket_name, object_path = location
            if not MtgjsonS3Handler().upload_file_with_retry(str(local_path), bucket_name, object_path):
                LOGGER.error("Could not archive the EDHREC rank snapshot; tomorrow's build will take a new one")
        return snapshot
    except Exception as e:
        LOGGER.error(f"EDHREC rank snapshot unavailable, using live ranks: {e}")
        return None


def apply_snapshot(cards_lf: pl.LazyFrame, snapshot: pl.DataFrame | None) -> pl.LazyFrame:
    """Replace edhrec_rank with the snapshot's value for every oracle id it covers.

    A card the snapshot has as unranked stays unranked until the next refresh;
    only oracle ids missing from the snapshot entirely take the live value.
    """
    if snapshot is None or snapshot.height == 0:
        return cards_lf

    frozen = snapshot.select(
        pl.col(_KEY),
        pl.col("edhrec_rank").alias("_snapshot_rank"),
        pl.lit(True).alias("_in_snapshot"),
    ).lazy()

    return (
        cards_lf.with_columns(_oracle_key(cards_lf.collect_schema()))
        .join(frozen, on=_KEY, how="left", maintain_order="left")
        .with_columns(
            pl.when(pl.col("_in_snapshot"))
            .then(pl.col("_snapshot_rank"))
            .otherwise(pl.col("edhrec_rank"))
            .cast(cards_lf.collect_schema()["edhrec_rank"])
            .alias("edhrec_rank")
        )
        .drop(_KEY, "_snapshot_rank", "_in_snapshot")
    )
