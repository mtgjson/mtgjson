"""Tests for the weekly edhrecRank snapshot (#1322)."""

from __future__ import annotations

import datetime
import shutil
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from mtgjson5.providers import edhrec_ranks

TODAY = datetime.date(2026, 11, 8)


def _cards(ranks: dict[str, int | None]) -> pl.LazyFrame:
    """Scryfall-shaped cards: two printings per oracle id, plus a reversible card."""
    rows = []
    for oracle_id, rank in ranks.items():
        rows += [
            {"id": f"{oracle_id}-a", "oracle_id": oracle_id, "edhrec_rank": rank, "card_faces": None},
            {"id": f"{oracle_id}-b", "oracle_id": oracle_id, "edhrec_rank": rank, "card_faces": None},
        ]
    return pl.LazyFrame(
        rows,
        schema={
            "id": pl.String,
            "oracle_id": pl.String,
            "edhrec_rank": pl.Int64,
            "card_faces": pl.List(pl.Struct({"oracle_id": pl.String})),
        },
    )


def _ranks_by_id(lf: pl.LazyFrame) -> dict[str, int | None]:
    return dict(lf.select("id", "edhrec_rank").collect().iter_rows())


class FakeBucket:
    """Stands in for S3 so a snapshot can outlive the 'container' (tmp dir) that made it."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configured: bool = True):
        self.store = tmp_path / "bucket"
        self.store.mkdir()
        self.uploads = 0
        bucket = self

        class Handler:
            def download_file(self, _bucket: str, _object_path: str, local_path: str) -> bool:
                archived = bucket.store / "snapshot.parquet"
                if not archived.exists():
                    return False
                shutil.copyfile(archived, local_path)
                return True

            def upload_file_with_retry(self, local_path: str, _bucket: str, _object_path: str) -> bool:
                bucket.uploads += 1
                shutil.copyfile(local_path, bucket.store / "snapshot.parquet")
                return True

        monkeypatch.setattr(edhrec_ranks, "MtgjsonS3Handler", Handler)
        monkeypatch.setattr(
            edhrec_ranks,
            "rank_snapshot_location",
            lambda: ("mtgjson-test", "edhrec/snapshot.parquet") if configured else None,
        )


@pytest.fixture(autouse=True)
def _default_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(edhrec_ranks, "rank_refresh_days", lambda: 7)


def _build(cards: pl.LazyFrame, container: Path, today: datetime.date) -> dict[str, int | None]:
    """One build in a fresh container: resolve the snapshot and apply it."""
    container.mkdir(exist_ok=True)
    live = edhrec_ranks.live_ranks(cards)
    snapshot = edhrec_ranks.resolve_snapshot(live, container / "snap.parquet", today=today)
    return _ranks_by_id(edhrec_ranks.apply_snapshot(cards, snapshot))


class TestWeeklyRanks:
    def test_ranks_hold_steady_inside_the_refresh_window(self, tmp_path, monkeypatch):
        bucket = FakeBucket(tmp_path, monkeypatch)
        first = _build(_cards({"sol": 1, "bolt": 158}), tmp_path / "day0", TODAY)
        later = _build(_cards({"sol": 2, "bolt": 160}), tmp_path / "day3", TODAY + datetime.timedelta(days=3))

        assert first == later == {"sol-a": 1, "sol-b": 1, "bolt-a": 158, "bolt-b": 158}
        assert bucket.uploads == 1

    def test_ranks_refresh_once_the_snapshot_is_a_week_old(self, tmp_path, monkeypatch):
        bucket = FakeBucket(tmp_path, monkeypatch)
        _build(_cards({"sol": 1}), tmp_path / "day0", TODAY)
        refreshed = _build(_cards({"sol": 2}), tmp_path / "day7", TODAY + datetime.timedelta(days=7))

        assert refreshed == {"sol-a": 2, "sol-b": 2}
        assert bucket.uploads == 2

    def test_new_cards_take_the_live_rank(self, tmp_path, monkeypatch):
        FakeBucket(tmp_path, monkeypatch)
        _build(_cards({"sol": 1}), tmp_path / "day0", TODAY)
        ranks = _build(_cards({"sol": 5, "spoiler": 900}), tmp_path / "day1", TODAY + datetime.timedelta(days=1))

        assert ranks == {"sol-a": 1, "sol-b": 1, "spoiler-a": 900, "spoiler-b": 900}

    def test_unranked_cards_stay_unranked_until_refresh(self, tmp_path, monkeypatch):
        """Gaining a rank mid-week would be churn too; it waits for the next snapshot."""
        FakeBucket(tmp_path, monkeypatch)
        _build(_cards({"sol": 1, "obscure": None}), tmp_path / "day0", TODAY)
        ranks = _build(_cards({"sol": 1, "obscure": 30000}), tmp_path / "day1", TODAY + datetime.timedelta(days=1))

        assert ranks["obscure-a"] is None

    def test_without_a_bucket_a_fresh_container_uses_live_ranks(self, tmp_path, monkeypatch):
        FakeBucket(tmp_path, monkeypatch, configured=False)
        _build(_cards({"sol": 1}), tmp_path / "day0", TODAY)
        ranks = _build(_cards({"sol": 2}), tmp_path / "day1", TODAY + datetime.timedelta(days=1))

        assert ranks == {"sol-a": 2, "sol-b": 2}

    def test_without_a_bucket_a_persistent_cache_still_holds_ranks(self, tmp_path, monkeypatch):
        FakeBucket(tmp_path, monkeypatch, configured=False)
        _build(_cards({"sol": 1}), tmp_path / "cache", TODAY)
        ranks = _build(_cards({"sol": 2}), tmp_path / "cache", TODAY + datetime.timedelta(days=1))

        assert ranks == {"sol-a": 1, "sol-b": 1}

    def test_empty_scryfall_data_keeps_the_existing_snapshot(self, tmp_path, monkeypatch):
        bucket = FakeBucket(tmp_path, monkeypatch)
        _build(_cards({"sol": 1}), tmp_path / "day0", TODAY)
        snapshot = edhrec_ranks.resolve_snapshot(
            edhrec_ranks.live_ranks(_cards({})),
            tmp_path / "day9.parquet",
            today=TODAY + datetime.timedelta(days=9),
        )

        assert snapshot is not None
        assert snapshot["edhrec_rank"].to_list() == [1]
        assert bucket.uploads == 1

    def test_reversible_cards_key_on_their_face_oracle_id(self, tmp_path, monkeypatch):
        FakeBucket(tmp_path, monkeypatch)
        reversible = pl.LazyFrame(
            [{"id": "rev", "oracle_id": None, "edhrec_rank": 40, "card_faces": [{"oracle_id": "sol"}]}],
            schema=_cards({}).collect_schema(),
        )
        _build(pl.concat([_cards({"sol": 1}), reversible]), tmp_path / "day0", TODAY)
        reversible = reversible.with_columns(pl.lit(41, dtype=pl.Int64).alias("edhrec_rank"))
        ranks = _build(
            pl.concat([_cards({"sol": 2}), reversible]), tmp_path / "day1", TODAY + datetime.timedelta(days=1)
        )

        assert ranks["rev"] == 1

    def test_card_order_is_preserved(self):
        cards = _cards({"z": 1, "a": 2, "m": 3})
        snapshot = edhrec_ranks.live_ranks(cards).with_columns(pl.lit(TODAY).alias("takenOn"))
        applied = edhrec_ranks.apply_snapshot(cards, snapshot)

        assert applied.select("id").collect()["id"].to_list() == cards.select("id").collect()["id"].to_list()
        assert applied.collect_schema() == cards.collect_schema()


class TestRefreshDaysConfig:
    @pytest.mark.parametrize(("raw", "expected"), [("", 7), ("3", 3), ("weekly", 7)])
    def test_reads_refresh_days(self, monkeypatch, raw, expected):
        monkeypatch.undo()
        monkeypatch.setattr(
            edhrec_ranks,
            "MtgjsonConfig",
            lambda: SimpleNamespace(get=lambda section, option, fallback="": raw or fallback),
        )
        assert edhrec_ranks.rank_refresh_days() == expected
