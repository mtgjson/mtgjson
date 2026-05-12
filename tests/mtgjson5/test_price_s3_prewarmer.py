"""Tests for S3PartitionPrewarmer: background S3 partition sync thread.

All tests monkeypatch `sync_missing_partitions_from_s3` so no real boto3 or
S3 calls happen. Designed to run sub-second.
"""

from __future__ import annotations

import threading

import pytest

from mtgjson5.build.prices import price_s3
from mtgjson5.build.prices.price_s3 import S3PartitionPrewarmer


def _block_in_sync(started: threading.Event, release: threading.Event):
    """Build a fake sync that signals when it starts and waits to be released."""

    def fake_sync(days: int = 90) -> int:
        started.set()
        release.wait(timeout=5.0)
        return 0

    return fake_sync


def test_start_background_returns_started_daemon_thread(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(
        price_s3,
        "sync_missing_partitions_from_s3",
        _block_in_sync(started, release),
    )

    prewarmer = S3PartitionPrewarmer.start_background()
    try:
        assert started.wait(timeout=2.0), "worker thread never started"
        assert prewarmer._thread is not None
        assert prewarmer._thread.daemon is True
        assert prewarmer._thread.is_alive()
    finally:
        release.set()
        prewarmer.wait(timeout=2.0)


def test_wait_blocks_until_done(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(
        price_s3,
        "sync_missing_partitions_from_s3",
        _block_in_sync(started, release),
    )

    prewarmer = S3PartitionPrewarmer.start_background()
    try:
        assert started.wait(timeout=2.0), "worker thread never started"
        # Worker is confirmed blocked inside fake_sync; wait must not be done.
        assert prewarmer.wait(timeout=0.0) is False
        release.set()
        # Worker finishes: wait returns True
        assert prewarmer.wait(timeout=2.0) is True
    finally:
        release.set()
        prewarmer.wait(timeout=2.0)


def test_run_invokes_sync_with_configured_days(monkeypatch):
    captured: list[int] = []

    def fake_sync(days: int = 90) -> int:
        captured.append(days)
        return 0

    monkeypatch.setattr(price_s3, "sync_missing_partitions_from_s3", fake_sync)

    # Default days=90
    p1 = S3PartitionPrewarmer.start_background()
    assert p1.wait(timeout=2.0) is True

    # Override days=30
    p2 = S3PartitionPrewarmer.start_background(days=30)
    assert p2.wait(timeout=2.0) is True

    assert captured == [90, 30]
