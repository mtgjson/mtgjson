"""Tests for manifest comparison logic."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys


def _run_compare(
    prev: dict,
    curr: dict,
    tmp_path: pathlib.Path,
    extra_args: list[str] | None = None,
) -> tuple[int, dict]:
    """Helper: write two manifests, run comparison, return (exit_code, report)."""
    prev_path = tmp_path / "previous.json"
    curr_path = tmp_path / "current.json"
    report_path = tmp_path / "report.json"
    prev_path.write_text(json.dumps(prev))
    curr_path.write_text(json.dumps(curr))

    cmd = [
        sys.executable,
        str(pathlib.Path("scripts/compare_manifests.py").resolve()),
        str(prev_path),
        str(curr_path),
        "--output",
        str(report_path),
    ]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    report = json.loads(report_path.read_text())
    return result.returncode, report


def _make_manifest(
    files: dict[str, int],
    record_counts: dict[str, int] | None = None,
    date: str = "2026-04-01",
) -> dict:
    """Build a minimal valid manifest dict."""
    total = sum(files.values())
    return {
        "meta": {
            "version": "5.3.0",
            "date": date,
            "git_commit": "abc1234",
            "generated_at": f"{date}T04:00:00Z",
            "total_files": len(files),
            "total_size_bytes": total,
        },
        "record_counts": record_counts or {},
        "files": {k: {"size_bytes": v} for k, v in files.items()},
    }


class TestIdenticalManifests:
    def test_identical_manifests_pass(self, tmp_path: pathlib.Path):
        m = _make_manifest({"AllPrintings.json": 1000, "10E.json": 500})
        exit_code, report = _run_compare(m, m, tmp_path)
        assert exit_code == 0
        assert report["status"] == "pass"
        assert report["missing_files"] == []
        assert report["new_files"] == []
        assert report["size_changes"] == []


class TestMissingFiles:
    def test_missing_file_is_fail(self, tmp_path: pathlib.Path):
        prev = _make_manifest({"AllPrintings.json": 1000, "OLD.json": 500})
        curr = _make_manifest({"AllPrintings.json": 1000})
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 2
        assert report["status"] == "fail"
        assert len(report["missing_files"]) == 1
        assert report["missing_files"][0]["path"] == "OLD.json"


class TestNewFiles:
    def test_new_file_is_info(self, tmp_path: pathlib.Path):
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 1000, "NEW.json": 500})
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 0
        assert report["status"] == "pass"
        assert len(report["new_files"]) == 1
        assert report["new_files"][0]["path"] == "NEW.json"


class TestSizeChanges:
    def test_small_decrease_warns(self, tmp_path: pathlib.Path):
        """A 5% decrease should warn (default warn threshold is 2%)."""
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 950}, date="2026-04-02")
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 1
        assert report["status"] == "warn"
        assert len(report["size_changes"]) == 1
        assert report["size_changes"][0]["severity"] == "warn"

    def test_large_decrease_fails(self, tmp_path: pathlib.Path):
        """A 15% decrease should fail (default fail threshold is 10%)."""
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 850}, date="2026-04-02")
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 2
        assert report["status"] == "fail"
        assert report["size_changes"][0]["severity"] == "fail"

    def test_size_increase_not_flagged(self, tmp_path: pathlib.Path):
        """Size increases are informational, never warn/fail."""
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 2000}, date="2026-04-02")
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 0
        assert report["status"] == "pass"

    def test_tiny_decrease_ignored(self, tmp_path: pathlib.Path):
        """A 1% decrease is below the 2% warn threshold."""
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 990}, date="2026-04-02")
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 0


class TestRecordCountChanges:
    def test_count_decrease_warns(self, tmp_path: pathlib.Path):
        """A 2% record count decrease should warn (default warn threshold is 1%)."""
        prev = _make_manifest(
            {"AllIdentifiers.json": 1000},
            record_counts={"AllIdentifiers": 100000},
        )
        curr = _make_manifest(
            {"AllIdentifiers.json": 980},
            record_counts={"AllIdentifiers": 98000},
            date="2026-04-02",
        )
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 1
        assert len(report["record_count_changes"]) == 1
        assert report["record_count_changes"][0]["severity"] == "warn"

    def test_count_increase_not_flagged(self, tmp_path: pathlib.Path):
        """Record count increases are informational."""
        prev = _make_manifest(
            {"AllIdentifiers.json": 1000},
            record_counts={"AllIdentifiers": 100000},
        )
        curr = _make_manifest(
            {"AllIdentifiers.json": 1100},
            record_counts={"AllIdentifiers": 105000},
            date="2026-04-02",
        )
        exit_code, report = _run_compare(prev, curr, tmp_path)
        assert exit_code == 0


class TestCustomThresholds:
    def test_custom_size_warn_threshold(self, tmp_path: pathlib.Path):
        """A 3% decrease with --size-warn-pct 5 should pass."""
        prev = _make_manifest({"AllPrintings.json": 1000})
        curr = _make_manifest({"AllPrintings.json": 970}, date="2026-04-02")
        exit_code, report = _run_compare(
            prev, curr, tmp_path, extra_args=["--size-warn-pct", "5"]
        )
        assert exit_code == 0


class TestPreviousManifestMissing:
    def test_missing_previous_passes(self, tmp_path: pathlib.Path):
        """When previous manifest doesn't exist, report pass with a note."""
        curr_path = tmp_path / "current.json"
        report_path = tmp_path / "report.json"
        curr = _make_manifest({"AllPrintings.json": 1000})
        curr_path.write_text(json.dumps(curr))

        cmd = [
            sys.executable,
            str(pathlib.Path("scripts/compare_manifests.py").resolve()),
            str(tmp_path / "nonexistent.json"),
            str(curr_path),
            "--output",
            str(report_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        report = json.loads(report_path.read_text())
        assert result.returncode == 0
        assert report["status"] == "pass"
        assert "first_build" in report or "note" in report
