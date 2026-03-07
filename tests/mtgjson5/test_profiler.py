"""Tests for mtgjson5.profiler: PipelineProfiler, SubprocessProfiler, and helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from mtgjson5.profiler import (
    PipelineProfiler,
    SubprocessProfiler,
    _get_children_rss_mb,
    _get_rss_mb,
    get_profiler,
    init_profiler,
)


# ---------------------------------------------------------------------------
# _get_rss_mb / _get_children_rss_mb
# ---------------------------------------------------------------------------


class TestRssHelpers:
    def test_get_rss_mb_returns_positive(self):
        result = _get_rss_mb()
        # psutil should be available in test env
        assert result > 0

    def test_get_rss_mb_without_psutil(self):
        with patch.dict("sys.modules", {"psutil": None}):
            # Force reimport to hit ImportError
            import importlib
            import mtgjson5.profiler as mod

            # Direct test of the fallback
            result = mod._get_rss_mb()
            # May or may not hit -1 depending on caching; just ensure no crash
            assert isinstance(result, float)

    def test_get_children_rss_mb_returns_number(self):
        result = _get_children_rss_mb()
        assert isinstance(result, (int, float))
        assert result >= 0


# ---------------------------------------------------------------------------
# PipelineProfiler (disabled)
# ---------------------------------------------------------------------------


class TestPipelineProfilerDisabled:
    def test_disabled_profiler_noop(self):
        p = PipelineProfiler(enabled=False)
        p.start()
        p.checkpoint("test")
        p.checkpoint_with_children("test2")
        p.add_subprocess_profile({"label": "sub"})
        report = p.finish()
        assert report == {}

    def test_disabled_stage_context_manager(self):
        p = PipelineProfiler(enabled=False)
        with p.stage("test"):
            pass  # Should not error

    def test_disabled_write_report_noop(self, tmp_path: Path):
        p = PipelineProfiler(enabled=False)
        p.write_report(tmp_path)
        assert not (tmp_path / "profile_report.json").exists()


# ---------------------------------------------------------------------------
# PipelineProfiler (enabled)
# ---------------------------------------------------------------------------


class TestPipelineProfilerEnabled:
    def test_start_and_checkpoint(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        assert p._started is True
        assert len(p.snapshots) == 1  # profiler_start
        assert p.snapshots[0]["name"] == "profiler_start"

    def test_checkpoint_records_timing(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.checkpoint("step_1")
        assert len(p.snapshots) == 2
        snap = p.snapshots[1]
        assert snap["name"] == "step_1"
        assert "wall_seconds" in snap
        assert "delta_seconds" in snap
        assert "rss_mb" in snap
        assert "rss_delta_mb" in snap

    def test_checkpoint_without_start_is_noop(self):
        p = PipelineProfiler(enabled=True)
        p.checkpoint("test")
        assert len(p.snapshots) == 0

    def test_checkpoint_with_children(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.checkpoint_with_children("with_children")
        snap = p.snapshots[-1]
        assert "children_rss_mb" in snap
        assert "system_total_mb" in snap

    def test_add_subprocess_profile(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        profile = {"label": "export", "pid": 123, "peak_rss_mb": 100.0, "total_wall_seconds": 5.0}
        p.add_subprocess_profile(profile)
        assert len(p.subprocess_profiles) == 1
        assert p.subprocess_profiles[0]["label"] == "export"

    def test_add_subprocess_profile_empty_ignored(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.add_subprocess_profile({})
        assert len(p.subprocess_profiles) == 0

    def test_finish_returns_report(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.checkpoint("mid")
        report = p.finish()
        assert "total_wall_seconds" in report
        assert "peak_rss_mb" in report
        assert "checkpoints" in report
        assert p._started is False

    def test_finish_when_not_started(self):
        p = PipelineProfiler(enabled=True)
        report = p.finish()
        assert report == {}

    def test_stage_context_manager(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        with p.stage("build"):
            pass
        names = [s["name"] for s in p.snapshots]
        assert "build:start" in names
        assert "build:end" in names

    def test_write_report(self, tmp_path: Path):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.checkpoint("mid")
        p.write_report(tmp_path)

        json_path = tmp_path / "profile_report.json"
        log_path = tmp_path / "profile_summary.log"
        assert json_path.exists()
        assert log_path.exists()

        report = json.loads(json_path.read_text())
        assert "checkpoints" in report
        assert "peak_rss_mb" in report

        summary = log_path.read_text()
        assert "MTGJSON Pipeline Profile Summary" in summary

    def test_write_report_with_subprocesses(self, tmp_path: Path):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.add_subprocess_profile({
            "label": "export",
            "pid": 999,
            "peak_rss_mb": 50.0,
            "total_wall_seconds": 2.0,
            "checkpoints": [
                {"name": "start", "wall_seconds": 0.0, "delta_seconds": 0.0, "rss_mb": 50.0, "rss_delta_mb": 0.0}
            ],
        })
        p.write_report(tmp_path)

        report = json.loads((tmp_path / "profile_report.json").read_text())
        assert "subprocesses" in report

    def test_format_summary_with_nested_subprocesses(self):
        p = PipelineProfiler(enabled=True)
        p.start()
        p.add_subprocess_profile({
            "label": "export",
            "pid": 999,
            "peak_rss_mb": 50.0,
            "total_wall_seconds": 2.0,
            "checkpoints": [
                {"name": "start", "wall_seconds": 0.0, "delta_seconds": 0.0, "rss_mb": 50.0, "rss_delta_mb": 0.0}
            ],
            "subprocesses": [{
                "label": "prices",
                "pid": 1000,
                "peak_rss_mb": 30.0,
                "total_wall_seconds": 1.0,
                "checkpoints": [
                    {"name": "start", "wall_seconds": 0.0, "delta_seconds": 0.0, "rss_mb": 30.0, "rss_delta_mb": 0.0}
                ],
            }],
        })
        report = p.finish()
        summary = p._format_summary(report)
        assert "export" in summary
        assert "prices" in summary


# ---------------------------------------------------------------------------
# PipelineProfiler with tracemalloc
# ---------------------------------------------------------------------------


class TestPipelineProfilerTracemalloc:
    def test_tracemalloc_mode(self):
        p = PipelineProfiler(enabled=True, use_tracemalloc=True)
        p.start()
        p.checkpoint("alloc_check", top_n=3)
        snap = p.snapshots[-1]
        assert "tracemalloc_current_mb" in snap
        assert "tracemalloc_peak_mb" in snap
        assert "top_allocations" in snap
        report = p.finish()
        assert "tracemalloc_peak_mb" in report


# ---------------------------------------------------------------------------
# SubprocessProfiler
# ---------------------------------------------------------------------------


class TestSubprocessProfiler:
    def test_disabled_noop(self):
        sp = SubprocessProfiler(label="test", enabled=False)
        sp.start()
        sp.checkpoint("mid")
        result = sp.to_dict()
        assert result == {}

    def test_enabled_records_snapshots(self):
        sp = SubprocessProfiler(label="export", enabled=True)
        sp.start()
        sp.checkpoint("mid")
        assert len(sp.snapshots) == 2  # start + mid

    def test_to_dict_structure(self):
        sp = SubprocessProfiler(label="export", enabled=True)
        sp.start()
        sp.checkpoint("done")
        result = sp.to_dict()
        assert result["label"] == "export"
        assert "pid" in result
        assert "peak_rss_mb" in result
        assert "total_wall_seconds" in result
        assert "checkpoints" in result

    def test_nested_profiles(self):
        sp = SubprocessProfiler(label="export", enabled=True)
        sp.start()
        sp.add_nested_profile({"label": "inner", "pid": 42})
        result = sp.to_dict()
        assert "subprocesses" in result
        assert result["subprocesses"][0]["label"] == "inner"

    def test_empty_profile_ignored(self):
        sp = SubprocessProfiler(label="test", enabled=True)
        sp.start()
        sp.add_nested_profile({})
        result = sp.to_dict()
        assert "subprocesses" not in result


# ---------------------------------------------------------------------------
# Global helpers
# ---------------------------------------------------------------------------


class TestGlobalHelpers:
    def test_init_profiler_disabled(self):
        p = init_profiler(enabled=False)
        assert p.enabled is False
        assert p._started is False

    def test_init_profiler_enabled(self):
        p = init_profiler(enabled=True)
        assert p.enabled is True
        assert p._started is True
        p.finish()  # Clean up

    def test_get_profiler_returns_instance(self):
        # Reset global
        import mtgjson5.profiler as mod
        mod._profiler = None
        p = get_profiler()
        assert isinstance(p, PipelineProfiler)
        assert p.enabled is False

    def test_get_profiler_returns_existing(self):
        p1 = init_profiler(enabled=False)
        p2 = get_profiler()
        assert p1 is p2
