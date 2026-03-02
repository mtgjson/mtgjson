"""
Pipeline memory and performance profiler.

Collects wall-clock timing and RSS (physical memory via psutil) at named
checkpoints throughout the build.  Optionally enables tracemalloc for Python
allocation tracking (adds significant overhead during Pydantic-heavy phases).

Activated via ``--profile`` CLI flag.  Zero overhead when disabled.
Use ``--profile-tracemalloc`` to additionally enable tracemalloc (slow).
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

_MB = 1024 * 1024


def _get_rss_mb() -> float:
    """Return current process RSS in MB, or -1 if psutil unavailable."""
    try:
        import psutil

        return float(psutil.Process(os.getpid()).memory_info().rss / _MB)
    except ImportError:
        return -1.0


def _get_children_rss_mb() -> float:
    """Return total RSS of all child processes in MB, or 0 if unavailable."""
    try:
        import psutil

        parent = psutil.Process(os.getpid())
        return sum(float(c.memory_info().rss / _MB) for c in parent.children(recursive=True))
    except (ImportError, Exception):
        return 0.0


class PipelineProfiler:
    """Collects timing and memory snapshots at named checkpoints."""

    def __init__(self, enabled: bool = False, use_tracemalloc: bool = False) -> None:
        self.enabled = enabled
        self._use_tracemalloc = use_tracemalloc
        self.snapshots: list[dict[str, Any]] = []
        self.subprocess_profiles: list[dict[str, Any]] = []
        self._start_time: float = 0.0
        self._last_time: float = 0.0
        self._started = False

    def start(self) -> None:
        """Begin profiling: optionally start tracemalloc and record baseline."""
        if not self.enabled:
            return
        if self._use_tracemalloc:
            import tracemalloc

            tracemalloc.start()
        self._start_time = time.perf_counter()
        self._last_time = self._start_time
        self._started = True
        self.checkpoint("profiler_start")
        mode = "tracemalloc + psutil RSS" if self._use_tracemalloc else "psutil RSS only"
        LOGGER.info("[profile] Profiling enabled (%s)", mode)

    def checkpoint(self, name: str, *, top_n: int = 0) -> None:
        """
        Record a named checkpoint with timing and memory data.

        Args:
            name: Human-readable checkpoint label.
            top_n: If > 0 and tracemalloc is active, include the top N
                   allocation sites.
        """
        if not self.enabled or not self._started:
            return

        now = time.perf_counter()
        wall = now - self._start_time
        delta = now - self._last_time
        self._last_time = now

        rss_mb = _get_rss_mb()
        prev_rss = self.snapshots[-1]["rss_mb"] if self.snapshots else rss_mb
        rss_delta = rss_mb - prev_rss

        snap: dict[str, Any] = {
            "name": name,
            "wall_seconds": round(wall, 3),
            "delta_seconds": round(delta, 3),
            "rss_mb": round(rss_mb, 1),
            "rss_delta_mb": round(rss_delta, 1),
        }

        if self._use_tracemalloc:
            import tracemalloc

            current, peak = tracemalloc.get_traced_memory()
            snap["tracemalloc_current_mb"] = round(current / _MB, 1)
            snap["tracemalloc_peak_mb"] = round(peak / _MB, 1)

            if top_n > 0:
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics("lineno")[:top_n]
                snap["top_allocations"] = [
                    {"location": str(s.traceback), "size_mb": round(s.size / _MB, 2)} for s in top_stats
                ]

        self.snapshots.append(snap)

        if self._use_tracemalloc:
            LOGGER.info(
                "[profile] %-40s  %7.1fs (+%.1fs)  RSS %7.1f MB (%+.1f)  tracemalloc %7.1f MB (peak %.1f MB)",
                name,
                wall,
                delta,
                rss_mb,
                rss_delta,
                snap["tracemalloc_current_mb"],
                snap["tracemalloc_peak_mb"],
            )
        else:
            LOGGER.info(
                "[profile] %-40s  %7.1fs (+%.1fs)  RSS %7.1f MB (%+.1f)",
                name,
                wall,
                delta,
                rss_mb,
                rss_delta,
            )

    def checkpoint_with_children(self, name: str) -> None:
        """Like checkpoint(), but also records total children RSS and system total."""
        if not self.enabled or not self._started:
            return
        self.checkpoint(name)
        children_rss = _get_children_rss_mb()
        snap = self.snapshots[-1]
        snap["children_rss_mb"] = round(children_rss, 1)
        snap["system_total_mb"] = round(snap["rss_mb"] + children_rss, 1)
        LOGGER.info(
            "[profile] %-40s  children RSS %7.1f MB  system total %7.1f MB",
            name,
            children_rss,
            snap["system_total_mb"],
        )

    def add_subprocess_profile(self, profile_dict: dict[str, Any]) -> None:
        """Attach a subprocess profiler's output to the parent report."""
        if not self.enabled or not profile_dict:
            return
        self.subprocess_profiles.append(profile_dict)
        LOGGER.info(
            "[profile] Received subprocess profile: %s (pid=%s, peak_rss=%.1f MB, wall=%.1fs)",
            profile_dict.get("label", "?"),
            profile_dict.get("pid", "?"),
            profile_dict.get("peak_rss_mb", -1),
            profile_dict.get("total_wall_seconds", 0),
        )

    def finish(self) -> dict[str, Any]:
        """Stop profiling and return the full report dict."""
        if not self.enabled or not self._started:
            return {}

        self.checkpoint("profiler_finish", top_n=20)

        total_time = time.perf_counter() - self._start_time
        self._started = False

        rss_values = [s["rss_mb"] for s in self.snapshots if s["rss_mb"] >= 0]
        peak_rss = max(rss_values) if rss_values else -1.0

        report: dict[str, Any] = {
            "total_wall_seconds": round(total_time, 3),
            "peak_rss_mb": round(peak_rss, 1),
            "checkpoints": self.snapshots,
        }

        if self.subprocess_profiles:
            report["subprocesses"] = self.subprocess_profiles

        if self._use_tracemalloc:
            import tracemalloc

            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            report["tracemalloc_peak_mb"] = round(peak / _MB, 1)

        self._log_summary(report)
        return report

    def write_report(self, output_dir: Path) -> None:
        """Write profile_report.json and profile_summary.log to *output_dir*."""
        if not self.enabled or not self.snapshots:
            return

        output_dir.mkdir(parents=True, exist_ok=True)

        report = {
            "total_wall_seconds": self.snapshots[-1]["wall_seconds"] if self.snapshots else 0,
            "peak_rss_mb": max((s["rss_mb"] for s in self.snapshots if s["rss_mb"] >= 0), default=-1),
            "checkpoints": self.snapshots,
        }
        if self.subprocess_profiles:
            report["subprocesses"] = self.subprocess_profiles
        if self._use_tracemalloc:
            report["tracemalloc_peak_mb"] = max((s.get("tracemalloc_peak_mb", 0) for s in self.snapshots), default=0)

        json_path = output_dir / "profile_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        LOGGER.info("[profile] Report written to %s", json_path)

        log_path = output_dir / "profile_summary.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(self._format_summary(report))
        LOGGER.info("[profile] Summary written to %s", log_path)

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """Context manager that checkpoints before and after a named stage."""
        if not self.enabled:
            yield
            return
        self.checkpoint(f"{name}:start")
        yield
        self.checkpoint(f"{name}:end")

    def _format_summary(self, report: dict[str, Any]) -> str:
        """Format a human-readable summary from the report dict."""
        has_tmalloc = self._use_tracemalloc
        lines = []
        lines.append("=" * 100)
        lines.append("MTGJSON Pipeline Profile Summary")
        lines.append("=" * 100)
        header = f"Total wall time: {report['total_wall_seconds']:.1f}s  |  Peak RSS: {report['peak_rss_mb']:.1f} MB"
        if has_tmalloc:
            header += f"  |  Peak tracemalloc: {report.get('tracemalloc_peak_mb', 0):.1f} MB"
        lines.append(header)
        lines.append("-" * 100)

        col_header = f"{'Checkpoint':<42} {'Wall(s)':>8} {'Delta(s)':>9} {'RSS(MB)':>9} {'RSS Δ':>8}"
        if has_tmalloc:
            col_header += f" {'tmalloc':>9} {'peak':>9}"
        lines.append(col_header)
        lines.append("-" * 100)

        for snap in report.get("checkpoints", []):
            row = (
                f"{snap['name']:<42} {snap['wall_seconds']:>8.1f} "
                f"{snap['delta_seconds']:>+9.1f} {snap['rss_mb']:>9.1f} "
                f"{snap['rss_delta_mb']:>+8.1f}"
            )
            if has_tmalloc:
                row += f" {snap.get('tracemalloc_current_mb', 0):>9.1f} {snap.get('tracemalloc_peak_mb', 0):>9.1f}"
            lines.append(row)

        # System total column for checkpoints that have children_rss_mb
        for snap in report.get("checkpoints", []):
            if "system_total_mb" in snap:
                lines.append(
                    f"  {'':>42} children_rss={snap['children_rss_mb']:.1f} MB  "
                    f"system_total={snap['system_total_mb']:.1f} MB"
                )

        lines.append("=" * 100)

        # Subprocess profiles
        for sp in report.get("subprocesses", []):
            lines.append("")
            lines.append(
                f"Subprocess: {sp['label']} (PID {sp.get('pid', '?')})  "
                f"peak_rss={sp.get('peak_rss_mb', -1):.1f} MB  "
                f"wall={sp.get('total_wall_seconds', 0):.1f}s"
            )
            lines.append(f"  {'Checkpoint':<42} {'Wall(s)':>8} {'Delta(s)':>9} {'RSS(MB)':>9} {'RSS Δ':>8}")
            lines.append("  " + "-" * 80)
            for snap in sp.get("checkpoints", []):
                lines.append(
                    f"  {snap['name']:<42} {snap['wall_seconds']:>8.1f} "
                    f"{snap['delta_seconds']:>+9.1f} {snap['rss_mb']:>9.1f} "
                    f"{snap['rss_delta_mb']:>+8.1f}"
                )
            # Nested subprocesses (e.g. prices inside exports)
            for nested in sp.get("subprocesses", []):
                lines.append(
                    f"  └─ {nested['label']} (PID {nested.get('pid', '?')})  "
                    f"peak_rss={nested.get('peak_rss_mb', -1):.1f} MB  "
                    f"wall={nested.get('total_wall_seconds', 0):.1f}s"
                )
                for snap in nested.get("checkpoints", []):
                    lines.append(
                        f"     {snap['name']:<39} {snap['wall_seconds']:>8.1f} "
                        f"{snap['delta_seconds']:>+9.1f} {snap['rss_mb']:>9.1f} "
                        f"{snap['rss_delta_mb']:>+8.1f}"
                    )

        # Top allocations from last snapshot
        last = report.get("checkpoints", [{}])[-1] if report.get("checkpoints") else {}
        if "top_allocations" in last:
            lines.append("")
            lines.append("Top Memory Allocations (tracemalloc):")
            lines.append("-" * 80)
            for alloc in last["top_allocations"]:
                lines.append(f"  {alloc['size_mb']:>8.2f} MB  {alloc['location']}")

        lines.append("")
        return "\n".join(lines)

    def _log_summary(self, report: dict[str, Any]) -> None:
        """Log a human-readable summary from the report dict."""
        summary = self._format_summary(report)
        for line in summary.split("\n"):
            LOGGER.info("[profile] %s", line)


class SubprocessProfiler:
    """Lightweight profiler for child processes. Sends data back via queue."""

    def __init__(self, label: str, enabled: bool = False) -> None:
        self.label = label
        self.enabled = enabled
        self.pid: int = os.getpid()
        self.snapshots: list[dict[str, Any]] = []
        self.nested_profiles: list[dict[str, Any]] = []
        self._start_time: float = 0.0
        self._last_time: float = 0.0

    def start(self) -> None:
        """Begin profiling: record baseline time and PID."""
        if not self.enabled:
            return
        self._start_time = time.perf_counter()
        self._last_time = self._start_time
        self.pid = os.getpid()
        self.checkpoint("start")

    def checkpoint(self, name: str) -> None:
        """Record a named checkpoint with timing and RSS data."""
        if not self.enabled:
            return
        now = time.perf_counter()
        wall = now - self._start_time
        delta = now - self._last_time
        self._last_time = now
        rss_mb = _get_rss_mb()
        prev_rss = self.snapshots[-1]["rss_mb"] if self.snapshots else rss_mb
        rss_delta = rss_mb - prev_rss

        self.snapshots.append({
            "name": name,
            "wall_seconds": round(wall, 3),
            "delta_seconds": round(delta, 3),
            "rss_mb": round(rss_mb, 1),
            "rss_delta_mb": round(rss_delta, 1),
        })

        LOGGER.info(
            "[profile:%s] %-40s  %7.1fs (+%.1fs)  RSS %7.1f MB (%+.1f)",
            self.label, name, wall, delta, rss_mb, rss_delta,
        )

    def add_nested_profile(self, profile_dict: dict[str, Any]) -> None:
        """Attach a nested subprocess profile (e.g. prices inside exports)."""
        if profile_dict:
            self.nested_profiles.append(profile_dict)

    def to_dict(self) -> dict[str, Any]:
        """Return picklable summary dict for queue transport."""
        if not self.enabled or not self.snapshots:
            return {}
        rss_values = [s["rss_mb"] for s in self.snapshots if s["rss_mb"] >= 0]
        result: dict[str, Any] = {
            "label": self.label,
            "pid": self.pid,
            "peak_rss_mb": round(max(rss_values), 1) if rss_values else -1.0,
            "total_wall_seconds": round(self.snapshots[-1]["wall_seconds"], 1) if self.snapshots else 0,
            "checkpoints": self.snapshots,
        }
        if self.nested_profiles:
            result["subprocesses"] = self.nested_profiles
        return result


_profiler: PipelineProfiler | None = None


def init_profiler(enabled: bool = False, use_tracemalloc: bool = False) -> PipelineProfiler:
    """Create and return the global profiler instance."""
    global _profiler
    _profiler = PipelineProfiler(enabled=enabled, use_tracemalloc=use_tracemalloc)
    if enabled:
        _profiler.start()
    return _profiler


def get_profiler() -> PipelineProfiler:
    """Return the global profiler (creates a disabled one if not yet initialized)."""
    global _profiler
    if _profiler is None:
        _profiler = PipelineProfiler(enabled=False)
    return _profiler
