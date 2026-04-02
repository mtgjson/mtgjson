#!/usr/bin/env python3
"""Compare two MTGJSON BuildManifest.json files and report regressions.

Usage:
    python scripts/compare_manifests.py <previous> <current> [options]

Exit codes:
    0 - pass (no issues)
    1 - warn (minor regressions detected)
    2 - fail (major regressions detected)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def compare_manifests(
    previous: dict,
    current: dict,
    *,
    size_warn_pct: float = 2.0,
    size_fail_pct: float = 10.0,
    count_warn_pct: float = 1.0,
    count_fail_pct: float = 5.0,
) -> dict:
    """Compare two manifest dicts and return a structured report.

    Args:
        previous: Previous build manifest.
        current: Current build manifest.
        size_warn_pct: Size decrease % to trigger a warning.
        size_fail_pct: Size decrease % to trigger a failure.
        count_warn_pct: Record count decrease % to trigger a warning.
        count_fail_pct: Record count decrease % to trigger a failure.

    Returns:
        Report dict with status, summary, and detailed findings.
    """
    prev_files = previous.get("files", {})
    curr_files = current.get("files", {})
    prev_counts = previous.get("record_counts", {})
    curr_counts = current.get("record_counts", {})

    missing_files = []
    new_files = []
    size_changes = []
    record_count_changes = []

    worst_severity = "pass"

    def escalate(severity: str) -> None:
        nonlocal worst_severity
        order = {"pass": 0, "info": 0, "warn": 1, "fail": 2}
        if order.get(severity, 0) > order.get(worst_severity, 0):
            worst_severity = severity

    # Check for missing files
    for path, prev_info in prev_files.items():
        if path not in curr_files:
            missing_files.append({
                "path": path,
                "previous_size_bytes": prev_info["size_bytes"],
            })
            escalate("fail")

    # Check for new files
    for path, curr_info in curr_files.items():
        if path not in prev_files:
            new_files.append({
                "path": path,
                "size_bytes": curr_info["size_bytes"],
            })

    # Check size changes for files present in both
    for path in sorted(prev_files.keys() & curr_files.keys()):
        prev_size = prev_files[path]["size_bytes"]
        curr_size = curr_files[path]["size_bytes"]

        if prev_size == 0:
            continue

        change_pct = ((curr_size - prev_size) / prev_size) * 100

        # Only flag decreases
        if change_pct < 0:
            abs_change = abs(change_pct)
            if abs_change >= size_fail_pct:
                severity = "fail"
            elif abs_change >= size_warn_pct:
                severity = "warn"
            else:
                continue

            size_changes.append({
                "path": path,
                "previous_size_bytes": prev_size,
                "current_size_bytes": curr_size,
                "change_pct": round(change_pct, 2),
                "severity": severity,
            })
            escalate(severity)

    # Check record count changes
    for key in sorted(prev_counts.keys() | curr_counts.keys()):
        prev_count = prev_counts.get(key)
        curr_count = curr_counts.get(key)

        if prev_count is None or curr_count is None:
            continue
        if prev_count == 0:
            continue

        change_pct = ((curr_count - prev_count) / prev_count) * 100

        if change_pct < 0:
            abs_change = abs(change_pct)
            if abs_change >= count_fail_pct:
                severity = "fail"
            elif abs_change >= count_warn_pct:
                severity = "warn"
            else:
                continue

            record_count_changes.append({
                "key": key,
                "previous_count": prev_count,
                "current_count": curr_count,
                "change_pct": round(change_pct, 2),
                "severity": severity,
            })
            escalate(severity)

    return {
        "status": worst_severity,
        "summary": {
            "previous_date": previous.get("meta", {}).get("date", ""),
            "current_date": current.get("meta", {}).get("date", ""),
            "previous_file_count": previous.get("meta", {}).get("total_files", 0),
            "current_file_count": current.get("meta", {}).get("total_files", 0),
            "previous_total_size": previous.get("meta", {}).get("total_size_bytes", 0),
            "current_total_size": current.get("meta", {}).get("total_size_bytes", 0),
        },
        "missing_files": missing_files,
        "new_files": new_files,
        "size_changes": size_changes,
        "record_count_changes": record_count_changes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare two MTGJSON BuildManifest.json files."
    )
    parser.add_argument("previous", help="Path to previous BuildManifest.json")
    parser.add_argument("current", help="Path to current BuildManifest.json")
    parser.add_argument(
        "--size-warn-pct",
        type=float,
        default=2.0,
        help="Size decrease %% for warnings (default: 2.0)",
    )
    parser.add_argument(
        "--size-fail-pct",
        type=float,
        default=10.0,
        help="Size decrease %% for failures (default: 10.0)",
    )
    parser.add_argument(
        "--count-warn-pct",
        type=float,
        default=1.0,
        help="Record count decrease %% for warnings (default: 1.0)",
    )
    parser.add_argument(
        "--count-fail-pct",
        type=float,
        default=5.0,
        help="Record count decrease %% for failures (default: 5.0)",
    )
    parser.add_argument(
        "--output",
        help="Write JSON report to file (default: stdout)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only output JSON, no human-readable summary",
    )
    args = parser.parse_args()

    prev_path = Path(args.previous)
    curr_path = Path(args.current)

    # Handle missing previous manifest (first build)
    if not prev_path.exists():
        current = json.loads(curr_path.read_text(encoding="utf-8"))
        report = {
            "status": "pass",
            "note": "No previous manifest found — first build comparison skipped.",
            "summary": {
                "previous_date": "",
                "current_date": current.get("meta", {}).get("date", ""),
                "previous_file_count": 0,
                "current_file_count": current.get("meta", {}).get("total_files", 0),
                "previous_total_size": 0,
                "current_total_size": current.get("meta", {}).get(
                    "total_size_bytes", 0
                ),
            },
            "missing_files": [],
            "new_files": [],
            "size_changes": [],
            "record_count_changes": [],
        }
    else:
        previous = json.loads(prev_path.read_text(encoding="utf-8"))
        current = json.loads(curr_path.read_text(encoding="utf-8"))
        report = compare_manifests(
            previous,
            current,
            size_warn_pct=args.size_warn_pct,
            size_fail_pct=args.size_fail_pct,
            count_warn_pct=args.count_warn_pct,
            count_fail_pct=args.count_fail_pct,
        )

    report_json = json.dumps(report, indent=2)

    if args.output:
        Path(args.output).write_text(report_json, encoding="utf-8")
    else:
        print(report_json)

    if not args.quiet:
        status = report["status"].upper()
        summary = report["summary"]
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Build Manifest Comparison: {status}", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)
        print(
            f"Previous: {summary['previous_date']} "
            f"({summary['previous_file_count']} files, "
            f"{summary['previous_total_size']:,} bytes)",
            file=sys.stderr,
        )
        print(
            f"Current:  {summary['current_date']} "
            f"({summary['current_file_count']} files, "
            f"{summary['current_total_size']:,} bytes)",
            file=sys.stderr,
        )

        if report.get("note"):
            print(f"\nNote: {report['note']}", file=sys.stderr)

        if report["missing_files"]:
            print(f"\nMISSING FILES ({len(report['missing_files'])}):", file=sys.stderr)
            for f in report["missing_files"]:
                print(f"  - {f['path']} (was {f['previous_size_bytes']:,} bytes)", file=sys.stderr)

        if report["new_files"]:
            print(f"\nNEW FILES ({len(report['new_files'])}):", file=sys.stderr)
            for f in report["new_files"]:
                print(f"  + {f['path']} ({f['size_bytes']:,} bytes)", file=sys.stderr)

        if report["size_changes"]:
            print(f"\nSIZE CHANGES ({len(report['size_changes'])}):", file=sys.stderr)
            for c in report["size_changes"]:
                print(
                    f"  [{c['severity'].upper()}] {c['path']}: "
                    f"{c['change_pct']:+.1f}% "
                    f"({c['previous_size_bytes']:,} -> {c['current_size_bytes']:,})",
                    file=sys.stderr,
                )

        if report["record_count_changes"]:
            print(
                f"\nRECORD COUNT CHANGES ({len(report['record_count_changes'])}):",
                file=sys.stderr,
            )
            for c in report["record_count_changes"]:
                print(
                    f"  [{c['severity'].upper()}] {c['key']}: "
                    f"{c['change_pct']:+.1f}% "
                    f"({c['previous_count']:,} -> {c['current_count']:,})",
                    file=sys.stderr,
                )

        print(f"\nResult: {status}", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)

    exit_code_map = {"pass": 0, "warn": 1, "fail": 2}
    return exit_code_map.get(report["status"], 2)


if __name__ == "__main__":
    sys.exit(main())
