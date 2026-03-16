#!/usr/bin/env python
"""Validate that our inline sealed compilation matches mtg-sealed-content outputs.

Usage:
    python scripts/validate_sealed_parity.py --stage products
    python scripts/validate_sealed_parity.py --stage products --skip-fetch --cache-dir /tmp/sealed-cache
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOGGER = logging.getLogger(__name__)

REPO = "mtgjson/mtg-sealed-content"
OUTPUT_FILES = {
    "products": "products.json",
    "contents": "contents.json",
    "deck_map": "deck_map.json",
}


# ---------------------------------------------------------------------------
# Fetching helpers
# ---------------------------------------------------------------------------


def resolve_head_sha() -> str:
    """Resolve the current HEAD SHA of the main branch via gh CLI."""
    result = subprocess.run(
        ["gh", "api", f"repos/{REPO}/git/ref/heads/main", "--jq", ".object.sha"],
        capture_output=True,
        text=True,
        check=True,
    )
    sha = result.stdout.strip()
    LOGGER.info("Resolved HEAD SHA: %s", sha)
    return sha


def fetch_tarball(sha: str, cache_dir: Path) -> Path:
    """Download the source tarball and extract product/content YAMLs."""
    import requests

    tarball_path = cache_dir / f"{sha}.tar.gz"
    if tarball_path.exists():
        LOGGER.info("Using cached tarball: %s", tarball_path)
    else:
        url = f"https://github.com/{REPO}/archive/{sha}.tar.gz"
        LOGGER.info("Downloading tarball: %s", url)
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        tarball_path.write_bytes(resp.content)
        LOGGER.info("Saved tarball (%d bytes)", len(resp.content))

    # Extract data/ YAML files
    extract_dir = cache_dir / "source"
    if extract_dir.exists() and any(extract_dir.iterdir()):
        LOGGER.info("Using cached extracted source: %s", extract_dir)
        return extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"mtg-sealed-content-{sha}/"

    with tarfile.open(tarball_path, "r:gz") as tf:
        for member in tf.getmembers():
            if not member.name.startswith(prefix):
                continue
            rel = member.name[len(prefix) :]
            # Only extract data/products/*.yaml and data/contents/*.yaml
            if rel.startswith(("data/products/", "data/contents/")) and rel.endswith(".yaml"):
                dest = extract_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                f = tf.extractfile(member)
                if f is not None:
                    dest.write_bytes(f.read())

    n_products = len(list((extract_dir / "data" / "products").glob("*.yaml")))
    n_contents = len(list((extract_dir / "data" / "contents").glob("*.yaml")))
    LOGGER.info("Extracted %d product YAMLs, %d content YAMLs", n_products, n_contents)
    return extract_dir


def fetch_output(sha: str, name: str, cache_dir: Path) -> dict | None:
    """Download a compiled output JSON via LFS media URL."""
    import requests

    filename = OUTPUT_FILES[name]
    out_path = cache_dir / "outputs" / filename
    if out_path.exists():
        LOGGER.info("Using cached output: %s", out_path)
        return json.loads(out_path.read_text(encoding="utf-8"))

    url = f"https://media.githubusercontent.com/media/{REPO}/{sha}/outputs/{filename}"
    LOGGER.info("Downloading output: %s", url)
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.error("Failed to download %s: %s", filename, exc)
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    return json.loads(resp.content)


# ---------------------------------------------------------------------------
# Deep diff
# ---------------------------------------------------------------------------


def deep_diff(expected: object, actual: object, path: str = "") -> list[str]:
    """Compare two structures, returning a list of difference descriptions.

    Key order in dicts is irrelevant; lists are compared positionally.
    Int-vs-str type mismatches are flagged as warnings, not failures.
    """
    diffs: list[str] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        all_keys = set(expected.keys()) | set(actual.keys())
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in expected:
                diffs.append(f"EXTRA key at {child_path} (actual has it, expected does not)")
            elif key not in actual:
                diffs.append(f"MISSING key at {child_path} (expected has it, actual does not)")
            else:
                diffs.extend(deep_diff(expected[key], actual[key], child_path))
    elif isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            diffs.append(f"LIST length mismatch at {path}: expected {len(expected)}, got {len(actual)}")
        for i, (e, a) in enumerate(zip(expected, actual, strict=False)):
            diffs.extend(deep_diff(e, a, f"{path}[{i}]"))
    elif expected != actual:
        # Check for int/str type mismatch that's value-equivalent
        if _is_int_str_equiv(expected, actual):
            diffs.append(
                f"TYPE WARNING at {path}: {type(expected).__name__} vs {type(actual).__name__} (values equivalent: {expected!r} vs {actual!r})"
            )
        else:
            diffs.append(f"VALUE mismatch at {path}: expected {expected!r}, got {actual!r}")
    return diffs


def _is_int_str_equiv(a: object, b: object) -> bool:
    """Check if a and b are int/str with equivalent numeric values."""
    if isinstance(a, int) and isinstance(b, str):
        return str(a) == b
    if isinstance(a, str) and isinstance(b, int):
        return a == str(b)
    return False


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def validate_products(source_dir: Path, expected: dict) -> tuple[bool, list[str]]:
    """Validate compile_products() against the fetched products.json."""
    # Import inline so the script can be run standalone
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from mtgjson5.pipeline.stages.sealed import compile_products

    products_dir = source_dir / "data" / "products"
    actual = compile_products(products_dir)
    diffs = deep_diff(expected, actual)

    n_sets = len(actual)
    type_warnings = [d for d in diffs if d.startswith("TYPE WARNING")]
    real_diffs = [d for d in diffs if not d.startswith("TYPE WARNING")]

    if not real_diffs:
        print(f"[PASS] products.json: {n_sets} sets, 0 differences")
        if type_warnings:
            print(f"       ({len(type_warnings)} int/str type warnings)")
        return True, diffs
    else:
        print(f"[FAIL] products.json: {n_sets} sets, {len(real_diffs)} differences")
        for d in real_diffs[:50]:
            print(f"  - {d}")
        if len(real_diffs) > 50:
            print(f"  ... and {len(real_diffs) - 50} more")
        if type_warnings:
            print(f"  ({len(type_warnings)} int/str type warnings omitted)")
        return False, diffs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate sealed content compilation parity")
    parser.add_argument(
        "--stage",
        choices=["products", "contents", "deck_map", "all"],
        default="all",
        help="Which stage to validate (default: all)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory to cache downloads (default: temp dir)",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip fetching; reuse data already in cache-dir",
    )
    args = parser.parse_args()

    if args.cache_dir:
        cache_dir = args.cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
    else:
        cache_dir = Path(tempfile.mkdtemp(prefix="sealed-validate-"))
    LOGGER.info("Cache directory: %s", cache_dir)

    stages = [args.stage] if args.stage != "all" else ["products", "contents", "deck_map"]

    if args.skip_fetch:
        sha = "unknown"
        source_dir = cache_dir / "source"
        if not source_dir.exists():
            LOGGER.error("--skip-fetch specified but no cached source at %s", source_dir)
            sys.exit(1)
    else:
        sha = resolve_head_sha()
        source_dir = fetch_tarball(sha, cache_dir)

    all_pass = True
    for stage in stages:
        if stage != "products":
            LOGGER.warning("Stage '%s' not yet implemented — skipping", stage)
            continue

        if args.skip_fetch:
            out_path = cache_dir / "outputs" / OUTPUT_FILES[stage]
            if not out_path.exists():
                LOGGER.error("--skip-fetch but no cached output at %s", out_path)
                all_pass = False
                continue
            expected = json.loads(out_path.read_text(encoding="utf-8"))
        else:
            expected = fetch_output(sha, stage, cache_dir)

        if expected is None:
            LOGGER.error("Could not fetch expected output for %s", stage)
            all_pass = False
            continue

        passed, _ = validate_products(source_dir, expected)
        if not passed:
            all_pass = False

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
