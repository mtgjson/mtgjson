#!/usr/bin/env python3
"""Refresh the sealed product UUID pin file.

The pin file records the UUID each sealed product was first published with, so
a rename in mtg-sealed-content no longer mints a new UUID.  Run this whenever
new products land upstream::

    python scripts/update_sealed_uuid_pins.py

Products are read from a local mtg-sealed-content checkout when one is given,
from the build cache when it has been populated, and otherwise straight from
GitHub.  Pass --check to fail instead of writing, which is what CI wants.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from mtgjson5 import constants
from mtgjson5.pipeline.stages.sealed import compile_products
from mtgjson5.pipeline.stages.sealed_uuids import (
    PIN_RESOURCE_NAME,
    build_pins,
    dump_pins,
    load_pins,
)

LOGGER = logging.getLogger("update_sealed_uuid_pins")

TARBALL_URL = "https://api.github.com/repos/mtgjson/mtg-sealed-content/tarball/main"
CACHED_PRODUCTS_DIR = constants.CACHE_PATH / "sealed_yaml" / "data" / "products"


def _download_products(destination: Path) -> Path:
    """Download the mtg-sealed-content tarball and return its products dir."""
    LOGGER.info("Downloading %s", TARBALL_URL)
    request = urllib.request.Request(TARBALL_URL, headers={"User-Agent": "mtgjson-pin-updater"})
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read()

    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        members = [m for m in tar.getmembers() if "/data/products/" in m.name and m.name.endswith(".yaml")]
        if not members:
            raise RuntimeError("Tarball contained no data/products/*.yaml files")
        for member in members:
            member.name = Path(member.name).name
            tar.extract(member, destination)

    LOGGER.info("Extracted %d product files", len(members))
    return destination


def _resolve_products_dir(explicit: Path | None, staging: Path) -> Path:
    if explicit is not None:
        if not explicit.is_dir():
            raise SystemExit(f"Not a directory: {explicit}")
        return explicit
    if CACHED_PRODUCTS_DIR.is_dir() and any(CACHED_PRODUCTS_DIR.glob("*.yaml")):
        # Pins generated from a stale cache miss whatever landed upstream since,
        # so be loud about how old it is rather than just saying "cached".
        age_hours = (time.time() - CACHED_PRODUCTS_DIR.stat().st_mtime) / 3600
        log = LOGGER.warning if age_hours > 24 else LOGGER.info
        log(
            "Using cached products from %s (%.1fh old); pass --products-dir or clear the cache for a fresh copy",
            CACHED_PRODUCTS_DIR,
            age_hours,
        )
        return CACHED_PRODUCTS_DIR
    return _download_products(staging)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--products-dir",
        type=Path,
        default=None,
        help="mtg-sealed-content data/products directory (default: build cache, else download)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=constants.RESOURCE_PATH / PIN_RESOURCE_NAME,
        help="Pin file to write (default: the packaged resource)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the pin file is out of date instead of writing it",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with tempfile.TemporaryDirectory() as staging:
        products_dir = _resolve_products_dir(args.products_dir, Path(staging))
        products = compile_products(products_dir)

    if not products:
        raise SystemExit(f"No products compiled from {products_dir}")

    existing = load_pins(args.output, refresh=True) if args.output.exists() else {}
    updated = build_pins(products, existing)

    before = sum(len(v) for v in existing.values())
    after = sum(len(v) for v in updated.values())
    LOGGER.info(
        "Pins: %d -> %d (%d products in %d sets)", before, after, sum(len(p) for p in products.values()), len(products)
    )

    if args.check:
        current = json.dumps(existing, indent=1, sort_keys=True, ensure_ascii=False)
        proposed = json.dumps(updated, indent=1, sort_keys=True, ensure_ascii=False)
        if current != proposed:
            LOGGER.error("%s is out of date; run scripts/update_sealed_uuid_pins.py", args.output)
            return 1
        LOGGER.info("%s is up to date", args.output)
        return 0

    dump_pins(updated, args.output)
    LOGGER.info("Wrote %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
