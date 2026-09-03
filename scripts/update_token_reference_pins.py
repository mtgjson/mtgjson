#!/usr/bin/env python3
"""Refresh the token reference pin file.

``relatedCards.tokens`` follows Scryfall's ``all_parts``, which names one
printing of each related token and re-points it over time for tokens a set
never printed.  The pin file records which printing each card was first
published against so those references stop churning.  Run this whenever new
cards land::

    python scripts/update_token_reference_pins.py

References are read from the Scryfall bulk dump in the build cache.  Pass
--check to fail instead of writing, which is what CI wants.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from mtgjson5 import constants
from mtgjson5.pipeline.stages.token_references import (
    PIN_RESOURCE_NAME,
    build_pins,
    dump_pins,
    extract_printing_ids,
    extract_token_references,
    load_pins,
    pin_stats,
    serialize_pins,
)

LOGGER = logging.getLogger("update_token_reference_pins")

BULK_NAME = "all_cards.ndjson"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cards",
        type=Path,
        default=constants.CACHE_PATH / BULK_NAME,
        help=f"Scryfall bulk dump to read (default: the cached {BULK_NAME})",
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

    if not args.cards.exists():
        raise SystemExit(f"Scryfall dump not found: {args.cards}\nRun a build first, or pass --cards.")

    LOGGER.info("Reading token references from %s", args.cards)
    cards_lf = pl.scan_ndjson(args.cards, infer_schema_length=2000)
    references = extract_token_references(cards_lf)
    if references.height == 0:
        raise SystemExit(f"No token references found in {args.cards}")
    LOGGER.info("Found %d token references", references.height)

    # Liveness has to come from the whole dump, not just the ids still named by
    # an all_parts entry: a repointed printing usually keeps existing, and
    # treating it as retired would repoint the very pins that hold it steady.
    live_printings = extract_printing_ids(cards_lf)
    LOGGER.info("Dump contains %d printings", len(live_printings))

    existing = load_pins(args.output, refresh=True) if args.output.exists() else {}
    updated = build_pins(references, existing, live_printings)
    LOGGER.info("Pins: %s -> %s", pin_stats(existing), pin_stats(updated))

    if args.check:
        on_disk = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
        if on_disk != serialize_pins(updated):
            LOGGER.error("%s is out of date; run scripts/update_token_reference_pins.py", args.output)
            return 1
        LOGGER.info("%s is up to date", args.output)
        return 0

    dump_pins(updated, args.output)
    LOGGER.info("Wrote %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
