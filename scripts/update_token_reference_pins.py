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
    load_pins,
    pin_stats,
    scan_references_and_printings,
    serialize_pins,
)

LOGGER = logging.getLogger("update_token_reference_pins")

BULK_NAME = "all_cards.ndjson"

# Matches GlobalCache: a shallower sample never sees a reversible printing, so
# card_faces comes back without its oracle_id field and those references get
# filed under a different key than the pipeline will look them up by.
SCHEMA_SAMPLE = 100_000

# A refresh against a healthy dump repoints nothing — a pin only moves once its
# printing leaves Scryfall for good.  Any real volume means the dump is partial
# (a truncated download, or default_cards.ndjson passed by mistake), which would
# otherwise rewrite thousands of pins and churn the very output they hold still.
DEFAULT_MAX_REPOINTS = 25


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
    parser.add_argument(
        "--max-repoints",
        type=int,
        default=DEFAULT_MAX_REPOINTS,
        help=(
            "Abort if more than this many pins would move to a different printing "
            f"(default: {DEFAULT_MAX_REPOINTS}).  Raise it only once you have "
            "confirmed the dump is complete and Scryfall really did retire that many."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.cards.exists():
        raise SystemExit(f"Scryfall dump not found: {args.cards}\nRun a build first, or pass --cards.")

    LOGGER.info("Reading token references from %s", args.cards)
    cards_lf = pl.scan_ndjson(args.cards, infer_schema_length=SCHEMA_SAMPLE)

    # Liveness has to come from the whole dump, not just the ids still named by
    # an all_parts entry: a repointed printing usually keeps existing, and
    # treating it as retired would repoint the very pins that hold it steady.
    references, live_printings = scan_references_and_printings(cards_lf)
    if references.height == 0:
        raise SystemExit(f"No token references found in {args.cards}")
    LOGGER.info("Found %d token references across %d printings", references.height, len(live_printings))

    existing = load_pins(args.output, refresh=True) if args.output.exists() else {}
    merged = build_pins(references, existing, live_printings)
    LOGGER.info("Pins: %s -> %s", pin_stats(existing), pin_stats(merged.pins))

    if merged.repointed > args.max_repoints:
        raise SystemExit(
            f"{merged.repointed} pins would be repointed, over the --max-repoints limit of {args.max_repoints}.\n"
            f"That usually means {args.cards} is partial rather than that Scryfall retired that many printings."
        )

    if args.check:
        on_disk = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
        if on_disk != serialize_pins(merged.pins):
            LOGGER.error("%s is out of date; run scripts/update_token_reference_pins.py", args.output)
            return 1
        LOGGER.info("%s is up to date", args.output)
        return 0

    dump_pins(merged.pins, args.output)
    LOGGER.info("Wrote %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
