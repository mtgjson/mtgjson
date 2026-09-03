"""Stable token references for ``relatedCards.tokens``.

A card's related tokens come from Scryfall's ``all_parts``, which names one
specific *printing* of each token.  For a token the card's own set never
printed — Copy, Treasure, Clue and friends — Scryfall picks an arbitrary
printing and re-picks it over time.  MTGJSON follows that pointer faithfully,
so ``relatedCards.tokens`` churns even though nothing about the card or the
token changed.  The Copy token behind issue #1644 has pointed at three
different printings (``ttmt`` #1, ``tsos`` #1, ``tmsc`` #17).

This module keeps a pin file recording which printing each card was first
published against, keyed by the token's Scryfall oracle id — the identity that
survives a printing swap.  A pinned printing that has since disappeared from
Scryfall falls back to whatever ``all_parts`` currently names, so a pin can
never strand a reference.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mtgjson5 import constants

if TYPE_CHECKING:
    import polars as pl

LOGGER = logging.getLogger(__name__)

PIN_RESOURCE_NAME = "token_references.json"

# Pin file layout::
#
#     {"<card scryfall id>": {"<token oracle id>": "<token scryfall id>"}}
#
# (card, token oracle id) is a unique key: a card never relates to two distinct
# printings that share an oracle id.
PinFile = dict[str, dict[str, str]]

_pins_cache: PinFile | None = None

# The raw Scryfall dump is snake_case; GlobalCache normalizes it to camelCase
# before the pipeline sees it, so both spellings reach this module.
_ORACLE_COLUMNS = ("oracle_id", "oracleId")
_ALL_PARTS_COLUMNS = ("all_parts", "allParts")


def _pick_column(names: set[str], candidates: tuple[str, ...]) -> str | None:
    """Return the first candidate spelling present in ``names``."""
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def load_pins(path: Path | None = None, *, refresh: bool = False) -> PinFile:
    """Load the token reference pin file, caching it for the process."""
    global _pins_cache  # pylint: disable=global-statement

    if path is None and _pins_cache is not None and not refresh:
        return _pins_cache

    pin_path = path or (constants.RESOURCE_PATH / PIN_RESOURCE_NAME)
    if not pin_path.exists():
        LOGGER.warning("Token reference pin file not found: %s", pin_path)
        pins: PinFile = {}
    else:
        with pin_path.open("rb") as fp:
            pins = json.loads(fp.read())
        LOGGER.info(
            "Loaded %d token reference pins across %d cards",
            sum(len(v) for v in pins.values()),
            len(pins),
        )

    if path is None:
        _pins_cache = pins
    return pins


@dataclass(frozen=True)
class TokenPins:
    """Lookups ``add_token_ids()`` needs to apply pinned token references.

    Attributes:
        pins_lf: ``_card_sid``, ``_tok_oracle``, ``_pin_sid``.
        oracle_lf: ``_tok_sid``, ``_tok_oracle`` — maps the printing named in
            ``all_parts`` onto the oracle id the pin is filed under.  Only
            covers oracles someone actually pinned; the rest fall through to
            the live reference regardless.
    """

    pins_lf: pl.LazyFrame
    oracle_lf: pl.LazyFrame


def extract_token_references(cards_lf: pl.LazyFrame) -> pl.DataFrame:
    """Pull every ``all_parts`` token reference out of the Scryfall dump.

    Returns:
        DataFrame with ``_card_sid``, ``_tok_oracle``, ``_tok_sid``.  Rows whose
        token is missing from the dump (a dangling Scryfall reference) are
        dropped, since there is no oracle id to file them under.
    """
    import polars as pl

    cards_lf = cards_lf.lazy()
    names = set(cards_lf.collect_schema().names())
    oracle_col = _pick_column(names, _ORACLE_COLUMNS)
    parts_col = _pick_column(names, _ALL_PARTS_COLUMNS)
    if oracle_col is None or parts_col is None or "id" not in names:
        missing = [
            label
            for label, present in (("id", "id" in names), ("oracle_id", oracle_col), ("all_parts", parts_col))
            if not present
        ]
        raise ValueError(f"cards frame is missing required columns: {', '.join(missing)}")

    oracle = cards_lf.select(
        pl.col("id").alias("_tok_sid"),
        pl.col(oracle_col).alias("_tok_oracle"),
    )

    references = (
        cards_lf.filter(pl.col(parts_col).is_not_null())
        .select(["id", parts_col])
        .explode(parts_col)
        .filter(pl.col(parts_col).struct.field("component") == "token")
        .select(
            pl.col("id").alias("_card_sid"),
            pl.col(parts_col).struct.field("id").alias("_tok_sid"),
        )
    )

    return (
        references.join(oracle, on="_tok_sid", how="left")
        .filter(pl.col("_tok_oracle").is_not_null())
        .select(["_card_sid", "_tok_oracle", "_tok_sid"])
        # Sort before deduplicating: ``unique`` keeps an arbitrary row otherwise,
        # so a card naming two printings of one token oracle would flip between
        # runs and churn the pin file it is meant to keep still.
        .sort(["_card_sid", "_tok_oracle", "_tok_sid"])
        .unique(subset=["_card_sid", "_tok_oracle"], keep="first", maintain_order=True)
        .collect()
    )


def extract_printing_ids(cards_lf: pl.LazyFrame) -> set[str]:
    """Every Scryfall id present in the dump.

    ``build_pins()`` uses this to tell a pinned printing that has been retired
    from one that is merely no longer named by any ``all_parts`` entry.
    """
    import polars as pl

    return set(cards_lf.lazy().select(pl.col("id")).drop_nulls().collect().to_series().to_list())


def build_token_pins(cards_lf: pl.LazyFrame, pins: PinFile | None = None) -> TokenPins | None:
    """Build the lookups ``add_token_ids()`` applies, or None when unpinned."""
    import polars as pl

    if pins is None:
        pins = load_pins()
    if not pins:
        return None

    cards_lf = cards_lf.lazy()
    oracle_col = _pick_column(set(cards_lf.collect_schema().names()), _ORACLE_COLUMNS)
    if oracle_col is None:
        # Without oracle ids there is nothing to file pins against; following
        # Scryfall is still correct, just not pinned.
        LOGGER.warning("cards_lf has no oracle id column, token reference pins disabled")
        return None

    rows = [
        {"_card_sid": card_sid, "_tok_oracle": oracle, "_pin_sid": pin_sid}
        for card_sid, entries in pins.items()
        for oracle, pin_sid in entries.items()
    ]
    if not rows:
        return None

    schema = {"_card_sid": pl.String, "_tok_oracle": pl.String, "_pin_sid": pl.String}
    pins_lf = pl.DataFrame(rows, schema=schema).lazy()

    # Collected once and narrowed to the oracles actually pinned: this lookup is
    # joined in every batch, and leaving it lazy rescans the whole dump each time.
    # A printing whose oracle nobody pinned resolves to a null pin anyway.
    pinned_oracles = {row["_tok_oracle"] for row in rows}
    oracle_lf = (
        cards_lf.select(
            pl.col("id").alias("_tok_sid"),
            pl.col(oracle_col).alias("_tok_oracle"),
        )
        .filter(pl.col("_tok_oracle").is_in(pinned_oracles))
        .unique(subset=["_tok_sid"])
        .collect()
        .lazy()
    )

    LOGGER.info("Token reference pins prepared: %d entries", len(rows))
    return TokenPins(pins_lf=pins_lf, oracle_lf=oracle_lf)


def build_pins(
    references: pl.DataFrame,
    pins: PinFile | None,
    live_printings: set[str],
) -> PinFile:
    """Merge freshly extracted references into the existing pin file.

    An existing pin wins, which is the whole point — that is the printing the
    reference was published against.  The exception is a pin whose printing has
    since vanished from Scryfall: there is nothing left to preserve, so it is
    repointed at whatever ``all_parts`` names now.

    Args:
        references: Output of :func:`extract_token_references`.
        pins: Existing pin file; loaded from the packaged resource when None.
        live_printings: Every Scryfall id still in the dump, from
            :func:`extract_printing_ids`.  Required, and deliberately not
            derived from ``references``: a repoint is exactly the case where the
            old printing stops being referenced while still existing, so
            deriving it here would retire — and therefore repoint — every pin
            the moment it did its job.
    """
    if pins is None:
        pins = load_pins()

    updated: PinFile = {card: dict(entries) for card, entries in pins.items()}
    added = repointed = 0

    for row in references.iter_rows(named=True):
        card_pins = updated.setdefault(row["_card_sid"], {})
        existing = card_pins.get(row["_tok_oracle"])
        if existing is None:
            card_pins[row["_tok_oracle"]] = row["_tok_sid"]
            added += 1
        elif existing not in live_printings:
            LOGGER.info(
                "Token pin %s/%s pointed at retired printing %s, repointing to %s",
                row["_card_sid"],
                row["_tok_oracle"],
                existing,
                row["_tok_sid"],
            )
            card_pins[row["_tok_oracle"]] = row["_tok_sid"]
            repointed += 1

    LOGGER.info("Token reference pins: %d added, %d repointed", added, repointed)
    return {card: dict(sorted(entries.items())) for card, entries in sorted(updated.items()) if entries}


def serialize_pins(pins: PinFile) -> str:
    """Render a pin file with stable ordering so diffs stay readable."""
    return json.dumps(pins, indent=0, sort_keys=True, ensure_ascii=False) + "\n"


def dump_pins(pins: PinFile, path: Path) -> None:
    """Write a pin file with stable ordering so diffs stay readable."""
    path.write_text(serialize_pins(pins), encoding="utf-8")


def pin_stats(pins: PinFile) -> dict[str, Any]:
    """Summarise a pin file for logging."""
    return {"cards": len(pins), "references": sum(len(v) for v in pins.values())}
