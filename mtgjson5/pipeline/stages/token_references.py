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

One churn case stays unpinned by design: when the printing ``all_parts`` names
today is missing from the dump entirely, there is no oracle id to reach the
pin through, so the reference resolves to nothing — exactly what it did before
pins existed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mtgjson5 import constants
from mtgjson5.consts import TOKEN_LAYOUTS

if TYPE_CHECKING:
    import polars as pl

LOGGER = logging.getLogger(__name__)

PIN_RESOURCE_NAME = "token_references.json"

# Pin file layout::
#
#     {"<card scryfall id>": {"<token oracle id>": "<token scryfall id>"}}
#
# (card, token oracle id) is a unique key: a card that names two printings of
# one token oracle is not representable here, so it is left unpinned rather
# than silently collapsed — see :func:`extract_token_references`.
PinFile = dict[str, dict[str, str]]

_pins_cache: PinFile | None = None

# The raw Scryfall dump is snake_case; GlobalCache normalizes it to camelCase
# before the pipeline sees it, so both spellings reach this module.
_ORACLE_COLUMNS = ("oracle_id", "oracleId")
_ALL_PARTS_COLUMNS = ("all_parts", "allParts")
_FACES_COLUMNS = ("card_faces", "cardFaces")


def _pick_column(names: set[str], candidates: tuple[str, ...]) -> str | None:
    """Return the first candidate spelling present in ``names``."""
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def _face_oracle_expr(schema: pl.Schema) -> pl.Expr | None:
    """Oracle id carried on a printing's first face, when the dump exposes it."""
    import polars as pl

    faces_col = _pick_column(set(schema.names()), _FACES_COLUMNS)
    if faces_col is None:
        return None

    faces_dtype = schema.get(faces_col)
    if not isinstance(faces_dtype, pl.List) or not isinstance(faces_dtype.inner, pl.Struct):
        return None

    face_field = _pick_column({f.name for f in faces_dtype.inner.fields}, _ORACLE_COLUMNS)
    if face_field is None:
        # The field only appears once schema inference has seen a reversible
        # printing.  Readers must sample deeply enough (the pipeline scans
        # 100k records); a shallower scan silently loses the fallback.
        return None

    return pl.col(faces_col).list.first().struct.field(face_field)


def oracle_id_expr(schema: pl.Schema) -> pl.Expr | None:
    """Oracle id of a printing, falling back to its first face.

    Scryfall gives ``reversible_card`` printings no top-level ``oracle_id`` —
    the identity lives on each face instead.  Without the fallback their token
    references cannot be filed under any oracle, so they stay exposed to the
    repointing this module exists to stop.

    Returns:
        An unaliased expression, or None when the frame carries no oracle id
        column at all.
    """
    import polars as pl

    oracle_col = _pick_column(set(schema.names()), _ORACLE_COLUMNS)
    if oracle_col is None:
        return None

    face_oracle = _face_oracle_expr(schema)
    if face_oracle is None:
        return pl.col(oracle_col)
    return pl.coalesce(pl.col(oracle_col), face_oracle)


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
        pins_lf: ``_card_sid``, ``_tok_oracle``, ``_pinned_uuid``.  The pinned
            printing is resolved to its MTGJSON UUID once here rather than
            joined against the million-row UUID map inside every batch.
        oracle_lf: ``_tok_sid``, ``_tok_oracle`` — maps the printing named in
            ``all_parts`` onto the oracle id the pin is filed under.  Only
            covers oracles someone actually pinned; the rest fall through to
            the live reference regardless.
    """

    pins_lf: pl.LazyFrame
    oracle_lf: pl.LazyFrame


@dataclass(frozen=True)
class PinMerge:
    """Outcome of merging fresh references into an existing pin file.

    Attributes:
        pins: The merged pin file.
        added: References that had no pin yet.
        repointed: Pins whose printing had left Scryfall, so they were moved
            to the live reference.  A healthy refresh repoints nothing; a large
            count means the dump is partial, not that Scryfall retired a set.
    """

    pins: PinFile = field(default_factory=dict)
    added: int = 0
    repointed: int = 0


def _token_references_lf(cards_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Join every ``all_parts`` token reference onto its token's oracle id."""
    import polars as pl

    cards_lf = cards_lf.lazy()
    schema = cards_lf.collect_schema()
    names = set(schema.names())
    oracle = oracle_id_expr(schema)
    parts_col = _pick_column(names, _ALL_PARTS_COLUMNS)
    if oracle is None or parts_col is None or "id" not in names:
        missing = [
            label
            for label, present in (
                ("id", "id" in names),
                ("oracle_id", oracle is not None),
                ("all_parts", parts_col is not None),
            )
            if not present
        ]
        raise ValueError(f"cards frame is missing required columns: {', '.join(missing)}")

    oracle_lf = cards_lf.select(
        pl.col("id").alias("_tok_sid"),
        oracle.alias("_tok_oracle"),
    )

    references = cards_lf.filter(pl.col(parts_col).is_not_null())
    if "layout" in names:
        # Token-layout cards never publish relatedCards.tokens (signatures.py
        # nulls it), so pinning their references only pads the pin file.
        references = references.filter(~pl.col("layout").is_in(list(TOKEN_LAYOUTS)))

    return (
        references.select(["id", parts_col])
        .explode(parts_col)
        .filter(pl.col(parts_col).struct.field("component") == "token")
        .select(
            pl.col("id").alias("_card_sid"),
            pl.col(parts_col).struct.field("id").alias("_tok_sid"),
        )
        .join(oracle_lf, on="_tok_sid", how="left")
        .select(["_card_sid", "_tok_oracle", "_tok_sid"])
    )


def extract_token_references(cards_lf: pl.LazyFrame) -> pl.DataFrame:
    """Pull every pinnable ``all_parts`` token reference out of the dump.

    Returns:
        DataFrame with ``_card_sid``, ``_tok_oracle``, ``_tok_sid``, sorted for
        a stable pin file.  Two kinds of row are dropped: a token missing from
        the dump, which has no oracle id to file under, and a card naming two
        printings of one oracle, which the pin layout cannot represent.
    """
    return _finalize_references(_token_references_lf(cards_lf).collect())


def _finalize_references(joined: pl.DataFrame) -> pl.DataFrame:
    """Drop the references that cannot be pinned, saying so when it happens."""
    import polars as pl

    unfiled = joined.filter(pl.col("_tok_oracle").is_null())
    if unfiled.height:
        LOGGER.warning(
            "%d token reference(s) name a printing missing from the dump and cannot be pinned: %s",
            unfiled.height,
            sorted(set(unfiled["_tok_sid"].to_list()))[:10],
        )

    references = joined.filter(pl.col("_tok_oracle").is_not_null())

    # A card may legitimately name two printings of the same token oracle.  The
    # pin file is keyed on (card, oracle) and cannot hold both, and pinning one
    # of them would drop the other from the published output — so leave the
    # pair unpinned and let it follow Scryfall.
    ambiguous = pl.col("_tok_sid").n_unique().over(["_card_sid", "_tok_oracle"]) > 1
    dropped = references.filter(ambiguous)
    if dropped.height:
        LOGGER.warning(
            "%d reference(s) across %d card(s) name two printings of one token oracle; leaving them unpinned",
            dropped.height,
            dropped["_card_sid"].n_unique(),
        )

    return (
        references.filter(~ambiguous)
        .unique(subset=["_card_sid", "_tok_oracle", "_tok_sid"])
        .sort(["_card_sid", "_tok_oracle", "_tok_sid"])
    )


def _printing_ids_lf(cards_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Every Scryfall id present in the dump, one column named ``id``."""
    import polars as pl

    return cards_lf.lazy().select(pl.col("id")).drop_nulls().unique()


def extract_printing_ids(cards_lf: pl.LazyFrame) -> set[str]:
    """Every Scryfall id present in the dump.

    ``build_pins()`` uses this to tell a pinned printing that has been retired
    from one that is merely no longer named by any ``all_parts`` entry.
    """
    return set(_printing_ids_lf(cards_lf).collect().to_series().to_list())


def scan_references_and_printings(cards_lf: pl.LazyFrame) -> tuple[pl.DataFrame, set[str]]:
    """Both inputs :func:`build_pins` needs, from a single pass over the dump."""
    import polars as pl

    joined, printings = pl.collect_all([_token_references_lf(cards_lf), _printing_ids_lf(cards_lf)])
    return _finalize_references(joined), set(printings.to_series().to_list())


def build_token_pins(
    cards_lf: pl.LazyFrame,
    scryfall_uuid_lf: pl.LazyFrame,
    pins: PinFile | None = None,
) -> TokenPins | None:
    """Build the lookups ``add_token_ids()`` applies, or None when unpinned.

    Args:
        cards_lf: Any frame carrying ``id`` and an oracle id column — the raw
            dump, or a narrow projection of it.
        scryfall_uuid_lf: The global ``scryfallId`` -> ``uuid`` map.  Pinned
            printings are resolved through it here, once, so batches do not
            re-join it.  A pin the map cannot resolve yields a null UUID and
            falls back to the live reference.
        pins: Pin file to apply; loaded from the packaged resource when None.
    """
    import polars as pl

    if pins is None:
        pins = load_pins()
    if not pins:
        return None

    cards_lf = cards_lf.lazy()
    oracle = oracle_id_expr(cards_lf.collect_schema())
    if oracle is None:
        # Without oracle ids there is nothing to file pins against; following
        # Scryfall is still correct, just not pinned.
        LOGGER.warning("cards_lf has no oracle id column, token reference pins disabled")
        return None

    rows = [
        {"_card_sid": card_sid, "_tok_oracle": token_oracle, "_pin_sid": pin_sid}
        for card_sid, entries in pins.items()
        for token_oracle, pin_sid in entries.items()
    ]
    if not rows:
        return None

    schema = {"_card_sid": pl.String, "_tok_oracle": pl.String, "_pin_sid": pl.String}
    pins_lf = pl.DataFrame(rows, schema=schema).lazy()

    pins_lf = (
        pins_lf.join(
            scryfall_uuid_lf.select(
                pl.col("scryfallId").alias("_pin_sid"),
                pl.col("uuid").alias("_pinned_uuid"),
            ),
            on="_pin_sid",
            how="left",
        )
        .drop("_pin_sid")
        .collect()
        .lazy()
    )

    # Collected once and narrowed to the oracles actually pinned: this lookup is
    # joined in every batch, and leaving it lazy rescans the whole dump each time.
    # A printing whose oracle nobody pinned resolves to a null pin anyway.
    pinned_oracles = {row["_tok_oracle"] for row in rows}
    oracle_lf = (
        cards_lf.select(
            pl.col("id").alias("_tok_sid"),
            oracle.alias("_tok_oracle"),
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
) -> PinMerge:
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
    return PinMerge(
        pins={card: dict(sorted(entries.items())) for card, entries in sorted(updated.items()) if entries},
        added=added,
        repointed=repointed,
    )


def serialize_pins(pins: PinFile) -> str:
    """Render a pin file with stable ordering so diffs stay readable."""
    return json.dumps(pins, indent=0, sort_keys=True, ensure_ascii=False) + "\n"


def dump_pins(pins: PinFile, path: Path) -> None:
    """Write a pin file with stable ordering so diffs stay readable."""
    path.write_text(serialize_pins(pins), encoding="utf-8")


def pin_stats(pins: PinFile) -> dict[str, Any]:
    """Summarise a pin file for logging."""
    return {"cards": len(pins), "references": sum(len(v) for v in pins.values())}
