"""
Polars utilities for Scryfall data processing.

Schema generation from TypedDicts and vectorized DataFrame -> MTGJSON conversion.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Required, get_args, get_origin

import polars as pl

from .submodels import (
    SCRYFALL_TO_MTGJSON_FIELDS,
    ScryfallCard,
    ScryfallCardFace,
    ScryfallImageUris,
    ScryfallLegalities,
    ScryfallPrices,
    ScryfallRelatedCard,
    ScryfallSet,
)

if TYPE_CHECKING:
    from polars import DataFrame, LazyFrame, Schema


def python_to_polars(tp: Any) -> pl.DataType:
    """Convert Python type annotation to Polars dtype."""
    origin = get_origin(tp)
    args = get_args(tp)

    # Handle None/NoneType
    if tp is None or tp is type(None):
        return pl.Null  # type: ignore[return-value]

    # Unwrap Required[]
    if origin is Required:
        return python_to_polars(args[0])

    # Unwrap Optional (Union with None)
    if origin is type(None) or (hasattr(origin, "__origin__") and origin.__origin__ is type(None)):
        return pl.Null  # type: ignore[return-value]

    # Union types (X | Y or Optional[X])
    if _is_union(tp):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return python_to_polars(non_none[0])
        # Multiple non-None types - use first
        return python_to_polars(non_none[0]) if non_none else pl.Null  # type: ignore[return-value]

    # Literal types -> String (for enums)
    if origin is Literal:
        return pl.String  # type: ignore[return-value]

    # List types
    if origin is list:
        inner = args[0] if args else Any
        return pl.List(python_to_polars(inner))

    # Dict types
    if origin is dict:
        return pl.Struct({})  # Empty struct, will be inferred

    # TypedDict
    if _is_typeddict(tp):
        return typeddict_to_struct(tp)

    # Primitives
    type_map: dict[type, pl.DataType] = {
        str: pl.String,  # type: ignore[dict-item]
        int: pl.Int64,  # type: ignore[dict-item]
        float: pl.Float64,  # type: ignore[dict-item]
        bool: pl.Boolean,  # type: ignore[dict-item]
        bytes: pl.Binary,  # type: ignore[dict-item]
    }

    if tp in type_map:
        return type_map[tp]

    # Forward reference or unknown -> String
    return pl.String  # type: ignore[return-value]


def _is_union(tp: Any) -> bool:
    """Check if type is a Union."""
    import types
    import typing

    origin = get_origin(tp)
    return origin is typing.Union or isinstance(tp, types.UnionType)


def _is_typeddict(tp: Any) -> bool:
    """Check if type is a TypedDict."""
    return isinstance(tp, type) and issubclass(tp, dict) and hasattr(tp, "__annotations__")


def _get_typeddict_hints(td: type) -> dict[str, Any]:
    """Get resolved type hints from TypedDict."""
    import typing

    annotations = {}
    for base in reversed(td.__mro__):
        if hasattr(base, "__annotations__"):
            annotations.update(base.__annotations__)
    try:
        module = sys.modules.get(td.__module__, None)
        globalns = getattr(module, "__dict__", {}) if module else {}
        return typing.get_type_hints(td, globalns=globalns, localns={"Required": Required})
    except Exception:
        return annotations


def typeddict_to_struct(td: type) -> pl.Struct:
    """Convert TypedDict to Polars Struct type."""
    hints = _get_typeddict_hints(td)
    fields = {}
    for name, hint in sorted(hints.items()):
        fields[name] = python_to_polars(hint)
    return pl.Struct(fields)


def typeddict_to_schema(td: type) -> Schema:
    """Convert TypedDict to Polars Schema (for top-level DataFrames)."""
    hints = _get_typeddict_hints(td)
    schema = {}
    for name, hint in sorted(hints.items()):
        schema[name] = python_to_polars(hint)
    return pl.Schema(schema)


# Pre-built schemas
SCRYFALL_CARD_SCHEMA: Schema = typeddict_to_schema(ScryfallCard)
SCRYFALL_SET_SCHEMA: Schema = typeddict_to_schema(ScryfallSet)

# Struct types for nested objects
SCRYFALL_IMAGE_URIS_STRUCT: pl.Struct = typeddict_to_struct(ScryfallImageUris)
SCRYFALL_PRICES_STRUCT: pl.Struct = typeddict_to_struct(ScryfallPrices)
SCRYFALL_LEGALITIES_STRUCT: pl.Struct = typeddict_to_struct(ScryfallLegalities)
SCRYFALL_CARD_FACE_STRUCT: pl.Struct = typeddict_to_struct(ScryfallCardFace)
SCRYFALL_RELATED_CARD_STRUCT: pl.Struct = typeddict_to_struct(ScryfallRelatedCard)


# =============================================================================
# DataFrame Loading
# =============================================================================


def read_scryfall_ndjson(path: str, *, lazy: bool = True) -> LazyFrame | DataFrame:
    """Read Scryfall bulk NDJSON file."""
    lf = pl.scan_ndjson(path, schema=SCRYFALL_CARD_SCHEMA, ignore_errors=True)
    return lf if lazy else lf.collect()


def read_scryfall_json(path: str) -> DataFrame:
    """Read Scryfall JSON array file (e.g., all-cards.json)."""
    return pl.read_json(path, schema=SCRYFALL_CARD_SCHEMA)


def from_scryfall_dicts(cards: list[ScryfallCard]) -> DataFrame:
    """Create DataFrame from list of Scryfall card dicts."""
    return pl.DataFrame(cards, schema=SCRYFALL_CARD_SCHEMA)


def rename_to_mtgjson(df: DataFrame | LazyFrame) -> DataFrame | LazyFrame:
    """Rename Scryfall columns to MTGJSON convention."""
    existing = set(df.collect_schema().names()) if isinstance(df, pl.LazyFrame) else set(df.columns)
    renames = {k: v for k, v in SCRYFALL_TO_MTGJSON_FIELDS.items() if k in existing}
    return df.rename(renames)


def extract_identifiers(lf: LazyFrame) -> LazyFrame:
    """Extract identifier fields into MTGJSON Identifiers struct."""
    return lf.with_columns(
        pl.struct(
            pl.col("id").alias("scryfallId"),
            pl.col("oracle_id").alias("scryfallOracleId"),
            pl.col("illustration_id").alias("scryfallIllustrationId"),
            pl.col("card_back_id").alias("scryfallCardBackId"),
            pl.col("mtgo_id").cast(pl.String).alias("mtgoId"),
            pl.col("mtgo_foil_id").cast(pl.String).alias("mtgoFoilId"),
            pl.col("arena_id").cast(pl.String).alias("mtgArenaId"),
            pl.col("tcgplayer_id").cast(pl.String).alias("tcgplayerProductId"),
            pl.col("tcgplayer_etched_id").cast(pl.String).alias("tcgplayerEtchedProductId"),
            pl.col("cardmarket_id").cast(pl.String).alias("mcmId"),
            pl.col("multiverse_ids").list.first().cast(pl.String).alias("multiverseId"),
        ).alias("identifiers")
    )


def extract_legalities(lf: LazyFrame) -> LazyFrame:
    """Flatten legalities struct to individual columns or keep as struct."""
    # MTGJSON uses legalities as a flat dict, Scryfall uses nested struct
    # This extracts and keeps as struct for later serialization
    return lf.with_columns(pl.col("legalities").alias("legalities"))


def transform_boolean_fields(lf: LazyFrame) -> LazyFrame:
    """Transform Scryfall booleans to MTGJSON naming convention."""
    renames = {
        "full_art": "isFullArt",
        "oversized": "isOversized",
        "promo": "isPromo",
        "reprint": "isReprint",
        "reserved": "isReserved",
        "story_spotlight": "isStorySpotlight",
        "textless": "isTextless",
        "digital": "isOnlineOnly",
        "variation": "isAlternative",
        "content_warning": "hasContentWarning",
        "game_changer": "isGameChanger",
    }
    existing = set(lf.collect_schema().names())
    return lf.rename({k: v for k, v in renames.items() if k in existing})


def compute_availability(lf: LazyFrame) -> LazyFrame:
    """Compute MTGJSON availability from Scryfall games field."""
    return lf.with_columns(pl.col("games").alias("availability"))


def compute_finishes_flags(lf: LazyFrame) -> LazyFrame:
    """Compute hasFoil/hasNonFoil from finishes list."""
    return lf.with_columns(
        pl.col("finishes").list.contains("foil").alias("hasFoil"),
        pl.col("finishes").list.contains("nonfoil").alias("hasNonFoil"),
    )


def parse_type_line(lf: LazyFrame) -> LazyFrame:
    """Parse type_line into supertypes, types, subtypes."""
    # Split on " — " for subtypes, then parse left side
    return lf.with_columns(
        # Full type line
        pl.col("type_line").alias("type"),
        # Subtypes (after " — ")
        pl.when(pl.col("type_line").str.contains(" — "))
        .then(pl.col("type_line").str.split(" — ").list.get(1).str.split(" "))
        .otherwise(pl.lit([]))
        .alias("subtypes"),
    )


def normalize_mana_cost(lf: LazyFrame) -> LazyFrame:
    """Ensure mana_cost is present (empty string if null)."""
    return lf.with_columns(pl.col("mana_cost").fill_null("").alias("manaCost"))


def compute_mana_value(lf: LazyFrame) -> LazyFrame:
    """Alias cmc to manaValue."""
    return lf.with_columns(pl.col("cmc").alias("manaValue"))


def scryfall_to_mtgjson_pipeline(lf: LazyFrame) -> LazyFrame:
    """
    Full transformation pipeline: Scryfall LazyFrame -> MTGJSON-compatible LazyFrame.

    Applies all transformations in optimal order for query optimization.
    """
    return (
        lf
        # Rename fields first (cheap, enables further transforms)
        .pipe(rename_to_mtgjson)
        # Boolean field renames
        .pipe(transform_boolean_fields)  # type: ignore[arg-type]
        # Computed fields
        .pipe(compute_finishes_flags)
        .pipe(compute_availability)
        .pipe(compute_mana_value)
        .pipe(normalize_mana_cost)
        # Complex extractions
        .pipe(extract_identifiers)
        .pipe(parse_type_line)
    )


def row_to_card_set(row: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a single DataFrame row to MTGJSON CardSet dict.

    For use with df.iter_rows(named=True) when you need dicts.
    Filters out None values per MTGJSON convention.
    """
    return {k: v for k, v in row.items() if v is not None}


def df_to_card_dicts(df: DataFrame) -> list[dict[str, Any]]:
    """
    Convert DataFrame to list of MTGJSON card dicts.

    More efficient than row-by-row iteration for large DataFrames.
    """
    # to_dicts() is optimized in Polars
    return [{k: v for k, v in row.items() if v is not None} for row in df.to_dicts()]


def df_to_cards_by_set(df: DataFrame, set_col: str = "setCode") -> dict[str, list[dict]]:
    """
    Group cards by set code for AllPrintings structure.

    Returns: {set_code: [card_dict, ...], ...}
    """
    result: dict[str, list[dict]] = {}
    for row in df.to_dicts():
        set_code = row.get(set_col, "")
        if set_code not in result:
            result[set_code] = []
        result[set_code].append({k: v for k, v in row.items() if v is not None and k != set_col})
    return result


def partition_by_set(lf: LazyFrame, set_col: str = "setCode") -> dict[str, DataFrame]:
    """
    Partition LazyFrame by set code, returning dict of DataFrames.

    More memory efficient than collecting everything at once for large datasets.
    """
    # Get unique set codes
    set_codes = lf.select(pl.col(set_col).unique()).collect().to_series().to_list()

    return {code: lf.filter(pl.col(set_col) == code).collect() for code in set_codes}


def unnest_struct(df: DataFrame, col: str, prefix: str = "") -> DataFrame:
    """Unnest a struct column into separate columns."""
    return df.unnest(col).rename(
        {field: f"{prefix}{field}" for field in df.select(pl.col(col)).to_series().struct.fields} if prefix else {}
    )


def struct_to_dict(series: pl.Series) -> list[dict | None]:
    """Convert struct Series to list of dicts (with None filtering)."""
    return [{k: v for k, v in row.items() if v is not None} if row else None for row in series.to_list()]


def legalities_to_mtgjson(legalities_series: pl.Series) -> list[dict[str, str]]:
    """
    Convert Scryfall legalities struct to MTGJSON format.

    Scryfall: {standard: "legal", modern: "not_legal", ...}
    MTGJSON:  {standard: "Legal", modern: "Not Legal", ...}
    """
    status_map = {
        "legal": "Legal",
        "not_legal": "Not Legal",
        "restricted": "Restricted",
        "banned": "Banned",
    }

    result: list[dict[str, Any]] = []
    for row in legalities_series.to_list():
        if row is None:
            result.append({})
        else:
            result.append(
                {
                    fmt: status_map.get(status, status)
                    for fmt, status in row.items()
                    if status and status != "not_legal"  # MTGJSON omits not_legal
                }
            )
    return result


def process_in_batches(
    lf: LazyFrame,
    batch_size: int = 10000,
    transform: Callable[[DataFrame], DataFrame] | None = None,
) -> list[dict[str, Any]]:
    """
    Process LazyFrame in batches to control memory usage.

    Args:
        lf: Source LazyFrame
        batch_size: Rows per batch
        transform: Optional transform function for each batch DataFrame

    Yields:
        Batches of card dicts
    """
    # Get total count
    total = lf.select(pl.len()).collect().item()
    results = []

    for offset in range(0, total, batch_size):
        batch_df = lf.slice(offset, batch_size).collect()

        if transform:
            batch_df = transform(batch_df)

        results.extend(df_to_card_dicts(batch_df))

    return results


def streaming_to_ndjson(
    lf: LazyFrame,
    output_path: str,
    transform: Callable[[DataFrame], DataFrame] | None = None,
    batch_size: int = 10000,
) -> int:
    """
    Stream LazyFrame to NDJSON file with optional transformation.

    Returns: Number of rows written
    """
    import orjson

    total = lf.select(pl.len()).collect().item()
    written = 0

    with open(output_path, "wb") as f:
        for offset in range(0, total, batch_size):
            batch_df = lf.slice(offset, batch_size).collect()

            if transform:
                batch_df = transform(batch_df)

            for row in batch_df.to_dicts():
                clean = {k: v for k, v in row.items() if v is not None}
                f.write(orjson.dumps(clean))
                f.write(b"\n")
                written += 1

    return written
