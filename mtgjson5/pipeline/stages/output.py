"""
Serialization, renaming, filtering, and sink operations.

Handles JSON serialization cleanup, final column renaming,
token filtering, ID mapping extraction, and parquet writing.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any

import polars as pl

from mtgjson5 import constants
from mtgjson5.consts import (
    EXCLUDE_FROM_OUTPUT,
    OMIT_EMPTY_LIST_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SCRYFALL_COLUMNS_TO_DROP,
    SORTED_LIST_FIELDS,
    TOKEN_LAYOUTS,
)
from mtgjson5.data import PipelineContext
from mtgjson5.models.schemas import (
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    TOKEN_EXCLUDE,
)
from mtgjson5.pipeline.stages.derived import link_foil_nonfoil_versions
from mtgjson5.pipeline.stages.relationships import add_variations
from mtgjson5.utils import LOGGER, to_camel_case


def prepare_cards_for_json(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean DataFrame for JSON serialization.

    Applies MTGJSON conventions:
    - Fill null with [] for required list fields
    - Nullify empty lists for omit fields
    - Nullify False for optional bool fields
    - Nullify empty strings/zero values for optional fields
    - Drop internal columns (prefixed with _)

    Args:
        df: Cards DataFrame

    Returns:
        Cleaned DataFrame ready for serialization
    """
    expressions: list[pl.Expr] = []

    # Fill null with [] for required list fields
    for field in REQUIRED_LIST_FIELDS:
        if field in df.columns:
            expressions.append(pl.col(field).fill_null([]).alias(field))

    # Nullify empty lists for omit fields
    for field in OMIT_EMPTY_LIST_FIELDS:
        if field in df.columns:
            expressions.append(pl.when(pl.col(field).list.len() == 0).then(None).otherwise(pl.col(field)).alias(field))

    # Nullify False for optional bool fields
    for field in OPTIONAL_BOOL_FIELDS:
        if field in df.columns:
            expressions.append(
                pl.when(pl.col(field) == True)  # pylint: disable=singleton-comparison
                .then(True)
                .otherwise(None)
                .alias(field)
            )

    # Handle other optional fields by type
    for field in OTHER_OPTIONAL_FIELDS:
        if field not in df.columns:
            continue

        col_type = df.schema[field]

        if col_type in (pl.String, pl.Utf8):
            # Empty strings to null
            expressions.append(pl.when(pl.col(field) == "").then(None).otherwise(pl.col(field)).alias(field))
        elif isinstance(col_type, pl.List):
            # Empty lists to null
            expressions.append(pl.when(pl.col(field).list.len() == 0).then(None).otherwise(pl.col(field)).alias(field))
        elif isinstance(col_type, pl.Struct):
            # Clean struct sub-fields
            struct_fields = col_type.fields
            cleaned_fields = []

            for sf in struct_fields:
                sf_expr = pl.col(field).struct.field(sf.name)

                if sf.dtype in (pl.String, pl.Utf8):
                    cleaned_fields.append(pl.when(sf_expr == "").then(None).otherwise(sf_expr).alias(sf.name))
                elif isinstance(sf.dtype, pl.List):
                    cleaned_fields.append(pl.when(sf_expr.list.len() == 0).then(None).otherwise(sf_expr).alias(sf.name))
                else:
                    cleaned_fields.append(sf_expr.alias(sf.name))

            # Rebuild struct, preserve null if original was null
            expressions.append(
                pl.when(pl.col(field).is_null()).then(None).otherwise(pl.struct(cleaned_fields)).alias(field)
            )
        elif col_type in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            # Nullify zero values
            expressions.append(pl.when(pl.col(field) == 0).then(None).otherwise(pl.col(field)).alias(field))

    # Apply all transformations
    if expressions:
        df = df.with_columns(expressions)

    # Drop internal columns
    df = df.select([c for c in df.columns if not c.startswith("_")])

    return df


def clean_nested(
    obj: Any,
    omit_empty: bool = True,
    field_handlers: dict[str, Callable[[Any], Any]] | None = None,
    current_path: str = "",
) -> Any:
    """
    Recursively clean any nested structure.

    Args:
        obj: Any Python object to clean
        omit_empty: If True, omit empty lists/dicts/None values
        field_handlers: Dict of field_path -> handler_function
        current_path: Internal tracking of nested path

    Returns:
        Cleaned version of the input object
    """
    if obj is None:
        return None

    # Handle dictionaries
    if isinstance(obj, dict):
        result: dict[str, Any] = {}

        for key, value in sorted(obj.items()):  # Sort keys
            # Skip excluded fields
            if key in EXCLUDE_FROM_OUTPUT:
                continue

            field_path = f"{current_path}.{key}" if current_path else key

            # Custom handler
            if field_handlers and field_path in field_handlers:
                cleaned_value = field_handlers[field_path](value)
            else:
                cleaned_value = clean_nested(
                    value,
                    omit_empty=omit_empty,
                    field_handlers=field_handlers,
                    current_path=field_path,
                )

            # Omit None, but keep required list fields as []
            if cleaned_value is None and omit_empty:
                if key in REQUIRED_LIST_FIELDS:
                    result[key] = []
                # Keep legalities as {} instead of None
                elif key == "legalities":
                    result[key] = {}
                continue

            # Omit False for optional bool fields (except required set-level booleans)
            if (
                omit_empty
                and key in OPTIONAL_BOOL_FIELDS
                and cleaned_value is False
                and key not in REQUIRED_SET_BOOL_FIELDS
            ):
                continue

            # Omit empty collections (except required list fields and legalities)
            if omit_empty and isinstance(cleaned_value, dict | list) and not cleaned_value:
                if isinstance(cleaned_value, list) and key in REQUIRED_LIST_FIELDS:
                    pass  # Keep empty required list
                elif isinstance(cleaned_value, dict) and key == "legalities":
                    result[key] = {}  # Keep empty legalities as {}
                else:
                    continue

            result[key] = cleaned_value

        return result if result or not omit_empty else None

    # Handle lists
    if isinstance(obj, list):
        result_list: list[Any] = []

        for item in obj:
            cleaned_item = clean_nested(
                item,
                omit_empty=omit_empty,
                field_handlers=field_handlers,
                current_path=current_path,
            )

            if cleaned_item is None and omit_empty:
                continue

            result_list.append(cleaned_item)

        # Sort list if it's a sortable field
        field_name = current_path.split(".")[-1] if current_path else ""
        if field_name in SORTED_LIST_FIELDS and result_list:
            with contextlib.suppress(TypeError):
                result_list = sorted(result_list)

        # Sort rulings by date (desc), then text
        elif field_name == "rulings" and result_list and isinstance(result_list[0], dict):
            result_list = sorted(result_list, key=lambda r: (r.get("date", ""), r.get("text", "")))

        if not result_list and omit_empty and field_name not in REQUIRED_LIST_FIELDS:
            return None

        return result_list

    # Handle tuples -> list
    if isinstance(obj, tuple):
        return clean_nested(list(obj), omit_empty, field_handlers, current_path)

    # Handle sets -> sorted list
    if isinstance(obj, set):
        return clean_nested(sorted(obj), omit_empty, field_handlers, current_path)

    # Primitives pass through
    return obj


def dataframe_to_cards_list(
    df: pl.DataFrame,
    sort_by: tuple[str, ...] = ("number", "side"),
    use_model: type | None = None,
) -> list[dict[str, Any]]:
    """
    Convert cards DataFrame to cleaned list of dicts.

    Args:
        df: Cards DataFrame
        sort_by: Columns to sort by (default: number, side)
        use_model: Optional Pydantic model class for serialization.
                   If provided, uses model's to_polars_dict method.

    Returns:
        List of cleaned card dictionaries
    """
    # Sort for consistent output
    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        if "number" in sort_cols:
            df = df.with_columns(pl.col("number").str.zfill(10).alias("_sort_num"))
            sort_cols = ["_sort_num" if c == "number" else c for c in sort_cols]
        df = df.sort(sort_cols, nulls_last=True)
        if "_sort_num" in df.columns:
            df = df.drop("_sort_num")

    # Use model serialization if provided
    if use_model is not None:
        models = use_model.from_dataframe(df)  # type: ignore[attr-defined]
        return [m.to_polars_dict(exclude_none=True) for m in models]

    # Otherwise use DataFrame-level cleaning + clean_nested
    df = prepare_cards_for_json(df)
    raw_dicts = df.to_dicts()
    return [clean_nested(card, omit_empty=True) for card in raw_dicts]


def drop_raw_scryfall_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Drop all raw Scryfall columns after they've been transformed to MTGJSON format.
    """
    return lf.drop(SCRYFALL_COLUMNS_TO_DROP, strict=False)


# 6.2:
def rename_all_the_things(lf: pl.LazyFrame, output_type: str = "card_set") -> pl.LazyFrame:
    """
    Final transformation: Renames internal columns to MTGJSON CamelCase,
    builds nested structs, and selects only fields valid for the output_type.
    """

    special_renames = {
        "set": "setCode",
        "cmc": "convertedManaCost",
    }

    schema = lf.collect_schema()
    rename_map = {}
    for col in schema.names():
        if col in special_renames:
            rename_map[col] = special_renames[col]
        elif "_" in col:
            rename_map[col] = to_camel_case(col)

    if rename_map:
        lf = lf.rename(rename_map, strict=False)

    multiface_layouts = [
        "split",
        "aftermath",
        "transform",
        "modal_dfc",
        "meld",
        "adventure",
    ]

    lf = lf.with_columns(
        [
            pl.when(pl.col("layout").is_in(multiface_layouts))
            .then(pl.col("faceManaValue"))
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias("faceManaValue"),
            pl.when(pl.col("layout").is_in(multiface_layouts))
            .then(pl.col("faceManaValue"))
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias("faceConvertedManaCost"),
        ]
    )
    # Define field exclusions per output type
    if output_type == "card_set":
        allowed_fields = ALL_CARD_FIELDS
    elif output_type == "card_token":
        allowed_fields = ALL_CARD_FIELDS - TOKEN_EXCLUDE
    elif output_type == "card_atomic":
        allowed_fields = ALL_CARD_FIELDS - ATOMIC_EXCLUDE
    elif output_type == "card_deck":
        allowed_fields = ALL_CARD_FIELDS - CARD_DECK_EXCLUDE
    elif output_type == "full_card":
        allowed_fields = ALL_CARD_FIELDS
    else:
        raise ValueError(f"Unknown output type: {output_type}")

    renamed_schema = lf.collect_schema()
    existing_cols = set(renamed_schema.names())
    final_cols = sorted(existing_cols & allowed_fields)

    LOGGER.debug(f"Selecting {len(final_cols)} columns for output type '{output_type}':{', '.join(final_cols)}")

    return lf.select(final_cols)


# 6.7:
def filter_out_tokens(df: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Separate tokens from main cards.

    Tokens are identified by:
    - layout in {"token", "double_faced_token", "emblem", "art_series"}
    - type == "Dungeon"
    - "Token" in type string
    - type == "Card" (non-playable helper cards: theme cards, substitute cards, counters)

    Returns:
        Tuple of (cards_df, tokens_df) - cards without tokens, and the filtered tokens
    """

    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
        | (pl.col("type") == "Card")
    )

    tokens_df = df.filter(is_token)
    cards_df = df.filter(~is_token)

    return cards_df, tokens_df


def _build_id_mappings(ctx: PipelineContext, lf: pl.LazyFrame) -> None:
    """
    Extract ID -> UUID mappings for price builder.

    Builds parquet files for:
    - tcg_to_uuid: TCGPlayer product ID -> UUID
    - tcg_etched_to_uuid: TCGPlayer etched product ID -> UUID
    - mtgo_to_uuid: MTGO ID -> UUID
    - scryfall_to_uuid: Scryfall ID -> UUID

    Uses a single .collect() to extract all 4 ID fields at once,
    then splits into per-mapping DataFrames.
    """
    cache_path = constants.CACHE_PATH

    mapping_configs = [
        ("tcgplayerProductId", "tcg_to_uuid", "tcg_to_uuid_lf"),
        ("tcgplayerEtchedProductId", "tcg_etched_to_uuid", "tcg_etched_to_uuid_lf"),
        ("mtgoId", "mtgo_to_uuid", "mtgo_to_uuid_lf"),
        ("scryfallId", "scryfall_to_uuid", "scryfall_to_uuid_lf"),
    ]

    try:
        combined_df = lf.select(
            [
                pl.col("uuid"),
                *[pl.col("identifiers").struct.field(cfg[0]).alias(cfg[0]) for cfg in mapping_configs],
            ]
        ).collect()
    except Exception as e:
        LOGGER.warning(f"Failed to collect ID mappings: {e}")
        return

    for id_col, parquet_name, cache_attr in mapping_configs:
        try:
            mapping_df = combined_df.select(["uuid", id_col]).filter(pl.col(id_col).is_not_null()).unique()
            if len(mapping_df) > 0:
                path = cache_path / f"{parquet_name}.parquet"
                mapping_df.write_parquet(path)
                if ctx._cache is not None:
                    setattr(ctx._cache, cache_attr, mapping_df.lazy())
                LOGGER.info(f"Built {parquet_name} mapping: {len(mapping_df):,} entries")
        except Exception as e:
            LOGGER.warning(f"Failed to build {parquet_name} mapping: {e}")

    del combined_df


def sink_cards(ctx: PipelineContext) -> None:
    """Sink cards and tokens to partitioned parquet files."""
    cards_dir = constants.CACHE_PATH / "_parquet"
    tokens_dir = constants.CACHE_PATH / "_parquet_tokens"
    for path in [cards_dir, tokens_dir]:
        path.mkdir(parents=True, exist_ok=True)

    lf = ctx.final_cards_lf
    if lf is None:
        LOGGER.warning("sink_cards: final_cards_lf is None, returning early")
        return

    default_langs = ctx.languages_lf

    if default_langs is not None:
        # Join with default language mapping to filter to primary language per card
        lf = lf.join(default_langs, on=["scryfallId", "language"], how="semi")
        LOGGER.info("Filtered to default language per card using default_cards mapping")
    else:
        # Fallback: English only (if default_cards not loaded)
        LOGGER.warning("languages not available, filtering to English only")
        lf = lf.filter(pl.col("language") == "English")

    lf = link_foil_nonfoil_versions(lf)

    lf = add_variations(lf)

    # Build ID -> UUID mappings for price builder
    _build_id_mappings(ctx, lf)

    # Split into cards and tokens, apply final renames
    cards_lf, tokens_lf = filter_out_tokens(lf)
    clf = rename_all_the_things(cards_lf, output_type="card_set")
    tlf = rename_all_the_things(tokens_lf, output_type="card_token")

    for lazy_frame, out_dir, label in [
        (clf, cards_dir, "cards"),
        (tlf, tokens_dir, "tokens"),
    ]:
        LOGGER.info(f"Collecting {label}...")

        # Collect ONCE - don't re-evaluate the lazy plan for each chunk
        df = lazy_frame.collect()
        LOGGER.info(f"  Collected {df.height:,} {label}")

        if df.height == 0:
            LOGGER.info(f"  No {label} to write")
            continue

        # Partition by setCode and write each partition
        LOGGER.info(f"  Partitioning and writing {label}...")
        partitions = df.partition_by("setCode", as_dict=False)
        LOGGER.info(f"  Writing {len(partitions)} set partitions...")

        for set_df in partitions:
            s_code = set_df.get_column("setCode")[0]
            from mtgjson5.polars_utils import get_windows_safe_set_code

            safe_code = get_windows_safe_set_code(s_code)
            set_path = out_dir / f"setCode={safe_code}"
            set_path.mkdir(exist_ok=True)
            set_df.write_parquet(set_path / "0.parquet")

        LOGGER.info(f"  {label} complete")

    LOGGER.info("All parquet sinks complete.")
