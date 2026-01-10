"""
MTGJSON card data pipeline.

This module contains the complete data pipeline for transforming Scryfall bulk data
into MTGJSON format, including card processing, set building, and output generation.
"""

import contextlib
import json
from collections.abc import Callable
from functools import partial
from typing import Any
from uuid import UUID, uuid5

import polars as pl
import polars_hash as plh

from mtgjson5 import constants
from mtgjson5.consts import (
    EXCLUDE_FROM_OUTPUT,
    OMIT_EMPTY_LIST_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SORTED_LIST_FIELDS,
)
from mtgjson5.context import PipelineContext
from mtgjson5.models.schema.scryfall import CardFace
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_models.schemas import (
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    TOKEN_EXCLUDE,
)
from mtgjson5.pipeline.expressions import (
    calculate_cmc_expr,
    extract_colors_from_mana_expr,
    order_finishes_expr,
)
from mtgjson5.providers.cardmarket.monolith import CardMarketProvider
from mtgjson5.utils import LOGGER, to_camel_case, to_snake_case


# Check if polars_hash has uuidhash namespace
_HAS_UUIDHASH = hasattr(plh.col("_test"), "uuidhash")
_DNS_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
# List of raw Scryfall columns to drop after transformation to MTGJSON format
_SCRYFALL_COLUMNS_TO_DROP = [
    # Dropped in add_basic_fields (renamed to MTGJSON names)
    "lang",  # -> language (via replace_strict)
    "frame",  # -> frameVersion
    "fullArt",  # -> isFullArt
    "textless",  # -> isTextless
    "oversized",  # -> isOversized
    "promo",  # -> isPromo
    "reprint",  # -> isReprint
    "storySpotlight",  # -> isStorySpotlight
    "reserved",  # -> isReserved
    "foil",  # dropped (finishes provides hasFoil)
    "nonfoil",  # dropped (finishes provides hasNonFoil)
    "cmc",  # -> manaValue
    "typeLine",  # -> type (face-aware)
    "oracleText",  # -> text (face-aware)
    # NOTE: printedText NOT dropped - MTGJSON name same as Scryfall, face-aware version replaces it
    "printedTypeLine",  # -> printedType (face-aware)
    # Dropped in add_card_attributes (renamed to MTGJSON names)
    "contentWarning",  # -> hasContentWarning
    "handModifier",  # -> hand
    "lifeModifier",  # -> life
    "gameChanger",  # -> isGameChanger
    "digital",  # -> isOnlineOnly
    # Dropped in add_identifiers_struct (consumed into identifiers struct)
    "mcmId",  # intermediate column from CardMarket join
    "mcmMetaId",  # intermediate column from CardMarket join
    "illustrationId",  # -> identifiers.scryfallIllustrationId
    "arenaId",  # -> identifiers.mtgArenaId
    "mtgoId",  # -> identifiers.mtgoId
    "mtgoFoilId",  # -> identifiers.mtgoFoilId
    "tcgplayerId",  # -> identifiers.tcgplayerProductId
    "tcgplayerEtchedId",  # -> identifiers.tcgplayerEtchedProductId
    # NOTE: Fields where Scryfall name == MTGJSON name are NOT dropped:
    # borderColor, colorIdentity, frameEffects, securityStamp, manaCost,
    # flavorText, colorIndicator, flavorName, allParts, artistIds,
    # edhrecRank, promoTypes, attractionLights
    # Internal temp columns
    "_meld_face_name",  # temp column for meld card faceName assignment
]

_ASCII_REPLACEMENTS: dict[str, str] = {
    "Æ": "AE", "æ": "ae", "Œ": "OE", "œ": "oe", "ß": "ss",
    "É": "E", "È": "E", "Ê": "E", "Ë": "E",
    "Á": "A", "À": "A", "Â": "A", "Ä": "A", "Ã": "A",
    "Í": "I", "Ì": "I", "Î": "I", "Ï": "I",
    "Ó": "O", "Ò": "O", "Ô": "O", "Ö": "O", "Õ": "O",
    "Ú": "U", "Ù": "U", "Û": "U", "Ü": "U",
    "Ý": "Y", "Ñ": "N", "Ç": "C",
    "é": "e", "è": "e", "ê": "e", "ë": "e",
    "á": "a", "à": "a", "â": "a", "ä": "a", "ã": "a",
    "í": "i", "ì": "i", "î": "i", "ï": "i",
    "ó": "o", "ò": "o", "ô": "o", "ö": "o", "õ": "o",
    "ú": "u", "ù": "u", "û": "u", "ü": "u",
    "ý": "y", "ÿ": "y", "ñ": "n", "ç": "c",
}


def _uuid5_expr(col_name: str) -> pl.Expr:
    """Generate UUID5 from a column name using DNS namespace."""
    if _HAS_UUIDHASH:
        return plh.col(col_name).uuidhash.uuid5()
    return pl.col(col_name).map_elements(
        lambda x: str(uuid5(_DNS_NAMESPACE, x)) if x else None,
        return_dtype=pl.String,
    )


def _uuid5_concat_expr(col1: pl.Expr, col2: pl.Expr, default: str = "a") -> pl.Expr:
    """Generate UUID5 from concatenation of two columns."""
    if _HAS_UUIDHASH:
        return plh.col(col1.meta.output_name()).uuidhash.uuid5_concat(col2, default=default)
    # Fallback: concat columns then generate uuid5
    return pl.concat_str([col1, col2.fill_null(default)], separator="").map_elements(
        lambda x: str(uuid5(_DNS_NAMESPACE, x)) if x else None,
        return_dtype=pl.String,
    )

def _ascii_name_expr(col: str | pl.Expr) -> pl.Expr:
    """
    Normalize card name to ASCII.

    Uses str.replace_many for efficient batch replacement.
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return expr.str.replace_many(_ASCII_REPLACEMENTS)


def explode_card_faces(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Explode multi-face cards into separate rows per face.

    Single-face cards get face_id=0. Multi-face cards (split, transform,
    modal_dfc, meld, etc.) get one row per face with face_id=0,1,2...

    Uses split/process/concat pattern to avoid list operations on null columns.

    Also adds:
    - _row_id: Original row index for linking faces later
    - face_id: 0-based index of this face
    - side: Letter side identifier ("a", "b", "c", etc.)
    - _face_data: The face struct (for multi-face) or typed null (for single-face)
    """
    face_struct_schema = CardFace.polars_schema()
    lf = lf.with_row_index("_row_id")

    schema = lf.collect_schema()
    if "cardFaces" not in schema.names():
        return lf.with_columns(
            pl.lit(0).alias("faceId"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).cast(face_struct_schema).alias("_face_data"),
        )

    lf = lf.with_columns(
        pl.int_ranges(pl.col("cardFaces").list.len()).alias("_face_idx")
    )

    lf = lf.explode(["cardFaces", "_face_idx"])

    return lf.with_columns(
        pl.col("cardFaces").alias("_face_data"),
        pl.col("_face_idx").fill_null(0).alias("faceId"),
        # side is only set for actual multi-face cards (where _face_idx is not null)
        pl.when(pl.col("_face_idx").is_not_null())
        .then(
            pl.col("_face_idx").replace_strict(
                {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"},
                default="a",
                return_dtype=pl.String,
            )
        )
        .otherwise(pl.lit(None).cast(pl.String))
        .alias("side"),
    ).drop(["cardFaces", "_face_idx"])


def assign_meld_sides(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Assign side field for meld layout cards.

    Meld cards don't have cardFaces, so explode_card_faces leaves side=None.
    This function uses meld_triplets to assign:
    - side="a" for meld parts (front faces)
    - side="b" for melded result (back face)

    The meld_triplets dict maps card names to [part_a, part_b, result].
    """
    if not ctx.meld_triplets:
        return lf

    # Build set of melded result names (3rd element in each triplet)
    melded_results: set[str] = set()
    meld_parts: set[str] = set()
    for triplet in ctx.meld_triplets.values():
        if len(triplet) == 3:
            meld_parts.add(triplet[0])
            meld_parts.add(triplet[1])
            melded_results.add(triplet[2])

    is_meld = pl.col("layout") == "meld"
    is_melded_result = pl.col("name").is_in(list(melded_results))
    is_meld_part = pl.col("name").is_in(list(meld_parts))

    # Only update side for meld cards where side is currently null
    side_is_null = pl.col("side").is_null()

    return lf.with_columns(
        pl.when(is_meld & side_is_null & is_melded_result)
        .then(pl.lit("b"))
        .when(is_meld & side_is_null & is_meld_part)
        .then(pl.lit("a"))
        .otherwise(pl.col("side"))
        .alias("side")
    )


def update_meld_names(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Update name for meld cards and store original name as _meld_face_name.

    For meld front sides (side="a"):
    - _meld_face_name = original name (e.g., "Mishra, Claimed by Gix")
    - name = "{original name} // {melded result name}" (e.g., "Mishra, Claimed by Gix // Mishra, Lost to Phyrexia")

    For meld back sides (side="b"):
    - _meld_face_name = original name
    - name = original name (no change)

    Note: _meld_face_name is used later by add_basic_fields to set faceName correctly.
    """
    if not ctx.meld_triplets:
        # Add empty column for consistency
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_meld_face_name"))

    # Build mapping from front name to melded result name
    front_to_result: dict[str, str] = {}
    for triplet in ctx.meld_triplets.values():
        if len(triplet) == 3:
            part_a, part_b, result = triplet
            front_to_result[part_a] = result
            front_to_result[part_b] = result

    if not front_to_result:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_meld_face_name"))

    # Create a DataFrame for the mapping
    mapping_df = pl.DataFrame({
        "name": list(front_to_result.keys()),
        "_melded_result_name": list(front_to_result.values()),
    })

    lf = lf.join(mapping_df.lazy(), on="name", how="left")

    # Store original name for meld cards before updating
    # Update name for meld front sides to include " // {melded result}"
    is_meld = pl.col("layout") == "meld"
    is_front = pl.col("side") == "a"
    has_result = pl.col("_melded_result_name").is_not_null()

    return (
        lf.with_columns(
            # Store original name for meld cards (used for faceName later)
            pl.when(is_meld)
            .then(pl.col("name"))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_meld_face_name"),
        )
        .with_columns(
            # Update name to "Front // Melded" for meld front sides
            pl.when(is_meld & is_front & has_result)
            .then(pl.col("name") + pl.lit(" // ") + pl.col("_melded_result_name"))
            .otherwise(pl.col("name"))
            .alias("name"),
        )
        .drop("_melded_result_name")
    )


def detect_aftermath_layout(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Detect aftermath cards and update their layout from 'split' to 'aftermath'.

    Scryfall uses layout='split' for both true split cards and aftermath cards.
    MTGJSON distinguishes them: if the back face's oracle_text starts with
    "Aftermath", the layout should be 'aftermath'.

    This function:
    1. Checks if layout is 'split'
    2. Looks at the back face (side='b') oracle_text
    3. If it starts with 'Aftermath', updates layout for all faces of that card
    """
    # Get oracle_text from face data for split cards
    face_oracle = pl.col("_face_data").struct.field("oracle_text")

    # Mark rows where oracle_text starts with "Aftermath"
    has_aftermath = (
        (pl.col("layout") == "split")
        & (pl.col("side") == "b")
        & face_oracle.is_not_null()
        & face_oracle.str.starts_with("Aftermath")
    )

    # Use window function to propagate aftermath detection to all faces of same card
    # _row_id groups faces of the same original card
    is_aftermath_card = has_aftermath.max().over("_row_id")

    return lf.with_columns(
        pl.when(is_aftermath_card)
        .then(pl.lit("aftermath"))
        .otherwise(pl.col("layout"))
        .alias("layout")
    )


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
            expressions.append(
                pl.when(pl.col(field).list.len() == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )

    # Nullify False for optional bool fields
    for field in OPTIONAL_BOOL_FIELDS:
        if field in df.columns:
            expressions.append(
                pl.when(pl.col(field) == True)  # noqa: E712
                .then(True)
                .otherwise(None)
                .alias(field)
            )

    # Handle other optional fields by type
    for field in OTHER_OPTIONAL_FIELDS:
        if field not in df.columns:
            continue

        col_type = df.schema[field]

        if col_type == pl.String or col_type == pl.Utf8:
            # Empty strings to null
            expressions.append(
                pl.when(pl.col(field) == "")
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )
        elif isinstance(col_type, pl.List):
            # Empty lists to null
            expressions.append(
                pl.when(pl.col(field).list.len() == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )
        elif isinstance(col_type, pl.Struct):
            # Clean struct sub-fields
            struct_fields = col_type.fields
            cleaned_fields = []

            for sf in struct_fields:
                sf_expr = pl.col(field).struct.field(sf.name)

                if sf.dtype == pl.String or sf.dtype == pl.Utf8:
                    cleaned_fields.append(
                        pl.when(sf_expr == "")
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf.name)
                    )
                elif isinstance(sf.dtype, pl.List):
                    cleaned_fields.append(
                        pl.when(sf_expr.list.len() == 0)
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf.name)
                    )
                else:
                    cleaned_fields.append(sf_expr.alias(sf.name))

            # Rebuild struct, preserve null if original was null
            expressions.append(
                pl.when(pl.col(field).is_null())
                .then(None)
                .otherwise(pl.struct(cleaned_fields))
                .alias(field)
            )
        elif col_type in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            # Nullify zero values
            expressions.append(
                pl.when(pl.col(field) == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )

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
            if (omit_empty and key in OPTIONAL_BOOL_FIELDS
                and cleaned_value is False and key not in REQUIRED_SET_BOOL_FIELDS):
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
        models = use_model.from_dataframe(df)
        return [m.to_polars_dict(exclude_none=True) for m in models]

    # Otherwise use DataFrame-level cleaning + clean_nested
    df = prepare_cards_for_json(df)
    raw_dicts = df.to_dicts()
    return [clean_nested(card, omit_empty=True) for card in raw_dicts]


def drop_raw_scryfall_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Drop all raw Scryfall columns after they've been transformed to MTGJSON format.

    Call this AFTER all transformation functions have completed but BEFORE
    rename_all_the_things. This is the centralized cleanup function that
    replaces scattered .drop() calls throughout the pipeline.
    """
    return lf.drop(_SCRYFALL_COLUMNS_TO_DROP, strict=False)


# 1.1.1: Format planeswalker ability text
def format_planeswalker_text(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Wrap planeswalker loyalty ability costs in square brackets.

    Transforms: "+1: Draw a card" -> "[+1]: Draw a card"
    Transforms: "−3: Target creature..." -> "[−3]: Target creature..."

    This matches MTGJSON's historical format for planeswalker cards.
    The regex matches ability costs at the start of lines.
    """
    # Pattern matches: +N, −N (unicode minus), or 0 followed by : at line start
    # Note: \u2212 is the Unicode minus sign used by Scryfall
    # Captures the full cost (e.g., +1, −3, +X, 0) and wraps in brackets
    return lf.with_columns(
        pl.col("text")
        .str.replace_all(r"(?m)^([+\u2212−]?[\dX]+):", r"[$1]:")
        .alias("text")
    )


# 1.1.2: Add originalReleaseDate for promo cards
def add_original_release_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set originalReleaseDate for cards with release dates different from their set.

    Per MTGJSON spec, this field is for promotional cards printed outside
    of a cycle window (e.g., Secret Lair drops, FNM promos). It captures
    when THIS specific card printing was released, not when the card was
    first printed.

    Only set when card's releasedAt differs from set's setReleasedAt.
    """
    return lf.with_columns(
        pl.when(
            pl.col("releasedAt").is_not_null()
            & pl.col("setReleasedAt").is_not_null()
            & (pl.col("releasedAt") != pl.col("setReleasedAt"))
        )
        .then(pl.col("releasedAt"))
        .otherwise(pl.lit(None).cast(pl.String))
        .alias("originalReleaseDate")
    )


# 1.2: Add basic fields
def add_basic_fields(lf: pl.LazyFrame, _set_release_date: str = "") -> pl.LazyFrame:
    """
    Add basic card fields: name, setCode, language, etc.

    Maps Scryfall column names to MTGJSON names.
    For multi-face cards, the name is the face-specific name.
    """

    def face_field(field_name: str) -> pl.Expr:
        # For multi-face cards, prefer face-specific data; for single-face, use root field
        # _face_data is now properly typed as a struct (even when null), so coalesce works
        # Note: struct fields are snake_case (from Scryfall JSON), root columns are camelCase
        struct_field = (
            to_snake_case(field_name) if "_" not in field_name else field_name
        )
        return pl.coalesce(
            pl.col("_face_data").struct.field(struct_field),
            pl.col(field_name),
        )

    face_name = face_field("name")
    ascii_name = _ascii_name_expr(face_name)
    return (
        lf.rename(
            {
                # Core identifiers (card-level only)
                "id": "scryfallId",
                "oracleId": "oracleId",
                "set": "setCode",
                "collectorNumber": "number",
                "cardBackId": "cardBackId",
            }
        )
        .with_columns(
            [
                # Card-level name (full name with // for multi-face)
                # Scryfall's root "name" has the full name like "Invasion of Ravnica // Guildpact Paragon"
                pl.col("name").alias("name"),
                # Face-specific name (only for multi-face cards)
                # faceName is the individual face's name
                # For meld cards: use _meld_face_name (original name before " // MeldResult" was added)
                # For other multi-face: use face_field("name")
                pl.when(pl.col("layout") == "meld")
                .then(pl.col("_meld_face_name"))
                .when(
                    pl.col("layout").is_in(
                        [
                            "transform",
                            "modal_dfc",
                            "reversible_card",
                            "flip",
                            "split",
                            "aftermath",
                            "adventure",
                            "battle",
                            "double_faced_token",
                        ]
                    )
                )
                .then(face_field("name"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceName"),
                # Face-specific flavor name (only for multi-face cards)
                # NOTE: flavor_name is NOT in Scryfall bulk data card_faces - only in API
                # Production MTGJSON likely fetches this from a separate source
                # For now, use top-level flavorName (which is None for multi-face cards)
                pl.when(
                    pl.col("layout").is_in(
                        [
                            "transform",
                            "modal_dfc",
                            "meld",
                            "reversible_card",
                            "flip",
                            "split",
                            "adventure",
                            "battle",
                            "double_faced_token",
                        ]
                    )
                )
                .then(pl.col("flavorName"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceFlavorName"),
                # Face-aware fields (must have explicit aliases to avoid duplicates)
                face_field("manaCost").alias("manaCost"),
                face_field("typeLine").alias("type"),
                face_field("oracleText").alias("text"),
                face_field("flavorText").alias("flavorText"),
                face_field("power").alias("power"),
                face_field("toughness").alias("toughness"),
                face_field("loyalty").alias("loyalty"),
                face_field("defense").alias("defense"),
                face_field("artist").alias("artist"),
                face_field("watermark").alias("watermark"),
                face_field("illustrationId").alias("illustrationId"),
                face_field("colorIndicator").alias("colorIndicator"),
                # Colors: For split/aftermath, calculate from face mana_cost
                # since Scryfall bulk data has null face colors for these layouts
                pl.when(pl.col("layout").is_in(["split", "aftermath"]))
                .then(
                    extract_colors_from_mana_expr(
                        pl.col("_face_data").struct.field("mana_cost")
                    )
                )
                .otherwise(face_field("colors"))
                .alias("colors"),
                face_field("printedText").alias("printedText"),
                face_field("printedTypeLine").alias("printedType"),
                face_field("printedName").alias("printedName"),
                # Face-specific printed name (only for multi-face cards with localized content)
                pl.when(
                    pl.col("layout").is_in(
                        [
                            "transform",
                            "modal_dfc",
                            "meld",
                            "reversible_card",
                            "flip",
                            "split",
                            "adventure",
                            "battle",
                            "double_faced_token",
                        ]
                    )
                )
                .then(face_field("printedName"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("facePrintedName"),
                # Card-level fields (not face-specific)
                pl.col("setCode").str.to_uppercase(),
                pl.col("cmc").alias("manaValue"),
                pl.col("colorIdentity"),
                pl.col("borderColor"),
                pl.col("frame").alias("frameVersion"),
                pl.col("frameEffects"),
                pl.col("securityStamp"),
                pl.col("fullArt").alias("isFullArt"),
                pl.col("textless").alias("isTextless"),
                pl.col("oversized").alias("isOversized"),
                pl.col("promo").alias("isPromo"),
                pl.col("reprint").alias("isReprint"),
                pl.col("storySpotlight").alias("isStorySpotlight"),
                pl.col("reserved").alias("isReserved"),
                # hasFoil/hasNonFoil are computed from finishes in add_card_attributes()
                pl.col("flavorName"),
                pl.col("allParts"),
                # Language mapping
                pl.col("lang")
                .replace_strict(
                    {
                        "en": "English",
                        "es": "Spanish",
                        "fr": "French",
                        "de": "German",
                        "it": "Italian",
                        "pt": "Portuguese (Brazil)",
                        "ja": "Japanese",
                        "ko": "Korean",
                        "ru": "Russian",
                        "zhs": "Chinese Simplified",
                        "zht": "Chinese Traditional",
                        "he": "Hebrew",
                        "la": "Latin",
                        "grc": "Ancient Greek",
                        "ar": "Arabic",
                        "sa": "Sanskrit",
                        "ph": "Phyrexian",
                    },
                    default=pl.col("lang"),
                    return_dtype=pl.String,
                )
                .alias("language")
            ]
        )
        .with_columns(
            pl.when(ascii_name != face_name)
            .then(ascii_name)
            .otherwise(None)
            .alias("asciiName"),
        )
    )


# 1.3: Parse type_line into supertypes, types, subtypes
def parse_type_line_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Parse type_line into supertypes, types, subtypes using Polars expressions.

    Converts "Legendary Creature - Human Wizard" into:
    - supertypes: ["Legendary"]
    - types: ["Creature"]
    - subtypes: ["Human", "Wizard"]
    """
    super_types_list = list(constants.SUPER_TYPES)

    # already renamed type_line -> type
    type_line = pl.col("type").fill_null("Card")

    # Split on em-dash
    split_type = type_line.str.split(" — ")

    return (
        lf.with_columns(
            split_type.list.first().alias("_types_part"),
            split_type.list.get(1, null_on_oob=True).alias("_subtypes_part"),
        )
        .with_columns(
            pl.col("_types_part").str.split(" ").alias("_type_words"),
        )
        .with_columns(
            pl.col("_type_words")
            .list.eval(pl.element().filter(pl.element().is_in(super_types_list)))
            .alias("supertypes"),
            pl.col("_type_words")
            .list.eval(pl.element().filter(~pl.element().is_in(super_types_list)))
            .alias("types"),
            pl.when(pl.col("_subtypes_part").is_not_null())
            .then(pl.col("_subtypes_part").str.strip_chars().str.split(" "))
            .otherwise(pl.lit([]).cast(pl.List(pl.String)))
            .alias("subtypes"),
        )
        .drop(["_types_part", "_subtypes_part", "_type_words"])
    )


# 1.4: Add mana cost, mana value, and colors
def add_mana_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mana cost, mana value, and colors.

    For multi-face cards (split, aftermath, adventure, etc.), faceManaValue is
    calculated from the face's mana_cost string using the same logic as legacy code.
    """
    # Get face's mana_cost for multi-face cards
    face_mana_cost = pl.col("_face_data").struct.field("mana_cost")

    return lf.with_columns(
        # manaCost already exists from add_basic_fields rename
        pl.col("colors").fill_null([]).alias("colors"),
        pl.col("colorIdentity").fill_null([]),
        # manaValue/convertedManaCost are floats (e.g., 2.5 for split cards)
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("manaValue"),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("convertedManaCost"),
        # faceManaValue: calculate from face's mana_cost for multi-face cards
        pl.when(pl.col("_face_data").is_not_null())
        .then(calculate_cmc_expr(face_mana_cost))
        .otherwise(pl.col("manaValue").cast(pl.Float64).fill_null(0.0))
        .alias("faceManaValue"),
    )


# 1.5: Add card attributes
def add_card_attributes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add card attributes. Runs after add_basic_fields.
    """
    return lf.with_columns(
        pl.col("rarity"),
        pl.col("frameEffects").fill_null([]).list.sort().alias("frameEffects"),
        pl.col("artist").fill_null(""),
        pl.col("artistIds").fill_null([]),
        pl.col("watermark"),
        order_finishes_expr("finishes").fill_null([]).alias("finishes"),
        pl.col("finishes").list.contains("foil").fill_null(False).alias("hasFoil"),
        pl.col("finishes")
        .list.contains("nonfoil")
        .fill_null(False)
        .alias("hasNonFoil"),
        pl.col("contentWarning").alias("hasContentWarning"),
        (pl.col("setType") == "funny").alias("_is_funny_set"),
        pl.col("loyalty"),
        pl.col("defense"),
        pl.col("power"),
        pl.col("toughness"),
        pl.col("handModifier").alias("hand"),
        pl.col("lifeModifier").alias("life"),
        pl.col("edhrecRank").alias("edhrecRank"),
        pl.col("promoTypes").fill_null([]).alias("promoTypes"),
        pl.col("booster").alias("_in_booster"),
        pl.col("gameChanger").fill_null(False).alias("isGameChanger"),
        pl.col("layout"),
        pl.col("keywords").fill_null([]).alias("_all_keywords"),
        pl.col("attractionLights").alias("attractionLights"),
        pl.col("allParts").fill_null([]).alias("_all_parts"),
    )


# 1.6: Filter keywords for face
def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter keywords using a safer method for the Polars optimizer.
    """
    # We use a batch-based map to avoid making the query plan too complex
    # but still keep it fast.
    def _filter_logic(s: pl.Series) -> pl.Series:
        # s is a series of structs: {'text': str, 'keywords': list[str]}
        out = []
        for row in s:
            txt = (row['text'] or "").lower()
            kws = row['_all_keywords'] or []
            out.append([k for k in kws if k.lower() in txt])
        return pl.Series(out, dtype=pl.List(pl.String))

    return lf.with_columns(
        pl.struct(["text", "_all_keywords"])
        .map_batches(_filter_logic)
        .alias("keywords")
    ).drop("_all_keywords")

# 1.7: Add booster types and isStarter
def add_booster_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute boosterTypes and isStarter based on Scryfall booster field and promoTypes.

    - If card is in boosters (booster=True), add "default"
    - If promoTypes contains "starterdeck" or "planeswalkerdeck", add "deck"
    - isStarter is True when card is NOT in boosters (starter deck exclusive cards)
    """
    # Extract common subexpression to avoid duplicate computation
    has_starter_promo = (
        pl.col("promoTypes")
        .list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"]))
        .list.len()
        > 0
    )
    in_booster = pl.col("_in_booster").fill_null(False)

    return lf.with_columns(
        [
            # boosterTypes computation
            pl.when(in_booster)
            .then(
                pl.when(has_starter_promo)
                .then(pl.lit(["default", "deck"]))
                .otherwise(pl.lit(["default"]))
            )
            .otherwise(
                pl.when(has_starter_promo)
                .then(pl.lit(["deck"]))
                .otherwise(pl.lit([]).cast(pl.List(pl.String)))
            )
            .alias("boosterTypes"),
            # isStarter: True for cards NOT found in boosters (starter deck exclusive)
            pl.when(~pl.col("_in_booster").fill_null(True))
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
            .alias("isStarter"),
        ]
    ).drop("_in_booster")


# 1.8: Build legalities struct
def add_legalities_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Builds legalities struct from Scryfall's legalities column.

    Uses dynamically discovered format names instead of hardcoded list.
    """
    # Unnest the source struct to get individual format columns
    lf = lf.unnest("legalities")

    # Use discovered formats from source data
    formats = (
        ctx.categoricals.legalities
        if ctx.categoricals is not None and ctx.categoricals.legalities is not None
        else []
    )

    if not formats:
        # Fallback: return empty struct if no formats discovered
        return lf.with_columns(pl.lit(None).alias("legalities"))

    # Build expressions for each format
    struct_fields = []
    for fmt in formats:
        expr = (
            pl.when(
                pl.col(fmt).is_not_null()
                & (pl.col(fmt) != "not_legal")
                & (pl.col("setType") != "memorabilia")
            )
            .then(pl.col(fmt).str.to_titlecase())
            .otherwise(pl.lit(None))
            .alias(fmt)
        )
        struct_fields.append(expr)

    # Repack into struct and drop the unpacked columns
    return lf.with_columns(pl.struct(struct_fields).alias("legalities")).drop(
        formats, strict=False
    )


# 1.9: Build availability list from games column
def add_availability_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build availability list from games column.

    Uses dynamically discovered game platforms.
    """
    schema = lf.collect_schema()

    if "games" not in schema.names():
        return lf.with_columns(
            pl.lit([]).cast(pl.List(pl.String)).alias("availability")
        )

    # Use discovered platforms
    categoricals = ctx.categoricals
    platforms = categoricals.games if categoricals else []

    if not platforms:
        # Fallback: just pass through games as availability
        return lf.with_columns(pl.col("games").alias("availability"))

    games_dtype = schema["games"]

    # Handle struct format (from parquet) vs list format (from JSON)
    if isinstance(games_dtype, pl.Struct):
        # Struct format: {paper: true, arena: false, mtgo: true}
        return lf.with_columns(
            pl.concat_list(
                [
                    pl.when(pl.col("games").struct.field(p).fill_null(False))
                    .then(pl.lit(p))
                    .otherwise(pl.lit(None))
                    for p in platforms
                ]
            )
            .list.drop_nulls()
            .list.sort()
            .alias("availability")
        )
    # List format: ["paper", "mtgo"]
    return lf.with_columns(pl.col("games").list.sort().alias("availability"))


def join_cardmarket_ids(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add CardMarket (MCM) identifiers to cards.

    Uses Scryfall's cardmarket_id (renamed to cardmarketId after normalization)
    as mcmId. The mcmMetaId requires API lookups and is only available from
    the mcm_lookup table if populated.
    """
    mcm_df = ctx.mcm_lookup_df

    if mcm_df is None:
        return lf.with_columns(
            [
                pl.col("cardmarketId").cast(pl.String).alias("mcmId"),
                pl.lit(None).cast(pl.String).alias("mcmMetaId"),
            ]
        )

    # Handle LazyFrame from cache
    if isinstance(mcm_df, pl.LazyFrame):
        mcm_df = mcm_df.collect()

    if len(mcm_df) == 0:
        return lf.with_columns(
            [
                pl.col("cardmarketId").cast(pl.String).alias("mcmId"),
                pl.lit(None).cast(pl.String).alias("mcmMetaId"),
            ]
        )

    # Lookup table available - join to get mcmMetaId
    mcm_lookup = mcm_df.lazy()

    lf = lf.with_columns(
        [
            # Lowercase name for matching
            pl.col("name").str.to_lowercase().alias("_join_name"),
            # Scryfall numbers often have leading zeros (e.g., "001"),
            # while MCM strips them. We strip them here to match.
            pl.col("number").str.strip_chars_start("0").alias("_join_number"),
        ]
    )

    # Left join on Set + Name + Number
    lf = lf.join(
        mcm_lookup,
        left_on=["setCode", "_join_name", "_join_number"],
        right_on=["setCode", "nameLower", "number"],
        how="left",
    )

    # Use Scryfall's cardmarketId as fallback for mcmId if lookup missed
    lf = lf.with_columns(
        pl.coalesce(pl.col("mcmId"), pl.col("cardmarketId").cast(pl.String)).alias(
            "mcmId"
        )
    )

    # Keep mcmId and mcmMetaId columns - they'll be added to identifiers struct later
    lf = lf.drop(["_join_name", "_join_number"])

    return lf


# 2.2: add identifiers struct
def add_identifiers_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build the identifiers struct lazily.
    """
    return lf.with_columns(
        pl.struct(
            scryfallId=pl.col("scryfallId"),
            scryfallOracleId=pl.coalesce(
                pl.col("_face_data").struct.field("oracle_id"),
                pl.col("oracleId"),
            ),
            scryfallIllustrationId=pl.coalesce(
                pl.col("_face_data").struct.field("illustration_id"),
                pl.col("illustrationId"),
            ),
            scryfallCardBackId=pl.col("cardBackId"),
            # MCM IDs from CardMarket lookup (mcmId, mcmMetaId columns from join_cardmarket_ids)
            mcmId=pl.col("mcmId"),
            mcmMetaId=pl.col("mcmMetaId"),
            mtgArenaId=pl.col("arenaId").cast(pl.String),
            mtgoId=pl.col("mtgoId").cast(pl.String),
            mtgoFoilId=pl.col("mtgoFoilId").cast(pl.String),
            multiverseId=pl.col("multiverseIds")
            .list.get(pl.col("faceId").fill_null(0), null_on_oob=True)
            .cast(pl.String),
            tcgplayerProductId=pl.col("tcgplayerId").cast(pl.String),
            tcgplayerEtchedProductId=pl.col("tcgplayerEtchedId").cast(pl.String),
            cardKingdomId=pl.col("cardKingdomId"),
            cardKingdomFoilId=pl.col("cardKingdomFoilId"),
            cardKingdomEtchedId=pl.col("cardKingdomEtchedId"),
        ).alias("identifiers")
    )


# 2.7: join gatherer data
def join_gatherer_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join Gatherer original text and type by multiverse ID.
    gatherer_df has multiverseId, originalText, originalType columns.
    """
    gatherer_df = ctx.gatherer_df

    if gatherer_df is None:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("originalText"),
                pl.lit(None).cast(pl.String).alias("originalType"),
            ]
        )

    # Handle LazyFrame from cache
    if isinstance(gatherer_df, pl.LazyFrame):
        gatherer_df = gatherer_df.collect()

    if len(gatherer_df) == 0:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("originalText"),
                pl.lit(None).cast(pl.String).alias("originalType"),
            ]
        )

    # Extract multiverse_id from identifiers for join
    lf = lf.with_columns(
        pl.col("identifiers").struct.field("multiverseId").alias("_mv_id_lookup")
    )

    lf = lf.join(
        gatherer_df.lazy(),
        left_on="_mv_id_lookup",
        right_on="multiverseId",
        how="left",
    )

    return lf.drop("_mv_id_lookup")


def add_identifiers_v4_uuid(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mtgjsonV4Id to identifiers struct.

    Uses struct-based batch computation for v4 UUID formula.
    """
    # V4 UUID: Token uses different formula than normal cards
    card_name = pl.coalesce(pl.col("faceName"), pl.col("name")).fill_null("")
    scryfall_id = pl.col("scryfallId").fill_null("")
    is_token = pl.col("types").list.set_intersection(pl.lit(["Token", "Card"])).list.len() > 0

    token_source = pl.concat_str([
        card_name,
        pl.col("colors").list.join("").fill_null(""),
        pl.col("power").fill_null(""),
        pl.col("toughness").fill_null(""),
        pl.col("side").fill_null(""),
        pl.col("setCode").fill_null("").str.slice(1).str.to_uppercase(),
        scryfall_id,
    ])
    normal_source = pl.concat_str([pl.lit("sf"), scryfall_id, card_name])

    # Combine sources and generate UUID5
    combined_source = pl.when(is_token).then(token_source).otherwise(normal_source)
    lf = lf.with_columns(
        combined_source.alias("_v4_source")
    )
    lf = lf.with_columns(
        _uuid5_expr("_v4_source").alias("_mtgjsonV4Id")
    ).drop("_v4_source")
    return lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            [pl.col("_mtgjsonV4Id").alias("mtgjsonV4Id")]
        )
    ).drop("_mtgjsonV4Id")


# 4.0: add otherFaceIds
def add_other_face_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link multi-face cards via Scryfall ID.

    Since scryfallId is shared by all faces of a split card/MDFC,
    grouping by scryfallId gathers all sibling UUIDs.

    Meld cards are special: they have different scryfallIds for each component,
    so we use cardParts (from name_lf) to link them by name instead.
    """
    # Group by Scryfall ID to get list of MTGJSON UUIDs for this object
    face_links = (
        lf.select(["scryfallId", "uuid"])
        .group_by("scryfallId")
        .agg(pl.col("uuid").alias("_all_uuids"))
    )

    lf = (
        lf.join(face_links, on="scryfallId", how="left")
        .with_columns(
            pl.col("_all_uuids")
            .list.set_difference(pl.col("uuid").cast(pl.List(pl.String)))
            .alias("otherFaceIds")
        )
        .drop("_all_uuids")
    )

    # Handle meld cards separately - they have different scryfallIds per component
    # Use lazy self-join via cardParts (already joined from name_lf) - no collect needed
    from mtgjson5.pipeline.lookups import add_meld_other_face_ids

    lf = add_meld_other_face_ids(lf)

    return lf


# 4.1: add variations and isAlternative
def add_variations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized detection of Variations and Alternatives.

    Variations: Cards with the same base name and face name but different UUID
    is_alternative: Within cards sharing a "printing key", only the first is NOT alternative
    """

    # Normalize to base name by stripping " (" and beyond
    # Also fill null faceName with empty string for grouping (null != null in group_by)
    lf = lf.with_columns(
        pl.col("name").str.split(" (").list.first().alias("_base_name"),
        pl.col("faceName").fill_null("").alias("_faceName_for_group"),
    )

    # Collect all UUIDs for each (set, base_name, faceName) group
    variation_groups = (
        lf.select(["setCode", "_base_name", "_faceName_for_group", "uuid"])
        .group_by(["setCode", "_base_name", "_faceName_for_group"])
        .agg(pl.col("uuid").alias("_group_uuids"))
    )

    # Join back to attach the full UUID list to each card
    lf = lf.join(
        variation_groups,
        on=["setCode", "_base_name", "_faceName_for_group"],
        how="left",
    )

    # Variations = group UUIDs minus self UUID
    # Use concat_list to wrap uuid in a list for set_difference (not implode which aggregates all rows)
    lf = lf.with_columns(
        pl.when(pl.col("_group_uuids").list.len() > 1)
        .then(
            pl.col("_group_uuids")
            .list.set_difference(pl.concat_list(pl.col("uuid")))
            .list.sort()
        )
        .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        .alias("variations")
    )

    # Build the "printing key" that defines uniqueness within a set
    frame_effects_str = pl.col("frameEffects").list.sort().list.join(",").fill_null("")

    finishes_str = pl.col("finishes").list.sort().list.join(",").fill_null("")

    # Base key: name|border|frame|effects|side
    base_key = pl.concat_str(
        [
            pl.col("name"),
            pl.lit("|"),
            pl.col("borderColor").fill_null(""),
            pl.lit("|"),
            pl.col("frameVersion").fill_null(""),
            pl.lit("|"),
            frame_effects_str,
            pl.lit("|"),
            pl.col("side").fill_null(""),
        ]
    )

    # For UNH/10E, also include finishes in the key
    printing_key = (
        pl.when(pl.col("setCode").is_in(["UNH", "10E"]))
        .then(pl.concat_str([base_key, pl.lit("|"), finishes_str]))
        .otherwise(base_key)
        .alias("_printing_key")
    )

    lf = lf.with_columns(printing_key)

    # Within each printing key, the card with the lowest collector number is "canonical"
    # All others with the same key are alternatives
    # Zero-pad number for proper string sorting (e.g., "8" -> "000008" < "000227")
    first_number_expr = (
        pl.col("number").str.zfill(10).min().over(["setCode", "_printing_key"])
    )
    canonical_expr = pl.col("number").str.zfill(10) == first_number_expr

    basic_lands = [
        "Plains",
        "Island",
        "Swamp",
        "Mountain",
        "Forest",
        "Snow-Covered Plains",
        "Snow-Covered Island",
        "Snow-Covered Swamp",
        "Snow-Covered Mountain",
        "Snow-Covered Forest",
        "Wastes",
    ]

    lf = lf.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0)  # Has variations
            & (~pl.col("name").is_in(basic_lands))  # Not a basic land
            & (~canonical_expr)  # Not the canonical (lowest collector number) in group
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )
    # Cleanup temp columns
    return lf.drop(
        ["_base_name", "_faceName_for_group", "_group_uuids", "_printing_key"]
    )


# 4.2: add leadership skills
def add_leadership_skills_expr(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Determine if a card can be a commander/oathbreaker/brawl commander.

    Uses vectorized string operations instead of per-card checks.
    """
    # Override cards that can always be commander
    override_cards = ["Grist, the Hunger Tide"]

    # Commander legal check
    is_legendary = pl.col("type").str.contains("Legendary")
    is_creature = pl.col("type").str.contains("Creature")
    is_vehicle_or_spacecraft = pl.col("type").str.contains("Vehicle|Spacecraft")
    has_power_toughness = (
        pl.col("power").is_not_null() & pl.col("toughness").is_not_null()
    )
    is_front_face = pl.col("side").is_null() | (pl.col("side") == "a")
    can_be_commander_text = pl.col("text").str.contains("can be your commander")
    is_override = pl.col("name").is_in(override_cards)

    is_commander_legal = (
        is_override
        | (
            is_legendary
            & (is_creature | (is_vehicle_or_spacecraft & has_power_toughness))
            & is_front_face
        )
        | can_be_commander_text
    )

    # Oathbreaker legal = is a planeswalker
    is_oathbreaker_legal = pl.col("type").str.contains("Planeswalker")

    # Brawl legal = set is in Standard AND (commander or oathbreaker eligible)
    standard_sets = ctx.standard_legal_sets
    is_in_standard = pl.col("setCode").is_in(standard_sets or set())
    is_brawl_legal = is_in_standard & (is_commander_legal | is_oathbreaker_legal)

    return lf.with_columns(
        pl.when(is_commander_legal | is_oathbreaker_legal | is_brawl_legal)
        .then(
            pl.struct(
                brawl=is_brawl_legal,
                commander=is_commander_legal,
                oathbreaker=is_oathbreaker_legal,
            )
        )
        .otherwise(pl.lit(None))
        .alias("leadershipSkills")
    )


# 4.3: add reverseRelated
def add_reverse_related(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute reverseRelated for tokens from all_parts.

    For tokens, this lists the names of cards that create/reference this token.
    """
    # Extract names from all_parts where name differs from card name
    # all_parts is List[Struct{name, ...}]
    return lf.with_columns(
        pl.col("_all_parts")
        .list.eval(pl.element().struct.field("name"))
        .list.set_difference(pl.col("name").cast(pl.List(pl.String)))
        .list.sort()
        .alias("reverseRelated")
    ).drop("_all_parts")


# 4.5: add_alternative_deck_limit
def add_alternative_deck_limit(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Mark cards that don't have the standard 4-copy deck limit.

    Combines two detection methods:
    1. Pre-computed list from Scryfall's cards_without_limits
    2. Oracle text pattern matching for cards not in the list

    Pattern matching catches newer cards like Templar Knight that may not
    be in the pre-computed list yet.
    """
    unlimited_cards = ctx.unlimited_cards or set()

    # Oracle text pattern matching
    # Note: oracleText is renamed to "text" in add_basic_fields(), so we use that column name
    oracle_text = pl.coalesce(
        pl.col("_face_data").struct.field("oracle_text"),
        pl.col("text"),
    ).fill_null("").str.to_lowercase()

    # Pattern 1: "a deck can have any number of cards named"
    # Use single regex instead of multiple str.contains() calls for performance
    pattern1 = oracle_text.str.contains(r"deck.*any.*number.*cards.*named")

    # Pattern 2: "have up to ... cards named ... in your deck"
    pattern2 = oracle_text.str.contains(r"have.*up.*to.*cards.*named.*deck")

    # Combine pre-computed list with pattern matching
    in_list = pl.col("name").is_in(list(unlimited_cards)) if unlimited_cards else pl.lit(False)
    matches_pattern = pattern1 | pattern2

    return lf.with_columns(
        pl.when(in_list | matches_pattern)
        .then(pl.lit(True))
        .otherwise(pl.lit(None).cast(pl.Boolean))
        .alias("hasAlternativeDeckLimit")
    )


# 4.6:
def add_is_funny(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Vectorized 'isFunny' logic.

    Note: This still uses hardcoded "funny" check since it's a semantic
    value not just a categorical enumeration. But we could validate that
    "funny" exists in categoricals.set_types if desired.
    """
    categoricals = ctx.categoricals
    # Validate "funny" is a known set_type (optional sanity check)
    if categoricals is None or "funny" not in categoricals.set_types:
        # No funny sets exist - return all null
        return lf.with_columns(pl.lit(None).cast(pl.Boolean).alias("isFunny"))

    return lf.with_columns(
        pl.when(pl.col("setType") != "funny")
        .then(pl.lit(None))
        .when(pl.col("setCode") == "UNF")
        .then(
            pl.when(pl.col("securityStamp") == "acorn")
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
        )
        .otherwise(pl.lit(True))
        .alias("isFunny")
    )


# 4.7:
def add_is_timeshifted(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized 'isTimeshifted' logic.
    """
    return lf.with_columns(
        pl.when((pl.col("frameVersion") == "future") | (pl.col("setCode") == "TSB"))
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isTimeshifted")
    )


# 4.8: add purchaseUrls struct
def add_purchase_urls_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Build purchaseUrls struct with SHA256 redirect hashes.

    Hash is computed from (base + path + uuid), matching legacy url_keygen() behavior.
    """
    redirect_base = "https://mtgjson.com/links/"
    ck_base = "https://www.cardkingdom.com/"

    # CK URLs from join (path like "mtg/set/card")
    ck_url = pl.col("cardKingdomUrl")
    ckf_url = pl.col("cardKingdomFoilUrl")
    cke_url = pl.col("cardKingdomEtchedUrl")

    # Access identifier fields from inside the identifiers struct
    mcm_id = pl.col("identifiers").struct.field("mcmId")
    tcg_id = pl.col("identifiers").struct.field("tcgplayerProductId")
    tcge_id = pl.col("identifiers").struct.field("tcgplayerEtchedProductId")

    return (
        lf.with_columns(
            [
                # Card Kingdom: hash(base + path + uuid)
                plh.concat_str([pl.lit(ck_base), ck_url, pl.col("uuid")])
                .chash.sha2_256().str.slice(0, 16)
                .alias("_ck_hash"),
                plh.concat_str([pl.lit(ck_base), ckf_url, pl.col("uuid")])
                .chash.sha2_256().str.slice(0, 16)
                .alias("_ckf_hash"),
                plh.concat_str([pl.lit(ck_base), cke_url, pl.col("uuid")])
                .chash.sha2_256().str.slice(0, 16)
                .alias("_cke_hash"),
                # TCGPlayer: hash(tcgplayer_product_id + uuid)
                plh.concat_str([tcg_id.cast(pl.String), pl.col("uuid")])
                .chash.sha2_256().str.slice(0, 16)
                .alias("_tcg_hash"),
                plh.concat_str([tcge_id.cast(pl.String), pl.col("uuid")])
                .chash.sha2_256().str.slice(0, 16)
                .alias("_tcge_hash"),
                # Cardmarket: hash(mcm_id + uuid + BUFFER + mcm_meta_id)
                plh.concat_str(
                    [
                        mcm_id.cast(pl.String),
                        pl.col("uuid"),
                        pl.lit(constants.CARD_MARKET_BUFFER),
                        pl.col("identifiers")
                        .struct.field("mcmMetaId")
                        .cast(pl.String)
                        .fill_null(""),
                    ]
                )
                .chash.sha2_256().str.slice(0, 16)
                .alias("_cm_hash"),
            ]
        )
        .with_columns(
            pl.struct(
                [
                    pl.when(ck_url.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_ck_hash"))
                    .otherwise(None)
                    .alias("cardKingdom"),
                    pl.when(ckf_url.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_ckf_hash"))
                    .otherwise(None)
                    .alias("cardKingdomFoil"),
                    pl.when(cke_url.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_cke_hash"))
                    .otherwise(None)
                    .alias("cardKingdomEtched"),
                    pl.when(mcm_id.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_cm_hash"))
                    .otherwise(None)
                    .alias("cardmarket"),
                    pl.when(tcg_id.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_tcg_hash"))
                    .otherwise(None)
                    .alias("tcgplayer"),
                    pl.when(tcge_id.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_tcge_hash"))
                    .otherwise(None)
                    .alias("tcgplayerEtched"),
                ]
            ).alias("purchaseUrls")
        )
        .drop(
            [
                "_ck_hash",
                "_ckf_hash",
                "_cke_hash",
                "_cm_hash",
                "_tcg_hash",
                "_tcge_hash",
            ]
        )
    )


# 5.0: apply manual overrides
def apply_manual_overrides(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Apply manual field overrides keyed by UUID.

    Handles special cases like Final Fantasy meld cards.
    """
    overrides = ctx.manual_overrides
    if not overrides:
        return lf

    # Map old field names to new pipeline names
    field_name_map = {
        "collectorNumber": "number",
        "id": "scryfallId",
        "oracleId": "oracleId",
        "set": "setCode",
        "cardBackId": "cardBackId",
    }

    # Group overrides by field (using mapped names)
    field_overrides: dict[str, dict[str, Any]] = {}
    for uuid_key, fields in overrides.items():
        for field_name, value in fields.items():
            if field_name.startswith("__"):
                continue
            mapped_field = field_name_map.get(field_name, field_name)
            if mapped_field not in field_overrides:
                field_overrides[mapped_field] = {}
            field_overrides[mapped_field][uuid_key] = value

    # Get column names once
    schema_names = lf.collect_schema().names()

    # Apply each field's overrides
    for field_name, uuid_map in field_overrides.items():
        if field_name not in schema_names:
            continue

        # Determine return dtype from first value
        sample_value = next(iter(uuid_map.values()))
        return_dtype: pl.List | type[pl.String]
        if isinstance(sample_value, list):
            return_dtype = pl.List(pl.String)
        elif isinstance(sample_value, str):
            return_dtype = pl.String
        else:
            return_dtype = pl.String

        lf = lf.with_columns(
            pl.when(pl.col("uuid").is_in(list(uuid_map.keys())))
            .then(
                pl.col("uuid").replace_strict(
                    uuid_map,
                    default=pl.col(field_name),
                    return_dtype=return_dtype,
                )
            )
            .otherwise(pl.col(field_name))
            .alias(field_name)
        )

    return lf


# 5.2:
def add_rebalanced_linkage(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.

    Adds:
    - isRebalanced: True for Alchemy rebalanced cards (A-Name or promo_types contains 'rebalanced')
    - originalPrintings: UUIDs of the original card (on rebalanced cards)
    - rebalancedPrintings: UUIDs of the rebalanced version (on original cards)
    """
    # Rebalanced cards: names starting with "A-" or promo_types contains 'rebalanced'
    # Original cards: names that match the stripped "A-" version

    is_rebalanced = pl.col("name").str.starts_with("A-") | pl.col(
        "promoTypes"
    ).list.contains("rebalanced")

    # Add isRebalanced boolean (True for rebalanced, null otherwise)
    lf = lf.with_columns(
        pl.when(is_rebalanced)
        .then(pl.lit(True))
        .otherwise(pl.lit(None).cast(pl.Boolean))
        .alias("isRebalanced")
    )

    is_rebalanced = pl.col("name").str.starts_with("A-")
    original_name_expr = pl.col("name").str.replace("^A-", "")

    # Build rebalanced -> original name mapping with UUIDs
    rebalanced_map = (
        lf.filter(is_rebalanced)
        .select(
            [
                original_name_expr.alias("_original_name"),
                pl.col("uuid"),
            ]
        )
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_rebalanced_uuids"))
    )

    # Build original name -> original UUIDs mapping
    # (cards that DON'T start with A- but whose name matches a rebalanced card's base name)
    original_map = (
        lf.filter(~is_rebalanced)
        .select(
            [
                pl.col("name").alias("_original_name"),
                pl.col("uuid"),
            ]
        )
        .join(
            rebalanced_map.select("_original_name").unique(),
            on="_original_name",
            how="semi",  # Only keep names that have a rebalanced version
        )
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_original_uuids"))
    )

    # Join rebalancedPrintings onto original cards (by name)
    lf = lf.join(
        rebalanced_map,
        left_on="name",
        right_on="_original_name",
        how="left",
    ).rename({"_rebalanced_uuids": "rebalancedPrintings"})

    # Join originalPrintings onto rebalanced cards (by stripped name)
    lf = lf.join(
        original_map,
        left_on=original_name_expr,
        right_on="_original_name",
        how="left",
    ).rename({"_original_uuids": "originalPrintings"})

    return lf


# 5.3:
def link_foil_nonfoil_versions(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link foil and non-foil versions that have different card details.

    Only applies to specific sets: CN2, FRF, ONS, 10E, UNH.
    Adds mtgjsonFoilVersionId and mtgjsonNonFoilVersionId to identifiers.

    Uses window functions instead of self-joins to avoid query plan complexity.
    """
    foil_link_sets = {"CN2", "FRF", "ONS", "10E", "UNH"}

    in_target_sets = pl.col("setCode").is_in(foil_link_sets)
    ill_id_expr = pl.col("identifiers").struct.field("scryfallIllustrationId")

    # Add illustration_id column for windowing
    lf = lf.with_columns(ill_id_expr.alias("_ill_id"))

    # Window partition key
    partition = ["setCode", "_ill_id"]

    # Condition for cards we care about
    is_candidate = in_target_sets & pl.col("_ill_id").is_not_null()

    # Add window computations - count and first/last in each partition
    lf = lf.with_columns(
        [
            # Count cards in group
            pl.when(is_candidate)
            .then(pl.len().over(partition))
            .otherwise(pl.lit(0))
            .alias("_pair_count"),
            # First UUID in group
            pl.when(is_candidate)
            .then(pl.col("uuid").first().over(partition))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_first_uuid"),
            # Last UUID in group
            pl.when(is_candidate)
            .then(pl.col("uuid").last().over(partition))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_last_uuid"),
            # First finishes in group
            pl.when(is_candidate)
            .then(pl.col("finishes").first().over(partition))
            .otherwise(pl.lit(None).cast(pl.List(pl.String)))
            .alias("_first_finishes"),
            # Last finishes in group
            pl.when(is_candidate)
            .then(pl.col("finishes").last().over(partition))
            .otherwise(pl.lit(None).cast(pl.List(pl.String)))
            .alias("_last_finishes"),
        ]
    )

    # Compute other card's uuid and finishes (for pairs only)
    is_pair = pl.col("_pair_count") == 2
    is_first = pl.col("uuid") == pl.col("_first_uuid")

    lf = lf.with_columns(
        [
            # Other UUID: if I'm first, other is last; if I'm last, other is first
            pl.when(is_pair)
            .then(
                pl.when(is_first)
                .then(pl.col("_last_uuid"))
                .otherwise(pl.col("_first_uuid"))
            )
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_other_uuid"),
            # Other finishes
            pl.when(is_pair)
            .then(
                pl.when(is_first)
                .then(pl.col("_last_finishes"))
                .otherwise(pl.col("_first_finishes"))
            )
            .otherwise(pl.lit(None).cast(pl.List(pl.String)))
            .alias("_other_finishes"),
        ]
    )

    # Determine foil status - card is foil-only if "nonfoil" NOT in finishes
    is_foil_only = ~pl.col("finishes").list.contains("nonfoil")
    other_is_foil_only = pl.col("_other_finishes").is_not_null() & ~pl.col(
        "_other_finishes"
    ).list.contains("nonfoil")

    # Only link if one is foil and one is not (XOR condition)
    valid_pair = is_pair & pl.col("_other_finishes").is_not_null() & (is_foil_only != other_is_foil_only)

    lf = lf.with_columns(
        [
            # If this card is foil-only, link to nonfoil version
            pl.when(valid_pair & is_foil_only)
            .then(pl.col("_other_uuid"))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_nonfoil_version"),
            # If this card is NOT foil-only, link to foil version
            pl.when(valid_pair & ~is_foil_only)
            .then(pl.col("_other_uuid"))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("_foil_version"),
        ]
    )

    # Inject into identifiers struct
    lf = lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            [
                pl.col("_foil_version").alias("mtgjsonFoilVersionId"),
                pl.col("_nonfoil_version").alias("mtgjsonNonFoilVersionId"),
            ]
        )
    )

    # Cleanup temp columns
    return lf.drop(
        [
            "_ill_id",
            "_pair_count",
            "_first_uuid",
            "_last_uuid",
            "_first_finishes",
            "_last_finishes",
            "_other_uuid",
            "_other_finishes",
            "_foil_version",
            "_nonfoil_version",
        ],
        strict=False,
    )


# 5.5:
def add_secret_lair_subsets(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add subsets field for Secret Lair (SLD) cards.

    Links collector numbers to drop names.
    """
    sld_df = ctx.sld_subsets_df

    if sld_df is None:
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("subsets"))

    # Handle LazyFrame from cache
    if isinstance(sld_df, pl.LazyFrame):
        sld_df = sld_df.collect()

    if len(sld_df) == 0:
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("subsets"))

    # Rename the subsets column before joining to avoid conflicts
    sld_renamed = sld_df.rename({"subsets": "_sld_subsets"})
    lf = lf.join(
        sld_renamed.lazy(),
        on="number",
        how="left",
    )

    return lf.with_columns(
        pl.when(pl.col("setCode") == "SLD")
        .then(pl.col("_sld_subsets"))
        .otherwise(pl.lit(None))
        .alias("subsets")
    ).drop("_sld_subsets", strict=False)


# 5.6:
def add_source_products(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add sourceProducts field linking cards to sealed products.

    Uses SealedDataProvider.card_to_products_df for lazy join.

    Note: At this stage, Scryfall's boolean `foil`/`nonfoil` columns still exist.
    We rename the product columns with _sp_ prefix to avoid collision, then drop them.
    """
    card_to_products_df = ctx.card_to_products_df

    SOURCE_PRODUCTS_STRUCT = pl.Struct([
        pl.Field("etched", pl.List(pl.String)),
        pl.Field("foil", pl.List(pl.String)),
        pl.Field("nonfoil", pl.List(pl.String)),
    ])

    if card_to_products_df is None:
        return lf.with_columns(
            pl.lit(None).cast(SOURCE_PRODUCTS_STRUCT).alias("sourceProducts")
        )

    # card_to_products_df can be LazyFrame (from cache) or DataFrame
    products_lf = (
        card_to_products_df
        if isinstance(card_to_products_df, pl.LazyFrame)
        else card_to_products_df.lazy()
    )

    # Rename product columns to avoid collision with Scryfall's boolean foil/nonfoil
    products_lf = products_lf.rename(
        {
            "foil": "_sp_foil",
            "nonfoil": "_sp_nonfoil",
            "etched": "_sp_etched",
        }
    )

    return (
        lf.join(
            products_lf,
            on="uuid",
            how="left",
        )
        .with_columns(
            pl.struct(
                [
                    pl.col("_sp_foil").alias("foil"),
                    pl.col("_sp_nonfoil").alias("nonfoil"),
                    pl.col("_sp_etched").alias("etched"),
                ]
            ).alias("sourceProducts")
        )
        .drop(["_sp_foil", "_sp_nonfoil", "_sp_etched"])
    )


# 6.2:
def rename_all_the_things(
    lf: pl.LazyFrame, output_type: str = "card_set"
) -> pl.LazyFrame:
    """
    Final transformation: Renames internal columns to MTGJSON CamelCase,
    builds nested structs, and selects only fields valid for the output_type.
    """

    # Special renames that don't follow simple snake_case -> camelCase conversion
    special_renames = {
        "set": "setCode",
        "cmc": "convertedManaCost",
    }

    # Build rename map: snake_case columns -> camelCase using to_camel_case utility
    # Most columns are already camelCase from Scryfall, but internal columns use snake_case
    schema = lf.collect_schema()
    rename_map = {}
    for col in schema.names():
        if col in special_renames:
            rename_map[col] = special_renames[col]
        elif "_" in col:
            # Convert snake_case to camelCase
            rename_map[col] = to_camel_case(col)
        # Otherwise, keep as-is (already camelCase or single word)

    # We use strict=False because some source cols might be missing in specific batches
    if rename_map:
        lf = lf.rename(rename_map, strict=False)

    # For non-multiface cards, these should be null (omitted from output)
    multiface_layouts = [
        "split",
        "aftermath",
        "flip",
        "transform",
        "modal_dfc",
        "meld",
        "adventure",
        "reversible_card",
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

    # Get the allowed fields for this specific output type
    # ATOMIC_EXCLUDE contains fields to remove when building atomic cards from set cards
    # TOKEN_EXCLUDE contains fields to remove when building token cards
    if output_type == "card_set":
        # CardSet keeps all fields - no exclusions for set-specific data
        allowed_fields = ALL_CARD_FIELDS
    elif output_type == "card_token":
        allowed_fields = ALL_CARD_FIELDS - TOKEN_EXCLUDE
    elif output_type == "card_atomic":
        # Atomic cards exclude printing-specific fields (setCode, uuid, etc.)
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

    LOGGER.debug(
        f"Selecting {len(final_cols)} columns for output type '{output_type}':{', '.join(final_cols)}"
    )

    return lf.select(final_cols)


# 6.7:
def filter_out_tokens(df: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Separate tokens from main cards.

    Tokens are identified by:
    - layout in {"token", "double_faced_token", "emblem", "art_series"}
    - type == "Dungeon"
    - "Token" in type string

    Returns:
        Tuple of (cards_df, tokens_df) - cards without tokens, and the filtered tokens
    """

    is_token = (
        pl.col("layout").is_in(constants.TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

    tokens_df = df.filter(is_token)
    cards_df = df.filter(~is_token)

    return cards_df, tokens_df


def sink_cards(ctx: PipelineContext) -> None:
    """Sink cards and tokens to partitioned parquet files.

    Uses default_card_languages mapping from cache to determine which language
    each card should use. For most cards this is English, but for foreign-only
    sets (like 4BB, FBB) it's the primary printed language.

    Foreign language data for normal sets is aggregated into the 'foreignData'
    field for English cards via join_set_number().
    """
    cards_dir = constants.CACHE_PATH / "_parquet"
    tokens_dir = constants.CACHE_PATH / "_parquet_tokens"
    for path in [cards_dir, tokens_dir]:
        path.mkdir(parents=True, exist_ok=True)

    lf = ctx.final_cards_lf
    if lf is None:
        LOGGER.warning("sink_cards: final_cards_lf is None, returning early")
        return

    # Use pre-computed default_card_languages from cache
    # This mapping was derived from Scryfall's default_cards bulk file during cache loading
    default_langs = ctx.default_card_languages
    if default_langs is not None:
        # Join with default language mapping to filter to primary language per card
        lf = lf.join(default_langs, on=["scryfallId", "language"], how="semi")
        LOGGER.info("Filtered to default language per card using default_cards mapping")
    else:
        # Fallback: English only (if default_cards not loaded)
        LOGGER.warning("default_card_languages not available, filtering to English only")
        lf = lf.filter(pl.col("language") == "English")

    # Compute variations AFTER language filtering so variation UUIDs only reference
    # cards that exist in the final output (not filtered-out language versions)
    lf = add_variations(lf)

    # Split into cards and tokens, apply final renames
    cards_lf, tokens_lf = filter_out_tokens(lf)
    clf = rename_all_the_things(cards_lf, output_type="card_set")
    tlf = rename_all_the_things(tokens_lf, output_type="card_token")

    for lazy_frame, out_dir, label in [(clf, cards_dir, "cards"), (tlf, tokens_dir, "tokens")]:
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
            set_path = out_dir / f"setCode={s_code}"
            set_path.mkdir(exist_ok=True)
            set_df.write_parquet(set_path / "0.parquet")

        LOGGER.info(f"  {label} complete")

    LOGGER.info("All parquet sinks complete.")


def join_identifiers(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all scryfallId+side-based lookups.

    Replaces:
    - join_card_kingdom_data()
    - add_uuid_expr() (partial - cachedUuid)
    - add_orientations()

    Gets from identifiers_lf:
    - cardKingdomId, cardKingdomFoilId, cardKingdomUrl, cardKingdomFoilUrl
    - orientation
    - cachedUuid

    After this join, use add_uuid_from_cache() to compute final uuid.
    """
    if ctx.identifiers_lf is None:
        # Fallback: add null columns
        return lf.with_columns(
            pl.lit(None).cast(pl.String).alias("cardKingdomId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilUrl"),
            pl.lit(None).cast(pl.String).alias("orientation"),
            pl.lit(None).cast(pl.String).alias("cachedUuid"),
        )

    # Prepare join key: fill null side with "a"
    lf = lf.with_columns(pl.col("side").fill_null("a").alias("_side_for_join"))

    # Single join on scryfallId + side
    lf = lf.join(
        ctx.identifiers_lf,
        left_on=["scryfallId", "_side_for_join"],
        right_on=["scryfallId", "side"],
        how="left",
    )

    # Add etched placeholders (Card Kingdom doesn't have etched data)
    lf = lf.with_columns(
        pl.lit(None).cast(pl.String).alias("cardKingdomEtchedId"),
        pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
    )

    return lf.drop("_side_for_join", strict=False)


def join_oracle_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all oracleId-based lookups.

    Replaces:
    - join_rulings()
    - join_edhrec_data()
    - join_printings()

    Gets from oracle_data_lf:
    - rulings: List[Struct{source, publishedAt, comment}]
    - edhrecSaltiness: Float64
    - edhrecRank: Int64
    - printings: List[String]

    Note: originalReleaseDate is computed separately in add_original_release_date()
    based on card-specific vs set release dates (for promos).
    """
    if ctx.oracle_data_lf is None:
        return lf.with_columns(
            pl.lit([]).alias("rulings"),
            pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
            pl.lit(None).cast(pl.Int64).alias("edhrecRank"),
            pl.lit([]).cast(pl.List(pl.String)).alias("printings"),
        )

    lf = lf.join(
        ctx.oracle_data_lf,
        on="oracleId",
        how="left",
    )

    # Fill nulls with appropriate defaults
    return lf.with_columns(
        pl.col("rulings").fill_null([]),
        pl.col("printings").fill_null([]).list.sort(),
    )


def propagate_salt_to_tokens(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Propagate edhrecSaltiness from parent cards to tokens.

    Tokens have their own oracleId which isn't in EDHREC's data.
    We inherit saltiness from the parent card via reverseRelated.

    For example:
    - "Aven Initiate" token has reverseRelated=["Aven Initiate"]
    - We look up "Aven Initiate" card's salt and assign it to the token
    """
    # Identify tokens
    is_token = (
        pl.col("layout").is_in(constants.TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

    # Build parent salt lookup: name -> edhrecSaltiness (first non-null)
    # Only include non-tokens with salt values
    parent_salt = (
        lf.filter(~is_token & pl.col("edhrecSaltiness").is_not_null())
        .select(["name", "edhrecSaltiness"])
        .group_by("name")
        .agg(pl.col("edhrecSaltiness").first())
    )

    # Explode reverseRelated to join with parent salt
    # For tokens with NULL salt and non-empty reverseRelated
    tokens_needing_salt = lf.filter(
        is_token
        & pl.col("edhrecSaltiness").is_null()
        & pl.col("reverseRelated").is_not_null()
        & (pl.col("reverseRelated").list.len() > 0)
    ).with_columns(
        pl.col("reverseRelated").list.first().alias("_parent_name")
    )

    # Join to get parent salt
    tokens_with_salt = tokens_needing_salt.join(
        parent_salt.rename({"edhrecSaltiness": "_parent_salt"}),
        left_on="_parent_name",
        right_on="name",
        how="left",
    ).select(["uuid", "_parent_salt"])

    # Update original DataFrame with inherited salt
    lf = lf.join(
        tokens_with_salt,
        on="uuid",
        how="left",
    ).with_columns(
        pl.coalesce(pl.col("edhrecSaltiness"), pl.col("_parent_salt")).alias("edhrecSaltiness")
    ).drop("_parent_salt", strict=False)

    return lf


def calculate_duel_deck(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate duelDeck field for Duel Deck sets (DD* and GS1).

    The algorithm detects deck boundaries by looking for basic lands:
    - All cards start in deck 'a'
    - When we see a basic land followed by a non-basic land, switch to deck 'b'
    - Tokens and emblems are skipped (no duelDeck)

    This matches the original set_builder.py logic that some psychopath deemed necessary i guess?
    """
    is_duel_deck_set = pl.col("setCode").str.starts_with("DD") | (pl.col("setCode") == "GS1")
    basic_land_names = list(constants.BASIC_LAND_NAMES)

    # Only process cards from duel deck sets
    lf.filter(is_duel_deck_set)

    # Skip if no duel deck sets
    # We'll mark this as needing duelDeck calculation
    lf = lf.with_columns(
        pl.lit(None).cast(pl.String).alias("duelDeck")
    )

    # Filter to just duel deck set cards
    dd_cards = (
        lf.filter(is_duel_deck_set)
        .with_columns(
            # Mark basic lands
            pl.col("name").is_in(basic_land_names).alias("_is_basic_land"),
            # Mark tokens/emblems (skip these)
            pl.col("type").str.contains("Token|Emblem").fill_null(False).alias("_is_token"),
            # Extract numeric part of collector number for sorting
            pl.col("number").str.extract(r"^(\d+)", 1).cast(pl.Int32).alias("_num_sort"),
        )
        .sort(["setCode", "_num_sort", "number"])
    )

    # For each set, we need to detect the transition from land to non-land
    # Use a window function to check if previous card was a basic land
    dd_cards = dd_cards.with_columns(
        pl.col("_is_basic_land").shift(1).over("setCode").fill_null(False).alias("_prev_was_land"),
    ).with_columns(
        # Transition occurs when previous was basic land and current is not basic land and not token
        (
            pl.col("_prev_was_land")
            & ~pl.col("_is_basic_land")
            & ~pl.col("_is_token")
        ).alias("_is_transition"),
    ).with_columns(
        # Count transitions to get deck number
        pl.col("_is_transition").cum_sum().over("setCode").alias("_deck_num"),
    ).with_columns(
        # Convert deck number to letter, skip tokens
        pl.when(pl.col("_is_token"))
        .then(pl.lit(None).cast(pl.String))
        .otherwise(
            # 0 -> 'a', 1 -> 'b', etc.
            pl.lit("a").str.replace("a", "")
            + pl.col("_deck_num").map_elements(lambda n: chr(ord("a") + n), return_dtype=pl.String)
        )
        .alias("_calc_duelDeck"),
    ).select(["uuid", "_calc_duelDeck"])

    # Join back calculated duelDeck values
    lf = lf.join(
        dd_cards.rename({"_calc_duelDeck": "_dd_calculated"}),
        on="uuid",
        how="left",
    ).with_columns(
        pl.coalesce(
            pl.col("_dd_calculated"),
            pl.col("duelDeck")
        ).alias("duelDeck")
    ).drop("_dd_calculated")
    
    return lf


def join_set_number_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all setCode+number-based lookups.

    Replaces:
    - join_foreign_data()

    Gets from set_number_lf:
    - foreignData: List[Struct{faceName, flavorText, identifiers{multiverseId, scryfallId}, language, multiverseId, name, text, type, uuid}]
    """
    if ctx.set_number_lf is None:
        return lf.with_columns(
            pl.lit([]).alias("foreignData"),
        )

    lf = lf.join(
        ctx.set_number_lf,
        left_on=["setCode", "number"],
        right_on=["setCode", "number"],
        how="left",
    )

    # Fill nulls and handle language-specific logic
    return lf.with_columns(
        # foreignData only applies to English cards - non-English cards don't have foreign data
        pl.when(pl.col("language") == "English")
        .then(pl.col("foreignData").fill_null([]))
        .otherwise(pl.lit([]))
        .alias("foreignData"),
    )


def join_name_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all name-based lookups.

    Replaces:
    - add_related_cards_struct() (partial - spellbook)
    - add_meld_card_parts()

    Gets from name_lf:
    - spellbook: List[String]
    - cardParts: List[String]

    Note: For meld cards, we also need to check faceName as the join key,
    so this does a second lookup if name lookup misses for meld layouts.
    """
    if ctx.name_lf is None:
        return lf.with_columns(
            pl.lit(None).cast(pl.List(pl.String)).alias("_spellbook_list"),
            pl.lit(None).cast(pl.List(pl.String)).alias("cardParts"),
        )

    # Get columns available in name_lf
    name_lf_cols = ctx.name_lf.collect_schema().names()
    has_cardparts_col = "cardParts" in name_lf_cols
    has_spellbook_col = "spellbook" in name_lf_cols

    # First join on name
    lf = lf.join(
        ctx.name_lf,
        on="name",
        how="left",
    )

    # Handle cardParts for meld cards (join by faceName if name miss)
    if has_cardparts_col:
        # Build faceName lookup (only for meld)
        face_lookup = ctx.name_lf.filter(pl.col("cardParts").is_not_null()).select(
            ["name", pl.col("cardParts").alias("_face_cardParts")]
        )

        lf = lf.join(
            face_lookup,
            left_on="faceName",
            right_on="name",
            how="left",
            suffix="_face",
        )

        # Coalesce: prefer name lookup, fall back to faceName lookup
        lf = lf.with_columns(
            pl.coalesce(
                pl.col("cardParts"),
                pl.col("_face_cardParts"),
            ).alias("cardParts")
        ).drop("_face_cardParts", strict=False)
    else:
        # No cardParts data available
        lf = lf.with_columns(
            pl.lit(None).cast(pl.List(pl.String)).alias("cardParts")
        )

    # Rename spellbook for compatibility with add_related_cards_from_context
    if has_spellbook_col:
        lf = lf.rename({"spellbook": "_spellbook_list"})
    else:
        lf = lf.with_columns(
            pl.lit(None).cast(pl.List(pl.String)).alias("_spellbook_list")
        )

    return lf


def join_face_flavor_names(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join face flavor name data from resource file.

    Multi-face cards (like LMAR Marvel cards) have per-face flavor names
    that aren't in Scryfall bulk data. This joins the supplemental data.

    Requires: scryfallId, side columns
    Updates: faceFlavorName, flavorName (if override exists)
    """
    if ctx.face_flavor_names_df is None:
        return lf

    # Join on scryfallId and side
    lf = lf.join(
        ctx.face_flavor_names_df.lazy(),
        on=["scryfallId", "side"],
        how="left",
    )

    # Override faceFlavorName and flavorName with joined values if present
    return lf.with_columns(
        pl.coalesce(pl.col("_faceFlavorName"), pl.col("faceFlavorName")).alias(
            "faceFlavorName"
        ),
        pl.coalesce(pl.col("_flavorNameOverride"), pl.col("flavorName")).alias(
            "flavorName"
        ),
    ).drop(["_faceFlavorName", "_flavorNameOverride"], strict=False)


def join_signatures(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join signature data for world championship gold-border cards.

    Replaces: add_token_signatures() (partial - memorabilia part)

    Gets from signatures_lf:
    - signature: String (player name for gold-border memorabilia)

    Art Series signatures still use artist field (handled separately).
    """
    if ctx.signatures_lf is None:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_wc_signature"))

    # Extract number prefix for join (e.g., "gb" from "gb123")
    lf = lf.with_columns(
        pl.col("number").str.extract(r"^([^0-9]+)", 1).alias("_num_prefix")
    )

    lf = lf.join(
        ctx.signatures_lf,
        left_on=["setCode", "_num_prefix"],
        right_on=["setCode", "numberPrefix"],
        how="left",
    )

    # Rename to avoid conflict with final signature column
    return lf.rename({"signature": "_wc_signature"}).drop("_num_prefix", strict=False)


def add_uuid_from_cache(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute uuid from pre-joined cachedUuid or compute fresh.

    Requires cachedUuid column from join_identifiers().
    Falls back to computing uuid5 if no cached value exists.

    This replaces the complex add_uuid_expr() function.
    """
    return lf.with_columns(
        pl.coalesce(
            pl.col("cachedUuid"),
            _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
        ).alias("uuid")
    ).drop("cachedUuid", strict=False)


def add_signatures_combined(
    lf: pl.LazyFrame,
    _ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add signature field combining Art Series and world championship logic.

    Requires _wc_signature column from join_signatures().

    Logic:
    - Art Series (except MH1): signature = artist
    - Memorabilia with gold border: signature from world_championship lookup

    Also updates finishes to include "signed" where signature exists.
    """
    # Condition expressions
    is_art_series = pl.col("setName").str.ends_with("Art Series") & (
        pl.col("setCode") != "MH1"
    )
    is_memorabilia = pl.col("setType") == "memorabilia"

    # Extract number parts for memorabilia logic
    lf = lf.with_columns(
        [
            pl.col("number").str.extract(r"^[^0-9]+([0-9]+)", 1).alias("_num_digits"),
            pl.col("number").str.extract(r"^[^0-9]+[0-9]+(.*)", 1).alias("_num_suffix"),
        ]
    )

    # Compute signature field
    memorabilia_signature = (
        pl.when(
            (pl.col("borderColor") == "gold")
            & pl.col("_wc_signature").is_not_null()
            & ~((pl.col("_num_digits") == "0") & (pl.col("_num_suffix") == "b"))
        )
        .then(pl.col("_wc_signature"))
        .otherwise(pl.lit(None))
    )

    lf = lf.with_columns(
        pl.when(is_art_series)
        .then(pl.col("artist"))
        .when(is_memorabilia)
        .then(memorabilia_signature)
        .otherwise(pl.lit(None))
        .alias("signature")
    )

    # Update finishes to include "signed" where signature exists
    lf = lf.with_columns(
        pl.when(
            pl.col("signature").is_not_null()
            & ~pl.col("finishes").list.contains("signed")
        )
        .then(pl.col("finishes").list.concat(pl.lit(["signed"])))
        .otherwise(pl.col("finishes"))
        .alias("finishes")
    )

    # Cleanup temp columns
    return lf.drop(["_num_digits", "_num_suffix", "_wc_signature"], strict=False)


def add_related_cards_from_context(
    lf: pl.LazyFrame,
    _ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build relatedCards struct using pre-joined spellbook data.

    Requires _spellbook_list column from join_name_data().
    Requires reverseRelated column from add_reverse_related().

    Logic (from legacy add_related_cards):
    - Tokens: get relatedCards with reverseRelated (and spellbook if alchemy set)
    - Non-tokens in alchemy sets: get relatedCards with spellbook only

    Replaces: add_related_cards_struct()
    """
    is_token = (
        pl.col("layout").is_in(constants.TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

    # Alchemy spellbook check (applies to both tokens and non-tokens)
    is_alchemy = pl.col("setType").str.to_lowercase().str.contains("alchemy")
    has_spellbook = (
        is_alchemy
        & pl.col("_spellbook_list").is_not_null()
        & (pl.col("_spellbook_list").list.len() > 0)
    )

    # Tokens get reverseRelated
    has_reverse = pl.col("reverseRelated").is_not_null() & (
        pl.col("reverseRelated").list.len() > 0
    )

    # Build struct based on what data is present
    # For tokens: include both spellbook and reverseRelated
    # For non-tokens: include spellbook only (if alchemy set)
    return lf.with_columns(
        pl.when(is_token & (has_spellbook | has_reverse))
        .then(
            pl.struct(
                spellbook=pl.col("_spellbook_list"),
                reverseRelated=pl.col("reverseRelated"),
            )
        )
        .when(~is_token & has_spellbook)
        .then(
            pl.struct(
                spellbook=pl.col("_spellbook_list"),
                reverseRelated=pl.lit(None).cast(pl.List(pl.String)),
            )
        )
        .otherwise(pl.lit(None))
        .alias("relatedCards")
    ).drop("_spellbook_list", strict=False)


# 6.8:
def build_cards(
    ctx: "PipelineContext",
) -> "PipelineContext":
    """
    Build all cards using consolidated joins.

    This is the refactored version of build_cards() that uses the
    consolidated lookup tables for ~50% fewer joins.

    Args:
        ctx: PipelineContext with consolidated lookups
        set_codes: Optional list of set codes to filter

    Returns:
        PipelineContext with final_cards_lf set
    """
    set_codes = ctx.sets_to_build
    output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Executing card pipeline...")
    if ctx.sets_df is None:
        raise ValueError("sets_df is not available in context")
    sets_raw = ctx.sets_df.rename({"code": "set"})
    # Ensure it's a LazyFrame for the join
    sets_lf = sets_raw.lazy() if isinstance(sets_raw, pl.DataFrame) else sets_raw

    # Filter early by set codes before expensive operations
    # Include English cards + non-English cards with unique collector numbers
    # (like Japanese showcase arts or Phyrexian text variants that don't exist in English)
    if ctx.cards_lf is None:
        raise ValueError("cards_lf is not available in context")

    base_lf = ctx.cards_lf.with_columns(pl.col("set").str.to_uppercase().alias("_set_upper"))
    if set_codes:
        base_lf = base_lf.filter(pl.col("_set_upper").is_in(set_codes))

    # Get English collector numbers per set (with a marker column)
    english_numbers = (
        base_lf.filter(pl.col("lang") == "en")
        .select(["_set_upper", "collectorNumber"])
        .unique()
        .with_columns(pl.lit(True).alias("_has_english"))
    )

    # Include: English cards + non-English cards whose collector number doesn't exist in English
    lf = base_lf.join(
        english_numbers,
        on=["_set_upper", "collectorNumber"],
        how="left",
    ).filter(
        # English cards OR non-English cards with no English equivalent
        (pl.col("lang") == "en") | pl.col("_has_english").is_null()
    ).drop(["_has_english", "_set_upper"])

    # Apply scryfall_id filter for deck-only builds
    if ctx.scryfall_id_filter:
        lf = lf.filter(pl.col("id").is_in(ctx.scryfall_id_filter))
        LOGGER.info(f"Applied scryfall_id filter: {len(ctx.scryfall_id_filter):,} IDs")

    # explicitly select needed set columns for join to reduce memory usage
    set_columns_needed = ["set", "setType", "setReleasedAt", "block", "foilOnly", "nonfoilOnly"]

    # Select only those columns before joining
    lf = base_lf.with_columns(pl.col("set").str.to_uppercase()).join(
        sets_lf.select([c for c in set_columns_needed if c in sets_lf.collect_schema().names()]),
        on="set",
        how="left"
    )

    # Per-card transforms (streaming OK)
    lf = (
        lf.pipe(explode_card_faces)
        .pipe(partial(assign_meld_sides, ctx=ctx))
        .pipe(partial(update_meld_names, ctx=ctx))
        .pipe(detect_aftermath_layout)
        .pipe(add_basic_fields)
        .pipe(format_planeswalker_text)  # Wrap loyalty costs in brackets: +1: -> [+1]:
        .pipe(add_original_release_date)  # Set for promos with card-specific release dates
        .drop([
            "lang", "frame", "fullArt", "textless", "oversized", "promo", "reprint",
            "storySpotlight", "reserved", "digital", "cmc", "typeLine", "oracleText",
            "printedTypeLine", "setReleasedAt"  # setReleasedAt only needed for originalReleaseDate computation
        ], strict=False)
        .pipe(partial(join_face_flavor_names, ctx=ctx))
        .pipe(parse_type_line_expr)
        .pipe(add_mana_info)
        .pipe(add_card_attributes)
        .pipe(filter_keywords_for_face)
        .pipe(add_booster_types)
        .drop(["contentWarning", "handModifier", "lifeModifier", "gameChanger", "_in_booster", "_meld_face_name"], strict=False)
        .pipe(partial(add_legalities_struct, ctx=ctx))
        .pipe(partial(add_availability_struct, ctx=ctx))
    )
    
    lf = (
        lf.pipe(partial(join_identifiers, ctx=ctx))
        .pipe(partial(join_oracle_data, ctx=ctx))
        .pipe(partial(join_set_number_data, ctx=ctx))
        .pipe(partial(join_name_data, ctx=ctx))
        .pipe(partial(join_cardmarket_ids, ctx=ctx))
    )

    # CHECKPOINT: Materialize after joins to reset lazy plan
    LOGGER.info("Checkpoint: materializing after joins...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(add_identifiers_struct)
        .drop(["mcmId", "mcmMetaId", "arenaId", "mtgoId", "mtgoFoilId", "tcgplayerId", "tcgplayerEtchedId", "illustrationId", "cardBackId"], strict=False)
        .pipe(add_uuid_from_cache)
        .pipe(add_identifiers_v4_uuid)
    )

    lf = lf.pipe(calculate_duel_deck)

    lf = lf.pipe(partial(join_gatherer_data, ctx=ctx))

    # CHECKPOINT: Materialize before self-join heavy operations (add_other_face_ids, add_variations, etc.)
    LOGGER.info("Checkpoint: materializing before relationship operations...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(add_other_face_ids)
        # NOTE: add_variations moved to sink_cards() AFTER language filtering
        # to avoid including UUIDs of non-English cards that get filtered out
        .pipe(partial(add_leadership_skills_expr, ctx=ctx))
        .pipe(add_reverse_related)
        .pipe(propagate_salt_to_tokens)
        .pipe(partial(add_related_cards_from_context, _ctx=ctx))
        .pipe(partial(add_alternative_deck_limit, ctx=ctx))
        .drop(["_face_data"], strict=False)
        .pipe(partial(add_is_funny, ctx=ctx))
        .pipe(add_is_timeshifted)
        .pipe(add_purchase_urls_struct)
    )

    # CHECKPOINT: Materialize to break lazy plan complexity before self-join heavy operations
    LOGGER.info("Checkpoint: materializing before final enrichment...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    # Final enrichment (includes self-join patterns)
    lf = (
        lf.pipe(partial(apply_manual_overrides, ctx=ctx))
        .pipe(add_rebalanced_linkage)
        .pipe(link_foil_nonfoil_versions)
        .pipe(partial(add_secret_lair_subsets, ctx=ctx))
        .pipe(partial(add_source_products, ctx=ctx))
    )

    # Signatures + cleanup
    lf = (
        lf.pipe(partial(join_signatures, ctx=ctx))
        .pipe(partial(add_signatures_combined, _ctx=ctx))
        .pipe(drop_raw_scryfall_columns)
    )

    ctx.final_cards_lf = lf

    # Sink to partitioned parquet for build module
    sink_cards(ctx)

    return ctx


def _expand_card_list(
    decks: pl.DataFrame,
    cards_df: pl.DataFrame,
    col: str,
) -> pl.DataFrame:
    """
    Expand a deck card list column by joining with full card data.

    Takes deck DataFrame with _deck_id and a card list column containing
    [{uuid, count, isFoil, isEtched}, ...] and expands each reference to
    a full card object.

    Args:
        decks: Decks DataFrame with _deck_id and the list column
        cards_df: Full cards DataFrame with all card fields
        col: Name of the card list column to expand (e.g., "mainBoard")

    Returns:
        DataFrame with _deck_id and expanded card list column
    """
    if col not in decks.columns:
        return decks.select("_deck_id").with_columns(pl.lit([]).alias(col))

    exploded = (
        decks.select(["_deck_id", col]).explode(col).filter(pl.col(col).is_not_null())
    )

    if len(exploded) == 0:
        return decks.select("_deck_id").unique().with_columns(pl.lit([]).alias(col))

    exploded = exploded.with_columns(
        pl.col(col).struct.field("uuid").alias("_ref_uuid"),
        pl.col(col).struct.field("count"),
        pl.col(col).struct.field("isFoil"),
        pl.col(col).struct.field("isEtched"),
    ).drop(col)

    joined = exploded.join(
        cards_df,
        left_on="_ref_uuid",
        right_on="uuid",
        how="left",
    ).with_columns(pl.col("_ref_uuid").alias("uuid"))

    card_cols = [c for c in joined.columns if c not in ("_deck_id", "_ref_uuid")]

    result = joined.group_by("_deck_id").agg(pl.struct(card_cols).alias(col))

    all_deck_ids = decks.select("_deck_id").unique()
    result = all_deck_ids.join(result, on="_deck_id", how="left").with_columns(
        pl.col(col).fill_null([])
    )

    return result


def build_expanded_decks_df(
    ctx: PipelineContext,
    set_codes: list[str] | str | None = None,
) -> pl.DataFrame:
    """
    Build decks DataFrame with fully expanded card objects.

    Unlike build_decks() which produces minimal {count, uuid} references,
    this function joins with card data to produce complete card objects
    in each deck's card lists.

    Args:
        ctx: Pipeline context with deck data.
        set_codes: Optional set code(s) filter.

    Returns:
        DataFrame with deck structure containing fully expanded card objects.
    """
    if ctx.decks_df is None:
        LOGGER.warning("GitHub decks data not loaded in cache")
        return pl.DataFrame()

    # Filter decks by set codes first (before collecting UUIDs)
    # Use set_codes param if provided, otherwise fall back to ctx.sets_to_build
    filter_codes = set_codes or ctx.sets_to_build
    decks_lf = ctx.decks_df
    if filter_codes:
        if isinstance(filter_codes, str):
            decks_lf = decks_lf.filter(pl.col("setCode") == filter_codes.upper())
        else:
            upper_codes = [s.upper() for s in filter_codes]
            decks_lf = decks_lf.filter(pl.col("setCode").is_in(upper_codes))

    decks_df = decks_lf.collect()

    if len(decks_df) == 0:
        LOGGER.info("No decks found for specified sets")
        return pl.DataFrame()

    # Collect all UUIDs referenced in decks (mainBoard, sideBoard, commander, tokens)
    all_uuids: set[str] = set()
    for col in ["mainBoard", "sideBoard", "commander", "tokens"]:
        if col in decks_df.columns:
            for card_list in decks_df[col].to_list():
                if card_list:
                    for card_ref in card_list:
                        if isinstance(card_ref, dict) and card_ref.get("uuid"):
                            all_uuids.add(card_ref["uuid"])

    LOGGER.info(f"Deck expansion needs {len(all_uuids):,} unique UUIDs")

    if not all_uuids:
        LOGGER.warning("No card UUIDs found in deck references")
        return pl.DataFrame()

    # Read cards from parquet, filtered by UUIDs
    # Both cards and tokens are in the same parquet dir (tokens have T prefix set codes)
    parquet_dir = constants.CACHE_PATH / "_parquet"

    uuid_list = list(all_uuids)

    # Scan all parquet (cards + tokens) with UUID filter
    cards_df = pl.DataFrame()
    if parquet_dir.exists():
        cards_lf = pl.scan_parquet(parquet_dir / "**/*.parquet")
        cards_df = cards_lf.filter(pl.col("uuid").is_in(uuid_list)).collect()
        LOGGER.info(
            f"Loaded {len(cards_df):,} cards/tokens for deck expansion (filtered)"
        )

    available_cols = decks_df.columns

    # Add unique deck identifier for re-aggregation
    decks_df = decks_df.with_row_index("_deck_id")

    # Card list columns to expand (using cards_df)
    card_list_cols = ["mainBoard", "sideBoard", "commander"]

    # Expand each card list column (cards_df includes both cards and tokens)
    expanded_lists = {}
    for col in card_list_cols:
        expanded_lists[col] = _expand_card_list(decks_df, cards_df, col)

    # Expand tokens (also from cards_df - tokens have T-prefix set codes)
    expanded_lists["tokens"] = _expand_card_list(decks_df, cards_df, "tokens")

    # Start with deck metadata
    result = decks_df.select(
        "_deck_id",
        "setCode",
        pl.col("setCode").alias("code"),
        "name",
        "type",
        (
            pl.col("releaseDate")
            if "releaseDate" in available_cols
            else pl.lit(None).cast(pl.String).alias("releaseDate")
        ),
        # sealedProductUuids should stay null when not present (don't fill with [])
        (
            pl.col("sealedProductUuids")
            if "sealedProductUuids" in available_cols
            else pl.lit(None).cast(pl.List(pl.String)).alias("sealedProductUuids")
        ),
        (
            pl.col("sourceSetCodes").fill_null([])
            if "sourceSetCodes" in available_cols
            else pl.lit([]).cast(pl.List(pl.String)).alias("sourceSetCodes")
        ),
        (
            pl.col("displayCommander").fill_null([])
            if "displayCommander" in available_cols
            else pl.lit([]).cast(pl.List(pl.String)).alias("displayCommander")
        ),
        (
            pl.col("planes").fill_null([])
            if "planes" in available_cols
            else pl.lit([]).cast(pl.List(pl.String)).alias("planes")
        ),
        (
            pl.col("schemes").fill_null([])
            if "schemes" in available_cols
            else pl.lit([]).cast(pl.List(pl.String)).alias("schemes")
        ),
    )

    # Join expanded card lists
    for col in [*card_list_cols, "tokens"]:
        result = result.join(expanded_lists[col], on="_deck_id", how="left")

    result = result.drop("_deck_id")

    return result


def build_sealed_products_lf(
    ctx: PipelineContext, _set_code: str | None = None
) -> pl.LazyFrame:
    """
    Build sealed products LazyFrame with contents struct.

    Joins github_sealed_products with github_sealed_contents
    and aggregates contents by type (card, sealed, other).
    Also builds purchaseUrls from identifiers.

    Args:
        set_code: Optional set code filter. If None, returns all sets.

    Returns:
        LazyFrame with columns: setCode, name, category, subtype, releaseDate,
        identifiers (struct), contents (struct), purchaseUrls (struct), uuid
    """
    products_lf = ctx.sealed_products_df
    contents_lf = ctx.sealed_contents_df
    if products_lf is None or contents_lf is None:
        LOGGER.warning("GitHub sealed products data not loaded in cache")
        return pl.DataFrame()

    # Convert to LazyFrames for processing if needed
    if not isinstance(products_lf, pl.LazyFrame):
        products_lf = products_lf.lazy()

    if not isinstance(contents_lf, pl.LazyFrame):
        contents_lf = contents_lf.lazy()

    # Aggregate contents by product and content_type
    # Each content type becomes a list of structs
    card_contents = (
        contents_lf.filter(pl.col("contentType") == "card")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                name=pl.col("name"),
                number=pl.col("number"),
                set=pl.col("set"),
                uuid=pl.col("uuid"),
                foil=pl.col("foil"),
            ).alias("_card_list")
        )
    )

    sealed_contents = (
        contents_lf.filter(pl.col("contentType") == "sealed")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                count=pl.col("count"),
                name=pl.col("name"),
                set=pl.col("set"),
                uuid=pl.col("uuid"),
            ).alias("_sealed_list")
        )
    )

    other_contents = (
        contents_lf.filter(pl.col("contentType") == "other")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                name=pl.col("name"),
            ).alias("_other_list")
        )
    )

    # Extract product-level cardCount (same value per product, take first)
    product_card_count = (
        contents_lf
        .filter(pl.col("cardCount").is_not_null())
        .group_by(["setCode", "productName"])
        .agg(pl.col("cardCount").first().alias("cardCount"))
    )

    # Join contents to products
    result = (
        products_lf.join(card_contents, on=["setCode", "productName"], how="left")
        .join(sealed_contents, on=["setCode", "productName"], how="left")
        .join(other_contents, on=["setCode", "productName"], how="left")
        .join(product_card_count, on=["setCode", "productName"], how="left")
    )

    # Build contents struct
    result = result.with_columns(
        pl.struct(
            card=pl.col("_card_list"),
            sealed=pl.col("_sealed_list"),
            other=pl.col("_other_list"),
        ).alias("contents")
    ).drop(["_card_list", "_sealed_list", "_other_list"])

    # Generate UUID for each product (uuid5 from product name)
    result = result.with_columns(
        _uuid5_expr("productName").alias("uuid")
    )

    # Build purchaseUrls from identifiers if present
    # create MTGJSON redirect URLs
    base_url = "https://mtgjson.com/links/"

    # Build URL hash columns for each provider
    purchase_url_fields = []
    hash_cols_added: list[str] = []  # Track hash columns to avoid extra schema call

    # Check if identifiers column exists and extract provider IDs
    result_schema = result.collect_schema()
    result_cols = result_schema.names()
    # Cache optional column checks now to avoid schema call later
    has_release_date = "releaseDate" in result_cols
    has_release_date_snake = "release_date" in result_cols
    has_card_count = "cardCount" in result_cols

    if "identifiers" in result_cols:
        id_schema = result_schema.get("identifiers")
        if isinstance(id_schema, pl.Struct):
            id_fields = {f.name for f in id_schema.fields}

            # Card Kingdom
            if "cardKingdomId" in id_fields:
                result = result.with_columns(
                    plh.concat_str([pl.col("uuid"), pl.lit("cardKingdom")])
                    .chash.sha2_256().str.slice(0, 16)
                    .alias("_ck_hash")
                )
                hash_cols_added.append("_ck_hash")
                purchase_url_fields.append(
                    pl.when(
                        pl.col("identifiers")
                        .struct.field("cardKingdomId")
                        .is_not_null()
                    )
                    .then(pl.lit(base_url) + pl.col("_ck_hash"))
                    .otherwise(None)
                    .alias("cardKingdom")
                )

            # TCGPlayer
            if "tcgplayerProductId" in id_fields:
                result = result.with_columns(
                    plh.concat_str([pl.col("uuid"), pl.lit("tcgplayer")])
                    .chash.sha2_256().str.slice(0, 16)
                    .alias("_tcg_hash")
                )
                hash_cols_added.append("_tcg_hash")
                purchase_url_fields.append(
                    pl.when(
                        pl.col("identifiers")
                        .struct.field("tcgplayerProductId")
                        .is_not_null()
                    )
                    .then(pl.lit(base_url) + pl.col("_tcg_hash"))
                    .otherwise(None)
                    .alias("tcgplayer")
                )

    # Build purchaseUrls struct if we have any fields
    if purchase_url_fields:
        result = result.with_columns(
            pl.struct(purchase_url_fields).alias("purchaseUrls")
        )
        # Drop temporary hash columns (use tracked list, avoid schema call)
        if hash_cols_added:
            result = result.drop(hash_cols_added, strict=False)
    else:
        # Add empty purchaseUrls struct
        result = result.with_columns(pl.struct([]).alias("purchaseUrls"))

    # Select final columns (all already camelCase from cache normalization)
    select_cols = [
        "setCode",
        pl.col("productName").alias("name"),
        pl.col("category").str.to_lowercase(),
        pl.col("subtype").str.to_lowercase(),
        "identifiers",
        "contents",
        "purchaseUrls",
        "uuid",
    ]

    # Add optional columns if available (use cached checks, avoid schema call)
    # releaseDate (might be release_date from model)
    if has_release_date:
        select_cols.insert(4, "releaseDate")
    elif has_release_date_snake:
        select_cols.insert(4, pl.col("release_date").alias("releaseDate"))

    # cardCount from contents aggregation
    if has_card_count:
        select_cols.append("cardCount")

    sealed_products_lf = result.select(select_cols)

    return sealed_products_lf


def build_set_metadata_df(
    ctx: PipelineContext,
) -> pl.DataFrame:
    """
    Build a DataFrame containing all set-level metadata.

    Includes MTGJSON set-level fields:
    - Core: code, name, releaseDate, type, block
    - Sizes: baseSetSize, totalSetSize
    - Identifiers: mcmId, mcmName, tcgplayerGroupId, mtgoCode, keyruneCode, tokenSetCode
    - Flags: isFoilOnly, isOnlineOnly, isNonFoilOnly
    - Nested: booster, translations
    - Languages: languages (derived from foreign data)

    Excludes Scryfall-only fields:
    - object, uri, scryfallUri, searchUri, iconSvgUri (except for keyruneCode extraction)
    """
    if ctx is None:
        ctx = PipelineContext.from_global_cache()

    sets_lf = ctx.sets_df
    if sets_lf is None:
        raise ValueError("sets_df is not available in context")
    # Keep as LazyFrame for initial operations, will collect later when needed
    if not isinstance(sets_lf, pl.LazyFrame):
        sets_lf = sets_lf.lazy()

    # Get booster configs from cache
    booster_lf = ctx.boosters_df
    if booster_lf is not None:
        if not isinstance(booster_lf, pl.LazyFrame):
            booster_lf = booster_lf.lazy()
    else:
        booster_lf = (
            pl.DataFrame({"setCode": [], "config": []})
            .cast({"setCode": pl.String, "config": pl.String})
            .lazy()
        )

    # Load translations (keyed by SET NAME, not code)
    translations_by_name: dict[str, dict[str, str | None]] = {}
    translations_path = constants.RESOURCE_PATH / "mkm_set_name_translations.json"
    if translations_path.exists():
        with translations_path.open(encoding="utf-8") as f:
            raw_translations = json.load(f)
            for set_name, langs in raw_translations.items():
                translations_by_name[set_name] = {
                    "Chinese Simplified": langs.get("zhs"),
                    "Chinese Traditional": langs.get("zht"),
                    "French": langs.get("fr"),
                    "German": langs.get("de"),
                    "Italian": langs.get("it"),
                    "Japanese": langs.get("ja"),
                    "Korean": langs.get("ko"),
                    "Portuguese (Brazil)": langs.get("pt"),
                    "Russian": langs.get("ru"),
                    "Spanish": langs.get("es"),
                }

    # Get MCM set data from CardMarket provider
    mcm_set_map: dict[str, dict[str, Any]] = {}
    try:
        cardmarket_provider = CardMarketProvider()
        mcm_set_map = cardmarket_provider.set_map or {}
    except Exception as e:
        LOGGER.warning(f"Could not load CardMarket set data: {e}")

    # Get available columns from sets_lf schema
    available_cols = sets_lf.collect_schema().names()

    # Scryfall-only fields to exclude from output
    scryfall_only_fields = {
        "object",
        "uri",
        "scryfallUri",
        "searchUri",
        "iconSvgUri",
        "id",
        "scryfall_id",
        "arena_code",
        "scryfall_set_uri",
    }

    # Build set metadata DataFrame with all MTGJSON fields
    base_exprs = [
        pl.col("code").str.to_uppercase().alias("code"),
        pl.col("name"),
        pl.col("setReleasedAt").alias("releaseDate"),
        pl.col("setType").alias("type"),
        pl.col("digital").alias("isOnlineOnly"),
        pl.col("foilOnly").alias("isFoilOnly"),
    ]

    # Add optional columns if they exist
    if "mtgoCode" in available_cols:
        base_exprs.append(pl.col("mtgoCode").str.to_uppercase().alias("mtgoCode"))
    if "tcgplayerId" in available_cols:
        base_exprs.append(pl.col("tcgplayerId").alias("tcgplayerGroupId"))
    if "nonfoilOnly" in available_cols:
        base_exprs.append(pl.col("nonfoilOnly").alias("isNonFoilOnly"))
    if "parentSetCode" in available_cols:
        base_exprs.append(
            pl.col("parentSetCode").str.to_uppercase().alias("parentCode")
        )
    if "block" in available_cols:
        base_exprs.append(pl.col("block"))

    # Add set sizes from Scryfall data
    if "cardCount" in available_cols:
        base_exprs.append(pl.col("cardCount").alias("totalSetSize"))
    if "printedSize" in available_cols:
        base_exprs.append(pl.col("printedSize").alias("baseSetSize"))
    elif "cardCount" in available_cols:
        # Fall back to cardCount if printedSize not available
        base_exprs.append(pl.col("cardCount").alias("baseSetSize"))

    # Keyrune code from icon URL (use Scryfall field but don't include raw URL)
    if "iconSvgUri" in available_cols:
        base_exprs.append(
            pl.col("iconSvgUri")
            .str.extract(r"/([^/]+)\.svg", 1)
            .str.to_uppercase()
            .alias("keyruneCode")
        )

    # Token set code (from join or computed)
    if "tokenSetCode" in available_cols:
        base_exprs.append(
            pl.coalesce(
                pl.col("tokenSetCode"),
                pl.when(pl.col("code").str.starts_with("T"))
                .then(pl.col("code").str.to_uppercase())
                .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase()),
            ).alias("tokenSetCode")
        )
    else:
        base_exprs.append(
            pl.when(pl.col("code").str.starts_with("T"))
            .then(pl.col("code").str.to_uppercase())
            .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase())
            .alias("tokenSetCode")
        )

    set_meta = sets_lf.with_columns(base_exprs)

    # Drop Scryfall-only columns that may have been carried through
    set_meta_cols = set_meta.collect_schema().names()
    cols_to_drop = [
        c
        for c in set_meta_cols
        if c in scryfall_only_fields or c.lower() in scryfall_only_fields
    ]
    if cols_to_drop:
        set_meta = set_meta.drop(cols_to_drop, strict=False)

    # Join with booster configs
    set_meta = set_meta.join(
        booster_lf.with_columns(pl.col("setCode").str.to_uppercase().alias("code")),
        on="code",
        how="left",
    ).rename({"config": "booster"})

    # Add MCM, Cardsphere, translations, and languages data via map lookups
    if isinstance(set_meta, pl.LazyFrame):
        set_meta_df = set_meta.collect()
    else:
        set_meta_df = set_meta
    set_records = set_meta_df.to_dicts()
    for record in set_records:
        set_name = record.get("name", "")
        set_code = record.get("code", "")

        # Keep booster as JSON string - will be parsed in build/assemble.py
        # Parsing here causes Polars to create a union struct schema with all
        # possible keys from all sets when we create pl.DataFrame(set_records)

        # MCM data (lookup by lowercased set name)
        mcm_data = mcm_set_map.get(set_name.lower(), {})
        record["mcmId"] = mcm_data.get("mcmId")
        record["mcmName"] = mcm_data.get("mcmName")
        # MCM Extras set ID (e.g., "Throne of Eldraine: Extras")
        record["mcmIdExtras"] = cardmarket_provider.get_extras_set_id(set_name)

        # isPartialPreview: True if build date is before release date
        from datetime import date
        release_date = record.get("releaseDate")
        if release_date:
            build_date = date.today().isoformat()
            record["isPartialPreview"] = build_date < release_date if build_date < release_date else None
        else:
            record["isPartialPreview"] = None

        # Translations (lookup by set name)
        record["translations"] = translations_by_name.get(
            set_name,
            {
                "Chinese Simplified": None,
                "Chinese Traditional": None,
                "French": None,
                "German": None,
                "Italian": None,
                "Japanese": None,
                "Korean": None,
                "Portuguese (Brazil)": None,
                "Russian": None,
                "Spanish": None,
            },
        )

        # Ensure baseSetSize and totalSetSize have defaults
        if record.get("baseSetSize") is None:
            record["baseSetSize"] = record.get("totalSetSize", 0)
        if record.get("totalSetSize") is None:
            record["totalSetSize"] = record.get("baseSetSize", 0)

        # Remove any Scryfall-only fields that slipped through
        for scry_field in scryfall_only_fields:
            record.pop(scry_field, None)

    # Add sets from additional_sets.json resource (MTGJSON-specific sets like Q01, DD3, FWB, MB1)
    # These are sets not in Scryfall but defined in MTGJSON's resource files
    existing_codes = {r["code"] for r in set_records}

    additional_sets_path = constants.RESOURCE_PATH / "additional_sets.json"
    if additional_sets_path.exists():
        with additional_sets_path.open(encoding="utf-8") as f:
            additional_sets = json.load(f)

        for code, set_data in additional_sets.items():
            code_upper = code.upper()
            if code_upper not in existing_codes:
                new_record = {
                    "code": code_upper,
                    "name": set_data.get("name", code_upper),
                    "releaseDate": set_data.get("released_at"),
                    "type": set_data.get("set_type", "box"),
                    "isOnlineOnly": set_data.get("digital", False),
                    "isFoilOnly": set_data.get("foil_only", False),
                    "isNonFoilOnly": set_data.get("nonfoil_only", False),
                    "parentCode": set_data.get("parent_set_code", "").upper() if set_data.get("parent_set_code") else None,
                    "block": set_data.get("block"),
                    "tcgplayerGroupId": set_data.get("tcgplayer_id"),
                    "baseSetSize": 0,
                    "totalSetSize": 0,
                    "keyruneCode": code_upper,
                    "tokenSetCode": f"T{code_upper}",
                    "translations": {
                        "Chinese Simplified": None,
                        "Chinese Traditional": None,
                        "French": None,
                        "German": None,
                        "Italian": None,
                        "Japanese": None,
                        "Korean": None,
                        "Portuguese (Brazil)": None,
                        "Russian": None,
                        "Spanish": None,
                    },
                    "isPartialPreview": None,
                }
                set_records.append(new_record)
                LOGGER.debug(f"Added additional set: {code_upper}")

    return pl.DataFrame(set_records)
