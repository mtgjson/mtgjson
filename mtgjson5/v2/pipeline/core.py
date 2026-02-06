"""
MTGJSON card data pipeline.

This module contains the complete data pipeline for transforming Scryfall bulk data
into MTGJSON format, including card processing, set building, and output generation.
"""

import contextlib
import json
from collections.abc import Callable
from datetime import date
from functools import partial
from typing import Any

import polars as pl
import polars_hash as plh

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import LOGGER, to_camel_case, to_snake_case
from mtgjson5.v2.consts import (
    BASIC_LAND_NAMES,
    CARD_MARKET_BUFFER,
    EXCLUDE_FROM_OUTPUT,
    LANGUAGE_MAP,
    MULTI_WORD_SUB_TYPES,
    OMIT_EMPTY_LIST_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SORTED_LIST_FIELDS,
    SUPER_TYPES,
    TOKEN_LAYOUTS,
)
from mtgjson5.v2.data import PipelineContext
from mtgjson5.v2.models.schemas import (
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    TOKEN_EXCLUDE,
)
from mtgjson5.v2.models.scryfall import CardFace
from mtgjson5.v2.pipeline.expressions import (
    calculate_cmc_expr,
    extract_colors_from_mana_expr,
    order_finishes_expr,
    sort_colors_wubrg_expr,
)
from mtgjson5.v2.pipeline.lookups import add_meld_other_face_ids, apply_meld_overrides

# List of raw Scryfall columns to drop after transformation to MTGJSON format
_SCRYFALL_COLUMNS_TO_DROP = [
    "lang",  # -> language (via replace_strict)
    "frame",  # -> frameVersion
    "fullArt",  # -> isFullArt
    "textless",  # -> isTextless
    "oversized",  # -> isOversized
    "promo",  # -> isPromo
    "reprint",  # -> isReprint
    "storySpotlight",  # -> isStorySpotlight
    "reserved",  # -> isReserved
    "digital",  # -> isOnlineOnly
    "foil",  # dropped (finishes provides hasFoil)
    "nonfoil",  # dropped (finishes provides hasNonFoil)
    "cmc",  # -> manaValue
    "typeLine",  # -> type (face-aware)
    "oracleText",  # -> text (face-aware)
    "printedTypeLine",  # -> printedType (face-aware)
    "contentWarning",  # -> hasContentWarning
    "handModifier",  # -> hand
    "lifeModifier",  # -> life
    "gameChanger",  # -> isGameChanger
    "mcmId",  # intermediate column from CardMarket join
    "mcmMetaId",  # intermediate column from CardMarket join
    "illustrationId",  # -> identifiers.scryfallIllustrationId
    "arenaId",  # -> identifiers.mtgArenaId
    "mtgoId",  # -> identifiers.mtgoId
    "mtgoFoilId",  # -> identifiers.mtgoFoilId
    "tcgplayerId",  # -> identifiers.tcgplayerProductId
    "tcgplayerEtchedId",  # -> identifiers.tcgplayerEtchedProductId
    "_meld_face_name",  # temp column for meld card faceName assignment
]

_ASCII_REPLACEMENTS: dict[str, str] = {
    "Æ": "AE",
    "æ": "ae",
    "Œ": "OE",
    "œ": "oe",
    "ß": "ss",
    "É": "E",
    "È": "E",
    "Ê": "E",
    "Ë": "E",
    "Á": "A",
    "À": "A",
    "Â": "A",
    "Ä": "A",
    "Ã": "A",
    "Í": "I",
    "Ì": "I",
    "Î": "I",
    "Ï": "I",
    "Ó": "O",
    "Ò": "O",
    "Ô": "O",
    "Ö": "O",
    "Õ": "O",
    "Ú": "U",
    "Ù": "U",
    "Û": "U",
    "Ü": "U",
    "Ý": "Y",
    "Ñ": "N",
    "Ç": "C",
    "é": "e",
    "è": "e",
    "ê": "e",
    "ë": "e",
    "á": "a",
    "à": "a",
    "â": "a",
    "ä": "a",
    "ã": "a",
    "í": "i",
    "ì": "i",
    "î": "i",
    "ï": "i",
    "ó": "o",
    "ò": "o",
    "ô": "o",
    "ö": "o",
    "õ": "o",
    "ú": "u",
    "ù": "u",
    "û": "u",
    "ü": "u",
    "ý": "y",
    "ÿ": "y",
    "ñ": "n",
    "ç": "c",
    "꞉": "",  # U+A789 modifier letter colon (ACR cards - Ratonhnhake:ton)
    "Š": "S",  # WC97/WC99 tokens (Šlemr)
    "š": "s",
    "®": "",  # UGL card (trademark symbol)
}


def _uuid5_expr(col_name: str) -> pl.Expr:
    """Generate UUID5 from a column name using DNS namespace."""
    return plh.col(col_name).uuidhash.uuid5()


def _uuid5_concat_expr(col1: pl.Expr, col2: pl.Expr, default: str = "a") -> pl.Expr:
    """Generate UUID5 from concatenation of two columns."""
    return plh.col(col1.meta.output_name()).uuidhash.uuid5_concat(col2, default=default)


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
    mapping_df = pl.DataFrame(
        {
            "name": list(front_to_result.keys()),
            "_melded_result_name": list(front_to_result.values()),
        }
    )

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
                pl.when(
                    pl.col(field) == True
                )  # noqa: E712  # pylint: disable=singleton-comparison
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

                if sf.dtype in (pl.String, pl.Utf8):
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
            if (
                omit_empty
                and key in OPTIONAL_BOOL_FIELDS
                and cleaned_value is False
                and key not in REQUIRED_SET_BOOL_FIELDS
            ):
                continue

            # Omit empty collections (except required list fields and legalities)
            if (
                omit_empty
                and isinstance(cleaned_value, dict | list)
                and not cleaned_value
            ):
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
        elif (
            field_name == "rulings" and result_list and isinstance(result_list[0], dict)
        ):
            result_list = sorted(
                result_list, key=lambda r: (r.get("date", ""), r.get("text", ""))
            )

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
    return lf.drop(_SCRYFALL_COLUMNS_TO_DROP, strict=False)


def format_planeswalker_text(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Wrap planeswalker loyalty ability costs in square brackets.
    """
    return lf.with_columns(
        pl.col("text")
        .str.replace_all(r"(?m)^([+\u2212−]?[\dX]+):", r"[$1]:")
        .alias("text")
    )  # noqa: RUF001


def add_original_release_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set originalReleaseDate for cards with release dates different from their set.
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
    """

    def face_field(field_name: str) -> pl.Expr:
        struct_field = (
            to_snake_case(field_name) if "_" not in field_name else field_name
        )
        return pl.coalesce(
            pl.col("_face_data").struct.field(struct_field),
            pl.col(field_name),
        )

    face_name = face_field("name")
    ascii_face_name = _ascii_name_expr(face_name)
    ascii_full_name = _ascii_name_expr(pl.col("name"))
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
                pl.col("name").alias("name"),
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
                            "art_series",
                        ]
                    )
                )
                .then(face_field("name"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceName"),
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
                .then(face_field("flavorName"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceFlavorName"),
                # Face-aware fields (must have explicit aliases to avoid duplicates)
                face_field("manaCost").alias("manaCost"),
                face_field("typeLine").alias("type"),
                pl.when(pl.col("layout") == "art_series")
                .then(pl.lit(None).cast(pl.String))
                .otherwise(face_field("oracleText"))
                .alias("text"),
                face_field("flavorText").alias("flavorText"),
                face_field("power").alias("power"),
                face_field("toughness").alias("toughness"),
                face_field("loyalty").alias("loyalty"),
                face_field("defense").alias("defense"),
                face_field("artist").alias("artist"),
                face_field("watermark").alias("watermark"),
                face_field("illustrationId").alias("illustrationId"),
                face_field("colorIndicator").alias("colorIndicator"),
                pl.when(pl.col("layout").is_in(["split", "aftermath"]))
                .then(
                    extract_colors_from_mana_expr(
                        pl.col("_face_data").struct.field("mana_cost")
                    )
                )
                .when((pl.col("layout") == "adventure") & (pl.col("side") == "a"))
                .then(sort_colors_wubrg_expr(face_field("colors")))
                .when(
                    (pl.col("layout") == "adventure")
                    & (pl.col("side") == "b")
                    & (
                        pl.col("typeLine")
                        .str.contains(r"(?i)\bLand\b")
                        .max()
                        .over("scryfallId")
                    )
                )
                .then(pl.lit([]).cast(pl.List(pl.String)))
                .when((pl.col("layout") == "adventure") & (pl.col("side") == "b"))
                .then(
                    extract_colors_from_mana_expr(
                        pl.col("_face_data").struct.field("mana_cost")
                    )
                )
                .otherwise(sort_colors_wubrg_expr(face_field("colors")))
                .alias("colors"),
                face_field("printedText").alias("printedText"),
                face_field("printedTypeLine").alias("printedType"),
                face_field("printedName").alias("printedName"),
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
                pl.col("setCode").str.to_uppercase(),
                pl.col("cmc").alias("manaValue"),
                pl.col("colorIdentity"),
                pl.col("producedMana"),
                pl.col("borderColor"),
                pl.col("frame").alias("frameVersion"),
                pl.col("frameEffects"),
                pl.col("securityStamp"),
                pl.col("fullArt").alias("isFullArt"),
                pl.col("textless").alias("isTextless"),
                pl.when(pl.col("setCode").str.to_uppercase() == "OC21")
                .then(pl.lit(True))
                .otherwise(pl.col("oversized"))
                .alias("isOversized"),
                pl.col("promo").alias("isPromo"),
                pl.col("reprint").alias("isReprint"),
                pl.col("storySpotlight").alias("isStorySpotlight"),
                pl.col("reserved").alias("isReserved"),
                pl.col("digital").alias("isOnlineOnly"),
                pl.coalesce(face_field("flavorName"), pl.col("printedName")).alias(
                    "flavorName"
                ),
                pl.col("allParts"),
                pl.col("lang")
                .replace_strict(
                    LANGUAGE_MAP, default=pl.col("lang"), return_dtype=pl.String
                )
                .alias("language"),
            ]
        )
        .with_columns(
            pl.when(ascii_face_name != face_name)
            .then(ascii_full_name)
            .otherwise(None)
            .alias("asciiName"),
            pl.col("booster").alias("_in_booster"),
            pl.col("promoTypes").fill_null([]).alias("promoTypes"),
        )
    )


# 1.3: Parse type_line into supertypes, types, subtypes
def parse_type_line_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Parse type_line into supertypes, types, and subtypes
    """
    super_types_list = list(SUPER_TYPES)
    multi_word_subtypes = list(MULTI_WORD_SUB_TYPES)

    type_line = pl.col("type").fill_null("Card")
    split_type = type_line.str.split(" — ")

    subtypes_part = pl.col("_subtypes_part").str.strip_chars()

    subtypes_processed = subtypes_part
    for mw_subtype in multi_word_subtypes:
        subtypes_processed = subtypes_processed.str.replace_all(
            mw_subtype, mw_subtype.replace(" ", "\x00")
        )

    subtypes_expr = (
        pl.when(pl.col("_subtypes_part").is_null())
        .then(pl.lit([]).cast(pl.List(pl.String)))
        .when(pl.col("_types_part").str.starts_with("Plane"))
        .then(pl.concat_list([pl.col("_subtypes_part").str.strip_chars()]))
        .otherwise(
            subtypes_processed.str.split(" ").list.eval(
                pl.element().str.replace_all("\x00", " ")
            )
        )
    )

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
            subtypes_expr.alias("subtypes"),
        )
        .drop(["_types_part", "_subtypes_part", "_type_words"])
    )


# 1.4: Add mana cost, mana value, and colors
def add_mana_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mana cost, mana value, and colors.
    """
    face_mana_cost = pl.col("_face_data").struct.field("mana_cost")

    return lf.with_columns(
        pl.col("colors").fill_null([]).alias("colors"),
        pl.col("colorIdentity").fill_null([]),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("manaValue"),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("convertedManaCost"),
        pl.when(pl.col("_face_data").is_not_null())
        .then(calculate_cmc_expr(face_mana_cost))
        .otherwise(pl.col("manaValue").cast(pl.Float64).fill_null(0.0))
        .alias("faceManaValue"),
    )


def fix_manavalue_for_multiface(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Fix manaValue for multi-face cards.
    """
    face_mana_cost = pl.col("_face_data").struct.field("mana_cost")
    face_cmc = calculate_cmc_expr(face_mana_cost)

    # Layouts where each face has its own CMC
    face_specific_layouts = ["modal_dfc", "reversible_card"]
    use_face_cmc = pl.col("layout").is_in(face_specific_layouts)

    fixed_mana_value = (
        pl.when(pl.col("_face_data").is_not_null() & use_face_cmc)
        .then(face_cmc)
        .otherwise(pl.col("manaValue"))
    )

    return lf.with_columns(
        fixed_mana_value.alias("manaValue"),
        fixed_mana_value.alias("convertedManaCost"),
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
        pl.col("gameChanger").fill_null(False).alias("isGameChanger"),
        pl.col("layout"),
        pl.col("keywords").fill_null([]).alias("_all_keywords"),
        pl.col("attractionLights").alias("attractionLights"),
        pl.col("allParts").fill_null([]).alias("_all_parts"),
    )


# 1.5.1: Fix promoTypes
def fix_promo_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Fix promoTypes:
    1. Add 'planeswalkerstamped' when collector number ends with 'p'
    2. Remove 'planeswalkerdeck' (covered by boosterTypes.deck)
    3. Convert empty lists to null
    """
    return (
        lf
        # Add planeswalkerstamped for 'p' suffix cards
        .with_columns(
            pl.when(pl.col("number").str.ends_with("p"))
            .then(pl.col("promoTypes").list.concat(pl.lit(["planeswalkerstamped"])))
            .otherwise(pl.col("promoTypes"))
            .alias("promoTypes")
        )
        # Remove planeswalkerdeck
        .with_columns(
            pl.col("promoTypes")
            .list.eval(pl.element().filter(pl.element() != "planeswalkerdeck"))
            .alias("promoTypes")
        )
        # Empty list -> null
        .with_columns(
            pl.when(pl.col("promoTypes").list.len() == 0)
            .then(pl.lit(None).cast(pl.List(pl.String)))
            .otherwise(pl.col("promoTypes"))
            .alias("promoTypes")
        )
    )


def apply_card_enrichment(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Apply card enrichment from card_enrichment.json.

    Enrichment data adds promo_types (e.g., neon ink colors) based on
    set code and collector number.
    """
    enrichment_data = ctx.card_enrichment
    if not enrichment_data:
        return lf

    # Flatten enrichment data into rows: (setCode, number, promo_types_to_add)
    rows = []
    for set_code, cards in enrichment_data.items():
        for number, data in cards.items():
            enrichment = data.get("enrichment", {})
            promo_types = enrichment.get("promo_types", [])
            if promo_types:
                rows.append(
                    {
                        "setCode": set_code.upper(),
                        "number": str(number),
                        "enrichmentPromoTypes": promo_types,
                    }
                )

    if not rows:
        return lf

    enrichment_df = pl.DataFrame(rows).lazy()

    # Left join enrichment data
    lf = lf.join(
        enrichment_df,
        on=["setCode", "number"],
        how="left",
    )

    # Merge enrichment promo_types into promoTypes
    return lf.with_columns(
        pl.when(pl.col("enrichmentPromoTypes").is_not_null())
        .then(
            pl.col("promoTypes")
            .fill_null([])
            .list.concat(pl.col("enrichmentPromoTypes"))
            .list.unique()
        )
        .otherwise(pl.col("promoTypes"))
        .alias("promoTypes")
    ).drop("enrichmentPromoTypes")


def fix_power_toughness_for_multiface(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Fix power/toughness for multi-face cards, using face data when available.
    """
    has_face_data = pl.col("_face_data").is_not_null()
    face_power = pl.col("_face_data").struct.field("power")
    face_toughness = pl.col("_face_data").struct.field("toughness")

    return lf.with_columns(
        pl.when(has_face_data)
        .then(face_power)
        .otherwise(pl.col("power"))
        .alias("power"),
        pl.when(has_face_data)
        .then(face_toughness)
        .otherwise(pl.col("toughness"))
        .alias("toughness"),
    )


def propagate_watermark_to_faces(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    For multi-face cards, propagate face a's watermark to all faces.

    MDFCs, split cards etc. should have the same watermark on all faces.
    Face a (first face) determines the watermark for the entire card.
    This applies even when other faces have their own watermarks (e.g., DGM split cards).
    """
    # Always use the first face's watermark for all faces of a multi-face card
    first_face_watermark = pl.col("watermark").first().over("scryfallId")
    return lf.with_columns(
        pl.when(pl.col("side").is_not_null())
        .then(first_face_watermark)  # Multi-face: use first face's watermark
        .otherwise(pl.col("watermark"))  # Single-face: keep as-is
        .alias("watermark")
    )


def apply_watermark_overrides(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Apply watermark overrides for cards with watermark 'set'.

    Some reprints have a 'set' watermark in Scryfall but MTGJSON enhances this
    to include the original set code, e.g., 'set (LEA)' for Alpha reprints.
    """
    overrides_lf = ctx.watermark_overrides_lf
    if overrides_lf is None:
        return lf

    schema = lf.collect_schema()
    if "watermark" not in schema.names():
        return lf

    lf = lf.join(
        overrides_lf,
        on=["setCode", "name"],
        how="left",
    )

    lf = lf.with_columns(
        pl.when(
            (pl.col("watermark") == "set") & pl.col("_watermarkOverride").is_not_null()
        )
        .then(pl.col("_watermarkOverride"))
        .otherwise(pl.col("watermark"))
        .alias("watermark")
    ).drop("_watermarkOverride")

    return lf


def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter keywords to only those present in the face's text.
    """

    def _filter_logic(s: pl.Series) -> pl.Series:
        out = []
        for row in s:
            txt = (row["text"] or "").lower()
            kws = row["_all_keywords"] or []
            out.append([k for k in kws if k.lower() in txt])
        return pl.Series(out, dtype=pl.List(pl.String))

    return lf.with_columns(
        pl.struct(["text", "_all_keywords"])
        .map_batches(_filter_logic)
        .alias("keywords")
    ).drop("_all_keywords")


def add_booster_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute boosterTypes and isStarter based on Scryfall booster field and promoTypes.
    """
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
            pl.when(~pl.col("_in_booster").fill_null(True))
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
            .alias("isStarter"),
        ]
    ).drop("_in_booster")


def add_legalities_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Builds legalities struct from Scryfall's legalities column.
    """
    lf = lf.unnest("legalities")

    formats = (
        ctx.categoricals.legalities
        if ctx.categoricals is not None and ctx.categoricals.legalities is not None
        else []
    )

    if not formats:
        return lf.with_columns(pl.lit(None).alias("legalities"))

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

    return lf.with_columns(pl.struct(struct_fields).alias("legalities")).drop(
        formats, strict=False
    )


def add_availability_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build availability list from games column.
    """
    schema = lf.collect_schema()

    if "games" not in schema.names():
        return lf.with_columns(
            pl.lit([]).cast(pl.List(pl.String)).alias("availability")
        )

    categoricals = ctx.categoricals
    platforms = categoricals.games if categoricals else []

    if not platforms:
        return lf.with_columns(pl.col("games").alias("availability"))

    games_dtype = schema["games"]

    if isinstance(games_dtype, pl.Struct):
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
    return lf.with_columns(pl.col("games").list.sort().alias("availability"))


def remap_availability_values(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Remap Scryfall game names to MTGJSON availability names.
    - astral -> shandalar (Microprose Shandalar game)
    - sega -> dreamcast (Sega Dreamcast game)
    """
    schema = lf.collect_schema()

    if "availability" not in schema.names():
        return lf

    return lf.with_columns(
        pl.col("availability")
        .list.eval(pl.element().replace({"astral": "shandalar", "sega": "dreamcast"}))
        .list.sort()
    )


def fix_availability_from_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add platforms to availability if their respective ID fields are present.
    """
    schema = lf.collect_schema()

    if "availability" not in schema.names():
        return lf

    exprs = []

    if "mtgoId" in schema.names():
        exprs.append(
            pl.when(
                pl.col("mtgoId").is_not_null()
                & ~pl.col("availability").list.contains("mtgo")
            )
            .then(pl.col("availability").list.concat(pl.lit(["mtgo"])))
            .otherwise(pl.col("availability"))
            .alias("availability")
        )

    if "arenaId" in schema.names():
        exprs.append(
            pl.when(
                pl.col("arenaId").is_not_null()
                & ~pl.col("availability").list.contains("arena")
            )
            .then(pl.col("availability").list.concat(pl.lit(["arena"])))
            .otherwise(pl.col("availability"))
            .alias("availability")
        )

    for expr in exprs:
        lf = lf.with_columns(expr)

    if exprs:
        lf = lf.with_columns(pl.col("availability").list.sort())

    return lf


def join_cardmarket_ids(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add CardMarket identifiers to cards.

    MCM data has two cases:
    1. Modern sets: MCM has collector numbers, join on setCode + name + number
    2. Older sets: MCM has empty numbers, join on setCode + name only
    """
    mcm_df = ctx.mcm_lookup_lf

    if mcm_df is None:
        return lf.with_columns(
            [
                pl.col("cardmarketId").cast(pl.String).alias("mcmId"),
                pl.lit(None).cast(pl.String).alias("mcmMetaId"),
            ]
        )

    if isinstance(mcm_df, pl.LazyFrame):
        mcm_df = mcm_df.collect()  # type: ignore[assignment]

    if len(mcm_df) == 0:  # type: ignore[arg-type]
        return lf.with_columns(
            [
                pl.col("cardmarketId").cast(pl.String).alias("mcmId"),
                pl.lit(None).cast(pl.String).alias("mcmMetaId"),
            ]
        )

    # Split MCM lookup: with numbers vs without numbers
    mcm_with_num = mcm_df.filter(pl.col("number") != "").lazy()
    mcm_no_num = mcm_df.filter(pl.col("number") == "").lazy()

    lf = lf.with_columns(
        [
            pl.col("name").str.to_lowercase().alias("_join_name"),
            # Scryfall numbers often have leading zeros (e.g., "001"),
            # while MCM strips them. We strip them here to match.
            pl.col("number").str.strip_chars_start("0").alias("_join_number"),
        ]
    )

    # First pass: join on setCode + name + number (modern sets with numbers)
    lf = lf.join(
        mcm_with_num,
        left_on=["setCode", "_join_name", "_join_number"],
        right_on=["setCode", "nameLower", "number"],
        how="left",
    )

    # Second pass: for unmatched cards, try joining on setCode + name only
    # (older sets where MCM has no collector numbers)
    lf = lf.join(
        mcm_no_num.select(["setCode", "nameLower", "mcmId", "mcmMetaId"]).rename(
            {"mcmId": "_mcmId2", "mcmMetaId": "_mcmMetaId2"}
        ),
        left_on=["setCode", "_join_name"],
        right_on=["setCode", "nameLower"],
        how="left",
    )

    # Coalesce: prefer first join results, fall back to second join, then Scryfall
    lf = lf.with_columns(
        [
            pl.coalesce(
                pl.col("mcmId"),
                pl.col("_mcmId2"),
                pl.col("cardmarketId").cast(pl.String),
            ).alias("mcmId"),
            pl.coalesce(
                pl.col("mcmMetaId"),
                pl.col("_mcmMetaId2"),
            ).alias("mcmMetaId"),
        ]
    )
    lf = lf.drop(["_join_name", "_join_number", "_mcmId2", "_mcmMetaId2"])

    return lf


# 2.2: add identifiers struct
def add_identifiers_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build the identifiers struct
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
            mcmId=pl.col("mcmId"),
            mcmMetaId=pl.col("mcmMetaId"),
            mtgArenaId=pl.col("arenaId").cast(pl.String),
            mtgoId=pl.col("mtgoId").cast(pl.String),
            mtgoFoilId=pl.col("mtgoFoilId").cast(pl.String),
            multiverseId=pl.col("multiverseIds")
            .list.get(
                pl.min_horizontal(
                    pl.col("faceId").fill_null(0),
                    (pl.col("multiverseIds").list.len() - 1).clip(lower_bound=0),
                ),
                null_on_oob=True,
            )
            .cast(pl.String),
            tcgplayerProductId=pl.col("tcgplayerId").cast(pl.String),
            tcgplayerEtchedProductId=pl.col("tcgplayerEtchedId").cast(pl.String),
            cardKingdomId=pl.col("cardKingdomId"),
            cardKingdomFoilId=pl.col("cardKingdomFoilId"),
            cardKingdomEtchedId=pl.col("cardKingdomEtchedId"),
            cardsphereId=pl.col("cardsphereId"),
            cardsphereFoilId=pl.col("cardsphereFoilId"),
            deckboxId=pl.col("deckboxId"),
            mtgjsonFoilVersionId=pl.lit(None).cast(pl.String),
            mtgjsonNonFoilVersionId=pl.lit(None).cast(pl.String),
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
    gatherer_df = ctx.gatherer_lf

    if gatherer_df is None:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("originalText"),
                pl.lit(None).cast(pl.String).alias("originalType"),
            ]
        )

    # Handle LazyFrame from cache
    if isinstance(gatherer_df, pl.LazyFrame):
        gatherer_df = gatherer_df.collect()  # type: ignore[assignment]

    if len(gatherer_df) == 0:  # type: ignore[arg-type]
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
    is_token = (
        pl.col("types").list.set_intersection(pl.lit(["Token", "Card"])).list.len() > 0
    )

    token_source = pl.concat_str(
        [
            card_name,
            pl.col("colors").list.join("").fill_null(""),
            pl.col("power").fill_null(""),
            pl.col("toughness").fill_null(""),
            pl.col("side").fill_null(""),
            pl.col("setCode").fill_null("").str.slice(1).str.to_uppercase(),
            scryfall_id,
        ]
    )
    normal_source = pl.concat_str([pl.lit("sf"), scryfall_id, card_name])

    # Combine sources and generate UUID5
    combined_source = pl.when(is_token).then(token_source).otherwise(normal_source)
    lf = lf.with_columns(combined_source.alias("_v4_source"))
    lf = lf.with_columns(_uuid5_expr("_v4_source").alias("_mtgjsonV4Id")).drop(
        "_v4_source"
    )
    return lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            [pl.col("_mtgjsonV4Id").alias("mtgjsonV4Id")]
        )
    ).drop("_mtgjsonV4Id")


def add_other_face_ids(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Link multi-face cards via Scryfall ID or name (for meld cards).
    """
    face_links = (
        lf.select(["scryfallId", "uuid", "side"])
        .with_columns(pl.struct(["uuid", "side"]).alias("_face_struct"))
        .sort(["scryfallId", "side"])
        .group_by("scryfallId")
        .agg(pl.col("_face_struct").alias("_all_faces"))
    )

    def _filter_self_from_faces(row: dict) -> list[str]:
        """Filter out current uuid from faces list while preserving side order."""
        all_faces = row["all_faces"]
        self_uuid = row["self_uuid"]
        if all_faces is None:
            return []
        return [f["uuid"] for f in all_faces if f["uuid"] != self_uuid]

    lf = (
        lf.join(face_links, on="scryfallId", how="left")
        .with_columns(
            pl.struct(
                [
                    pl.col("uuid").alias("self_uuid"),
                    pl.col("_all_faces").alias("all_faces"),
                ]
            )
            .map_elements(_filter_self_from_faces, return_dtype=pl.List(pl.String))
            .alias("otherFaceIds")
        )
        .drop("_all_faces")
    )

    lf = add_meld_other_face_ids(lf)

    if ctx.meld_overrides:
        lf = apply_meld_overrides(lf, ctx.meld_overrides)

    is_token = pl.col("types").list.contains("Token")
    has_all_parts = pl.col("_all_parts").list.len() > 0
    face_name_has_double_slash = pl.col("faceName").fill_null("").str.contains("//")
    is_same_name_pattern = (
        pl.col("name").str.split(" // ").list.unique().list.len() == 1
    )
    all_parts_has_no_double_slash = (
        pl.col("_all_parts")  # pylint: disable=singleton-comparison
        .list.eval(pl.element().struct.field("name").str.contains("//").any())
        .list.first()
        .fill_null(False)
        == False  # noqa: E712 - Polars expression, not Python comparison
    )
    is_same_name_reversible_with_no_slash_parts = (
        (pl.col("layout") == "reversible_card")
        & has_all_parts
        & is_same_name_pattern
        & all_parts_has_no_double_slash
    )

    lf = lf.with_columns(
        pl.when(
            (is_token & has_all_parts)
            | face_name_has_double_slash
            | is_same_name_reversible_with_no_slash_parts
        )
        .then(pl.lit(None).cast(pl.List(pl.String)))
        .otherwise(pl.col("otherFaceIds"))
        .alias("otherFaceIds")
    )

    return lf


# 4.1: add variations and isAlternative
def add_variations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add variations (other printings with same name/faceName in the same set)
    """
    lf = lf.with_columns(
        pl.col("name").str.split(" (").list.first().alias("_base_name"),
        pl.col("faceName").fill_null("").alias("_faceName_for_group"),
    )

    variation_groups = (
        lf.select(["setCode", "_base_name", "_faceName_for_group", "uuid"])
        .group_by(["setCode", "_base_name", "_faceName_for_group"])
        .agg(pl.col("uuid").alias("_group_uuids"))
    )

    lf = lf.join(
        variation_groups,
        on=["setCode", "_base_name", "_faceName_for_group"],
        how="left",
    )

    lf = lf.with_columns(
        pl.when(pl.col("_group_uuids").list.len() > 1)
        .then(
            pl.col("_group_uuids").list.set_difference(pl.concat_list(pl.col("uuid")))
        )
        .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        .alias("variations")
    )

    lf = lf.with_columns(
        pl.when(
            pl.col("otherFaceIds").is_not_null()
            & (pl.col("otherFaceIds").list.len() > 0)
        )
        .then(pl.col("variations").list.set_difference(pl.col("otherFaceIds")))
        .otherwise(pl.col("variations"))
        .alias("variations")
    )

    uuid_to_number = lf.select(
        [
            "uuid",
            # Extract collector number digits for sorting:
            pl.coalesce(
                pl.col("number").str.extract(r"-(\d+)", 1),  # After dash (PLST)
                pl.col("number").str.extract(r"(\d+)", 1),  # First digits (regular)
            )
            .cast(pl.Int64)
            .fill_null(999999)
            .alias("_num_int"),
            pl.col("number").alias("_num_str"),
        ]
    )

    # Explode variations to individual rows, join with number, sort within groups, re-aggregate
    variations_sorted = (
        lf.select(["uuid", "variations"])
        .explode("variations")
        .filter(pl.col("variations").is_not_null())
        .join(
            uuid_to_number.rename(
                {
                    "uuid": "variations",
                    "_num_int": "_var_num_int",
                    "_num_str": "_var_num_str",
                }
            ),
            on="variations",
            how="left",
        )
        .group_by("uuid")
        .agg(
            pl.col("variations")
            .sort_by(["_var_num_int", "_var_num_str"])
            .alias("_variations_sorted")
        )
    )

    # Join back and replace variations with sorted version
    lf = lf.join(variations_sorted, on="uuid", how="left")
    lf = lf.with_columns(
        pl.coalesce(pl.col("_variations_sorted"), pl.col("variations")).alias(
            "variations"
        )
    ).drop("_variations_sorted")

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

    printing_key = (
        pl.when(pl.col("setCode").is_in(["UNH", "10E"]))
        .then(pl.concat_str([base_key, pl.lit("|"), finishes_str]))
        .otherwise(base_key)
        .alias("_printing_key")
    )

    lf = lf.with_columns(printing_key)

    # Within each printing key, the card with the lowest collector number is "canonical"
    number_digits_expr = (
        pl.col("number")
        .str.extract_all(r"\d")
        .list.join("")
        .str.replace(r"^$", "100000")
        .cast(pl.Int64)
    )

    lf = lf.with_columns(number_digits_expr.alias("_number_digits"))

    # Use rank to find the canonical entry (lowest numeric value, then alphabetical tiebreaker)
    # The card with rank 1 within each group is canonical
    # Struct comparison is lexicographic: first by _number_digits, then by number string
    rank_expr = (
        pl.struct("_number_digits", "number")
        .rank("ordinal")
        .over(["setCode", "_printing_key"])
    )
    canonical_expr = rank_expr == 1

    lf = lf.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0)
            & (~pl.col("name").is_in(list(BASIC_LAND_NAMES)))
            & (~canonical_expr)
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )

    # Also set isAlternative=True for Alchemy rebalanced cards (A- prefix)
    lf = lf.with_columns(
        pl.when(pl.col("name").str.starts_with("A-"))
        .then(pl.lit(True))
        .otherwise(pl.col("isAlternative"))
        .alias("isAlternative")
    )

    # Cleanup temp columns
    return lf.drop(
        [
            "_base_name",
            "_faceName_for_group",
            "_group_uuids",
            "_printing_key",
            "_number_digits",
        ]
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
    """
    return lf.with_columns(
        pl.col("_all_parts")
        .list.eval(pl.element().struct.field("name"))
        .list.set_difference(pl.col("name").cast(pl.List(pl.String)))
        .list.sort()
        .fill_null([])
        .alias("reverseRelated")
    ).drop("_all_parts")


# 4.5: add_alternative_deck_limit
def add_alternative_deck_limit(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Mark cards that don't have the standard 4-copy deck limit.
    """
    unlimited_cards = ctx.unlimited_cards or set()

    oracle_text = (
        pl.coalesce(
            pl.col("_face_data").struct.field("oracle_text"),
            pl.col("text"),
        )
        .fill_null("")
        .str.to_lowercase()
    )
    pattern1 = oracle_text.str.contains(r"deck.*any.*number.*cards.*named")
    pattern2 = oracle_text.str.contains(r"have.*up.*to.*cards.*named.*deck")

    in_list = (
        pl.col("name").is_in(list(unlimited_cards))
        if unlimited_cards
        else pl.lit(False)
    )
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
    Add isFunny flag based on setType and special cases.
    """
    categoricals = ctx.categoricals
    if categoricals is None or "funny" not in categoricals.set_types:
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
    Add isTimeshifted flag based on frameVersion and setCode.
    """
    return lf.with_columns(
        pl.when((pl.col("frameVersion") == "future") | (pl.col("setCode") == "TSB"))
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isTimeshifted")
    )


# 4.8: add purchaseUrls struct
def add_purchase_urls_struct(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:  # pylint: disable=no-member
    """Build purchaseUrls struct with SHA256 redirect hashes."""
    redirect_base = "https://mtgjson.com/links/"
    ck_base = "https://www.cardkingdom.com/"

    ck_url = pl.col("cardKingdomUrl")
    ckf_url = pl.col("cardKingdomFoilUrl")
    cke_url = pl.col("cardKingdomEtchedUrl")

    mcm_id = pl.col("identifiers").struct.field("mcmId")
    tcg_id = pl.col("identifiers").struct.field("tcgplayerProductId")
    tcge_id = pl.col("identifiers").struct.field("tcgplayerEtchedProductId")

    return (
        lf.with_columns(
            [
                # Card Kingdom: hash(base + path + uuid)
                plh.concat_str(  # pylint: disable=no-member
                    [pl.lit(ck_base), ck_url, pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_ck_hash"),
                plh.concat_str(  # pylint: disable=no-member
                    [pl.lit(ck_base), ckf_url, pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_ckf_hash"),
                plh.concat_str(  # pylint: disable=no-member
                    [pl.lit(ck_base), cke_url, pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_cke_hash"),
                # TCGPlayer: hash(tcgplayer_product_id + uuid)
                plh.concat_str(  # pylint: disable=no-member
                    [tcg_id.cast(pl.String), pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_tcg_hash"),
                plh.concat_str(  # pylint: disable=no-member
                    [tcge_id.cast(pl.String), pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_tcge_hash"),
                # Cardmarket: hash(mcm_id + uuid + BUFFER + mcm_meta_id)
                plh.concat_str(  # pylint: disable=no-member
                    [
                        mcm_id.cast(pl.String),
                        pl.col("uuid"),
                        pl.lit(CARD_MARKET_BUFFER),
                        pl.col("identifiers")
                        .struct.field("mcmMetaId")
                        .cast(pl.String)
                        .fill_null(""),
                    ]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
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
def add_rebalanced_linkage(lf: pl.LazyFrame, ctx: "PipelineContext") -> pl.LazyFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.
    """
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
    # Handle multi-face rebalanced cards: "A-Front // A-Back" -> "Front // Back"
    original_name_expr = pl.col("name").str.replace_all("A-", "")

    # Filter to default language only for UUID aggregation
    # This ensures we only link UUIDs that will exist in final output
    default_langs = ctx.languages_lf
    if default_langs is not None:
        default_lang_lf = lf.join(
            default_langs, on=["scryfallId", "language"], how="semi"
        )
    else:
        # Fallback to English
        default_lang_lf = lf.filter(pl.col("language") == "English")

    rebalanced_map = (
        default_lang_lf.filter(is_rebalanced)
        .select(
            [
                pl.col("setCode"),
                original_name_expr.alias("_original_name"),
                pl.col("uuid"),
                pl.col("side").fill_null(""),  # Include side for ordering
            ]
        )
        .sort(["setCode", "_original_name", "side"])  # Sort by side before aggregating
        .group_by(["setCode", "_original_name"])
        .agg(pl.col("uuid").alias("_rebalanced_uuids"))  # Preserve order from sort
    )

    original_map = (
        default_lang_lf.filter(~is_rebalanced)
        .select(
            [
                pl.col("setCode"),
                pl.col("name").alias("_original_name"),
                pl.col("uuid"),
                pl.col("number"),
                pl.col("number")
                .str.extract(r"(\d+)")
                .cast(pl.Int64)
                .alias("_number_int"),
                pl.col("side").fill_null(""),  # Include side for ordering
            ]
        )
        .join(
            rebalanced_map.select(["setCode", "_original_name"]).unique(),
            on=["setCode", "_original_name"],
            how="semi",  # Only keep names that have a rebalanced version in same set
        )
        .sort(
            ["setCode", "_original_name", "_number_int", "number", "side"]
        )  # Sort by numeric, string, side
        .group_by(["setCode", "_original_name"])
        .agg(pl.col("uuid").alias("_original_uuids"))  # Preserve order from sort
    )

    # Join rebalancedPrintings onto original cards (by set + name)
    lf = lf.join(
        rebalanced_map,
        left_on=["setCode", "name"],
        right_on=["setCode", "_original_name"],
        how="left",
    ).rename({"_rebalanced_uuids": "rebalancedPrintings"})

    # Join originalPrintings onto rebalanced cards (by set + stripped name)
    lf = lf.join(
        original_map,
        left_on=["setCode", original_name_expr],
        right_on=["setCode", "_original_name"],
        how="left",
    ).rename({"_original_uuids": "originalPrintings"})

    # originalPrintings should only be set for rebalanced cards, not originals
    lf = lf.with_columns(
        pl.when(is_rebalanced)
        .then(pl.col("originalPrintings"))
        .otherwise(pl.lit(None).cast(pl.List(pl.String)))
        .alias("originalPrintings")
    )

    return lf


# 5.3:
def link_foil_nonfoil_versions(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link foil and non-foil versions that have different card details.

    Only applies to specific sets: CN2, FRF, ONS, 10E, UNH.
    Adds mtgjsonFoilVersionId and mtgjsonNonFoilVersionId to identifiers.

    Matches set_builder behavior exactly:
    - Groups cards by (setCode, illustrationId)
    - First card seen in group becomes the "anchor"
    - All subsequent cards link back to anchor
    - If subsequent has "nonfoil" in finishes: anchor gets nonfoilVersionId (last wins)
    - Uses Python iteration to match exact overwrite semantics (even if slower and seems buggy??)
    """
    foil_link_sets = {"CN2", "FRF", "ONS", "10E", "UNH"}

    # Collect only target sets for iteration (small number of cards)
    target_df = lf.filter(pl.col("setCode").is_in(foil_link_sets)).collect()

    if len(target_df) == 0:
        return lf

    version_links: dict[str, tuple[str | None, str | None]] = {}

    cards_seen: dict[tuple[str, str], str] = {}  # (setCode, ill_id) -> first_uuid

    for row in target_df.iter_rows(named=True):
        uuid = row["uuid"]
        set_code = row["setCode"]
        ill_id = (
            row["identifiers"].get("scryfallIllustrationId")
            if row["identifiers"]
            else None
        )
        finishes = row["finishes"] or []

        if ill_id is None:
            version_links[uuid] = (None, None)
            continue

        key = (set_code, ill_id)

        if key not in cards_seen:
            # First card with this illustration - store it
            cards_seen[key] = uuid
            version_links[uuid] = (None, None)
            continue

        # Subsequent card with same illustration - link to first
        first_uuid = cards_seen[key]

        # Get current links for first card (may have been set by previous cards)
        first_foil, first_nonfoil = version_links.get(first_uuid, (None, None))

        if "nonfoil" in finishes:
            # Current card has nonfoil -> current is the nonfoil version
            # First card points to current as its nonfoil version (overwrites previous)
            # Current card points to first as its foil version
            first_nonfoil = uuid
            version_links[first_uuid] = (first_foil, first_nonfoil)
            version_links[uuid] = (first_uuid, None)  # foilVersionId = first
        else:
            # Current card is foil-only
            # First card points to current as its foil version (overwrites previous)
            # Current card points to first as its nonfoil version
            first_foil = uuid
            version_links[first_uuid] = (first_foil, first_nonfoil)
            version_links[uuid] = (None, first_uuid)  # nonfoilVersionId = first

    # Create lookup DataFrame
    links_df = pl.DataFrame(
        {
            "uuid": list(version_links.keys()),
            "_foil_version": [v[0] for v in version_links.values()],
            "_nonfoil_version": [v[1] for v in version_links.values()],
        }
    ).lazy()

    # Join back to main LazyFrame
    lf = lf.join(links_df, on="uuid", how="left")

    # Update identifiers struct with version IDs
    lf = lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            [
                pl.col("_foil_version").alias("mtgjsonFoilVersionId"),
                pl.col("_nonfoil_version").alias("mtgjsonNonFoilVersionId"),
            ]
        )
    )

    # Cleanup temp columns
    return lf.drop(["_foil_version", "_nonfoil_version"], strict=False)


# 5.5:
def add_secret_lair_subsets(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Add subsets field for Secret Lair (SLD) cards.

    Links collector numbers to drop names.
    """
    sld_df = ctx.sld_subsets_lf

    if sld_df is None:
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("subsets"))

    # Handle LazyFrame from cache
    if isinstance(sld_df, pl.LazyFrame):
        sld_df = sld_df.collect()  # type: ignore[assignment]

    if len(sld_df) == 0:  # type: ignore[arg-type]
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("subsets"))

    # Rename the subsets column before joining to avoid conflicts
    sld_renamed = sld_df.rename({"subsets": "_sld_subsets"})
    lf = lf.join(
        sld_renamed.lazy(),
        on="number",
        how="left",
    )

    # Only apply subsets to SLD cards, NOT tokens (tokens have "Token" in types)
    return lf.with_columns(
        pl.when((pl.col("setCode") == "SLD") & ~pl.col("types").list.contains("Token"))
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
    """
    card_to_products_df = ctx.card_to_products_lf

    source_products_struct = pl.Struct(
        [
            pl.Field("etched", pl.List(pl.String)),
            pl.Field("foil", pl.List(pl.String)),
            pl.Field("nonfoil", pl.List(pl.String)),
        ]
    )

    if card_to_products_df is None:
        return lf.with_columns(
            pl.lit(None).cast(source_products_struct).alias("sourceProducts")
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

    These mappings are used by PriceBuilderContext to map provider IDs to UUIDs.
    """
    cache_path = constants.CACHE_PATH

    # TCGPlayer product ID -> UUID
    try:
        tcg_df = (
            lf.select(
                [
                    pl.col("uuid"),
                    pl.col("identifiers")
                    .struct.field("tcgplayerProductId")
                    .alias("tcgplayerProductId"),
                ]
            )
            .filter(pl.col("tcgplayerProductId").is_not_null())
            .unique()
            .collect()
        )
        if len(tcg_df) > 0:
            tcg_path = cache_path / "tcg_to_uuid.parquet"
            tcg_df.write_parquet(tcg_path)
            if ctx._cache is not None:
                ctx._cache.tcg_to_uuid_lf = tcg_df.lazy()
            LOGGER.info(f"Built tcg_to_uuid mapping: {len(tcg_df):,} entries")
    except Exception as e:
        LOGGER.warning(f"Failed to build tcg_to_uuid mapping: {e}")

    # TCGPlayer etched product ID -> UUID
    try:
        tcg_etched_df = (
            lf.select(
                [
                    pl.col("uuid"),
                    pl.col("identifiers")
                    .struct.field("tcgplayerEtchedProductId")
                    .alias("tcgplayerEtchedProductId"),
                ]
            )
            .filter(pl.col("tcgplayerEtchedProductId").is_not_null())
            .unique()
            .collect()
        )
        if len(tcg_etched_df) > 0:
            tcg_etched_path = cache_path / "tcg_etched_to_uuid.parquet"
            tcg_etched_df.write_parquet(tcg_etched_path)
            if ctx._cache is not None:
                ctx._cache.tcg_etched_to_uuid_lf = tcg_etched_df.lazy()
            LOGGER.info(
                f"Built tcg_etched_to_uuid mapping: {len(tcg_etched_df):,} entries"
            )
    except Exception as e:
        LOGGER.warning(f"Failed to build tcg_etched_to_uuid mapping: {e}")

    # MTGO ID -> UUID
    try:
        mtgo_df = (
            lf.select(
                [
                    pl.col("uuid"),
                    pl.col("identifiers").struct.field("mtgoId").alias("mtgoId"),
                ]
            )
            .filter(pl.col("mtgoId").is_not_null())
            .unique()
            .collect()
        )
        if len(mtgo_df) > 0:
            mtgo_path = cache_path / "mtgo_to_uuid.parquet"
            mtgo_df.write_parquet(mtgo_path)
            if ctx._cache is not None:
                ctx._cache.mtgo_to_uuid_lf = mtgo_df.lazy()
            LOGGER.info(f"Built mtgo_to_uuid mapping: {len(mtgo_df):,} entries")
    except Exception as e:
        LOGGER.warning(f"Failed to build mtgo_to_uuid mapping: {e}")

    # Scryfall ID -> UUID
    try:
        scryfall_df = (
            lf.select(
                [
                    pl.col("uuid"),
                    pl.col("identifiers")
                    .struct.field("scryfallId")
                    .alias("scryfallId"),
                ]
            )
            .filter(pl.col("scryfallId").is_not_null())
            .unique()
            .collect()
        )
        if len(scryfall_df) > 0:
            scryfall_path = cache_path / "scryfall_to_uuid.parquet"
            scryfall_df.write_parquet(scryfall_path)
            if ctx._cache is not None:
                ctx._cache.scryfall_to_uuid_lf = scryfall_df.lazy()
            LOGGER.info(f"Built scryfall_to_uuid mapping: {len(scryfall_df):,} entries")
    except Exception as e:
        LOGGER.warning(f"Failed to build scryfall_to_uuid mapping: {e}")


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
            from mtgjson5.v2.utils import get_windows_safe_set_code

            safe_code = get_windows_safe_set_code(s_code)
            set_path = out_dir / f"setCode={safe_code}"
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
    """
    if ctx.identifiers_lf is None:
        # Fallback: add null columns
        return lf.with_columns(
            pl.lit(None).cast(pl.String).alias("cardKingdomId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
            pl.lit(None).cast(pl.String).alias("cardsphereId"),
            pl.lit(None).cast(pl.String).alias("cardsphereFoilId"),
            pl.lit(None).cast(pl.String).alias("deckboxId"),
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

    return lf.drop("_side_for_join", strict=False)


def join_oracle_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all oracleId-based lookups.
    """
    if ctx.oracle_data_lf is None:
        return lf.with_columns(
            pl.lit([]).alias("rulings"),
            pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
            pl.lit(None).cast(pl.Int64).alias("edhrecRank"),
            pl.lit([]).cast(pl.List(pl.String)).alias("printings"),
        )

    # For multi-face cards, oracle_id may be in _face_data rather than oracleId column
    # Coalesce to get the correct value before joining
    lf = lf.with_columns(
        pl.coalesce(
            pl.col("_face_data").struct.field("oracle_id"),
            pl.col("oracleId"),
        ).alias("oracleId")
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
    """
    # Identify tokens
    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

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
    ).with_columns(pl.col("reverseRelated").list.first().alias("_parent_name"))

    # Join to get parent salt
    tokens_with_salt = tokens_needing_salt.join(
        parent_salt.rename({"edhrecSaltiness": "_parent_salt"}),
        left_on="_parent_name",
        right_on="name",
        how="left",
    ).select(["uuid", "_parent_salt"])

    # Update original DataFrame with inherited salt
    lf = (
        lf.join(
            tokens_with_salt,
            on="uuid",
            how="left",
        )
        .with_columns(
            pl.coalesce(pl.col("edhrecSaltiness"), pl.col("_parent_salt")).alias(
                "edhrecSaltiness"
            )
        )
        .drop("_parent_salt", strict=False)
    )

    return lf


def calculate_duel_deck(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate duelDeck field for Duel Deck sets (DD* and GS1).

    The algorithm detects deck boundaries by looking for basic lands:
    - All cards start in deck 'a'
    - When we see a basic land followed by a non-basic land, switch to deck 'b'
    - Tokens and emblems are skipped (no duelDeck)
    """
    is_duel_deck_set = pl.col("setCode").str.starts_with("DD") | (
        pl.col("setCode") == "GS1"
    )
    basic_land_names = list(BASIC_LAND_NAMES)

    # Only process cards from duel deck sets
    lf.filter(is_duel_deck_set)

    # Skip if no duel deck sets
    # We'll mark this as needing duelDeck calculation
    lf = lf.with_columns(pl.lit(None).cast(pl.String).alias("duelDeck"))

    # Filter to just duel deck set cards
    dd_cards = (
        lf.filter(is_duel_deck_set)
        .with_columns(
            # Mark basic lands
            pl.col("name").is_in(basic_land_names).alias("_is_basic_land"),
            # Mark tokens/emblems (skip these)
            pl.col("type")
            .str.contains("Token|Emblem")
            .fill_null(False)
            .alias("_is_token"),
            # Extract numeric part of collector number for sorting
            pl.col("number")
            .str.extract(r"^(\d+)", 1)
            .cast(pl.Int32)
            .alias("_num_sort"),
        )
        .sort(["setCode", "_num_sort", "number"])
    )

    # For each set, we need to detect the transition from land to non-land
    # Use a window function to check if previous card was a basic land
    dd_cards = (
        dd_cards.with_columns(
            pl.col("_is_basic_land")
            .shift(1)
            .over("setCode")
            .fill_null(False)
            .alias("_prev_was_land"),
        )
        .with_columns(
            # Transition occurs when previous was basic land and current is not basic land and not token
            (
                pl.col("_prev_was_land")
                & ~pl.col("_is_basic_land")
                & ~pl.col("_is_token")
            ).alias("_is_transition"),
        )
        .with_columns(
            # Count transitions to get deck number
            pl.col("_is_transition")
            .cum_sum()
            .over("setCode")
            .alias("_deck_num"),
        )
        .with_columns(
            # Convert deck number to letter, skip tokens
            pl.when(pl.col("_is_token"))
            .then(pl.lit(None).cast(pl.String))
            .otherwise(
                # 0 -> 'a', 1 -> 'b', etc.
                pl.lit("a").str.replace("a", "")
                + pl.col("_deck_num").map_elements(
                    lambda n: chr(ord("a") + n), return_dtype=pl.String
                )
            )
            .alias("_calc_duelDeck"),
        )
        .select(["uuid", "_calc_duelDeck"])
    )

    # Join back calculated duelDeck values
    lf = (
        lf.join(
            dd_cards.rename({"_calc_duelDeck": "_dd_calculated"}),
            on="uuid",
            how="left",
        )
        .with_columns(
            pl.coalesce(pl.col("_dd_calculated"), pl.col("duelDeck")).alias("duelDeck")
        )
        .drop("_dd_calculated")
    )

    return lf


def join_set_number_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join foreignData from set_number lookup.

    foreignData only applies to the "default" language for each card
    (English for most sets, primary printed language for foreign-only sets).
    """
    if ctx.set_number_lf is None:
        return lf.with_columns(pl.lit([]).alias("foreignData"))

    lf = lf.join(
        ctx.set_number_lf,
        left_on=["setCode", "number"],
        right_on=["setCode", "number"],
        how="left",
    )

    # Determine which cards should have foreignData
    if ctx.languages_lf is not None:
        lf = lf.join(
            ctx.languages_lf.select(
                [
                    pl.col("scryfallId"),
                    pl.col("language").alias("_default_language"),
                ]
            ),
            on="scryfallId",
            how="left",
        )
        lf = lf.with_columns(
            pl.when(pl.col("language") == pl.col("_default_language"))
            .then(pl.col("foreignData").fill_null([]))
            .otherwise(pl.lit([]))
            .alias("foreignData"),
        ).drop("_default_language")
    else:
        lf = lf.with_columns(
            pl.when(pl.col("language") == "English")
            .then(pl.col("foreignData").fill_null([]))
            .otherwise(pl.lit([]))
            .alias("foreignData"),
        )

    return lf


def fix_foreigndata_for_faces(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Fix foreignData after face explosion: deduplicate, fix UUIDs, fix face fields.

    foreignData is built in context.py BEFORE face explosion using first-face data
    and side="a". After explosion, multi-face cards need:
    1. Deduplication (each face had identical foreignData arrays)
    2. UUID regeneration (include actual side, not always "a")
    3. Face field correction (faceName/text/type for side != "a")
    """
    side_to_index = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}

    # Build per-face lookup for non-primary faces
    face_lookup = None
    if ctx.cards_lf is not None:
        cards_df = (
            ctx.cards_lf.collect()
            if isinstance(ctx.cards_lf, pl.LazyFrame)
            else ctx.cards_lf
        )

        face_lookup = (
            cards_df.filter(
                (pl.col("lang") != "en") & (pl.col("cardFaces").list.len() > 1)
            )
            .with_columns(
                [
                    pl.col("set").str.to_uppercase().alias("setCode"),
                    pl.col("lang")
                    .replace_strict(LANGUAGE_MAP, default=pl.col("lang"))
                    .alias("language"),
                    pl.int_ranges(pl.col("cardFaces").list.len()).alias("_face_idx"),
                ]
            )
            .explode(["cardFaces", "_face_idx"])
            .with_columns(
                [
                    pl.col("_face_idx").alias("face_index"),
                    pl.coalesce(
                        pl.col("cardFaces").struct.field("printed_name"),
                        pl.col("cardFaces").struct.field("name"),
                    ).alias("_faceName"),
                    pl.col("cardFaces").struct.field("printed_text").alias("_text"),
                    pl.col("cardFaces")
                    .struct.field("printed_type_line")
                    .alias("_type"),
                    pl.col("cardFaces")
                    .struct.field("flavor_text")
                    .alias("_flavorText"),
                ]
            )
            .select(
                [
                    "setCode",
                    pl.col("collectorNumber").alias("number"),
                    "language",
                    "face_index",
                    "_faceName",
                    "_text",
                    "_type",
                    "_flavorText",
                ]
            )
            .lazy()
        )

    # Process all foreignData in one pass
    fd_processed = (
        lf.select(["scryfallId", "setCode", "number", "side", "foreignData"])
        .with_columns(
            [
                pl.col("side").fill_null("a").alias("_side"),
                pl.col("side").fill_null("").alias("_side_key"),
            ]
        )
        .explode("foreignData")
        .filter(pl.col("foreignData").is_not_null())
        .with_columns(
            [
                pl.col("foreignData").struct.field("language").alias("_fd_lang"),
                pl.col("_side")
                .replace_strict(side_to_index, default=0)
                .cast(pl.Int64)
                .alias("_face_index"),
            ]
        )
        # Regenerate UUID: scryfallId + side + "_" + language
        .with_columns(
            pl.concat_str(
                [
                    pl.col("scryfallId"),
                    pl.col("_side"),
                    pl.lit("_"),
                    pl.col("_fd_lang"),
                ]
            ).alias("_uuid_source")
        )
        .with_columns(_uuid5_expr("_uuid_source").alias("_new_uuid"))
    )

    # Join face lookup for non-primary faces if available
    if face_lookup is not None:
        # Add a marker column to face_lookup to track successful joins
        face_lookup = face_lookup.with_columns(pl.lit(True).alias("_has_face_data"))
        fd_processed = fd_processed.join(
            face_lookup,
            left_on=["setCode", "number", "_fd_lang", "_face_index"],
            right_on=["setCode", "number", "language", "face_index"],
            how="left",
        )
        # Rebuild struct with corrected values for non-side-a, original for side-a
        # Use face_lookup values (even NULL) when join matched, original otherwise
        fd_processed = fd_processed.with_columns(
            pl.struct(
                [
                    pl.when(
                        (pl.col("_side") != "a")
                        & pl.col("_has_face_data").fill_null(False)
                    )
                    .then(pl.col("_faceName"))
                    .when(pl.col("_side") != "a")
                    .then(
                        pl.coalesce(
                            pl.col("_faceName"),
                            pl.col("foreignData").struct.field("faceName"),
                        )
                    )
                    .otherwise(pl.col("foreignData").struct.field("faceName"))
                    .alias("faceName"),
                    pl.when(
                        (pl.col("_side") != "a")
                        & pl.col("_has_face_data").fill_null(False)
                    )
                    .then(pl.col("_flavorText"))
                    .when(pl.col("_side") != "a")
                    .then(
                        pl.coalesce(
                            pl.col("_flavorText"),
                            pl.col("foreignData").struct.field("flavorText"),
                        )
                    )
                    .otherwise(pl.col("foreignData").struct.field("flavorText"))
                    .alias("flavorText"),
                    pl.col("foreignData").struct.field("identifiers"),
                    pl.col("foreignData").struct.field("language"),
                    pl.col("foreignData").struct.field("multiverseId"),
                    pl.col("foreignData").struct.field("name"),
                    pl.when(
                        (pl.col("_side") != "a")
                        & pl.col("_has_face_data").fill_null(False)
                    )
                    .then(pl.col("_text"))
                    .when(pl.col("_side") != "a")
                    .then(
                        pl.coalesce(
                            pl.col("_text"), pl.col("foreignData").struct.field("text")
                        )
                    )
                    .otherwise(pl.col("foreignData").struct.field("text"))
                    .alias("text"),
                    pl.when(
                        (pl.col("_side") != "a")
                        & pl.col("_has_face_data").fill_null(False)
                    )
                    .then(pl.col("_type"))
                    .when(pl.col("_side") != "a")
                    .then(
                        pl.coalesce(
                            pl.col("_type"), pl.col("foreignData").struct.field("type")
                        )
                    )
                    .otherwise(pl.col("foreignData").struct.field("type"))
                    .alias("type"),
                    pl.col("_new_uuid").alias("uuid"),
                ]
            ).alias("foreignData")
        )
    else:
        # Just fix UUID
        fd_processed = fd_processed.with_columns(
            pl.struct(
                [
                    pl.col("foreignData").struct.field("faceName"),
                    pl.col("foreignData").struct.field("flavorText"),
                    pl.col("foreignData").struct.field("identifiers"),
                    pl.col("foreignData").struct.field("language"),
                    pl.col("foreignData").struct.field("multiverseId"),
                    pl.col("foreignData").struct.field("name"),
                    pl.col("foreignData").struct.field("text"),
                    pl.col("foreignData").struct.field("type"),
                    pl.col("_new_uuid").alias("uuid"),
                ]
            ).alias("foreignData")
        )

    # Re-aggregate by card+side (deduplication happens naturally here)
    fd_final = fd_processed.group_by(
        ["scryfallId", "setCode", "number", "_side_key"]
    ).agg(pl.col("foreignData").alias("_foreignData_fixed"))

    # Join back
    lf = lf.with_columns(pl.col("side").fill_null("").alias("_side_key"))
    lf = lf.join(
        fd_final, on=["scryfallId", "setCode", "number", "_side_key"], how="left"
    )
    return lf.with_columns(
        pl.coalesce(pl.col("_foreignData_fixed"), pl.lit([])).alias("foreignData")
    ).drop(["_foreignData_fixed", "_side_key"])


def join_name_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all name-based lookups.
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
        # Keep cardParts for ALL meld layout cards (including reprints)
        # Fixes: #1425, #1427 - reprints were excluded by set filter
        lf = lf.with_columns(
            pl.when(pl.col("layout") == "meld")
            .then(
                pl.coalesce(
                    pl.col("cardParts"),
                    pl.col("_face_cardParts"),
                )
            )
            .otherwise(pl.lit(None))
            .alias("cardParts")
        ).drop("_face_cardParts", strict=False)
    else:
        # No cardParts data available
        lf = lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("cardParts"))

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
    Add UUID from cachedUuid if present, else generate new UUID.
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
    Combine signature logic for art series and memorabilia cards.
    """
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
    """
    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
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
def build_cards(ctx: PipelineContext) -> PipelineContext:
    """
    Main card building pipeline.
    """
    set_codes = ctx.sets_to_build
    output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Executing card pipeline...")
    if ctx.sets_lf is None:
        raise ValueError("sets_df is not available in context")
    sets_raw = ctx.sets_lf.rename({"code": "set"})
    sets_lf = sets_raw.lazy() if isinstance(sets_raw, pl.DataFrame) else sets_raw

    if ctx.cards_lf is None:
        raise ValueError("cards_lf is not available in context")

    base_lf = ctx.cards_lf.with_columns(
        pl.col("set").str.to_uppercase().alias("_set_upper")
    )
    if set_codes:
        base_lf = base_lf.filter(pl.col("_set_upper").is_in(set_codes))

    # Pattern: "A // B // A" where first and third parts are identical
    base_lf = (
        base_lf.with_columns(
            [
                (pl.col("name").str.count_matches(" // ") + 1).alias("_num_parts"),
                pl.col("name")
                .str.extract(r"^([^/]+) // ", 1)
                .str.strip_chars()
                .alias("_part0"),
                pl.col("name")
                .str.extract(r" // ([^/]+) // ", 1)
                .str.strip_chars()
                .alias("_part1"),
                pl.col("name")
                .str.extract(r" // ([^/]+)$", 1)
                .str.strip_chars()
                .alias("_part2"),
            ]
        )
        .with_columns(
            pl.when(
                (pl.col("_set_upper") == "TDM")
                & (pl.col("_num_parts") == 3)
                & (pl.col("_part0") == pl.col("_part2"))
                & pl.col("_part1").is_not_null()
            )
            .then(pl.concat_str([pl.col("name"), pl.lit(" // "), pl.col("_part1")]))
            .otherwise(pl.col("name"))
            .alias("name")
        )
        .drop(["_num_parts", "_part0", "_part1", "_part2"])
    )

    english_numbers = (
        base_lf.filter(pl.col("lang") == "en")
        .select(["_set_upper", "collectorNumber"])
        .unique()
        .with_columns(pl.lit(True).alias("_has_english"))
    )

    lf = (
        base_lf.join(
            english_numbers,
            on=["_set_upper", "collectorNumber"],
            how="left",
        )
        .filter(
            # English cards OR non-English cards with no English equivalent
            (pl.col("lang") == "en")
            | pl.col("_has_english").is_null()
        )
        .drop(["_has_english", "_set_upper"])
    )

    # Apply scryfall_id filter for deck-only builds
    if ctx.scryfall_id_filter:
        lf = lf.filter(pl.col("id").is_in(ctx.scryfall_id_filter))
        LOGGER.info(f"Applied scryfall_id filter: {len(ctx.scryfall_id_filter):,} IDs")

    sets_schema = sets_lf.collect_schema().names()
    set_select_exprs = [pl.col("set")]
    if "setType" in sets_schema:
        set_select_exprs.append(pl.col("setType"))
    if "releasedAt" in sets_schema:
        set_select_exprs.append(pl.col("releasedAt").alias("setReleasedAt"))
    if "block" in sets_schema:
        set_select_exprs.append(pl.col("block"))
    if "foilOnly" in sets_schema:
        set_select_exprs.append(pl.col("foilOnly"))
    if "nonfoilOnly" in sets_schema:
        set_select_exprs.append(pl.col("nonfoilOnly"))

    lf = base_lf.with_columns(pl.col("set").str.to_uppercase()).join(
        sets_lf.select(set_select_exprs), on="set", how="left"
    )

    # Per-card transforms (streaming OK)
    lf = (
        lf.pipe(explode_card_faces)
        .pipe(partial(assign_meld_sides, ctx=ctx))
        .pipe(partial(update_meld_names, ctx=ctx))
        .pipe(detect_aftermath_layout)
        .pipe(add_basic_fields)
        .pipe(add_booster_types)
        .pipe(fix_promo_types)
        .pipe(partial(apply_card_enrichment, ctx=ctx))
        .pipe(fix_power_toughness_for_multiface)
        .pipe(propagate_watermark_to_faces)
        .pipe(partial(apply_watermark_overrides, ctx=ctx))
        .pipe(format_planeswalker_text)
        .pipe(add_original_release_date)
        .drop(
            [
                "lang",
                "frame",
                "fullArt",
                "textless",
                "oversized",
                "promo",
                "reprint",
                "storySpotlight",
                "reserved",
                "cmc",
                "typeLine",
                "oracleText",
                "printedTypeLine",
                "setReleasedAt",
            ],
            strict=False,
        )
        .pipe(partial(join_face_flavor_names, ctx=ctx))
        .pipe(parse_type_line_expr)
        .pipe(add_mana_info)
        .pipe(fix_manavalue_for_multiface)
        .pipe(add_card_attributes)
        .pipe(filter_keywords_for_face)
        .drop(
            [
                "contentWarning",
                "handModifier",
                "lifeModifier",
                "gameChanger",
                "_in_booster",
                "_meld_face_name",
                "isStarter",
            ],
            strict=False,
        )
        .pipe(partial(add_legalities_struct, ctx=ctx))
        .pipe(partial(add_availability_struct, ctx=ctx))
        .pipe(remap_availability_values)
    )

    lf = (
        lf.pipe(partial(join_identifiers, ctx=ctx))
        .pipe(partial(join_oracle_data, ctx=ctx))
        .pipe(partial(join_set_number_data, ctx=ctx))
        .pipe(fix_foreigndata_for_faces, ctx=ctx)
        .pipe(partial(join_name_data, ctx=ctx))
        .pipe(partial(join_cardmarket_ids, ctx=ctx))
        .pipe(fix_availability_from_ids)
    )

    # Collects are placed strategically to prevent crashing Polars from lazy plan complexity
    LOGGER.info("Checkpoint: materializing after joins...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(add_identifiers_struct)
        .drop(
            [
                "mcmId",
                "mcmMetaId",
                "arenaId",
                "mtgoId",
                "mtgoFoilId",
                "tcgplayerId",
                "tcgplayerEtchedId",
                "illustrationId",
                "cardBackId",
            ],
            strict=False,
        )
        .pipe(add_uuid_from_cache)
        .pipe(add_identifiers_v4_uuid)
    )

    lf = lf.pipe(calculate_duel_deck)
    lf = lf.pipe(partial(join_gatherer_data, ctx=ctx))

    LOGGER.info("Checkpoint: materializing before relationship operations...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(partial(add_other_face_ids, ctx=ctx))
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

    LOGGER.info("Checkpoint: materializing before final enrichment...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(partial(apply_manual_overrides, ctx=ctx))
        .pipe(partial(add_rebalanced_linkage, ctx=ctx))
        .pipe(partial(add_secret_lair_subsets, ctx=ctx))
        .pipe(partial(add_source_products, ctx=ctx))
    )

    lf = (
        lf.pipe(partial(join_signatures, ctx=ctx))
        .pipe(partial(add_signatures_combined, _ctx=ctx))
        .pipe(drop_raw_scryfall_columns)
    )

    ctx.final_cards_lf = lf

    # Sink for build module
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
    if ctx.decks_lf is None:
        LOGGER.warning("GitHub decks data not loaded in cache")
        return pl.DataFrame()

    # Filter decks by set codes first (before collecting UUIDs)
    # Use set_codes param if provided, otherwise fall back to ctx.sets_to_build
    filter_codes = set_codes or ctx.sets_to_build
    decks_lf = ctx.decks_lf
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

    card_list_cols = ["mainBoard", "sideBoard", "commander"]

    expanded_lists = {}
    for col in card_list_cols:
        expanded_lists[col] = _expand_card_list(decks_df, cards_df, col)

    expanded_lists["tokens"] = _expand_card_list(decks_df, cards_df, "tokens")

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
        # sealedProductUuids should stay null when not present
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
) -> pl.LazyFrame:  # pylint: disable=no-member
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
    products_lf = ctx.sealed_products_lf
    contents_lf = ctx.sealed_contents_lf
    if products_lf is None or contents_lf is None:
        LOGGER.warning("GitHub sealed products data not loaded in cache")
        return pl.DataFrame().lazy()

    if not isinstance(products_lf, pl.LazyFrame):
        products_lf = products_lf.lazy()

    if not isinstance(contents_lf, pl.LazyFrame):
        contents_lf = contents_lf.lazy()

    ck_sealed_urls_lf: pl.LazyFrame | None = None
    if ctx.card_kingdom_raw_lf is not None:
        ck_raw = ctx.card_kingdom_raw_lf
        ck_sealed_urls_lf = ck_raw.filter(pl.col("scryfall_id").is_null()).select(
            [
                pl.col("id").cast(pl.String).alias("_ck_id"),
                pl.col("url").alias("_ck_url"),
            ]
        )

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

    deck_contents = (
        contents_lf.filter(pl.col("contentType") == "deck")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                name=pl.col("name"),
                set=pl.col("set"),
            ).alias("_deck_list")
        )
    )

    pack_contents = (
        contents_lf.filter(pl.col("contentType") == "pack")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                code=pl.col("code"),
                set=pl.col("set"),
            ).alias("_pack_list")
        )
    )

    variable_contents = (
        contents_lf.filter(pl.col("contentType") == "variable")
        .group_by(["setCode", "productName"])
        .agg(
            pl.struct(
                configs=pl.col("configs"),
            ).alias("_variable_list")
        )
    )

    product_card_count = (
        contents_lf.filter(pl.col("cardCount").is_not_null())
        .group_by(["setCode", "productName"])
        .agg(pl.col("cardCount").first().alias("cardCount"))
    )

    result = (
        products_lf.join(card_contents, on=["setCode", "productName"], how="left")
        .join(sealed_contents, on=["setCode", "productName"], how="left")
        .join(other_contents, on=["setCode", "productName"], how="left")
        .join(deck_contents, on=["setCode", "productName"], how="left")
        .join(pack_contents, on=["setCode", "productName"], how="left")
        .join(variable_contents, on=["setCode", "productName"], how="left")
        .join(product_card_count, on=["setCode", "productName"], how="left")
    )

    result = result.with_columns(
        pl.struct(
            card=pl.col("_card_list"),
            deck=pl.col("_deck_list"),
            other=pl.col("_other_list"),
            pack=pl.col("_pack_list"),
            sealed=pl.col("_sealed_list"),
            variable=pl.col("_variable_list"),
        ).alias("contents")
    ).drop(
        [
            "_card_list",
            "_sealed_list",
            "_other_list",
            "_deck_list",
            "_pack_list",
            "_variable_list",
        ]
    )

    result = result.with_columns(_uuid5_expr("productName").alias("uuid"))

    result = result.with_columns(
        pl.when(pl.col("subtype").is_in(["REDEMPTION", "SECRET_LAIR_DROP"]))
        .then(pl.lit(None).cast(pl.String))
        .otherwise(pl.col("subtype"))
        .alias("subtype"),
        pl.when(pl.col("subtype") == "SECRET_LAIR_DROP")
        .then(pl.lit(None).cast(pl.String))
        .otherwise(pl.col("category"))
        .alias("category"),
    )

    if ck_sealed_urls_lf is not None:
        result = (
            result.with_columns(
                pl.col("identifiers").struct.field("cardKingdomId").alias("_ck_join_id")
            )
            .join(
                ck_sealed_urls_lf,
                left_on="_ck_join_id",
                right_on="_ck_id",
                how="left",
            )
            .drop("_ck_join_id")
        )

    base_url = "https://mtgjson.com/links/"

    purchase_url_fields = []
    hash_cols_added: list[str] = []  # Track hash columns to avoid extra schema call

    result_schema = result.collect_schema()
    result_cols = result_schema.names()

    has_release_date = "releaseDate" in result_cols
    has_release_date_snake = "release_date" in result_cols
    has_card_count = "cardCount" in result_cols
    has_language = "language" in result_cols
    has_ck_url = "_ck_url" in result_cols

    if "identifiers" in result_cols:
        id_schema = result_schema.get("identifiers")
        if isinstance(id_schema, pl.Struct):
            id_fields = {f.name for f in id_schema.fields}

            if "cardKingdomId" in id_fields and has_ck_url:
                ck_base = "https://www.cardkingdom.com/"
                ck_referral = "?partner=mtgjson&utm_source=mtgjson&utm_medium=affiliate&utm_campaign=mtgjson"
                result = result.with_columns(
                    plh.concat_str(  # pylint: disable=no-member
                        [
                            pl.lit(ck_base),
                            pl.col("_ck_url"),
                            pl.lit(ck_referral),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("_ck_hash")
                )
                hash_cols_added.append("_ck_hash")
                hash_cols_added.append("_ck_url")
                purchase_url_fields.append(
                    pl.when(
                        pl.col("identifiers")
                        .struct.field("cardKingdomId")
                        .is_not_null()
                        & pl.col("_ck_hash").is_not_null()
                    )
                    .then(pl.lit(base_url) + pl.col("_ck_hash"))
                    .otherwise(None)
                    .alias("cardKingdom")
                )

            if "tcgplayerProductId" in id_fields:
                result = result.with_columns(
                    plh.concat_str(  # pylint: disable=no-member
                        [
                            pl.col("identifiers").struct.field("tcgplayerProductId"),
                            pl.col("uuid"),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
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

    if purchase_url_fields:
        result = result.with_columns(
            pl.struct(purchase_url_fields).alias("purchaseUrls")
        )
        if hash_cols_added:
            result = result.drop(hash_cols_added, strict=False)
    else:
        result = result.with_columns(pl.struct([]).alias("purchaseUrls"))

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

    if has_release_date:
        select_cols.insert(4, "releaseDate")
    elif has_release_date_snake:
        select_cols.insert(4, pl.col("release_date").alias("releaseDate"))

    # cardCount from contents aggregation
    if has_card_count:
        select_cols.append("cardCount")

    # language (only for non-English products)
    if has_language:
        select_cols.append("language")

    sealed_products_lf = result.select(select_cols)

    return sealed_products_lf


def build_set_metadata_df(
    ctx: PipelineContext,
) -> pl.DataFrame:
    """
    Build a DataFrame containing set-level metadata.
    """
    if ctx is None:
        ctx = PipelineContext.from_global_cache()

    sets_lf = ctx.sets_lf
    if sets_lf is None:
        raise ValueError("sets_df is not available in context")
    if not isinstance(sets_lf, pl.LazyFrame):
        sets_lf = sets_lf.lazy()

    # Get booster configs from cache
    booster_lf = ctx.boosters_lf
    if booster_lf is not None:
        if not isinstance(booster_lf, pl.LazyFrame):
            booster_lf = booster_lf.lazy()
    else:
        booster_lf = (
            pl.DataFrame({"setCode": [], "config": []})
            .cast({"setCode": pl.String, "config": pl.String})
            .lazy()
        )

    mcm_set_map = ctx.mcm_set_map or {}

    available_cols = sets_lf.collect_schema().names()

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

    release_col = "releasedAt" if "releasedAt" in available_cols else "setReleasedAt"
    base_exprs = [
        pl.col("code").str.to_uppercase().alias("code"),
        pl.col("name").str.strip_chars(),
        pl.col(release_col).alias("releaseDate"),
        pl.col("setType").alias("type"),
        pl.col("digital").alias("isOnlineOnly"),
        pl.col("foilOnly").alias("isFoilOnly"),
    ]

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

    if "cardCount" in available_cols:
        base_exprs.append(pl.col("cardCount").alias("totalSetSize"))
    if "printedSize" in available_cols:
        base_exprs.append(pl.col("printedSize").alias("baseSetSize"))
    elif "cardCount" in available_cols:
        base_exprs.append(pl.col("cardCount").alias("baseSetSize"))

    if "iconSvgUri" in available_cols:
        base_exprs.append(
            pl.col("iconSvgUri")
            .str.extract(r"/([^/]+)\.svg", 1)
            .str.to_uppercase()
            .alias("keyruneCode")
        )

    if "tokenSetCode" in available_cols:
        base_exprs.append(pl.col("tokenSetCode").alias("tokenSetCode"))

    set_meta = sets_lf.with_columns(base_exprs)

    set_meta_cols = set_meta.collect_schema().names()
    cols_to_drop = [
        c
        for c in set_meta_cols
        if c in scryfall_only_fields or c.lower() in scryfall_only_fields
    ]
    if cols_to_drop:
        set_meta = set_meta.drop(cols_to_drop, strict=False)

    set_meta = set_meta.join(
        booster_lf.with_columns(pl.col("setCode").str.to_uppercase().alias("code")),
        on="code",
        how="left",
    ).rename({"config": "booster"})

    if isinstance(set_meta, pl.LazyFrame):
        set_meta_df = set_meta.collect()
    else:
        set_meta_df = set_meta
    set_records = set_meta_df.to_dicts()
    for record in set_records:
        set_name = record.get("name", "")

        mcm_data = mcm_set_map.get(set_name.lower(), {})
        record["mcmId"] = mcm_data.get("mcmId")
        record["mcmName"] = mcm_data.get("mcmName")
        record["mcmIdExtras"] = ctx.get_mcm_extras_set_id(set_name)

        record["isForeignOnly"] = (
            True if record.get("code", "") in constants.FOREIGN_SETS else None
        )

        release_date = record.get("releaseDate")
        if release_date:
            build_date = date.today().isoformat()
            record["isPartialPreview"] = (
                build_date < release_date if build_date < release_date else None
            )
        else:
            record["isPartialPreview"] = None

        if record.get("baseSetSize") is None:
            record["baseSetSize"] = record.get("totalSetSize", 0)
        if record.get("totalSetSize") is None:
            record["totalSetSize"] = record.get("baseSetSize", 0)

        for scry_field in scryfall_only_fields:
            record.pop(scry_field, None)

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
                    "isForeignOnly": (
                        True if code_upper in constants.FOREIGN_SETS else None
                    ),
                    "parentCode": (
                        set_data.get("parent_set_code", "").upper()
                        if set_data.get("parent_set_code")
                        else None
                    ),
                    "block": set_data.get("block"),
                    "tcgplayerGroupId": set_data.get("tcgplayer_id"),
                    "baseSetSize": 0,
                    "totalSetSize": 0,
                    "keyruneCode": code_upper,
                    "tokenSetCode": None,
                    "isPartialPreview": None,
                }
                set_records.append(new_record)
                LOGGER.debug(f"Added additional set: {code_upper}")

    # Explicit schema to avoid type inference issues with mixed None/bool values
    schema_overrides = {
        "isOnlineOnly": pl.Boolean,
        "isFoilOnly": pl.Boolean,
        "isNonFoilOnly": pl.Boolean,
        "isForeignOnly": pl.Boolean,
        "isPartialPreview": pl.Boolean,
    }
    return pl.DataFrame(set_records, schema_overrides=schema_overrides)
