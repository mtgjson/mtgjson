"""
Face explosion and layout normalization.

Splits multi-face cards into separate rows, assigns sides, normalizes layouts.
"""

from __future__ import annotations

import polars as pl
import polars_hash as plh

from mtgjson5.data import PipelineContext
from mtgjson5.models.scryfall import CardFace

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

    lf = lf.with_columns(pl.int_ranges(pl.col("cardFaces").list.len()).alias("_face_idx"))

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
            pl.when(is_meld).then(pl.col("name")).otherwise(pl.lit(None).cast(pl.String)).alias("_meld_face_name"),
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
        pl.when(is_aftermath_card).then(pl.lit("aftermath")).otherwise(pl.col("layout")).alias("layout")
    )
