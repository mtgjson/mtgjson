"""
MTGJSON card data pipeline.

This module contains the complete data pipeline for transforming Scryfall bulk data
into MTGJSON format, including card processing, set building, and output generation.
"""

import json
import os
import pathlib
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Any
from uuid import UUID, uuid5

import orjson
import polars as pl
import polars_hash as plh

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.context import PipelineContext
from mtgjson5.models.schema.scryfall import CardFace
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_models import (
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    TOKEN_EXCLUDE,
    clean_nested,
    dataframe_to_cards_list,
)
from mtgjson5.providers.v2.card_schemas import (
    NestedStructs,
    to_camel_case,
    to_snake_case,
)
from mtgjson5.providers.cardmarket.monolith import CardMarketProvider
from mtgjson5.referral_builder import fixup_referral_map, write_referral_map
from mtgjson5.utils import LOGGER, deep_sort_keys
from mtgjson5.pipeline.safe_ops import safe_drop, safe_rename
from mtgjson5.pipeline.validation import (
    validate_stage,
    STAGE_POST_EXPLODE,
    STAGE_POST_BASIC_FIELDS,
    STAGE_POST_IDENTIFIERS,
    STAGE_PRE_SINK,
)


# Check if polars_hash has uuidhash namespace
_HAS_UUIDHASH = hasattr(plh.col("_test"), "uuidhash")
_DNS_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _uuid5_from_expr(expr: pl.Expr) -> pl.Expr:
    """Generate UUID5 from any string expression using DNS namespace."""
    if _HAS_UUIDHASH:
        # Cast to use polars_hash's HExpr which has uuidhash
        return expr.pipe(lambda e: plh.col(e.meta.output_name())).uuidhash.uuid5()
    return expr.map_elements(
        lambda x: str(uuid5(_DNS_NAMESPACE, x)) if x else None,
        return_dtype=pl.String,
    )


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


def _ascii_name_expr(expr: pl.Expr) -> pl.Expr:
    """
    Build expression to normalize card name to ASCII.
    Pure Polars - stays lazy.
    """
    # Using str.replace_many is much faster than chaining .str.replace_all calls
    return expr.str.replace_many(
        {
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
        }
    )


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


# List of raw Scryfall columns to drop after transformation to MTGJSON format
SCRYFALL_COLUMNS_TO_DROP = [
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
    "printedText",  # -> printedText (face-aware, localized non-English text)
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
]


def drop_raw_scryfall_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Drop all raw Scryfall columns after they've been transformed to MTGJSON format.

    Call this AFTER all transformation functions have completed but BEFORE
    rename_all_the_things. This is the centralized cleanup function that
    replaces scattered .drop() calls throughout the pipeline.
    """
    return safe_drop(lf, SCRYFALL_COLUMNS_TO_DROP)


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
                .then(face_field("name"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceName"),
                # Face-specific flavor name (only for multi-face cards)
                # Note: flavor_name is a card-level field, not in card_faces struct
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
                # Face-aware fields
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
                face_field("colors").alias("colors"),
                face_field("printedText").alias("printedText"),
                face_field("printedTypeLine").alias("printedType"),
                # Card-level fields (not face-specific)
                pl.col("setCode").str.to_uppercase(),
                pl.col("cmc").alias("manaValue"),
                pl.col("colorIdentity").alias("colorIdentity"),
                pl.col("borderColor").alias("borderColor"),
                pl.col("frame").alias("frameVersion"),
                pl.col("frameEffects").alias("frameEffects"),
                pl.col("securityStamp").alias("securityStamp"),
                pl.col("fullArt").alias("isFullArt"),
                pl.col("textless").alias("isTextless"),
                pl.col("oversized").alias("isOversized"),
                pl.col("promo").alias("isPromo"),
                pl.col("reprint").alias("isReprint"),
                pl.col("storySpotlight").alias("isStorySpotlight"),
                pl.col("reserved").alias("isReserved"),
                pl.col("foil").alias("hasFoil"),
                pl.col("nonfoil").alias("hasNonFoil"),
                pl.col("flavorName").alias("flavorName"),
                pl.col("allParts").alias("allParts"),
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
                .alias("language"),
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
    Add mana cost, mana value, and colors For multi-face cards, faceManaValue uses the face's cmc (back faces typically have 0).
    """
    return lf.with_columns(
        # manaCost already exists from add_basic_fields rename
        pl.col("colors").fill_null([]).alias("colors"),
        pl.col("colorIdentity").fill_null([]),
        # manaValue/convertedManaCost are floats (e.g., 2.5 for split cards)
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("manaValue"),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("convertedManaCost"),
        # faceManaValue: use face's cmc for multi-face cards, else card's manaValue
        pl.when(pl.col("_face_data").is_not_null())
        .then(pl.col("_face_data").struct.field("cmc").cast(pl.Float64).fill_null(0.0))
        .otherwise(pl.col("manaValue").cast(pl.Float64).fill_null(0.0))
        .alias("faceManaValue"),
    )


# 1.5: Add card attributes
def add_card_attributes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add card attributes. Runs after add_basic_fields.
    """
    return lf.with_columns(
        # number already exists from add_basic_fields (collector_number -> number)
        pl.col("rarity"),
        # borderColor already exists
        # frameVersion already exists
        pl.col("frameEffects").fill_null([]).list.sort().alias("frameEffects"),
        # securityStamp already exists
        pl.col("artist").fill_null(""),
        pl.col("artistIds").fill_null([]),
        pl.col("watermark"),
        pl.col("finishes").fill_null([]).alias("finishes"),
        pl.col("finishes").list.contains("foil").fill_null(False).alias("hasFoil"),
        pl.col("finishes")
        .list.contains("nonfoil")
        .fill_null(False)
        .alias("hasNonFoil"),
        pl.col("contentWarning").alias("hasContentWarning"),
        # isFullArt already exists
        pl.col("digital").alias("isOnlineOnly"),
        # isOversized, isPromo, isReprint, isReserved, isStorySpotlight, isTextless already exist
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
        # text already exists from add_basic_fields (oracle_text -> text)
        # flavorText already exists
        pl.col("keywords").fill_null([]).alias("_all_keywords"),
        pl.col("attractionLights").alias("attractionLights"),
        # allParts already exists from add_basic_fields rename
        pl.col("allParts").fill_null([]).alias("_all_parts"),
    )


# 1.6: Filter keywords for face
def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter keywords to only include those that appear in the card's text.

    Removes keywords from the _all_keywords list that don't appear in the card's
    oracle text, which is important for multi-face cards where keywords may only
    apply to specific faces.

    Uses struct + map_elements which allows Polars to correctly infer the output
    schema while processing row-by-row efficiently.
    """

    def filter_keywords_row(data: dict) -> list[str]:
        """Filter keywords for a single row based on text presence."""
        text = (data.get("text") or "").lower()
        keywords = data.get("_all_keywords") or []
        if not keywords:
            return []
        # Sort case-insensitively for consistent ordering
        return sorted((kw for kw in keywords if kw.lower() in text), key=str.lower)

    return lf.with_columns(
        pl.struct(["text", "_all_keywords"])
        .map_elements(filter_keywords_row, return_dtype=pl.List(pl.String))
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
    return lf.with_columns(
        [
            # boosterTypes computation
            pl.when(pl.col("_in_booster").fill_null(False))
            .then(
                pl.when(
                    pl.col("promoTypes")
                    .list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"]))
                    .list.len()
                    > 0
                )
                .then(pl.lit(["default", "deck"]))
                .otherwise(pl.lit(["default"]))
            )
            .otherwise(
                pl.when(
                    pl.col("promoTypes")
                    .list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"]))
                    .list.len()
                    > 0
                )
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
    return safe_drop(
        lf.with_columns(pl.struct(struct_fields).alias("legalities")),
        formats,
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
    """
    # Group by Scryfall ID to get list of MTGJSON UUIDs for this object
    face_links = (
        lf.select(["scryfallId", "uuid"])
        .group_by("scryfallId")
        .agg(pl.col("uuid").alias("_all_uuids"))
    )

    return (
        lf.join(face_links, on="scryfallId", how="left")
        .with_columns(
            pl.col("_all_uuids")
            .list.set_difference(pl.col("uuid").cast(pl.List(pl.String)))
            .alias("otherFaceIds")
        )
        .drop("_all_uuids")
    )


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
    pattern1 = (
        oracle_text.str.contains("deck")
        & oracle_text.str.contains("any")
        & oracle_text.str.contains("number")
        & oracle_text.str.contains("cards")
        & oracle_text.str.contains("named")
    )

    # Pattern 2: "have up to ... cards named ... in your deck"
    pattern2 = (
        oracle_text.str.contains("deck")
        & oracle_text.str.contains("have")
        & oracle_text.str.contains("up")
        & oracle_text.str.contains("to")
        & oracle_text.str.contains("cards")
        & oracle_text.str.contains("named")
    )

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

    Assumes setCode column exists.
    """
    foil_link_sets = {"CN2", "FRF", "ONS", "10E", "UNH"}

    in_target_sets = pl.col("setCode").is_in(foil_link_sets)

    # Extract illustration_id for grouping
    ill_id_expr = pl.col("identifiers").struct.field("scryfallIllustrationId")

    # Find pairs: same illustration_id, same set, exactly 2 cards
    pairs = (
        lf.filter(in_target_sets & ill_id_expr.is_not_null())
        .select(
            [
                pl.col("setCode"),
                ill_id_expr.alias("_ill_id"),
                pl.col("uuid"),
                pl.col("finishes"),
            ]
        )
        .group_by(["setCode", "_ill_id"])
        .agg(
            [
                pl.col("uuid"),
                pl.col("finishes"),
            ]
        )
        .filter(pl.col("uuid").list.len() == 2)
    )

    # Explode and determine foil status for each card in pair
    pair_cards = (
        pairs.with_columns(
            [
                pl.col("uuid").list.get(0).alias("uuid_0"),
                pl.col("uuid").list.get(1).alias("uuid_1"),
                pl.col("finishes").list.get(0).alias("finishes_0"),
                pl.col("finishes").list.get(1).alias("finishes_1"),
            ]
        )
        .with_columns(
            [
                # Card is foil-only if "nonfoil" NOT in its finishes
                ~pl.col("finishes_0").list.contains("nonfoil").alias("_is_foil_0"),
                ~pl.col("finishes_1").list.contains("nonfoil").alias("_is_foil_1"),
            ]
        )
        .filter(
            # Only process pairs where one is foil and one is nonfoil
            pl.col("_is_foil_0")
            != pl.col("_is_foil_1")
        )
    )

    # Build lookup: for each uuid, what's its foil/nonfoil counterpart?
    # If card 0 is foil: card 1's foil_version = card 0, card 0's nonfoil_version = card 1
    foil_map = pair_cards.select(
        [
            # For the non-foil card, its foil version is the foil card
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_1"))
            .otherwise(pl.col("uuid_0"))
            .alias("uuid"),
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_0"))
            .otherwise(pl.col("uuid_1"))
            .alias("_foil_version"),
        ]
    )

    nonfoil_map = pair_cards.select(
        [
            # For the foil card, its nonfoil version is the non-foil card
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_0"))
            .otherwise(pl.col("uuid_1"))
            .alias("uuid"),
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_1"))
            .otherwise(pl.col("uuid_0"))
            .alias("_nonfoil_version"),
        ]
    )

    # Join mappings back to main LazyFrame
    lf = lf.join(foil_map, on="uuid", how="left")
    lf = lf.join(nonfoil_map, on="uuid", how="left")

    # Inject into identifiers struct
    lf = lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            [
                pl.col("_foil_version").alias("mtgjsonFoilVersionId"),
                pl.col("_nonfoil_version").alias("mtgjsonNonFoilVersionId"),
            ]
        )
    ).drop(["_foil_version", "_nonfoil_version"])

    return lf


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

    return safe_drop(
        lf.with_columns(
            pl.when(pl.col("setCode") == "SLD")
            .then(pl.col("_sld_subsets"))
            .otherwise(pl.lit(None))
            .alias("subsets")
        ),
        ["_sld_subsets"],
    )


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

    if card_to_products_df is None:
        return lf.with_columns(
            pl.lit(None).cast(NestedStructs.SOURCE_PRODUCTS).alias("sourceProducts")
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

    # Some source cols might be missing in specific batches
    if rename_map:
        lf = safe_rename(lf, rename_map)

    # For non-multiface cards, these should be null (omitted from output)
    multiface_layouts = [
        "split",
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
    # CARD_SET_EXCLUDE contains fields ONLY on CardSet (printing-specific fields)
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
    """
    Sink card pipeline to partitioned parquet using streaming execution.

    Output structure (Hive-style partitioning, compatible with assemble_json_outputs):
        CACHE_PATH/_parquet/setCode={SET_CODE}/0.parquet
        CACHE_PATH/_parquet_tokens/setCode={SET_CODE}/0.parquet
        CACHE_PATH/_parquet_decks/setCode={SET_CODE}/0.parquet

    Uses pl.PartitionByKey with sink_parquet for true streaming - data flows
    directly from source to partitioned output files without full materialization.

    Memory Complexity: O(streaming_chunk_size) - never holds full 5M card dataset

    Trade-off: Each sink_parquet evaluates the base LazyFrame independently (3 scans).
    Acceptable because scans are streaming and OS page cache helps subsequent scans.
    """
    import time

    # Cards go to _parquet/ (assemble_json_outputs expects this)
    cards_dir = constants.CACHE_PATH / "_parquet"
    # Tokens go to cache/_parquet_tokens/
    tokens_dir = constants.CACHE_PATH / "_parquet_tokens"

    for path in [cards_dir, tokens_dir]:
        path.mkdir(parents=True, exist_ok=True)

    lf = ctx.final_cards_lf
    if lf is None:
        LOGGER.error("No final_cards_lf available in context")
        return
    cards_lf, tokens_lf = filter_out_tokens(lf)

    # Apply rename_all_the_things to get final column names before sink
    tlf = rename_all_the_things(tokens_lf, output_type="card_token")
    clf = rename_all_the_things(cards_lf, output_type="card_set")

    # Sink cards and tokens to partitioned parquet
    # Decks are built separately by scanning card parquet and expanding deck references
    outputs = [
        (clf, cards_dir, "cards"),
        (tlf, tokens_dir, "tokens"),
    ]

    for lazy_frame, out_dir, label in outputs:
        LOGGER.info(f"Sinking {label} to partitioned parquet...")
        start = time.time()
        lazy_frame.sink_parquet(
            pl.PartitionByKey(out_dir, by=["setCode"], include_key=True),
            mkdir=True,
        )
        LOGGER.info(f"{label.capitalize()} sink complete in {time.time() - start:.2f}s")


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

    return safe_drop(lf, ["_side_for_join"])


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
    - originalReleaseDate: String
    """
    if ctx.oracle_data_lf is None:
        return lf.with_columns(
            pl.lit([]).alias("rulings"),
            pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
            pl.lit(None).cast(pl.Int64).alias("edhrecRank"),
            pl.lit([]).cast(pl.List(pl.String)).alias("printings"),
            pl.lit(None).cast(pl.String).alias("originalReleaseDate"),
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
        # Only set originalReleaseDate for reprints
        pl.when(pl.col("isReprint"))
        .then(pl.col("originalReleaseDate"))
        .otherwise(pl.lit(None).cast(pl.String))
        .alias("originalReleaseDate"),
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
    )
    return safe_drop(lf, ["_parent_salt"])


def join_set_number_data(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Single join for all setCode+number-based lookups.

    Replaces:
    - join_foreign_data()
    - add_duel_deck_side()

    Gets from set_number_lf:
    - foreignData: List[Struct{faceName, flavorText, identifiers{multiverseId, scryfallId}, language, multiverseId, name, text, type, uuid}]
    - duelDeck: String
    """
    if ctx.set_number_lf is None:
        return lf.with_columns(
            pl.lit([]).alias("foreignData"),
            pl.lit(None).cast(pl.String).alias("duelDeck"),
        )

    lf = lf.join(
        ctx.set_number_lf,
        left_on=["setCode", "number"],
        right_on=["setCode", "number"],
        how="left",
    )

    # Fill nulls
    return lf.with_columns(
        pl.col("foreignData").fill_null([]),
        # Only keep duelDeck for DD* and GS1 sets
        pl.when(pl.col("setCode").str.starts_with("DD") | (pl.col("setCode") == "GS1"))
        .then(pl.col("duelDeck"))
        .otherwise(pl.lit(None))
        .alias("duelDeck"),
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

    # First join on name
    lf = lf.join(
        ctx.name_lf,
        on="name",
        how="left",
    )

    has_face_name = pl.col("faceName").is_not_null()
    is_meld = pl.col("layout") == "meld"
    _ = is_meld & has_face_name & pl.col("cardParts").is_null()

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
    lf = safe_drop(
        lf.with_columns(
            pl.coalesce(
                pl.col("cardParts"),
                pl.col("_face_cardParts"),
            ).alias("cardParts")
        ),
        ["_face_cardParts"],
    )

    # Rename spellbook for compatibility with add_related_cards_struct
    if "spellbook" in lf.collect_schema().names():
        lf = lf.rename({"spellbook": "_spellbook_list"})
    else:
        lf = lf.with_columns(
            pl.lit(None).cast(pl.List(pl.String)).alias("_spellbook_list")
        )

    return lf


def join_multiverse_bridge(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join MultiverseBridge data (cardsphere, deckbox IDs).

    Replaces: add_multiverse_bridge_ids()

    Gets from multiverse_bridge_lf:
    - cardsphereId: String
    - cardsphereFoilId: String
    - deckboxId: String

    These get merged into the identifiers struct.
    """
    if ctx.multiverse_bridge_lf is None:
        return lf

    lf = lf.join(
        ctx.multiverse_bridge_lf,
        on="scryfallId",
        how="left",
    )

    # Merge into identifiers struct
    return safe_drop(
        lf.with_columns(
            pl.col("identifiers")
            .struct.with_fields(
                pl.col("cardsphereId"),
                pl.col("cardsphereFoilId"),
                pl.col("deckboxId"),
            )
            .alias("identifiers")
        ),
        ["cardsphereId", "cardsphereFoilId", "deckboxId"],
    )


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
    return safe_drop(lf.rename({"signature": "_wc_signature"}), ["_num_prefix"])


def add_uuid_from_cache(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute uuid from pre-joined cachedUuid or compute fresh.

    Requires cachedUuid column from join_identifiers().
    Falls back to computing uuid5 if no cached value exists.

    This replaces the complex add_uuid_expr() function.
    """
    return safe_drop(
        lf.with_columns(
            pl.coalesce(
                pl.col("cachedUuid"),
                _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
            ).alias("uuid")
        ),
        ["cachedUuid"],
    )


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
    return safe_drop(lf, ["_num_digits", "_num_suffix", "_wc_signature"])


def add_related_cards_from_context(
    lf: pl.LazyFrame,
    _ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build relatedCards struct using pre-joined spellbook data.

    Requires _spellbook_list column from join_name_data().
    Requires reverseRelated column from add_reverse_related().

    IMPORTANT: relatedCards only appears on TOKENS, not regular cards.
    Regular cards may have reverseRelated as a top-level field, but not wrapped in relatedCards.

    Replaces: add_related_cards_struct()
    """
    # Only tokens get relatedCards struct
    is_token = (
        pl.col("layout").is_in(constants.TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

    # Build relatedCards struct - include if token AND has spellbook or reverseRelated data
    has_spellbook = (
        pl.col("setType").str.to_lowercase().str.contains("alchemy")
        & pl.col("_spellbook_list").is_not_null()
        & (pl.col("_spellbook_list").list.len() > 0)
    )
    has_reverse = pl.col("reverseRelated").is_not_null() & (
        pl.col("reverseRelated").list.len() > 0
    )

    return safe_drop(
        lf.with_columns(
            pl.when(is_token & (has_spellbook | has_reverse))
            .then(
                pl.struct(
                    spellbook=pl.col("_spellbook_list"),
                    reverseRelated=pl.col("reverseRelated"),
                )
            )
            .otherwise(pl.lit(None))
            .alias("relatedCards")
        ),
        ["_spellbook_list"],
    )


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
    if ctx.cards_lf is None:
        raise ValueError("cards_lf is not available in context")
    lf = ctx.cards_lf.filter(pl.col("lang") == "en")
    if set_codes:
        lf = lf.filter(pl.col("set").str.to_uppercase().is_in(set_codes))

    # Apply scryfall_id filter for deck-only builds
    if ctx.scryfall_id_filter:
        lf = lf.filter(pl.col("id").is_in(ctx.scryfall_id_filter))
        LOGGER.info(f"Applied scryfall_id filter: {len(ctx.scryfall_id_filter):,} IDs")

    lf = lf.with_columns(pl.col("set").str.to_uppercase()).join(
        sets_lf, on="set", how="left"
    )

    # Per-card transforms (streaming OK)
    lf = (
        lf.pipe(explode_card_faces)
        .pipe(partial(validate_stage, stage=STAGE_POST_EXPLODE, strict=False))
        .pipe(add_basic_fields)
        .pipe(partial(validate_stage, stage=STAGE_POST_BASIC_FIELDS, strict=False))
        .pipe(parse_type_line_expr)
        .pipe(add_mana_info)
        .pipe(add_card_attributes)
        .pipe(filter_keywords_for_face)
        .pipe(add_booster_types)
        .pipe(partial(add_legalities_struct, ctx=ctx))
        .pipe(partial(add_availability_struct, ctx=ctx))
    )
    
    lf = (
        lf.pipe(
            partial(join_identifiers, ctx=ctx)
        )
        .pipe(
            partial(join_oracle_data, ctx=ctx)
        )
        .pipe(
            partial(join_set_number_data, ctx=ctx)
        )
        .pipe(partial(join_name_data, ctx=ctx))  # name: spellbook + cardParts
        .pipe(partial(join_cardmarket_ids, ctx=ctx))
    )

    lf = (
        lf.pipe(add_identifiers_struct)
        .pipe(add_uuid_from_cache)
        .pipe(partial(validate_stage, stage=STAGE_POST_IDENTIFIERS, strict=False))
        .pipe(add_identifiers_v4_uuid)
    )

    lf = lf.pipe(partial(join_gatherer_data, ctx=ctx))

    lf = (
        lf.pipe(add_other_face_ids)  # FULL SCAN: groups by scryfallId
        .pipe(add_variations)  # FULL SCAN: groups by set+name
        .pipe(partial(add_leadership_skills_expr, ctx=ctx))
        .pipe(add_reverse_related)
        .pipe(propagate_salt_to_tokens)  # Inherit salt from parent cards to tokens
        .pipe(
            partial(add_related_cards_from_context, _ctx=ctx)
        )  # Uses pre-joined spellbook
        .pipe(partial(add_alternative_deck_limit, ctx=ctx))
        .pipe(partial(add_is_funny, ctx=ctx))
        .pipe(add_is_timeshifted)
        .pipe(add_purchase_urls_struct)
    )

    # Final enrichment
    lf = (
        lf.pipe(partial(apply_manual_overrides, ctx=ctx))
        .pipe(add_rebalanced_linkage)  # FULL SCAN
        .pipe(link_foil_nonfoil_versions)  # FULL SCAN
        .pipe(partial(join_multiverse_bridge, ctx=ctx))  # Pre-computed DF
        .pipe(partial(add_secret_lair_subsets, ctx=ctx))
        .pipe(partial(add_source_products, ctx=ctx))  # Late join on uuid
    )

    # Signatures + cleanup
    lf = (
        lf.pipe(partial(join_signatures, ctx=ctx))  # Pre-computed DF
        .pipe(partial(add_signatures_combined, _ctx=ctx))  # Combines art series + WC
        .pipe(drop_raw_scryfall_columns)
        .pipe(partial(validate_stage, stage=STAGE_PRE_SINK, strict=False))
    )

    ctx.final_cards_lf = lf
    ctx.final_cards_lf = lf

    # Sink to partitioned parquet for assemble_json_outputs
    sink_cards(ctx)

    return ctx


def _expand_card_list_v2(
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


def build_decks_expanded(
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
        expanded_lists[col] = _expand_card_list_v2(decks_df, cards_df, col)

    # Expand tokens (also from cards_df - tokens have T-prefix set codes)
    expanded_lists["tokens"] = _expand_card_list_v2(decks_df, cards_df, "tokens")

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


def write_deck_json_files(
    decks_df: pl.DataFrame,
    set_codes: list[str] | None = None,
    pretty_print: bool = False,
    output_dir: pathlib.Path | str | None = None,
) -> int:
    """
    Write individual deck JSON files directly from DataFrame.

    This is the streamlined alternative to assemble_deck_json_outputs()
    that takes a DataFrame directly instead of reading from parquet.

    Args:
        decks_df: DataFrame with expanded deck data from build_decks_expanded()
        set_codes: Optional list of set codes to filter
        pretty_print: If True, indent JSON output
        output_dir: Optional output directory override (for testing)

    Returns:
        Number of deck files written
    """
    if output_dir is None:
        output_dir = MtgjsonConfig().output_path
    else:
        output_dir = pathlib.Path(output_dir)

    if decks_df is None:
        LOGGER.info("No decks to write")
        return 0
    if isinstance(decks_df, pl.LazyFrame):
        decks_df = decks_df.collect()
    if len(decks_df) == 0:
        LOGGER.info("No decks to write")
        return 0

    # Filter by set codes if specified
    if set_codes:
        sets_filter = {s.upper() for s in set_codes}
        decks_df = decks_df.filter(
            pl.col("setCode").str.to_uppercase().is_in(sets_filter)
        )

    if len(decks_df) == 0:
        LOGGER.info("No decks found for specified sets")
        return 0

    meta = MtgjsonMetaObject()
    meta_dict = {"date": meta.date, "version": meta.version}

    count = 0
    for deck in decks_df.to_dicts():
        deck_name = deck.get("name", "Unknown")
        set_code = deck.get("setCode", deck.get("code", "UNK"))

        # Remove spaces and special chars, append set_code
        safe_name = "".join(c for c in deck_name if c.isalnum())
        filename = f"{safe_name}_{set_code}"

        # Create flat directory: decks/
        out_dir = output_dir / "decks"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Remove setCode from output (type is kept)
        deck_data = {k: v for k, v in deck.items() if k != "setCode"}
        # Clean nulls/empty/false from deck data
        deck_data = clean_nested(deck_data, omit_empty=True)

        output = {"meta": meta_dict, "data": deep_sort_keys(deck_data)}

        output_path = out_dir / f"{filename}.json"
        with output_path.open("wb") as f:
            if pretty_print:
                f.write(orjson.dumps(output, option=orjson.OPT_INDENT_2))
            else:
                f.write(orjson.dumps(output))

        # Write SHA256 hash
        sha_path = out_dir / f"{filename}.json.sha256"
        with output_path.open("rb") as f:
            import hashlib

            sha256_hash = hashlib.sha256(f.read()).hexdigest()
        sha_path.write_text(sha256_hash)

        count += 1

    LOGGER.info(f"Wrote {count} deck files to {out_dir}")
    return count


def build_sealed_products_df(
    ctx: PipelineContext, _set_code: str | None = None
) -> pl.DataFrame:
    """
    Build sealed products DataFrame with contents struct.

    Joins github_sealed_products with github_sealed_contents
    and aggregates contents by type (card, sealed, other).
    Also builds purchaseUrls from identifiers.

    Args:
        set_code: Optional set code filter. If None, returns all sets.

    Returns:
        DataFrame with columns: setCode, name, category, subtype, releaseDate,
        identifiers (struct), contents (struct), purchaseUrls (struct), uuid
    """
    products_lf = ctx.sealed_products_df
    contents_lf = ctx.sealed_contents_df
    if products_lf is None or contents_lf is None:
        LOGGER.warning("GitHub sealed products data not loaded in cache")
        return pl.DataFrame()

    # Convert to LazyFrames for processing (if DataFrame)
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
    # Similar to card purchaseUrls, create MTGJSON redirect URLs
    base_url = "https://mtgjson.com/links/"

    # Build URL hash columns for each provider
    purchase_url_fields = []

    # Check if identifiers column exists and extract provider IDs
    result_schema = result.collect_schema()
    result_cols = result_schema.names()
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
        # Drop temporary hash columns
        current_cols = result.collect_schema().names()
        hash_cols = [
            c for c in current_cols if c.startswith("_") and c.endswith("_hash")
        ]
        result = safe_drop(result, hash_cols)
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

    # Add optional columns if available
    final_cols = result.collect_schema().names()

    # releaseDate (might be release_date from model)
    if "releaseDate" in final_cols:
        select_cols.insert(4, "releaseDate")
    elif "release_date" in final_cols:
        select_cols.insert(4, pl.col("release_date").alias("releaseDate"))

    # cardCount from contents aggregation
    if "cardCount" in final_cols:
        select_cols.append("cardCount")

    result = result.select(select_cols)

    # Collect result to return DataFrame (result is always a LazyFrame at this point)
    return result.collect()


def build_set_metadata_df(
    ctx: PipelineContext,
) -> pl.DataFrame:
    """
    Build a DataFrame containing all set-level metadata.

    Includes all 23 MTGJSON set-level fields:
    - Core: code, name, releaseDate, type, block
    - Sizes: baseSetSize, totalSetSize
    - Identifiers: mcmId, mcmName, tcgplayerGroupId, mtgoCode, keyruneCode, tokenSetCode
    - External: cardsphereSetId
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

    # Get Cardsphere set IDs from MultiverseBridge
    cardsphere_sets: dict[str, int] = (
        ctx.multiverse_bridge_sets if ctx.multiverse_bridge_sets is not None else {}
    )

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
        pl.col("releasedAt").alias("releaseDate"),
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
        set_meta = safe_drop(set_meta, cols_to_drop)

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

        # Keep booster as JSON string - will be parsed in assemble_json_outputs()
        # Parsing here causes Polars to create a union struct schema with all
        # possible keys from all sets when we create pl.DataFrame(set_records)

        # MCM data (lookup by lowercased set name)
        mcm_data = mcm_set_map.get(set_name.lower(), {})
        record["mcmId"] = mcm_data.get("mcmId")
        record["mcmName"] = mcm_data.get("mcmName")

        # Cardsphere set ID (lookup by uppercase set code)
        record["cardsphereSetId"] = cardsphere_sets.get(set_code.upper())

        # Translations (lookup by set name)from
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

    return pl.DataFrame(set_records)


def assemble_json_outputs(
    ctx: PipelineContext,
    include_referrals: bool = False,
    parallel: bool = False,
    max_workers: int | None = None,
) -> None:
    """
    Read parquet partitions and assemble final JSON files per set.

    Combines cards, tokens, boosters, sealed products, decks into
    the full MTGJSON set structure.

    Args:
        set_codes: Optional list of set codes to process. If None, processes all.
        pretty_print: Whether to format JSON with indentation.
        include_referrals: Whether to build referral map for affiliate links.
        parallel: Use ThreadPoolExecutor for parallel set assembly.
        max_workers: Maximum parallel workers (default: based on CPU count).
    """
    output_dir = MtgjsonConfig().output_path
    parquet_dir = constants.CACHE_PATH / "_parquet"

    set_codes = ctx.sets_to_build

    cpu_count = os.cpu_count() or 4
    workers = max_workers or max(2, min(8, cpu_count // 2))

    if set_codes:
        sets_to_process = [s.upper() for s in set_codes]
    else:
        sets_to_process = [
            p.name.replace("setCode=", "")
            for p in parquet_dir.iterdir()
            if p.is_dir() and p.name.startswith("setCode=")
        ]

    LOGGER.info("Pre-loading shared data...")
    set_meta_df = build_set_metadata_df(ctx)
    if isinstance(set_meta_df, pl.LazyFrame):
        set_meta_df = set_meta_df.collect()
    set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

    # Load sealed products with proper contents structure
    sealed_products_df = build_sealed_products_df(ctx)
    if isinstance(sealed_products_df, pl.LazyFrame):
        sealed_products_df = sealed_products_df.collect()

    LOGGER.info("Fetching deck data...")
    decks_df = build_decks_expanded(ctx, set_codes=sets_to_process)
    if isinstance(decks_df, pl.LazyFrame):
        decks_df = decks_df.collect()

    # Build meta object
    meta = MtgjsonMetaObject()
    meta_dict = {"date": meta.date, "version": meta.version}

    # Pre-filter sealed products and decks by set
    sealed_by_set = {}
    decks_by_set = {}
    for set_code in sets_to_process:
        if len(sealed_products_df) > 0 and "setCode" in sealed_products_df.columns:
            raw_sealed = (
                sealed_products_df.filter(pl.col("setCode") == set_code)
                .drop("setCode")
                .to_dicts()
            )
            # Clean nulls from sealed products
            sealed_by_set[set_code] = [clean_nested(sp) for sp in raw_sealed]
        else:
            sealed_by_set[set_code] = []

        if decks_df is not None and len(decks_df) > 0 and "setCode" in decks_df.columns:
            raw_decks = (
                decks_df.filter(pl.col("setCode") == set_code)
                .drop("setCode")
                .to_dicts()
            )
            # Clean nulls from decks
            decks_by_set[set_code] = [
                clean_nested(d, omit_empty=False) for d in raw_decks
            ]
        else:
            decks_by_set[set_code] = []

    # Thread-safe referral collection
    all_referral_entries = []
    referral_lock = threading.Lock()
    string_regex = re.compile(re.escape("scryfall"), re.IGNORECASE)

    def assemble_single_set(set_code: str) -> tuple[str, bool]:
        """Assemble a single set's JSON file."""
        try:
            meta_row = set_meta.get(set_code, {})

            # Read cards for this set
            cards_path = parquet_dir / f"setCode={set_code}"
            if not cards_path.exists():
                LOGGER.warning(f"No cards found for {set_code}")
                return (set_code, False)

            cards_df = pl.read_parquet(cards_path / "*.parquet")
            cards = dataframe_to_cards_list(cards_df)

            tokens = []
            token_set_code = meta_row.get("tokenSetCode", f"T{set_code}")
            tokens_parquet_dir = constants.CACHE_PATH / "_parquet_tokens"
            tokens_path = tokens_parquet_dir / f"setCode={token_set_code}"
            if tokens_path.exists():
                tokens_df = pl.read_parquet(tokens_path / "*.parquet")
                tokens = dataframe_to_cards_list(tokens_df)

            # Get pre-filtered sealed products and decks
            set_sealed = sealed_by_set.get(set_code, [])
            set_decks = decks_by_set.get(set_code, [])

            # Get booster config - parse from JSON string
            booster_raw = meta_row.get("booster")
            booster = None
            if booster_raw and isinstance(booster_raw, str):
                try:
                    booster = json.loads(booster_raw)
                except json.JSONDecodeError:
                    booster = None

            # Calculate baseSetSize from metadata or count non-reprints
            base_set_size = meta_row.get("baseSetSize")
            if base_set_size is None:
                base_set_size = len([c for c in cards if not c.get("isReprint")])
                if base_set_size == 0:
                    base_set_size = len(cards)

            # Calculate totalSetSize from metadata or actual card count
            total_set_size = meta_row.get("totalSetSize") or len(cards)

            raw_translations = meta_row.get("translations", {})
            if not raw_translations or not isinstance(raw_translations, dict):
                raw_translations = {}
            # Build translations with all languages (full names), preserving None for missing
            translations = {
                lang_name: raw_translations.get(lang_name)
                for lang_name in constants.LANGUAGE_MAP.values()
            }

            # Get languages from foreign data on cards
            languages_set: set[str] = set()
            for card in cards:
                foreign_data = card.get("foreignData", [])
                if foreign_data:
                    for fd in foreign_data:
                        if isinstance(fd, dict) and fd.get("language"):
                            languages_set.add(fd["language"])
            languages_set.add("English")  # Always include English
            languages = sorted(languages_set)

            # Assemble final set object with all 23 required fields
            set_data = {
                "baseSetSize": base_set_size,
                "cards": cards,
                "code": set_code,
                "isFoilOnly": meta_row.get("isFoilOnly", False),
                "isOnlineOnly": meta_row.get("isOnlineOnly", False),
                "keyruneCode": meta_row.get("keyruneCode", set_code),
                "languages": languages,
                "name": meta_row.get("name", set_code),
                "releaseDate": meta_row.get("releaseDate", ""),
                "tokens": tokens,
                "totalSetSize": total_set_size,
                "translations": translations,
                "type": meta_row.get("type", ""),
            }

            # Add optional fields only if they have values
            if booster:
                set_data["booster"] = booster
            if set_sealed:
                set_data["sealedProduct"] = set_sealed
            if set_decks:
                set_data["decks"] = set_decks
            if meta_row.get("mtgoCode"):
                set_data["mtgoCode"] = meta_row["mtgoCode"]
            if meta_row.get("parentCode"):
                set_data["parentCode"] = meta_row["parentCode"]
            if meta_row.get("block"):
                set_data["block"] = meta_row["block"]
            if meta_row.get("tcgplayerGroupId"):
                set_data["tcgplayerGroupId"] = meta_row["tcgplayerGroupId"]
            if meta_row.get("tokenSetCode"):
                set_data["tokenSetCode"] = meta_row["tokenSetCode"]
            if meta_row.get("cardsphereSetId"):
                set_data["cardsphereSetId"] = meta_row["cardsphereSetId"]
            if meta_row.get("mcmId"):
                set_data["mcmId"] = meta_row["mcmId"]
            if meta_row.get("mcmName"):
                set_data["mcmName"] = meta_row["mcmName"]
            if meta_row.get("isNonFoilOnly"):
                set_data["isNonFoilOnly"] = meta_row["isNonFoilOnly"]

            # Final output - sort data keys recursively, meta comes first
            output = {"meta": meta_dict, "data": deep_sort_keys(set_data)}

            # Write JSON to output/<SET>.json
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{set_code}.json"
            with output_path.open("wb") as f:
                f.write(orjson.dumps(output))

            # Build referral entries if needed
            if include_referrals:
                local_entries = []
                for card in cards:
                    purchase_urls = card.get("purchaseUrls", {})
                    raw_urls = card.get("rawPurchaseUrls", {})
                    for service, url in purchase_urls.items():
                        if service in raw_urls and url:
                            url_id = url.split("/")[-1]
                            raw_url = string_regex.sub("mtgjson", raw_urls[service])
                            local_entries.append((url_id, raw_url))
                for product in set_sealed:
                    purchase_urls = product.get("purchaseUrls", {})
                    raw_urls = product.get("rawPurchaseUrls", {})
                    for service, url in purchase_urls.items():
                        if service in raw_urls and url:
                            url_id = url.split("/")[-1]
                            raw_url = string_regex.sub("mtgjson", raw_urls[service])
                            local_entries.append((url_id, raw_url))
                with referral_lock:
                    all_referral_entries.extend(local_entries)

            return (set_code, True)
        except Exception as e:
            LOGGER.error(f"Failed to assemble {set_code}: {e}")
            return (set_code, False)

    # Execute set assembly
    success_count = 0
    failed_sets = []

    if parallel:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(assemble_single_set, sc): sc for sc in sets_to_process
            }
            for future in as_completed(futures):
                set_code, success = future.result()
                if success:
                    success_count += 1
                else:
                    failed_sets.append(set_code)
    else:
        for set_code in sets_to_process:
            LOGGER.info(f"Assembling {set_code}...")
            _, success = assemble_single_set(set_code)
            if success:
                success_count += 1
            else:
                failed_sets.append(set_code)

    # Write referral map
    if include_referrals and all_referral_entries:
        write_referral_map(all_referral_entries)
        fixup_referral_map()

    # Assemble deck JSON files
    write_deck_json_files(decks_df)

    LOGGER.info(
        "JSON assembly complete: %d/%d sets%s",
        success_count,
        len(sets_to_process),
        f", {len(failed_sets)} failed" if failed_sets else "",
    )
    if failed_sets:
        LOGGER.warning(f"Failed sets: {failed_sets}")
