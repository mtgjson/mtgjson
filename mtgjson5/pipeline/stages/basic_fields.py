"""
Basic field mapping, type parsing, and card attributes.

Maps raw Scryfall fields to MTGJSON schema, parses type lines,
computes mana values, and adds remaining card attributes.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from mtgjson5.consts import LANGUAGE_MAP, MULTI_WORD_SUB_TYPES, SUPER_TYPES
from mtgjson5.data import PipelineContext
from mtgjson5.pipeline.expressions import (
    calculate_cmc_expr,
    extract_colors_from_mana_expr,
    order_finishes_expr,
    sort_colors_wubrg_expr,
)
from mtgjson5.pipeline.stages.explode import _ascii_name_expr
from mtgjson5.utils import to_snake_case


def format_planeswalker_text(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Wrap planeswalker loyalty ability costs in square brackets.
    """
    return lf.with_columns(pl.col("text").str.replace_all(r"(?m)^([+\u2212−]?[\dX]+):", r"[$1]:").alias("text"))


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
        struct_field = to_snake_case(field_name) if "_" not in field_name else field_name
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
                .then(extract_colors_from_mana_expr(pl.col("_face_data").struct.field("mana_cost")))
                .when((pl.col("layout") == "adventure") & (pl.col("side") == "a"))
                .then(sort_colors_wubrg_expr(face_field("colors")))
                .when(
                    (pl.col("layout") == "adventure")
                    & (pl.col("side") == "b")
                    & (pl.col("typeLine").str.contains(r"(?i)\bLand\b").max().over("scryfallId"))
                )
                .then(pl.lit([]).cast(pl.List(pl.String)))
                .when((pl.col("layout") == "adventure") & (pl.col("side") == "b"))
                .then(extract_colors_from_mana_expr(pl.col("_face_data").struct.field("mana_cost")))
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
                pl.coalesce(face_field("flavorName"), pl.col("printedName")).alias("flavorName"),
                pl.col("allParts"),
                pl.col("lang")
                .replace_strict(LANGUAGE_MAP, default=pl.col("lang"), return_dtype=pl.String)
                .alias("language"),
            ]
        )
        .with_columns(
            pl.when(ascii_face_name != face_name).then(ascii_full_name).otherwise(None).alias("asciiName"),
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
        subtypes_processed = subtypes_processed.str.replace_all(mw_subtype, mw_subtype.replace(" ", "\x00"))

    subtypes_expr = (
        pl.when(pl.col("_subtypes_part").is_null())
        .then(pl.lit([]).cast(pl.List(pl.String)))
        .when(pl.col("_types_part").str.starts_with("Plane"))
        .then(pl.concat_list([pl.col("_subtypes_part").str.strip_chars()]))
        .otherwise(subtypes_processed.str.split(" ").list.eval(pl.element().str.replace_all("\x00", " ")))
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
            pl.col("_type_words").list.eval(pl.element().filter(~pl.element().is_in(super_types_list))).alias("types"),
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
        pl.when(pl.col("_face_data").is_not_null() & use_face_cmc).then(face_cmc).otherwise(pl.col("manaValue"))
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
            pl.col("promoTypes").list.eval(pl.element().filter(pl.element() != "planeswalkerdeck")).alias("promoTypes")
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
    rows: list[dict[str, Any]] = []
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
        .then(pl.col("promoTypes").fill_null([]).list.concat(pl.col("enrichmentPromoTypes")).list.unique())
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
        pl.when(has_face_data).then(face_power).otherwise(pl.col("power")).alias("power"),
        pl.when(has_face_data).then(face_toughness).otherwise(pl.col("toughness")).alias("toughness"),
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
        pl.when((pl.col("watermark") == "set") & pl.col("_watermarkOverride").is_not_null())
        .then(pl.col("_watermarkOverride"))
        .otherwise(pl.col("watermark"))
        .alias("watermark")
    ).drop("_watermarkOverride")

    return lf


def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter keywords to only those present in the face's text.
    """
    lf = lf.with_row_index("_kw_idx")

    # Explode keywords, filter by text match, reaggregate
    kw_filtered = (
        lf.select(["_kw_idx", "text", "_all_keywords"])
        .explode("_all_keywords")
        .filter(
            pl.col("_all_keywords").is_not_null()
            & pl.col("text")
            .fill_null("")
            .str.to_lowercase()
            .str.contains(pl.col("_all_keywords").str.to_lowercase(), literal=True)
        )
        .group_by("_kw_idx")
        .agg(pl.col("_all_keywords").alias("keywords"))
    )

    return (
        lf.drop("_all_keywords")
        .join(kw_filtered, on="_kw_idx", how="left")
        .with_columns(pl.col("keywords").fill_null(pl.lit([]).cast(pl.List(pl.String))))
        .drop("_kw_idx")
    )


def add_booster_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute boosterTypes based on Scryfall booster field and promoTypes.
    """
    has_starter_promo = (
        pl.col("promoTypes").list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"])).list.len() > 0
    )
    in_booster = pl.col("_in_booster").fill_null(False)

    return lf.with_columns(
        [
            # boosterTypes computation
            pl.when(in_booster)
            .then(pl.when(has_starter_promo).then(pl.lit(["default", "deck"])).otherwise(pl.lit(["default"])))
            .otherwise(pl.when(has_starter_promo).then(pl.lit(["deck"])).otherwise(pl.lit([]).cast(pl.List(pl.String))))
            .alias("boosterTypes"),
        ]
    ).drop("_in_booster")
