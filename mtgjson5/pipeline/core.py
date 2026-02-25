"""
MTGJSON card data pipeline orchestrator.

Coordinates the stage modules to transform Scryfall bulk data
into MTGJSON format.
"""

from functools import partial

import polars as pl

from mtgjson5.data import PipelineContext
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.pipeline.stages.basic_fields import (
    add_basic_fields,
    add_booster_types,
    add_card_attributes,
    add_mana_info,
    add_original_release_date,
    apply_card_enrichment,
    apply_watermark_overrides,
    filter_keywords_for_face,
    fix_manavalue_for_multiface,
    fix_power_toughness_for_multiface,
    fix_promo_types,
    format_planeswalker_text,
    parse_type_line_expr,
    propagate_watermark_to_faces,
)
from mtgjson5.pipeline.stages.derived import (
    add_alternative_deck_limit,
    add_is_funny,
    add_is_timeshifted,
    add_purchase_urls_struct,
    add_rebalanced_linkage,
    add_secret_lair_subsets,
    add_source_products,
    apply_manual_overrides,
    calculate_duel_deck,
)
from mtgjson5.pipeline.stages.explode import (
    assign_meld_sides,
    detect_aftermath_layout,
    explode_card_faces,
    update_meld_names,
)
from mtgjson5.pipeline.stages.identifiers import (
    add_identifiers_struct,
    add_identifiers_v4_uuid,
    add_uuid_from_cache,
    fix_foreigndata_for_faces,
    join_cardmarket_ids,
    join_face_flavor_names,
    join_gatherer_data,
    join_identifiers,
    join_name_data,
    join_oracle_data,
    join_set_number_data,
    join_tcg_alt_foil_lookup,
)
from mtgjson5.pipeline.stages.legalities import (
    add_availability_struct,
    add_legalities_struct,
    fix_availability_from_ids,
    remap_availability_values,
)
from mtgjson5.pipeline.stages.metadata import (
    build_expanded_decks_df,
    build_sealed_products_lf,
    build_set_metadata_df,
)
from mtgjson5.pipeline.stages.output import (
    drop_raw_scryfall_columns,
    sink_cards,
)
from mtgjson5.pipeline.stages.relationships import (
    add_leadership_skills_expr,
    add_other_face_ids,
    add_reverse_related,
    add_token_ids,
    propagate_salt_to_tokens,
)
from mtgjson5.pipeline.stages.signatures import (
    add_related_cards_from_context,
    add_signatures_combined,
    join_signatures,
)
from mtgjson5.utils import LOGGER

# Re-export standalone builders for backwards compatibility
__all__ = [
    "build_cards",
    "build_expanded_decks_df",
    "build_sealed_products_lf",
    "build_set_metadata_df",
]


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

    base_lf = ctx.cards_lf.with_columns(pl.col("set").str.to_uppercase().alias("_set_upper"))
    if set_codes:
        base_lf = base_lf.filter(pl.col("_set_upper").is_in(set_codes))

    # Pattern: "A // B // A" where first and third parts are identical
    base_lf = (
        base_lf.with_columns(
            [
                (pl.col("name").str.count_matches(" // ") + 1).alias("_num_parts"),
                pl.col("name").str.extract(r"^([^/]+) // ", 1).str.strip_chars().alias("_part0"),
                pl.col("name").str.extract(r" // ([^/]+) // ", 1).str.strip_chars().alias("_part1"),
                pl.col("name").str.extract(r" // ([^/]+)$", 1).str.strip_chars().alias("_part2"),
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
            (pl.col("lang") == "en") | pl.col("_has_english").is_null()
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
        .pipe(partial(join_tcg_alt_foil_lookup, ctx=ctx))
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
                "tcgplayerAlternativeFoilId",
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

    # Derive scryfallId -> uuid mapping once for token resolution
    scryfall_uuid_lf = lf.select(["scryfallId", "uuid"])

    LOGGER.info("  Checkpoint complete")

    lf = (
        lf.pipe(partial(add_other_face_ids, ctx=ctx))
        .pipe(partial(add_leadership_skills_expr, ctx=ctx))
        .pipe(add_reverse_related)
        .pipe(partial(add_token_ids, scryfall_uuid_lf=scryfall_uuid_lf))
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
