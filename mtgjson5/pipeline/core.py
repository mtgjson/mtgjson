"""
MTGJSON card data pipeline orchestrator.

Coordinates the stage modules to transform Scryfall bulk data
into MTGJSON format.
"""

from __future__ import annotations

import gc
from functools import partial
from typing import TYPE_CHECKING

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
    _uuid5_concat_expr,
    assign_meld_sides,
    detect_aftermath_layout,
    explode_card_faces,
    update_meld_names,
)
from mtgjson5.pipeline.stages.identifiers import (
    add_identifiers_struct,
    add_identifiers_v4_uuid,
    add_sku_ids,
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
    build_id_mappings_from_parquet,
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
from mtgjson5.profiler import get_profiler
from mtgjson5.utils import LOGGER

if TYPE_CHECKING:
    from mtgjson5.profiler import PipelineProfiler

# Re-export standalone builders for backwards compatibility
__all__ = [
    "build_cards",
    "build_expanded_decks_df",
    "build_sealed_products_lf",
    "build_set_metadata_df",
]


def build_cards(
    ctx: PipelineContext,
    batch_size: int | str | None = "auto",
) -> PipelineContext:
    """
    Main card building pipeline.

    Args:
        ctx: Pipeline context with loaded cache and consolidated lookups.
        batch_size: Number of sets per batch. ``"auto"`` (default) picks
            ~50 sets/batch. An integer sets a custom batch size.
    """
    output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Executing card pipeline...")
    prof = get_profiler()
    prof.checkpoint("pipeline_start")

    effective_batch_size = _resolve_batch_size(batch_size, ctx.sets_to_build)
    return _build_cards_batched(ctx, effective_batch_size, prof)


def _build_cards_batched(ctx: PipelineContext, batch_size: int, prof: PipelineProfiler) -> PipelineContext:
    """Process the pipeline in batches of set codes to limit peak memory."""
    set_codes = ctx.sets_to_build

    if ctx.sets_lf is None:
        raise ValueError("sets_df is not available in context")
    sets_raw = ctx.sets_lf.rename({"code": "set"})
    sets_lf = sets_raw.lazy() if isinstance(sets_raw, pl.DataFrame) else sets_raw

    if ctx.cards_lf is None:
        raise ValueError("cards_lf is not available in context")

    sets_schema = sets_lf.collect_schema().names()
    set_select_exprs = _build_set_select_exprs(sets_schema)

    # Pre-pass: build global scryfallId -> uuid mapping for add_token_ids()
    scryfall_uuid_lf = _build_global_scryfall_uuid_map(ctx)
    prof.checkpoint("prepass_scryfall_uuid_map")

    # Determine batches
    all_codes = sorted(set_codes) if set_codes else _get_all_set_codes(ctx)

    batches = [all_codes[i : i + batch_size] for i in range(0, len(all_codes), batch_size)]
    LOGGER.info(f"Batched pipeline: {len(all_codes)} sets in {len(batches)} batches of ~{batch_size} sets each")

    for batch_idx, batch_codes in enumerate(batches):
        batch_label = f"batch_{batch_idx}"
        LOGGER.info(
            f"[{batch_label}] Processing {len(batch_codes)} sets: "
            f"{batch_codes[:5]}{'...' if len(batch_codes) > 5 else ''}"
        )

        lf = _prepare_batch_lf(ctx, batch_codes, sets_lf, set_select_exprs)

        try:
            lf = _run_pipeline_stages(ctx, lf, scryfall_uuid_lf=scryfall_uuid_lf, prof=prof, label=batch_label)
        except Exception:
            LOGGER.error(f"[{batch_label}] Pipeline failed for sets: {batch_codes}")
            raise

        ctx.final_cards_lf = lf
        sink_cards(ctx, skip_id_mappings=True)
        prof.checkpoint(f"{batch_label}/sink_complete")

        # Release batch memory
        ctx.final_cards_lf = None
        del lf
        gc.collect()
        LOGGER.info(f"[{batch_label}] Complete")

    # Post-batch: build ID mappings from all parquet partitions
    build_id_mappings_from_parquet(ctx)
    prof.checkpoint("post_batch_id_mappings")

    return ctx


def _run_pipeline_stages(
    ctx: PipelineContext,
    lf: pl.LazyFrame,
    *,
    scryfall_uuid_lf: pl.LazyFrame,
    prof: PipelineProfiler,
    label: str,
) -> pl.LazyFrame:
    """Run all pipeline stage groups on a LazyFrame.

    Args:
        ctx: Pipeline context with lookups.
        lf: Input LazyFrame (already filtered and joined with set metadata).
        scryfall_uuid_lf: Pre-built global scryfallId->uuid mapping for
            ``add_token_ids()``.
        prof: Profiler instance.
        label: Prefix for profiler checkpoint names (e.g. "batch_0").
    """
    prefix = f"{label}/" if label else ""

    # Stage 1: Per-card transforms
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
    prof.checkpoint(f"{prefix}stage1_transforms")

    # Stage 2: Identifier & data joins
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
    prof.checkpoint(f"{prefix}stage2_joins")

    # Collect 1: prevent Polars lazy plan complexity explosion
    LOGGER.info("Checkpoint: materializing after joins...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")
    prof.checkpoint(f"{prefix}collect_1_after_joins", top_n=10)

    # Stage 3: UUID & identifier structs
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
                "tcgplayerAlternativeFoilProductId",
                "illustrationId",
                "cardBackId",
            ],
            strict=False,
        )
        .pipe(add_uuid_from_cache)
        .pipe(add_identifiers_v4_uuid)
    )
    prof.checkpoint(f"{prefix}stage3_uuids")

    lf = lf.pipe(calculate_duel_deck)
    lf = lf.pipe(partial(join_gatherer_data, ctx=ctx))

    # Collect 2: materialize before relationship operations
    LOGGER.info("Checkpoint: materializing before relationship operations...")
    lf = lf.collect().lazy()

    LOGGER.info("  Checkpoint complete")
    prof.checkpoint(f"{prefix}collect_2_before_relationships", top_n=10)

    # Stage 4: Relationships
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
    prof.checkpoint(f"{prefix}stage4_relationships")

    # Collect 3: materialize before final enrichment
    LOGGER.info("Checkpoint: materializing before final enrichment...")
    lf = lf.collect().lazy()
    LOGGER.info("  Checkpoint complete")
    prof.checkpoint(f"{prefix}collect_3_before_enrichment", top_n=10)

    # Stage 5: Manual enrichment
    lf = (
        lf.pipe(partial(apply_manual_overrides, ctx=ctx))
        .pipe(partial(add_rebalanced_linkage, ctx=ctx))
        .pipe(partial(add_secret_lair_subsets, ctx=ctx))
        .pipe(partial(add_source_products, ctx=ctx))
    )
    prof.checkpoint(f"{prefix}stage5_enrichment")

    # Stage 6: Signatures and output prep
    lf = (
        lf.pipe(partial(join_signatures, ctx=ctx))
        .pipe(partial(add_signatures_combined, _ctx=ctx))
        .pipe(drop_raw_scryfall_columns)
    )
    prof.checkpoint(f"{prefix}stage6_signatures")

    # Stage 7: Per-finish SKU IDs (must run after finishes are finalized)
    lf = lf.pipe(add_sku_ids)
    prof.checkpoint(f"{prefix}stage7_sku_ids")

    return lf


def _build_global_scryfall_uuid_map(ctx: PipelineContext) -> pl.LazyFrame:
    """Build a global scryfallId -> uuid mapping from cards_lf.

    Uses a narrow scan (id, cardFaces columns only) to derive
    (scryfallId, side) pairs, then joins with uuid_cache to get
    cachedUuid, and falls back to uuid5(scryfallId, side).

    Returns a LazyFrame with columns [scryfallId, uuid].
    """
    cards_lf = ctx.cards_lf
    if cards_lf is None:
        raise ValueError("cards_lf is not available")

    # Narrow select: only need scryfall ID and face count
    narrow = cards_lf.select(["id", "cardFaces"]).with_columns(
        pl.col("cardFaces").list.len().fill_null(0).alias("_n_faces")
    )

    # Single-face cards: side = None (defaults to "a" in uuid5_concat)
    single = narrow.filter(pl.col("_n_faces") <= 1).select(
        pl.col("id").alias("scryfallId"),
        pl.lit(None).cast(pl.String).alias("side"),
    )

    # Multi-face cards: explode into one row per face with side a/b/c/d/e
    multi = (
        narrow.filter(pl.col("_n_faces") > 1)
        .with_columns(pl.int_ranges(pl.col("_n_faces")).alias("_face_idx"))
        .explode("_face_idx")
        .with_columns(
            pl.col("_face_idx")
            .replace_strict(
                {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"},
                default="a",
                return_dtype=pl.String,
            )
            .alias("side")
        )
        .select(pl.col("id").alias("scryfallId"), "side")
    )

    all_pairs = pl.concat([single, multi])

    # Fill null side with "a" to match join_identifiers / add_uuid_from_cache
    all_pairs = all_pairs.with_columns(pl.col("side").fill_null("a"))

    # Join with uuid_cache for legacy cachedUuids
    uuid_cache = ctx.uuid_cache_lf
    if uuid_cache is not None:
        all_pairs = all_pairs.join(
            uuid_cache.select(["scryfallId", "side", "cachedUuid"]),
            on=["scryfallId", "side"],
            how="left",
        )
    else:
        all_pairs = all_pairs.with_columns(pl.lit(None).cast(pl.String).alias("cachedUuid"))

    # Compute UUID: coalesce(cachedUuid, uuid5(scryfallId, side))
    all_pairs = (
        all_pairs.with_columns(
            pl.coalesce(
                pl.col("cachedUuid"),
                _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
            ).alias("uuid")
        )
        .select(["scryfallId", "uuid"])
        .unique()
    )

    LOGGER.info("Pre-pass: building global scryfallId -> uuid mapping...")
    result_df = all_pairs.collect()
    LOGGER.info(f"Pre-pass complete: {result_df.height:,} entries, ~{result_df.estimated_size('mb'):.1f} MB")

    return result_df.lazy()


def _resolve_batch_size(
    batch_size: int | str | None,
    set_codes: set[str] | None,
) -> int:
    """Resolve batch_size parameter to an integer."""
    n_sets = len(set_codes) if set_codes else 749
    if batch_size is None or (isinstance(batch_size, str) and batch_size.lower() == "auto"):
        return max(30, n_sets // 15)
    bs = int(batch_size)
    return bs if bs > 0 else max(30, n_sets // 15)


def _build_set_select_exprs(sets_schema: list[str]) -> list[pl.Expr]:
    """Build column selection expressions for sets join."""
    exprs: list[pl.Expr] = [pl.col("set")]
    if "setType" in sets_schema:
        exprs.append(pl.col("setType"))
    if "releasedAt" in sets_schema:
        exprs.append(pl.col("releasedAt").alias("setReleasedAt"))
    if "block" in sets_schema:
        exprs.append(pl.col("block"))
    if "foilOnly" in sets_schema:
        exprs.append(pl.col("foilOnly"))
    if "nonfoilOnly" in sets_schema:
        exprs.append(pl.col("nonfoilOnly"))
    return exprs


def _apply_tdm_name_fix(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Fix TDM triple-slash names: 'A // B // A' -> 'A // B // A // B'."""
    return (
        lf.with_columns(
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


def _prepare_batch_lf(
    ctx: PipelineContext,
    batch_codes: list[str],
    sets_lf: pl.LazyFrame,
    set_select_exprs: list[pl.Expr],
) -> pl.LazyFrame:
    """Filter cards_lf to a batch of set codes and join set metadata."""
    assert ctx.cards_lf is not None, "cards_lf must be loaded before building"
    base_lf = ctx.cards_lf.with_columns(pl.col("set").str.to_uppercase().alias("_set_upper"))
    base_lf = base_lf.filter(pl.col("_set_upper").is_in(batch_codes))

    base_lf = _apply_tdm_name_fix(base_lf)

    lf = base_lf.with_columns(pl.col("set").str.to_uppercase()).join(
        sets_lf.select(set_select_exprs), on="set", how="left"
    )

    if ctx.scryfall_id_filter:
        lf = lf.filter(pl.col("id").is_in(ctx.scryfall_id_filter))

    return lf


def _get_all_set_codes(ctx: PipelineContext) -> list[str]:
    """Get all unique set codes from cards_lf."""
    assert ctx.cards_lf is not None, "cards_lf must be loaded before building"
    return ctx.cards_lf.select(pl.col("set").str.to_uppercase().unique()).collect().to_series().sort().to_list()
