"""
Card-to-products resolver.

Maps sealed product contents to card UUIDs
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.pipeline.stages.explode import _uuid5_expr
from mtgjson5.utils import LOGGER

if TYPE_CHECKING:
    from mtgjson5.data.context import PipelineContext


def _resolve_card_contents(
    contents_lf: pl.LazyFrame,
    product_uuids: pl.LazyFrame,
) -> pl.LazyFrame | None:
    """Resolve contentType == 'card' rows to (card_uuid, product_uuid, finish)."""
    cards = contents_lf.filter(pl.col("contentType") == "card").select("setCode", "productName", "uuid", "foil")
    joined = cards.join(product_uuids, on=["setCode", "productName"], how="inner")
    resolved = joined.select(
        pl.col("uuid").alias("card_uuid"),
        pl.col("product_uuid"),
        pl.when(pl.col("foil") == True).then(pl.lit("foil")).otherwise(pl.lit("nonfoil")).alias("finish"),
    )
    return resolved


def _resolve_pack_contents(
    contents_lf: pl.LazyFrame,
    product_uuids: pl.LazyFrame,
    booster_sheet_cards_lf: pl.LazyFrame,
) -> pl.LazyFrame | None:
    """Resolve contentType == 'pack' via booster sheet cards."""
    packs = contents_lf.filter(pl.col("contentType") == "pack").select("setCode", "productName", "set", "code")
    joined = packs.join(product_uuids, on=["setCode", "productName"], how="inner")
    with_cards = joined.join(
        booster_sheet_cards_lf,
        left_on=["set", "code"],
        right_on=["setCode", "boosterType"],
        how="inner",
    )
    resolved = with_cards.select(
        pl.col("cardUuid").alias("card_uuid"),
        pl.col("product_uuid"),
        pl.when(pl.col("foil") == True).then(pl.lit("foil")).otherwise(pl.lit("nonfoil")).alias("finish"),
    )
    return resolved


def _resolve_deck_contents(
    contents_lf: pl.LazyFrame,
    product_uuids: pl.LazyFrame,
    deck_cards_lf: pl.LazyFrame,
) -> pl.LazyFrame | None:
    """Resolve contentType == 'deck' via deck card lists."""
    decks = contents_lf.filter(pl.col("contentType") == "deck").select("setCode", "productName", "name", "set")
    joined = decks.join(product_uuids, on=["setCode", "productName"], how="inner")
    with_cards = joined.join(
        deck_cards_lf,
        left_on=["name", "set"],
        right_on=["deckName", "setCode"],
        how="inner",
    )
    resolved = with_cards.select(
        pl.col("cardUuid").alias("card_uuid"),
        pl.col("product_uuid"),
        pl.when(pl.col("isEtched") == True)
        .then(pl.lit("etched"))
        .when(pl.col("isFoil") == True)
        .then(pl.lit("foil"))
        .otherwise(pl.lit("nonfoil"))
        .alias("finish"),
    )
    return resolved


def _resolve_sealed_contents(
    contents_lf: pl.LazyFrame,
    product_uuids: pl.LazyFrame,
    base_resolved: pl.LazyFrame,
) -> pl.LazyFrame | None:
    """Resolve contentType == 'sealed' — products containing other products.

    Iteratively looks up what cards the referenced product contains in the
    already-resolved data. A while loop controls convergence (max 5 iterations).
    """
    sealed = contents_lf.filter(pl.col("contentType") == "sealed")
    # The `name` column holds the referenced product name.
    # Select only needed columns to avoid collisions in downstream joins.
    sealed_with_uuids = (
        sealed.select("setCode", "productName", "name")
        .with_columns(_uuid5_expr("name").alias("ref_product_uuid"))
        .join(product_uuids, on=["setCode", "productName"], how="inner")
    )

    # Iterative resolution: look up cards that belong to the referenced product.
    all_new: list[pl.LazyFrame] = []
    current_resolved = base_resolved

    for _ in range(5):
        # For each sealed ref, find cards already resolved under the ref product.
        new_rows = sealed_with_uuids.join(
            current_resolved,
            left_on="ref_product_uuid",
            right_on="product_uuid",
            how="inner",
        ).select(
            pl.col("card_uuid"),
            # The parent product owns these cards.
            pl.col("product_uuid"),
            pl.col("finish"),
        )

        new_df = new_rows.collect()
        if new_df.is_empty():
            break

        new_lf = new_df.lazy()
        all_new.append(new_lf)
        # Extend the resolved pool so nested sealed refs can resolve further.
        current_resolved = pl.concat([current_resolved, new_lf], how="diagonal_relaxed").unique()

    if not all_new:
        return None
    return pl.concat(all_new, how="diagonal_relaxed").unique()


def _resolve_variable_contents(
    contents_lf: pl.LazyFrame,
    product_uuids: pl.LazyFrame,
    booster_sheet_cards_lf: pl.LazyFrame | None,
    deck_cards_lf: pl.LazyFrame | None,
) -> pl.LazyFrame | None:
    """Resolve contentType == 'variable' by exploding configs."""
    # Check configs column type — if it's Null (all nulls), skip entirely
    configs_dtype = contents_lf.collect_schema().get("configs")
    if configs_dtype is None or configs_dtype == pl.Null:
        return None

    variable = contents_lf.filter((pl.col("contentType") == "variable") & pl.col("configs").is_not_null())
    joined = variable.join(product_uuids, on=["setCode", "productName"], how="inner")

    # Explode the configs list and unnest the struct.
    exploded = joined.explode("configs").unnest("configs")

    frames: list[pl.LazyFrame] = []

    # --- card sub-type ---
    # Select only product_uuid + the sub-type column before explode/unnest
    # to avoid column name collisions (e.g. outer `uuid` vs card struct `uuid`).
    schema_names = exploded.collect_schema().names()
    if "card" in schema_names:
        card_part = (
            exploded.filter(pl.col("card").is_not_null()).select("product_uuid", "card").explode("card").unnest("card")
        )
        card_resolved = card_part.select(
            pl.col("uuid").alias("card_uuid"),
            pl.col("product_uuid"),
            pl.when(pl.col("foil") == True).then(pl.lit("foil")).otherwise(pl.lit("nonfoil")).alias("finish"),
        )
        frames.append(card_resolved)

    # --- pack sub-type ---
    if booster_sheet_cards_lf is not None and "pack" in schema_names:
        pack_part = (
            exploded.filter(pl.col("pack").is_not_null()).select("product_uuid", "pack").explode("pack").unnest("pack")
        )
        with_cards = pack_part.join(
            booster_sheet_cards_lf,
            left_on=["set", "code"],
            right_on=["setCode", "boosterType"],
            how="inner",
        )
        pack_resolved = with_cards.select(
            pl.col("cardUuid").alias("card_uuid"),
            pl.col("product_uuid"),
            pl.when(pl.col("foil") == True).then(pl.lit("foil")).otherwise(pl.lit("nonfoil")).alias("finish"),
        )
        frames.append(pack_resolved)

    # --- deck sub-type ---
    if deck_cards_lf is not None and "deck" in schema_names:
        deck_part = (
            exploded.filter(pl.col("deck").is_not_null()).select("product_uuid", "deck").explode("deck").unnest("deck")
        )
        with_cards = deck_part.join(
            deck_cards_lf,
            left_on=["name", "set"],
            right_on=["deckName", "setCode"],
            how="inner",
        )
        deck_resolved = with_cards.select(
            pl.col("cardUuid").alias("card_uuid"),
            pl.col("product_uuid"),
            pl.when(pl.col("isEtched") == True)
            .then(pl.lit("etched"))
            .when(pl.col("isFoil") == True)
            .then(pl.lit("foil"))
            .otherwise(pl.lit("nonfoil"))
            .alias("finish"),
        )
        frames.append(deck_resolved)

    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


def _aggregate_by_finish(resolved: pl.LazyFrame) -> pl.LazyFrame:
    """Group resolved cards by UUID, aggregate product UUIDs by finish."""
    return (
        resolved.group_by("card_uuid")
        .agg(
            pl.col("product_uuid").filter(pl.col("finish") == "foil").unique().alias("foil"),
            pl.col("product_uuid").filter(pl.col("finish") == "nonfoil").unique().alias("nonfoil"),
            pl.col("product_uuid").filter(pl.col("finish") == "etched").unique().alias("etched"),
        )
        .rename({"card_uuid": "uuid"})
    )


def build_card_to_products_lf(ctx: PipelineContext) -> pl.LazyFrame | None:
    """Build card-to-products mapping from sealed contents.

    Resolves all content types to card UUIDs.
    """
    contents_lf = ctx.sealed_contents_lf
    products_lf = ctx.sealed_products_lf
    if contents_lf is None or products_lf is None:
        LOGGER.warning("card_to_products: sealed data not available")
        return None

    # Product UUID lookup: (setCode, productName) → product_uuid
    product_uuids = products_lf.select(
        "setCode",
        "productName",
        _uuid5_expr("productName").alias("product_uuid"),
    )

    resolved_frames: list[pl.LazyFrame] = []

    # Direct card references
    card_resolved = _resolve_card_contents(contents_lf, product_uuids)
    if card_resolved is not None:
        resolved_frames.append(card_resolved)

    # Packs → booster sheet cards
    booster_cards = ctx.booster_sheet_cards_lf
    if booster_cards is not None:
        pack_resolved = _resolve_pack_contents(contents_lf, product_uuids, booster_cards)
        if pack_resolved is not None:
            resolved_frames.append(pack_resolved)

    # Decks → deck card lists
    deck_cards = ctx.deck_cards_lf
    if deck_cards is not None:
        deck_resolved = _resolve_deck_contents(contents_lf, product_uuids, deck_cards)
        if deck_resolved is not None:
            resolved_frames.append(deck_resolved)

    # Variable → explode configs and resolve sub-types
    variable_resolved = _resolve_variable_contents(contents_lf, product_uuids, booster_cards, deck_cards)
    if variable_resolved is not None:
        resolved_frames.append(variable_resolved)

    if not resolved_frames:
        LOGGER.warning("card_to_products: no content resolved")
        return None

    all_resolved = pl.concat(resolved_frames, how="diagonal_relaxed").unique()

    # Sealed → iterative resolution (needs base resolved data)
    sealed_resolved = _resolve_sealed_contents(contents_lf, product_uuids, all_resolved)
    if sealed_resolved is not None:
        all_resolved = pl.concat([all_resolved, sealed_resolved], how="diagonal_relaxed").unique()

    result = _aggregate_by_finish(all_resolved)
    count = result.select(pl.len()).collect().item()
    LOGGER.info(f"card_to_products: resolved {count:,} card-to-product mappings")
    return result
