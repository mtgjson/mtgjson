"""
Derived fields, flags, purchase URLs, and enrichment.

Adds boolean flags, purchase URL structs, manual overrides,
rebalanced linkage, foil/nonfoil versions, and special cases.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import polars_hash as plh

from mtgjson5.consts import BASIC_LAND_NAMES, CARD_MARKET_BUFFER
from mtgjson5.data import PipelineContext


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
    Add isFunny flag based on setType and special cases.
    """
    categoricals = ctx.categoricals
    if categoricals is None or "funny" not in categoricals.set_types:
        return lf.with_columns(pl.lit(None).cast(pl.Boolean).alias("isFunny"))

    return lf.with_columns(
        pl.when(pl.col("setType") != "funny")
        .then(pl.lit(None))
        .when(pl.col("setCode") == "UNF")
        .then(pl.when(pl.col("securityStamp") == "acorn").then(pl.lit(True)).otherwise(pl.lit(None)))
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
    tcga_id = pl.col("identifiers").struct.field("tcgplayerAlternativeFoilId")

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
                plh.concat_str(  # pylint: disable=no-member
                    [tcga_id.cast(pl.String), pl.col("uuid")]
                )
                .chash.sha2_256()
                .str.slice(0, 16)
                .alias("_tcga_hash"),
                # Cardmarket: hash(mcm_id + uuid + BUFFER + mcm_meta_id)
                plh.concat_str(  # pylint: disable=no-member
                    [
                        mcm_id.cast(pl.String),
                        pl.col("uuid"),
                        pl.lit(CARD_MARKET_BUFFER),
                        pl.col("identifiers").struct.field("mcmMetaId").cast(pl.String).fill_null(""),
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
                    pl.when(tcga_id.is_not_null())
                    .then(pl.lit(redirect_base) + pl.col("_tcga_hash"))
                    .otherwise(None)
                    .alias("tcgplayerAlternativeFoil"),
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
                "_tcga_hash",
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
def add_rebalanced_linkage(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.
    """
    is_rebalanced = pl.col("name").str.starts_with("A-") | pl.col("promoTypes").list.contains("rebalanced")

    # Add isRebalanced boolean (True for rebalanced, null otherwise)
    lf = lf.with_columns(
        pl.when(is_rebalanced).then(pl.lit(True)).otherwise(pl.lit(None).cast(pl.Boolean)).alias("isRebalanced")
    )

    is_rebalanced = pl.col("name").str.starts_with("A-")
    # Handle multi-face rebalanced cards: "A-Front // A-Back" -> "Front // Back"
    original_name_expr = pl.col("name").str.replace_all("A-", "")

    # Filter to default language only for UUID aggregation
    # This ensures we only link UUIDs that will exist in final output
    default_langs = ctx.languages_lf
    if default_langs is not None:
        default_lang_lf = lf.join(default_langs, on=["scryfallId", "language"], how="semi")
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
                pl.col("number").str.extract(r"(\d+)").cast(pl.Int64).alias("_number_int"),
                pl.col("side").fill_null(""),  # Include side for ordering
            ]
        )
        .join(
            rebalanced_map.select(["setCode", "_original_name"]).unique(),
            on=["setCode", "_original_name"],
            how="semi",  # Only keep names that have a rebalanced version in same set
        )
        .sort(["setCode", "_original_name", "_number_int", "number", "side"])  # Sort by numeric, string, side
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
        ill_id = row["identifiers"].get("scryfallIllustrationId") if row["identifiers"] else None
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
        return lf.with_columns(pl.lit(None).cast(source_products_struct).alias("sourceProducts"))

    # card_to_products_df can be LazyFrame (from cache) or DataFrame
    products_lf = card_to_products_df if isinstance(card_to_products_df, pl.LazyFrame) else card_to_products_df.lazy()

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


def calculate_duel_deck(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate duelDeck field for Duel Deck sets (DD* and GS1).

    The algorithm detects deck boundaries by looking for basic lands:
    - All cards start in deck 'a'
    - When we see a basic land followed by a non-basic land, switch to deck 'b'
    - Tokens and emblems are skipped (no duelDeck)
    """
    is_duel_deck_set = pl.col("setCode").str.starts_with("DD") | (pl.col("setCode") == "GS1")
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
            pl.col("type").str.contains("Token|Emblem").fill_null(False).alias("_is_token"),
            # Extract numeric part of collector number for sorting
            pl.col("number").str.extract(r"^(\d+)", 1).cast(pl.Int32).alias("_num_sort"),
        )
        .sort(["setCode", "_num_sort", "number"])
    )

    # For each set, we need to detect the transition from land to non-land
    # Use a window function to check if previous card was a basic land
    dd_cards = (
        dd_cards.with_columns(
            pl.col("_is_basic_land").shift(1).over("setCode").fill_null(False).alias("_prev_was_land"),
        )
        .with_columns(
            # Transition occurs when previous was basic land and current is not basic land and not token
            (pl.col("_prev_was_land") & ~pl.col("_is_basic_land") & ~pl.col("_is_token")).alias("_is_transition"),
        )
        .with_columns(
            # Count transitions to get deck number
            pl.col("_is_transition").cum_sum().over("setCode").alias("_deck_num"),
        )
        .with_columns(
            # Convert deck number to letter, skip tokens
            pl.when(pl.col("_is_token"))
            .then(pl.lit(None).cast(pl.String))
            .otherwise(
                pl.col("_deck_num").replace_strict(
                    {i: chr(ord("a") + i) for i in range(26)},
                    return_dtype=pl.String,
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
        .with_columns(pl.coalesce(pl.col("_dd_calculated"), pl.col("duelDeck")).alias("duelDeck"))
        .drop("_dd_calculated")
    )

    return lf
