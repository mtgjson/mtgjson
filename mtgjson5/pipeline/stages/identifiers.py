"""
External data joins, identifier building, and UUID generation.

Joins provider data (Card Kingdom, Oracle, CardMarket, etc.),
builds the identifiers struct, and generates UUIDs.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.consts import LANGUAGE_MAP
from mtgjson5.data import PipelineContext
from mtgjson5.pipeline.stages.explode import _uuid5_concat_expr, _uuid5_expr


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
            tcgplayerAlternativeFoilId=pl.col("tcgplayerAlternativeFoilId"),
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
    lf = lf.with_columns(pl.col("identifiers").struct.field("multiverseId").alias("_mv_id_lookup"))

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
    lf = lf.with_columns(_uuid5_expr("_v4_source").alias("_mtgjsonV4Id")).drop("_v4_source")
    return lf.with_columns(
        pl.col("identifiers").struct.with_fields([pl.col("_mtgjsonV4Id").alias("mtgjsonV4Id")])
    ).drop("_mtgjsonV4Id")


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


def join_tcg_alt_foil_lookup(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Join alternative foil TCGPlayer product IDs by base tcgplayerId.
    """
    if ctx.tcg_alt_foil_lf is None:
        return lf.with_columns(
            pl.lit(None).cast(pl.String).alias("tcgplayerAlternativeFoilId"),
        )

    lf = (
        lf.with_columns(pl.col("tcgplayerId").cast(pl.String).alias("_tcg_id_str"))
        .join(ctx.tcg_alt_foil_lf, left_on="_tcg_id_str", right_on="tcgplayerProductId", how="left")
        .drop("_tcg_id_str")
    )
    return lf


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
            ctx.cards_lf.filter((pl.col("lang") != "en") & (pl.col("cardFaces").list.len() > 1))
            .select(["lang", "cardFaces", "set", "collectorNumber"])
            .collect()
            if isinstance(ctx.cards_lf, pl.LazyFrame)
            else ctx.cards_lf.filter((pl.col("lang") != "en") & (pl.col("cardFaces").list.len() > 1))
        )

        face_lookup = (
            cards_df.with_columns(
                [
                    pl.col("set").str.to_uppercase().alias("setCode"),
                    pl.col("lang").replace_strict(LANGUAGE_MAP, default=pl.col("lang")).alias("language"),
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
                    pl.col("cardFaces").struct.field("printed_type_line").alias("_type"),
                    pl.col("cardFaces").struct.field("flavor_text").alias("_flavorText"),
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
                pl.col("_side").replace_strict(side_to_index, default=0).cast(pl.Int64).alias("_face_index"),
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
                    pl.when((pl.col("_side") != "a") & pl.col("_has_face_data").fill_null(False))
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
                    pl.when((pl.col("_side") != "a") & pl.col("_has_face_data").fill_null(False))
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
                    pl.when((pl.col("_side") != "a") & pl.col("_has_face_data").fill_null(False))
                    .then(pl.col("_text"))
                    .when(pl.col("_side") != "a")
                    .then(pl.coalesce(pl.col("_text"), pl.col("foreignData").struct.field("text")))
                    .otherwise(pl.col("foreignData").struct.field("text"))
                    .alias("text"),
                    pl.when((pl.col("_side") != "a") & pl.col("_has_face_data").fill_null(False))
                    .then(pl.col("_type"))
                    .when(pl.col("_side") != "a")
                    .then(pl.coalesce(pl.col("_type"), pl.col("foreignData").struct.field("type")))
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

    # Deduplicate foreignData entries by foreign scryfallId before re-aggregating
    fd_processed = (
        fd_processed.with_columns(
            pl.col("foreignData").struct.field("identifiers").struct.field("scryfallId").alias("_fd_scryfall_id")
        )
        .unique(subset=["scryfallId", "setCode", "number", "_side_key", "_fd_scryfall_id"])
        .drop("_fd_scryfall_id")
    )

    # Re-aggregate by card+side, sorted by language
    fd_final = (
        fd_processed.group_by(["scryfallId", "setCode", "number", "_side_key"])
        .agg(pl.col("foreignData").alias("_foreignData_fixed"))
        .with_columns(
            pl.col("_foreignData_fixed").list.eval(pl.element().sort_by(pl.element().struct.field("language")))
        )
    )

    # Join back
    lf = lf.with_columns(pl.col("side").fill_null("").alias("_side_key"))
    lf = lf.join(fd_final, on=["scryfallId", "setCode", "number", "_side_key"], how="left")
    return lf.with_columns(pl.coalesce(pl.col("_foreignData_fixed"), pl.lit([])).alias("foreignData")).drop(
        ["_foreignData_fixed", "_side_key"]
    )


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
        lf = lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("_spellbook_list"))

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
        pl.coalesce(pl.col("_faceFlavorName"), pl.col("faceFlavorName")).alias("faceFlavorName"),
        pl.coalesce(pl.col("_flavorNameOverride"), pl.col("flavorName")).alias("flavorName"),
    ).drop(["_faceFlavorName", "_flavorNameOverride"], strict=False)


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
