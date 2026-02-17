"""
Standalone builders for decks, sealed products, and set metadata.

These are separate entry points called by AssemblyContext, not by build_cards().
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import polars as pl
import polars_hash as plh

from mtgjson5 import constants
from mtgjson5.utils import LOGGER
from mtgjson5.v2.data import PipelineContext
from mtgjson5.v2.pipeline.stages.explode import _uuid5_expr


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

    exploded = decks.select(["_deck_id", col]).explode(col).filter(pl.col(col).is_not_null())

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
    result = all_deck_ids.join(result, on="_deck_id", how="left").with_columns(pl.col(col).fill_null([]))

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
        LOGGER.info(f"Loaded {len(cards_df):,} cards/tokens for deck expansion (filtered)")

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


def build_sealed_products_lf(ctx: PipelineContext, _set_code: str | None = None) -> pl.LazyFrame:  # pylint: disable=no-member
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
            result.with_columns(pl.col("identifiers").struct.field("cardKingdomId").alias("_ck_join_id"))
            .join(
                ck_sealed_urls_lf,
                left_on="_ck_join_id",
                right_on="_ck_id",
                how="left",
            )
            .drop("_ck_join_id")
        )

    base_url = "https://mtgjson.com/links/"

    purchase_url_fields: list[Any] = []
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
                        pl.col("identifiers").struct.field("cardKingdomId").is_not_null()
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
                    pl.when(pl.col("identifiers").struct.field("tcgplayerProductId").is_not_null())
                    .then(pl.lit(base_url) + pl.col("_tcg_hash"))
                    .otherwise(None)
                    .alias("tcgplayer")
                )

    if purchase_url_fields:
        result = result.with_columns(pl.struct(purchase_url_fields).alias("purchaseUrls"))
        if hash_cols_added:
            result = result.drop(hash_cols_added, strict=False)
    else:
        result = result.with_columns(pl.struct([]).alias("purchaseUrls"))

    select_cols: list[Any] = [
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
            pl.DataFrame({"setCode": [], "config": []}).cast({"setCode": pl.String, "config": pl.String}).lazy()
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
    base_exprs: list[Any] = [
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
        base_exprs.append(pl.col("parentSetCode").str.to_uppercase().alias("parentCode"))
    if "block" in available_cols:
        base_exprs.append(pl.col("block"))

    if "cardCount" in available_cols:
        base_exprs.append(pl.col("cardCount").alias("totalSetSize"))
    if "printedSize" in available_cols:
        base_exprs.append(pl.col("printedSize").alias("baseSetSize"))
    elif "cardCount" in available_cols:
        base_exprs.append(pl.col("cardCount").alias("baseSetSize"))

    if "iconSvgUri" in available_cols:
        base_exprs.append(pl.col("iconSvgUri").str.extract(r"/([^/]+)\.svg", 1).str.to_uppercase().alias("keyruneCode"))

    if "tokenSetCode" in available_cols:
        base_exprs.append(pl.col("tokenSetCode").alias("tokenSetCode"))

    set_meta = sets_lf.with_columns(base_exprs)

    set_meta_cols = set_meta.collect_schema().names()
    cols_to_drop = [c for c in set_meta_cols if c in scryfall_only_fields or c.lower() in scryfall_only_fields]
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

        record["isForeignOnly"] = True if record.get("code", "") in constants.FOREIGN_SETS else None

        release_date = record.get("releaseDate")
        if release_date:
            build_date = date.today().isoformat()
            record["isPartialPreview"] = build_date < release_date if build_date < release_date else None
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
                    "isForeignOnly": (True if code_upper in constants.FOREIGN_SETS else None),
                    "parentCode": (
                        set_data.get("parent_set_code", "").upper() if set_data.get("parent_set_code") else None
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
