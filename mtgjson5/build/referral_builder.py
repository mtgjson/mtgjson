"""
V2 Referral Map builder operations.

Builds ReferralMap.json for Nginx URL rewrites that redirect
/links/{hash} to actual purchase URLs with referral codes.
"""

# pylint: disable=no-member  # polars_hash adds .chash namespace dynamically

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig

if TYPE_CHECKING:
    from mtgjson5.data import PipelineContext

import polars_hash as plh

LOGGER = logging.getLogger(__name__)

# Provider URL bases and referral codes
CK_BASE = "https://www.cardkingdom.com/"
CK_REFERRAL = constants.CARD_KINGDOM_REFERRAL

# TCGPlayer affiliate link format (split for concatenation)
TCG_REFERRAL_PREFIX = (
    "https://partner.tcgplayer.com/c/4948039/1780961/21018?subId1=api&u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F"
)
TCG_REFERRAL_SUFFIX = "%3Fpage%3D1"


def build_referral_map_from_context(
    ctx: PipelineContext,
    parquet_dir: Path | None = None,
) -> pl.DataFrame:
    """
    Build referral map entries from pipeline context.

    Uses the identifiers_lf which has Card Kingdom URLs joined with scryfall IDs,
    and the parquet cache for TCGPlayer identifiers.

    Args:
        ctx: PipelineContext with identifier data
        parquet_dir: Path to parquet cache (for TCGPlayer entries)

    Returns:
        DataFrame with columns [hash, referral_url]
    """
    entries = []

    # Build Card Kingdom entries from identifiers_lf
    ck_entries = _build_ck_entries_from_identifiers(ctx)
    if ck_entries is not None and len(ck_entries) > 0:
        entries.append(ck_entries)
        LOGGER.info(f"  Card Kingdom: {len(ck_entries):,} entries")

    # Build TCGPlayer entries from parquet cache
    tcg_entries = _build_tcg_entries_from_parquet(parquet_dir)
    if tcg_entries is not None and len(tcg_entries) > 0:
        entries.append(tcg_entries)
        LOGGER.info(f"  TCGPlayer: {len(tcg_entries):,} entries")

    # Build Cardmarket entries
    cm_entries = _build_cardmarket_entries_from_parquet(ctx, parquet_dir)
    if cm_entries is not None and len(cm_entries) > 0:
        entries.append(cm_entries)
        LOGGER.info(f"  Cardmarket: {len(cm_entries):,} entries")

    if not entries:
        return pl.DataFrame({"hash": [], "referral_url": []})

    return pl.concat(entries).unique(subset=["hash"])


def _build_ck_entries_from_identifiers(ctx: PipelineContext) -> pl.DataFrame | None:
    """Build Card Kingdom referral entries from identifiers_lf."""
    if ctx.identifiers_lf is None:
        return None

    id_lf = ctx.identifiers_lf
    schema = id_lf.collect_schema()
    cols = set(schema.names())

    # Need cachedUuid (becomes uuid in pipeline) and CK URL columns
    required = {"cachedUuid", "cardKingdomUrl"}
    if not required.issubset(cols):
        LOGGER.debug(f"Missing CK columns, have: {cols}")
        return None

    entries = []

    # Regular Card Kingdom entries
    if "cardKingdomUrl" in cols:
        ck_df = (
            id_lf.select(["cachedUuid", "cardKingdomUrl"])
            .filter(pl.col("cardKingdomUrl").is_not_null() & pl.col("cachedUuid").is_not_null())
            .with_columns(
                [
                    # Hash: sha256(ck_base + url_path + uuid)[:16]
                    plh.concat_str(
                        [
                            pl.lit(CK_BASE),
                            pl.col("cardKingdomUrl"),
                            pl.col("cachedUuid"),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    # Referral: ck_base + url_path + ck_referral
                    plh.concat_str([pl.lit(CK_BASE), pl.col("cardKingdomUrl"), pl.lit(CK_REFERRAL)]).alias(
                        "referral_url"
                    ),
                ]
            )
            .select(["hash", "referral_url"])
            .collect()
        )
        if len(ck_df) > 0:
            entries.append(ck_df)

    # Foil Card Kingdom entries
    if "cardKingdomFoilUrl" in cols:
        ckf_df = (
            id_lf.select(["cachedUuid", "cardKingdomFoilUrl"])
            .filter(pl.col("cardKingdomFoilUrl").is_not_null() & pl.col("cachedUuid").is_not_null())
            .with_columns(
                [
                    plh.concat_str(
                        [
                            pl.lit(CK_BASE),
                            pl.col("cardKingdomFoilUrl"),
                            pl.col("cachedUuid"),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    plh.concat_str(
                        [
                            pl.lit(CK_BASE),
                            pl.col("cardKingdomFoilUrl"),
                            pl.lit(CK_REFERRAL),
                        ]
                    ).alias("referral_url"),
                ]
            )
            .select(["hash", "referral_url"])
            .collect()
        )
        if len(ckf_df) > 0:
            entries.append(ckf_df)

    # Etched Card Kingdom entries
    if "cardKingdomEtchedUrl" in cols:
        cke_df = (
            id_lf.select(["cachedUuid", "cardKingdomEtchedUrl"])
            .filter(pl.col("cardKingdomEtchedUrl").is_not_null() & pl.col("cachedUuid").is_not_null())
            .with_columns(
                [
                    plh.concat_str(
                        [
                            pl.lit(CK_BASE),
                            pl.col("cardKingdomEtchedUrl"),
                            pl.col("cachedUuid"),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    plh.concat_str(
                        [
                            pl.lit(CK_BASE),
                            pl.col("cardKingdomEtchedUrl"),
                            pl.lit(CK_REFERRAL),
                        ]
                    ).alias("referral_url"),
                ]
            )
            .select(["hash", "referral_url"])
            .collect()
        )
        if len(cke_df) > 0:
            entries.append(cke_df)

    if not entries:
        return None

    return pl.concat(entries)


def _build_tcg_entries_from_parquet(parquet_dir: Path | None) -> pl.DataFrame | None:
    """Build TCGPlayer referral entries from parquet cache.

    Uses the parquet cache which has the full identifiers struct.
    """
    if parquet_dir is None or not parquet_dir.exists():
        LOGGER.debug("No parquet cache available for TCGPlayer referrals")
        return None

    try:
        # Scan parquet files to get uuid and identifiers
        cards_lf = pl.scan_parquet(parquet_dir / "**/*.parquet")
        schema = cards_lf.collect_schema()
    except Exception as e:
        LOGGER.debug(f"Failed to scan parquet for TCGPlayer: {e}")
        return None

    if "identifiers" not in schema.names() or "uuid" not in schema.names():
        return None

    id_schema = schema.get("identifiers")
    if not isinstance(id_schema, pl.Struct):
        return None

    id_fields = {f.name for f in id_schema.fields}
    entries = []

    # TCGPlayer entries
    if "tcgplayerProductId" in id_fields:
        tcg_id = pl.col("identifiers").struct.field("tcgplayerProductId")
        tcg_df = (
            cards_lf.select(["uuid", "identifiers"])
            .filter(tcg_id.is_not_null())
            .with_columns(
                [
                    # Hash: sha256(tcg_id + uuid)[:16]
                    plh.concat_str([tcg_id.cast(pl.String), pl.col("uuid")])
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    tcg_id.cast(pl.String).alias("_tcg_id"),
                ]
            )
            .with_columns(
                plh.concat_str(
                    [
                        pl.lit(TCG_REFERRAL_PREFIX),
                        pl.col("_tcg_id"),
                        pl.lit(TCG_REFERRAL_SUFFIX),
                    ]
                ).alias("referral_url")
            )
            .select(["hash", "referral_url"])
            .unique(subset=["hash"])
            .collect()
        )
        if len(tcg_df) > 0:
            entries.append(tcg_df)

    # TCGPlayer etched entries
    if "tcgplayerEtchedProductId" in id_fields:
        tcge_id = pl.col("identifiers").struct.field("tcgplayerEtchedProductId")
        tcge_df = (
            cards_lf.select(["uuid", "identifiers"])
            .filter(tcge_id.is_not_null())
            .with_columns(
                [
                    plh.concat_str([tcge_id.cast(pl.String), pl.col("uuid")])
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    tcge_id.cast(pl.String).alias("_tcg_id"),
                ]
            )
            .with_columns(
                plh.concat_str(
                    [
                        pl.lit(TCG_REFERRAL_PREFIX),
                        pl.col("_tcg_id"),
                        pl.lit(TCG_REFERRAL_SUFFIX),
                    ]
                ).alias("referral_url")
            )
            .select(["hash", "referral_url"])
            .unique(subset=["hash"])
            .collect()
        )
        if len(tcge_df) > 0:
            entries.append(tcge_df)

    # TCGPlayer alternative foil entries
    if "tcgplayerAlternativeFoilProductId" in id_fields:
        tcga_id = pl.col("identifiers").struct.field("tcgplayerAlternativeFoilProductId")
        tcga_df = (
            cards_lf.select(["uuid", "identifiers"])
            .filter(tcga_id.is_not_null())
            .with_columns(
                [
                    plh.concat_str([tcga_id.cast(pl.String), pl.col("uuid")])
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    tcga_id.cast(pl.String).alias("_tcg_id"),
                ]
            )
            .with_columns(
                plh.concat_str(
                    [
                        pl.lit(TCG_REFERRAL_PREFIX),
                        pl.col("_tcg_id"),
                        pl.lit(TCG_REFERRAL_SUFFIX),
                    ]
                ).alias("referral_url")
            )
            .select(["hash", "referral_url"])
            .unique(subset=["hash"])
            .collect()
        )
        if len(tcga_df) > 0:
            entries.append(tcga_df)

    if not entries:
        return None

    return pl.concat(entries)


def _build_cardmarket_entries_from_parquet(
    ctx: PipelineContext,
    parquet_dir: Path | None,
) -> pl.DataFrame | None:
    """Build Cardmarket referral entries.

    Joins raw Scryfall data (for cardmarket URL) with parquet output
    (for uuid and mcmId/mcmMetaId) to build referral entries.
    """
    if ctx.cards_lf is None:
        LOGGER.debug("No cards_lf for Cardmarket referrals")
        return None

    if parquet_dir is None or not parquet_dir.exists():
        LOGGER.debug("No parquet cache for Cardmarket referrals")
        return None

    # Check if purchaseUris.cardmarket is available in raw scryfall data
    cards_schema = ctx.cards_lf.collect_schema()
    if "purchaseUris" not in cards_schema.names():
        LOGGER.debug("purchaseUris not in cards_lf schema")
        return None

    pu_schema = cards_schema.get("purchaseUris")
    if not isinstance(pu_schema, pl.Struct):
        return None

    pu_fields = {f.name for f in pu_schema.fields}
    if "cardmarket" not in pu_fields:
        LOGGER.debug("cardmarket not in purchaseUris fields")
        return None

    try:
        # Get cardmarket URLs from raw scryfall data
        cm_urls_lf = ctx.cards_lf.select(
            [
                pl.col("id").alias("_scryfall_id"),
                pl.col("purchaseUris").struct.field("cardmarket").alias("_cm_url"),
            ]
        ).filter(pl.col("_cm_url").is_not_null())

        # Get uuid and mcm IDs from parquet output
        cards_lf = pl.scan_parquet(parquet_dir / "**/*.parquet")
        parquet_schema = cards_lf.collect_schema()

        if "identifiers" not in parquet_schema.names() or "uuid" not in parquet_schema.names():
            return None

        id_schema = parquet_schema.get("identifiers")
        if not isinstance(id_schema, pl.Struct):
            return None

        id_fields = {f.name for f in id_schema.fields}
        if not {"mcmId", "mcmMetaId", "scryfallId"}.issubset(id_fields):
            LOGGER.debug("Missing mcmId/mcmMetaId/scryfallId in identifiers")
            return None

        # Extract needed fields from parquet
        mcm_data_lf = cards_lf.select(
            [
                pl.col("uuid"),
                pl.col("identifiers").struct.field("scryfallId").alias("_scryfall_id"),
                pl.col("identifiers").struct.field("mcmId"),
                pl.col("identifiers").struct.field("mcmMetaId"),
            ]
        ).filter(pl.col("mcmId").is_not_null())

        # Join scryfall URLs with parquet data
        joined = (
            mcm_data_lf.join(cm_urls_lf, on="_scryfall_id", how="inner")
            .filter(pl.col("_cm_url").is_not_null())
            .with_columns(
                [
                    # Hash: sha256(mcm_id + uuid + BUFFER + mcm_meta_id)[:16]
                    plh.concat_str(
                        [
                            pl.col("mcmId").cast(pl.String),
                            pl.col("uuid"),
                            pl.lit(constants.CARD_MARKET_BUFFER),
                            pl.col("mcmMetaId").cast(pl.String).fill_null(""),
                        ]
                    )
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    # Replace referrer=scryfall and utm_source=scryfall with mtgjson
                    pl.col("_cm_url").str.replace_all("scryfall", "mtgjson").alias("referral_url"),
                ]
            )
            .select(["hash", "referral_url"])
            .unique(subset=["hash"])
            .collect()
        )

        if len(joined) > 0:
            return joined

    except Exception as e:
        LOGGER.warning(f"Failed to build Cardmarket referrals: {e}")

    return None


def build_referral_map_from_sealed(sealed_df: pl.DataFrame | None) -> pl.DataFrame:
    """
    Build referral map entries from sealed products DataFrame.

    Args:
        sealed_df: DataFrame with sealed product data

    Returns:
        DataFrame with columns [hash, referral_url]
    """
    if sealed_df is None or len(sealed_df) == 0:
        return pl.DataFrame({"hash": [], "referral_url": []})

    entries = []
    schema = sealed_df.collect_schema()
    cols = set(schema.names())

    # Card Kingdom entries for sealed products
    # Sealed products hash includes the referral string
    if "_ck_url" in cols and "uuid" in cols:
        ck_entries = (
            sealed_df.select(["uuid", "_ck_url"])
            .filter(pl.col("_ck_url").is_not_null())
            .with_columns(
                [
                    # Hash: sha256(ck_base + url + ck_referral)[:16]
                    plh.concat_str([pl.lit(CK_BASE), pl.col("_ck_url"), pl.lit(CK_REFERRAL)])
                    .chash.sha2_256()
                    .str.slice(0, 16)
                    .alias("hash"),
                    plh.concat_str([pl.lit(CK_BASE), pl.col("_ck_url"), pl.lit(CK_REFERRAL)]).alias("referral_url"),
                ]
            )
            .select(["hash", "referral_url"])
        )
        if len(ck_entries) > 0:
            entries.append(ck_entries)

    # TCGPlayer entries for sealed products
    if "identifiers" in cols:
        id_schema = schema.get("identifiers")
        if isinstance(id_schema, pl.Struct):
            id_fields = {f.name for f in id_schema.fields}
            if "tcgplayerProductId" in id_fields:
                tcg_id = pl.col("identifiers").struct.field("tcgplayerProductId")
                tcg_entries = (
                    sealed_df.select(["uuid", "identifiers"])
                    .filter(tcg_id.is_not_null())
                    .with_columns(
                        [
                            plh.concat_str([tcg_id.cast(pl.String), pl.col("uuid")])
                            .chash.sha2_256()
                            .str.slice(0, 16)
                            .alias("hash"),
                            tcg_id.cast(pl.String).alias("_tcg_id"),
                        ]
                    )
                    .with_columns(
                        plh.concat_str(
                            [
                                pl.lit(TCG_REFERRAL_PREFIX),
                                pl.col("_tcg_id"),
                                pl.lit(TCG_REFERRAL_SUFFIX),
                            ]
                        ).alias("referral_url")
                    )
                    .select(["hash", "referral_url"])
                )
                if len(tcg_entries) > 0:
                    entries.append(tcg_entries)

    if not entries:
        return pl.DataFrame({"hash": [], "referral_url": []})

    return pl.concat(entries).unique(subset=["hash"])


def write_referral_map(referral_df: pl.DataFrame, output_path: Path) -> None:
    """
    Write referral map to Nginx map format file.

    Args:
        referral_df: DataFrame with [hash, referral_url] columns
        output_path: Directory to write ReferralMap.json
    """
    output_path.mkdir(parents=True, exist_ok=True)
    map_path = output_path / "ReferralMap.json"

    # Sort by hash for consistent output
    sorted_df = referral_df.sort("hash")

    with open(map_path, "w", encoding="utf-8") as f:
        for row in sorted_df.iter_rows(named=True):
            # Nginx map format: /links/{hash}\t{url};
            f.write(f"/links/{row['hash']}\t{row['referral_url']};\n")

    LOGGER.info(f"Wrote ReferralMap.json ({len(sorted_df):,} entries)")


def build_and_write_referral_map(
    ctx: PipelineContext,
    parquet_dir: Path | None = None,
    sealed_df: pl.DataFrame | None = None,
    output_path: Path | None = None,
) -> int:
    """
    Build and write the complete referral map.

    Args:
        ctx: PipelineContext with identifiers data
        parquet_dir: Path to parquet cache (for TCGPlayer entries)
        sealed_df: Sealed products DataFrame
        output_path: Output directory (defaults to MtgjsonConfig output path)

    Returns:
        Number of entries written
    """
    if output_path is None:
        output_path = MtgjsonConfig().output_path

    entries = []

    LOGGER.info("Building referral map from pipeline context...")
    card_entries = build_referral_map_from_context(ctx, parquet_dir)
    if len(card_entries) > 0:
        entries.append(card_entries)

    if sealed_df is not None and len(sealed_df) > 0:
        LOGGER.info("Building referral map from sealed products...")
        sealed_entries = build_referral_map_from_sealed(sealed_df)
        if len(sealed_entries) > 0:
            entries.append(sealed_entries)
            LOGGER.info(f"  Sealed: {len(sealed_entries):,} entries")

    if not entries:
        LOGGER.warning("No referral map entries generated")
        return 0

    combined = pl.concat(entries).unique(subset=["hash"])
    write_referral_map(combined, output_path)
    return len(combined)
