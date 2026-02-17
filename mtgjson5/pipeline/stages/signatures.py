"""
Signature handling and related cards.

Joins signature data and builds relatedCards struct.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.consts import TOKEN_LAYOUTS
from mtgjson5.data import PipelineContext


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
    lf = lf.with_columns(pl.col("number").str.extract(r"^([^0-9]+)", 1).alias("_num_prefix"))

    lf = lf.join(
        ctx.signatures_lf,
        left_on=["setCode", "_num_prefix"],
        right_on=["setCode", "numberPrefix"],
        how="left",
    )

    # Rename to avoid conflict with final signature column
    return lf.rename({"signature": "_wc_signature"}).drop("_num_prefix", strict=False)


def add_signatures_combined(
    lf: pl.LazyFrame,
    _ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Combine signature logic for art series and memorabilia cards.
    """
    is_art_series = pl.col("setName").str.ends_with("Art Series") & (pl.col("setCode") != "MH1")
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
        pl.when(pl.col("signature").is_not_null() & ~pl.col("finishes").list.contains("signed"))
        .then(pl.col("finishes").list.concat(pl.lit(["signed"])))
        .otherwise(pl.col("finishes"))
        .alias("finishes")
    )

    # Cleanup temp columns
    return lf.drop(["_num_digits", "_num_suffix", "_wc_signature"], strict=False)


def add_related_cards_from_context(
    lf: pl.LazyFrame,
    _ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build relatedCards struct using pre-joined spellbook data.
    """
    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS) | (pl.col("type") == "Dungeon") | pl.col("type").str.contains("Token")
    )

    # Alchemy spellbook check (applies to both tokens and non-tokens)
    is_alchemy = pl.col("setType").str.to_lowercase().str.contains("alchemy")
    has_spellbook = is_alchemy & pl.col("_spellbook_list").is_not_null() & (pl.col("_spellbook_list").list.len() > 0)

    # Tokens get reverseRelated
    has_reverse = pl.col("reverseRelated").is_not_null() & (pl.col("reverseRelated").list.len() > 0)

    # Non-tokens get token UUIDs
    has_tokens = pl.col("_token_uuids").is_not_null() & (pl.col("_token_uuids").list.len() > 0)

    # Build struct based on what data is present
    # For tokens: include reverseRelated and spellbook
    # For non-tokens: include spellbook (if alchemy) and tokens (if any)
    return lf.with_columns(
        pl.when(is_token & (has_spellbook | has_reverse))
        .then(
            pl.struct(
                spellbook=pl.col("_spellbook_list"),
                reverseRelated=pl.col("reverseRelated"),
                tokens=pl.lit(None).cast(pl.List(pl.String)),
            )
        )
        .when(~is_token & (has_spellbook | has_tokens))
        .then(
            pl.struct(
                spellbook=pl.col("_spellbook_list"),
                reverseRelated=pl.lit(None).cast(pl.List(pl.String)),
                tokens=pl.col("_token_uuids"),
            )
        )
        .otherwise(
            pl.lit(None).cast(
                pl.Struct(
                    {
                        "spellbook": pl.List(pl.String),
                        "reverseRelated": pl.List(pl.String),
                        "tokens": pl.List(pl.String),
                    }
                )
            )
        )
        .alias("relatedCards")
    ).drop(["_spellbook_list", "_token_uuids"], strict=False)
