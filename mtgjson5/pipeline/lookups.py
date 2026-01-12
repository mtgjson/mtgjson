"""
Lazy lookup operations for pipeline.

Provides functions that use existing pipeline data (like cardParts from name_lf)
to build lookups via lazy joins, eliminating mid-pipeline .collect() calls.
"""

from __future__ import annotations

import polars as pl


def add_meld_other_face_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add otherFaceIds for meld cards using cardParts column.

    Uses lazy self-join pattern to find UUIDs of related meld cards
    without any mid-pipeline collects.

    Assumes cardParts column exists (from join_name_data).

    Args:
        lf: LazyFrame with cardParts, name, uuid, setCode, otherFaceIds columns

    Returns:
        LazyFrame with meld otherFaceIds updated
    """
    # Cards with cardParts are meld results - cardParts lists all 3 names in triplet
    # For each card in a meld triplet, we need UUIDs of the other 2 cards

    # Step 1: Explode cardParts to get (setCode, result_name, meld_name) rows
    # This gives us all meld card names associated with each result
    meld_parts = (
        lf.filter(pl.col("cardParts").is_not_null())
        .select(["setCode", pl.col("name").alias("_result_name"), "cardParts"])
        .explode("cardParts")
        .rename({"cardParts": "_meld_name"})
    )

    # Step 2: Get name->uuid lookup for all meld cards via semi-join
    # (cards whose name appears in any cardParts list)
    meld_uuids = (
        lf.join(
            meld_parts.select(["setCode", "_meld_name"]).unique(),
            left_on=["setCode", "name"],
            right_on=["setCode", "_meld_name"],
            how="semi",
        )
        .select(["setCode", "name", "uuid"])
    )

    # Step 3: For each meld card, find its triplet via the result, then get other UUIDs
    # card -> (find which result contains this card) -> (get all parts of that result) -> (exclude self)
    card_to_triplet = (
        meld_uuids
        # Find the result that contains this card
        .join(
            meld_parts,
            left_on=["setCode", "name"],
            right_on=["setCode", "_meld_name"],
            how="inner",
        )
        .select(["setCode", "name", "_result_name"])
        # Get all cards in that result's triplet
        .join(
            meld_parts.rename({"_meld_name": "_other_name"}),
            on=["setCode", "_result_name"],
            how="inner",
        )
        # Exclude self
        .filter(pl.col("name") != pl.col("_other_name"))
        # Get UUIDs for the other cards
        .join(
            meld_uuids.rename({"name": "_other_name", "uuid": "_other_uuid"}),
            on=["setCode", "_other_name"],
            how="left",
        )
        # Aggregate other UUIDs per card
        .group_by(["setCode", "name"])
        .agg(pl.col("_other_uuid").drop_nulls().alias("_meld_other_uuids"))
    )

    # Step 4: Join back and update otherFaceIds for meld cards
    return (
        lf.join(card_to_triplet, on=["setCode", "name"], how="left")
        .with_columns(
            pl.when(pl.col("_meld_other_uuids").is_not_null())
            .then(pl.col("_meld_other_uuids"))
            .otherwise(pl.col("otherFaceIds"))
            .alias("otherFaceIds")
        )
        .drop("_meld_other_uuids")
    )


def apply_meld_overrides(lf: pl.LazyFrame, meld_overrides: dict) -> pl.LazyFrame:
    """
    Apply meld card overrides from resource file.

    Uses pre-computed UUID -> {otherFaceIds, cardParts} mappings to fix meld cards.

    Args:
        lf: LazyFrame with uuid, otherFaceIds, cardParts columns
        meld_overrides: Dict of uuid -> {otherFaceIds: [...], cardParts?: [...]}

    Returns:
        LazyFrame with meld otherFaceIds and cardParts fixed
    """
    if not meld_overrides:
        return lf

    # Build lookup DataFrames
    other_face_rows = [
        {"uuid": uuid, "_meld_otherFaceIds": data["otherFaceIds"]}
        for uuid, data in meld_overrides.items()
        if data.get("otherFaceIds")
    ]
    card_parts_rows = [
        {"uuid": uuid, "_meld_cardParts": data["cardParts"]}
        for uuid, data in meld_overrides.items()
        if data.get("cardParts")
    ]

    # Apply otherFaceIds overrides
    if other_face_rows:
        other_face_lf = pl.LazyFrame(other_face_rows)
        lf = (
            lf.join(other_face_lf, on="uuid", how="left")
            .with_columns(
                pl.when(pl.col("_meld_otherFaceIds").is_not_null())
                .then(pl.col("_meld_otherFaceIds"))
                .otherwise(pl.col("otherFaceIds"))
                .alias("otherFaceIds")
            )
            .drop("_meld_otherFaceIds")
        )

    # Apply cardParts overrides
    if card_parts_rows:
        card_parts_lf = pl.LazyFrame(card_parts_rows)
        lf = (
            lf.join(card_parts_lf, on="uuid", how="left")
            .with_columns(
                pl.when(pl.col("_meld_cardParts").is_not_null())
                .then(pl.col("_meld_cardParts"))
                .otherwise(pl.col("cardParts"))
                .alias("cardParts")
            )
            .drop("_meld_cardParts")
        )

    return lf


__all__ = [
    "add_meld_other_face_ids",
    "apply_meld_overrides",
]
