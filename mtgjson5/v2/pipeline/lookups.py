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

    Meld otherFaceIds rules:
    - Front faces (side a): otherFaceIds points only to the meld result (side b)
    - Meld result (side b): otherFaceIds points to both front faces (side a)

    cardParts format: [front1_name, front2_name, meld_result_name]
    The 3rd element (index 2) is always the meld result.

    Args:
        lf: LazyFrame with cardParts, faceName, uuid, setCode, otherFaceIds columns

    Returns:
        LazyFrame with meld otherFaceIds updated
    """
    # Get all meld cards with their role (front vs result)
    # cardParts[2] is always the meld result name
    meld_cards = (
        lf.filter(pl.col("cardParts").is_not_null())
        .select([
            "setCode",
            "faceName",
            "uuid",
            pl.col("cardParts").list.get(0).alias("_front1_name"),
            pl.col("cardParts").list.get(1).alias("_front2_name"),
            pl.col("cardParts").list.get(2).alias("_result_name"),
        ])
        .with_columns(
            (pl.col("faceName") == pl.col("_result_name")).alias("_is_result")
        )
    )

    # Build name->uuid lookup for all meld cards (unique by setCode + faceName)
    name_to_uuid = (
        meld_cards
        .select(["setCode", "faceName", "uuid"])
        .unique(subset=["setCode", "faceName"])
    )

    # For FRONT faces: otherFaceIds = [result_uuid]
    front_other_ids = (
        meld_cards
        .filter(~pl.col("_is_result"))
        .select(["setCode", "faceName", "_result_name"])
        .unique()
        .join(
            name_to_uuid.rename({"faceName": "_result_name", "uuid": "_result_uuid"}),
            on=["setCode", "_result_name"],
            how="left",
        )
        .group_by(["setCode", "faceName"])
        .agg(pl.col("_result_uuid").drop_nulls().unique().alias("_meld_other_uuids"))
    )

    # For RESULT: otherFaceIds = [front1_uuid, front2_uuid]
    result_other_ids = (
        meld_cards
        .filter(pl.col("_is_result"))
        .select(["setCode", "faceName", "_front1_name", "_front2_name"])
        .unique()
        # Join to get front1 uuid
        .join(
            name_to_uuid.rename({"faceName": "_front1_name", "uuid": "_front1_uuid"}),
            on=["setCode", "_front1_name"],
            how="left",
        )
        # Join to get front2 uuid
        .join(
            name_to_uuid.rename({"faceName": "_front2_name", "uuid": "_front2_uuid"}),
            on=["setCode", "_front2_name"],
            how="left",
        )
        .with_columns(
            pl.concat_list(["_front1_uuid", "_front2_uuid"])
            .list.drop_nulls()
            .list.unique()
            .alias("_meld_other_uuids")
        )
        .select(["setCode", "faceName", "_meld_other_uuids"])
    )

    # Combine front and result lookups
    all_meld_other_ids = pl.concat([front_other_ids, result_other_ids])

    # Join back and update otherFaceIds for meld cards
    return (
        lf.join(all_meld_other_ids, on=["setCode", "faceName"], how="left")
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
