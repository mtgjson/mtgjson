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

    Sets like FIN have multiple variants of the same meld triplet with different
    collector numbers (e.g., 99a/100a/99b, 282a/283a/282b). We use a two-pass
    approach:
    1. First try collector number proximity matching (numbers within 1)
    2. Fall back to name-only matching for sets without variants

    Args:
        lf: LazyFrame with cardParts, faceName, uuid, setCode, number, language,
            otherFaceIds columns

    Returns:
        LazyFrame with meld otherFaceIds updated
    """
    # Get all meld cards with their role (front vs result)
    # cardParts[2] is always the meld result name
    # Extract numeric base from collector number for variant matching
    meld_cards = (
        lf.filter(pl.col("cardParts").is_not_null())
        .select(
            [
                "setCode",
                "language",
                "number",
                "faceName",
                "uuid",
                pl.col("cardParts").list.get(0).alias("_front1_name"),
                pl.col("cardParts").list.get(1).alias("_front2_name"),
                pl.col("cardParts").list.get(2).alias("_result_name"),
            ]
        )
        .with_columns(
            [
                (pl.col("faceName") == pl.col("_result_name")).alias("_is_result"),
                # Extract numeric base from collector number (e.g., "99a" -> 99)
                pl.col("number").str.extract(r"^(\d+)", 1).cast(pl.Int32).alias("_num_base"),
            ]
        )
    )

    # Split into front and result cards
    front_cards = meld_cards.filter(~pl.col("_is_result"))
    result_cards = meld_cards.filter(pl.col("_is_result"))

    # For FRONT faces: find result card with matching name AND adjacent number base
    front_other_ids_strict = (
        front_cards.join(
            result_cards.select(
                [
                    "setCode",
                    "language",
                    pl.col("faceName").alias("_result_faceName"),
                    pl.col("uuid").alias("_result_uuid"),
                    pl.col("_num_base").alias("_result_num_base"),
                ]
            ),
            on=["setCode", "language"],
            how="left",
        )
        # Filter: result name matches AND number bases are close (within 1)
        .filter(
            (pl.col("_result_faceName") == pl.col("_result_name"))
            & ((pl.col("_num_base") - pl.col("_result_num_base")).abs() <= 1)
        )
        .group_by(["setCode", "language", "faceName", "_num_base"])
        .agg(pl.col("_result_uuid").first().alias("_meld_other_uuid"))
        .with_columns(
            pl.when(pl.col("_meld_other_uuid").is_not_null())
            .then(pl.concat_list([pl.col("_meld_other_uuid")]))
            .otherwise(pl.lit(None))
            .alias("_meld_other_uuids")
        )
        .select(["setCode", "language", "faceName", "_num_base", "_meld_other_uuids"])
    )

    # For RESULT cards: find front cards with matching names AND adjacent number bases
    result_other_ids_strict = (
        result_cards.join(
            front_cards.select(
                [
                    "setCode",
                    "language",
                    pl.col("faceName").alias("_front_faceName"),
                    pl.col("uuid").alias("_front_uuid"),
                    pl.col("_num_base").alias("_front_num_base"),
                    "_front1_name",
                    "_front2_name",
                ]
            ),
            on=["setCode", "language"],
            how="left",
        )
        .filter(
            (
                (pl.col("_front_faceName") == pl.col("_front1_name"))
                | (pl.col("_front_faceName") == pl.col("_front2_name"))
            )
            & ((pl.col("_num_base") - pl.col("_front_num_base")).abs() <= 1)
        )
        .group_by(["setCode", "language", "faceName", "_num_base"])
        .agg(pl.col("_front_uuid").unique().alias("_meld_other_uuids"))
        .select(["setCode", "language", "faceName", "_num_base", "_meld_other_uuids"])
    )

    # For FRONT faces: find ANY result card with matching name in the set
    front_other_ids_loose = (
        front_cards.join(
            result_cards.select(
                [
                    "setCode",
                    "language",
                    pl.col("faceName").alias("_result_faceName"),
                    pl.col("uuid").alias("_result_uuid"),
                ]
            ),
            on=["setCode", "language"],
            how="left",
        )
        .filter(pl.col("_result_faceName") == pl.col("_result_name"))
        .group_by(["setCode", "language", "faceName", "_num_base"])
        .agg(pl.col("_result_uuid").first().alias("_meld_other_uuid_loose"))
        .with_columns(
            pl.when(pl.col("_meld_other_uuid_loose").is_not_null())
            .then(pl.concat_list([pl.col("_meld_other_uuid_loose")]))
            .otherwise(pl.lit(None))
            .alias("_meld_other_uuids_loose")
        )
        .select(["setCode", "language", "faceName", "_num_base", "_meld_other_uuids_loose"])
    )

    # For RESULT cards: find ANY front cards with matching names in the set
    result_other_ids_loose = (
        result_cards.join(
            front_cards.select(
                [
                    "setCode",
                    "language",
                    pl.col("faceName").alias("_front_faceName"),
                    pl.col("uuid").alias("_front_uuid"),
                    "_front1_name",
                    "_front2_name",
                ]
            ),
            on=["setCode", "language"],
            how="left",
        )
        .filter(
            (pl.col("_front_faceName") == pl.col("_front1_name"))
            | (pl.col("_front_faceName") == pl.col("_front2_name"))
        )
        .group_by(["setCode", "language", "faceName", "_num_base"])
        .agg(pl.col("_front_uuid").unique().alias("_meld_other_uuids_loose"))
        .select(["setCode", "language", "faceName", "_num_base", "_meld_other_uuids_loose"])
    )

    # Combine strict matches
    all_strict = pl.concat([front_other_ids_strict, result_other_ids_strict])

    # Combine loose matches
    all_loose = pl.concat([front_other_ids_loose, result_other_ids_loose])

    # Add number base to main frame for joining
    lf_with_base = lf.with_columns(pl.col("number").str.extract(r"^(\d+)", 1).cast(pl.Int32).alias("_num_base"))

    # Join both strict and loose, prefer strict when available
    return (
        lf_with_base.join(
            all_strict, on=["setCode", "language", "faceName", "_num_base"], how="left"
        )
        .join(all_loose, on=["setCode", "language", "faceName", "_num_base"], how="left")
        .with_columns(
            # Prefer strict match, but fall back to loose when strict is incomplete.
            # Bug (#1449): strict can find only 1 of 2 front faces for a meld result
            # when the fronts have non-adjacent collector numbers. In that case the
            # strict list is non-null but incomplete, and coalesce would shadow the
            # complete loose match.  We detect incompleteness by checking whether
            # strict found fewer IDs than loose — but only override strict when strict
            # has fewer than 2 results, so variant sets (FIN) where strict correctly
            # finds 2 per variant aren't broken by a larger loose result.
            pl.when(
                pl.col("_meld_other_uuids").is_not_null()
                & ~(
                    (pl.col("_meld_other_uuids").list.len() < pl.lit(2))
                    & (
                        pl.col("_meld_other_uuids").list.len()
                        < pl.col("_meld_other_uuids_loose").list.len().fill_null(0)
                    )
                )
            )
            .then(pl.col("_meld_other_uuids"))
            .when(pl.col("_meld_other_uuids_loose").is_not_null())
            .then(pl.col("_meld_other_uuids_loose"))
            .otherwise(pl.col("otherFaceIds"))
            .alias("otherFaceIds")
        )
        .drop(["_meld_other_uuids", "_meld_other_uuids_loose", "_num_base"])
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
