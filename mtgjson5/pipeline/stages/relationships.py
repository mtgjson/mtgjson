"""
Cross-card relationships.

Links multi-face cards, computes variations, leadership skills,
reverse-related tokens, and token IDs.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.consts import BASIC_LAND_NAMES, TOKEN_LAYOUTS
from mtgjson5.data import PipelineContext
from mtgjson5.pipeline.lookups import add_meld_other_face_ids, apply_meld_overrides


def add_other_face_ids(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """
    Link multi-face cards via Scryfall ID or name (for meld cards).
    """
    face_links = (
        lf.select(["scryfallId", "uuid", "side"])
        .with_columns(pl.struct(["uuid", "side"]).alias("_face_struct"))
        .sort(["scryfallId", "side"])
        .group_by("scryfallId")
        .agg(pl.col("_face_struct").alias("_all_faces"))
    )

    def _filter_self_from_faces(row: dict) -> list[str]:
        """Filter out current uuid from faces list while preserving side order."""
        all_faces = row["all_faces"]
        self_uuid = row["self_uuid"]
        if all_faces is None:
            return []
        return [f["uuid"] for f in all_faces if f["uuid"] != self_uuid]

    lf = (
        lf.join(face_links, on="scryfallId", how="left")
        .with_columns(
            pl.struct(
                [
                    pl.col("uuid").alias("self_uuid"),
                    pl.col("_all_faces").alias("all_faces"),
                ]
            )
            .map_elements(_filter_self_from_faces, return_dtype=pl.List(pl.String))
            .alias("otherFaceIds")
        )
        .drop("_all_faces")
    )

    lf = add_meld_other_face_ids(lf)

    if ctx.meld_overrides:
        lf = apply_meld_overrides(lf, ctx.meld_overrides)

    is_token = pl.col("types").list.contains("Token")
    has_all_parts = pl.col("_all_parts").list.len() > 0
    face_name_has_double_slash = pl.col("faceName").fill_null("").str.contains("//")
    is_same_name_pattern = pl.col("name").str.split(" // ").list.unique().list.len() == 1
    all_parts_has_no_double_slash = (
        pl.col("_all_parts")  # pylint: disable=singleton-comparison
        .list.eval(pl.element().struct.field("name").str.contains("//").any())
        .list.first()
        .fill_null(False)
        == False
    )
    is_same_name_reversible_with_no_slash_parts = (
        (pl.col("layout") == "reversible_card") & has_all_parts & is_same_name_pattern & all_parts_has_no_double_slash
    )

    lf = lf.with_columns(
        pl.when((is_token & has_all_parts) | face_name_has_double_slash | is_same_name_reversible_with_no_slash_parts)
        .then(pl.lit(None).cast(pl.List(pl.String)))
        .otherwise(pl.col("otherFaceIds"))
        .alias("otherFaceIds")
    )

    return lf


# 4.1: add variations and isAlternative
def add_variations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add variations (other printings with same name/faceName in the same set)
    """
    lf = lf.with_columns(
        pl.col("name").str.split(" (").list.first().alias("_base_name"),
        pl.col("faceName").fill_null("").alias("_faceName_for_group"),
    )

    variation_groups = (
        lf.select(["setCode", "_base_name", "_faceName_for_group", "uuid"])
        .group_by(["setCode", "_base_name", "_faceName_for_group"])
        .agg(pl.col("uuid").alias("_group_uuids"))
    )

    lf = lf.join(
        variation_groups,
        on=["setCode", "_base_name", "_faceName_for_group"],
        how="left",
    )

    lf = lf.with_columns(
        pl.when(pl.col("_group_uuids").list.len() > 1)
        .then(pl.col("_group_uuids").list.set_difference(pl.concat_list(pl.col("uuid"))))
        .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        .alias("variations")
    )

    lf = lf.with_columns(
        pl.when(pl.col("otherFaceIds").is_not_null() & (pl.col("otherFaceIds").list.len() > 0))
        .then(pl.col("variations").list.set_difference(pl.col("otherFaceIds")))
        .otherwise(pl.col("variations"))
        .alias("variations")
    )

    uuid_to_number = lf.select(
        [
            "uuid",
            # Extract collector number digits for sorting:
            pl.coalesce(
                pl.col("number").str.extract(r"-(\d+)", 1),  # After dash (PLST)
                pl.col("number").str.extract(r"(\d+)", 1),  # First digits (regular)
            )
            .cast(pl.Int64)
            .fill_null(999999)
            .alias("_num_int"),
            pl.col("number").alias("_num_str"),
        ]
    ).unique(subset=["uuid"])

    # Explode variations to individual rows, join with number, sort within groups, re-aggregate
    variations_sorted = (
        lf.select(["uuid", "variations"])
        .unique(subset=["uuid"])
        .explode("variations")
        .filter(pl.col("variations").is_not_null())
        .join(
            uuid_to_number.rename(
                {
                    "uuid": "variations",
                    "_num_int": "_var_num_int",
                    "_num_str": "_var_num_str",
                }
            ),
            on="variations",
            how="left",
        )
        .group_by("uuid")
        .agg(pl.col("variations").sort_by(["_var_num_int", "_var_num_str"]).alias("_variations_sorted"))
    )

    # Join back and replace variations with sorted version
    lf = lf.join(variations_sorted, on="uuid", how="left")
    lf = lf.with_columns(pl.coalesce(pl.col("_variations_sorted"), pl.col("variations")).alias("variations")).drop(
        "_variations_sorted"
    )

    # Build the "printing key" that defines uniqueness within a set
    frame_effects_str = pl.col("frameEffects").list.sort().list.join(",").fill_null("")
    finishes_str = pl.col("finishes").list.sort().list.join(",").fill_null("")
    # Base key: name|border|frame|effects|side
    base_key = pl.concat_str(
        [
            pl.col("name"),
            pl.lit("|"),
            pl.col("borderColor").fill_null(""),
            pl.lit("|"),
            pl.col("frameVersion").fill_null(""),
            pl.lit("|"),
            frame_effects_str,
            pl.lit("|"),
            pl.col("side").fill_null(""),
        ]
    )

    printing_key = (
        pl.when(pl.col("setCode").is_in(["UNH", "10E"]))
        .then(pl.concat_str([base_key, pl.lit("|"), finishes_str]))
        .otherwise(base_key)
        .alias("_printing_key")
    )

    lf = lf.with_columns(printing_key)

    # Within each printing key, the card with the lowest collector number is "canonical"
    number_digits_expr = (
        pl.col("number").str.extract_all(r"\d").list.join("").str.replace(r"^$", "100000").cast(pl.Int64)
    )

    lf = lf.with_columns(number_digits_expr.alias("_number_digits"))

    # Use rank to find the canonical entry (lowest numeric value, then alphabetical tiebreaker)
    # The card with rank 1 within each group is canonical
    # Struct comparison is lexicographic: first by _number_digits, then by number string
    rank_expr = pl.struct("_number_digits", "number").rank("ordinal").over(["setCode", "_printing_key"])
    canonical_expr = rank_expr == 1

    lf = lf.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0) & (~pl.col("name").is_in(list(BASIC_LAND_NAMES))) & (~canonical_expr)
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )

    # Also set isAlternative=True for Alchemy rebalanced cards (A- prefix)
    lf = lf.with_columns(
        pl.when(pl.col("name").str.starts_with("A-"))
        .then(pl.lit(True))
        .otherwise(pl.col("isAlternative"))
        .alias("isAlternative")
    )

    # Cleanup temp columns
    return lf.drop(
        [
            "_base_name",
            "_faceName_for_group",
            "_group_uuids",
            "_printing_key",
            "_number_digits",
        ]
    )


# 4.2: add leadership skills
def add_leadership_skills_expr(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Determine if a card can be a commander/oathbreaker/brawl commander.

    Uses vectorized string operations instead of per-card checks.
    """
    # Override cards that can always be commander
    override_cards = ["Grist, the Hunger Tide"]

    is_legendary = pl.col("type").str.contains("Legendary")
    is_creature = pl.col("type").str.contains("Creature")
    is_vehicle_or_spacecraft = pl.col("type").str.contains("Vehicle|Spacecraft")
    has_power_toughness = pl.col("power").is_not_null() & pl.col("toughness").is_not_null()
    is_front_face = pl.col("side").is_null() | (pl.col("side") == "a")
    can_be_commander_text = pl.col("text").str.contains("can be your commander")
    is_override = pl.col("name").is_in(override_cards)

    is_commander_legal = (
        is_override
        | (is_legendary & (is_creature | (is_vehicle_or_spacecraft & has_power_toughness)) & is_front_face)
        | can_be_commander_text
    )

    # Oathbreaker legal = is a planeswalker
    is_oathbreaker_legal = pl.col("type").str.contains("Planeswalker")

    # Brawl legal = set is in Standard AND (commander or oathbreaker eligible)
    standard_sets = ctx.standard_legal_sets
    is_in_standard = pl.col("setCode").is_in(standard_sets or set())
    is_brawl_legal = is_in_standard & (is_commander_legal | is_oathbreaker_legal)

    return lf.with_columns(
        pl.when(is_commander_legal | is_oathbreaker_legal | is_brawl_legal)
        .then(
            pl.struct(
                brawl=is_brawl_legal,
                commander=is_commander_legal,
                oathbreaker=is_oathbreaker_legal,
            )
        )
        .otherwise(pl.lit(None))
        .alias("leadershipSkills")
    )


# 4.3: add reverseRelated
def add_reverse_related(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute reverseRelated for tokens from all_parts.
    """
    # Extract IDs and names from _all_parts into parallel lists
    with_extracted = lf.with_columns(
        pl.col("_all_parts").list.eval(pl.element().struct.field("id")).alias("_part_ids"),
        pl.col("_all_parts").list.eval(pl.element().struct.field("name")).alias("_part_names"),
    )

    # Explode, filter out self, aggregate names back
    exploded = (
        with_extracted.select(["uuid", "scryfallId", "_part_ids", "_part_names"])
        .explode(["_part_ids", "_part_names"])
        .filter(pl.col("_part_ids").is_not_null() & (pl.col("_part_ids") != pl.col("scryfallId")))
        .group_by("uuid")
        .agg(pl.col("_part_names").unique().sort().alias("reverseRelated"))
    )

    return (
        lf.join(exploded, on="uuid", how="left").with_columns(pl.col("reverseRelated").fill_null([]))
        # Note: _all_parts is dropped by add_token_ids which runs after this
    )


# 4.4: add token UUIDs to non-token cards
def add_token_ids(lf: pl.LazyFrame, scryfall_uuid_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Resolve token Scryfall IDs from _all_parts to MTGJSON UUIDs.

    Uses explode-join-aggregate pattern: extracts token scryfall IDs,
    explodes to one row per ID, joins with the scryfall->uuid mapping,
    then aggregates back to a list per card.
    """
    # Extract token Scryfall IDs from _all_parts where component == "token"
    with_token_ids = lf.with_columns(
        pl.col("_all_parts")
        .list.eval(
            pl.when(pl.element().struct.field("component") == "token")
            .then(pl.element().struct.field("id"))
            .otherwise(pl.lit(None))
        )
        .list.drop_nulls()
        .alias("_token_scryfall_ids")
    )

    # Explode to one row per token scryfall ID, join to resolve uuids, aggregate back
    resolved = (
        with_token_ids.select(["uuid", "_token_scryfall_ids"])
        .explode("_token_scryfall_ids")
        .filter(pl.col("_token_scryfall_ids").is_not_null())
        .join(
            scryfall_uuid_lf.select(
                pl.col("scryfallId").alias("_token_scryfall_ids"),
                pl.col("uuid").alias("_resolved_uuid"),
            ),
            on="_token_scryfall_ids",
            how="inner",
        )
        .group_by("uuid")
        .agg(pl.col("_resolved_uuid").sort().alias("_token_uuids"))
    )

    # Join resolved tokens back to main frame
    return (
        with_token_ids.join(resolved, on="uuid", how="left")
        .with_columns(pl.col("_token_uuids").fill_null([]))
        .drop(["_all_parts", "_token_scryfall_ids"], strict=False)
    )


def propagate_salt_to_tokens(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Propagate edhrecSaltiness from parent cards to tokens.
    """
    # Identify tokens
    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS) | (pl.col("type") == "Dungeon") | pl.col("type").str.contains("Token")
    )

    parent_salt = (
        lf.filter(~is_token & pl.col("edhrecSaltiness").is_not_null())
        .select(["name", "edhrecSaltiness"])
        .group_by("name")
        .agg(pl.col("edhrecSaltiness").first())
    )

    # Explode reverseRelated to join with parent salt
    # For tokens with NULL salt and non-empty reverseRelated
    tokens_needing_salt = lf.filter(
        is_token
        & pl.col("edhrecSaltiness").is_null()
        & pl.col("reverseRelated").is_not_null()
        & (pl.col("reverseRelated").list.len() > 0)
    ).with_columns(pl.col("reverseRelated").list.first().alias("_parent_name"))

    # Join to get parent salt
    tokens_with_salt = tokens_needing_salt.join(
        parent_salt.rename({"edhrecSaltiness": "_parent_salt"}),
        left_on="_parent_name",
        right_on="name",
        how="left",
    ).select(["uuid", "_parent_salt"])

    # Update original DataFrame with inherited salt
    lf = (
        lf.join(
            tokens_with_salt,
            on="uuid",
            how="left",
        )
        .with_columns(pl.coalesce(pl.col("edhrecSaltiness"), pl.col("_parent_salt")).alias("edhrecSaltiness"))
        .drop("_parent_salt", strict=False)
    )

    return lf
