"""
Scryfall bulk data ingestion pipeline.

Loads raw Scryfall data, validates against TypedDict schema,
normalizes column names, and produces a LazyFrame ready for
the MTGJSON card pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from mtgjson5.v2.models.scryfall import ScryfallCard

if TYPE_CHECKING:
    from polars import DataFrame, LazyFrame


# =============================================================================
# Column Name Normalization
# =============================================================================

# Scryfall snake_case -> camelCase (for columns not in SCRYFALL_TO_MTGJSON_FIELDS)
# These are the intermediate names expected by build_cards() BEFORE its own renames
SCRYFALL_TO_PIPELINE: dict[str, str] = {
    # Core fields (pipeline expects these names)
    "collector_number": "collectorNumber",
    "card_faces": "cardFaces",
    "type_line": "typeLine",
    "oracle_text": "oracleText",
    "oracle_id": "oracleId",
    "mana_cost": "manaCost",
    "color_identity": "colorIdentity",
    "color_indicator": "colorIndicator",
    "flavor_text": "flavorText",
    "flavor_name": "flavorName",
    "border_color": "borderColor",
    "frame_effects": "frameEffects",
    "security_stamp": "securityStamp",
    "full_art": "fullArt",
    "story_spotlight": "storySpotlight",
    "card_back_id": "cardBackId",
    "image_status": "imageStatus",
    "image_uris": "imageUris",
    "highres_image": "highresImage",
    "illustration_id": "illustrationId",
    "printed_name": "printedName",
    "printed_text": "printedText",
    "printed_type_line": "printedTypeLine",
    "promo_types": "promoTypes",
    "all_parts": "allParts",
    "related_uris": "relatedUris",
    "purchase_uris": "purchaseUris",
    "artist_ids": "artistIds",
    "multiverse_ids": "multiverseIds",
    "produced_mana": "producedMana",
    "attraction_lights": "attractionLights",
    "content_warning": "contentWarning",
    "hand_modifier": "handModifier",
    "life_modifier": "lifeModifier",
    "game_changer": "gameChanger",
    "edhrec_rank": "edhrecRank",
    "penny_rank": "pennyRank",
    "set_type": "setType",
    "set_name": "setName",
    "set_id": "setId",
    "set_uri": "setUri",
    "set_search_uri": "setSearchUri",
    "scryfall_uri": "scryfallUri",
    "scryfall_set_uri": "scryfallSetUri",
    "prints_search_uri": "printsSearchUri",
    "rulings_uri": "rulingsUri",
    "released_at": "releasedAt",
    # External IDs
    "mtgo_id": "mtgoId",
    "mtgo_foil_id": "mtgoFoilId",
    "arena_id": "arenaId",
    "tcgplayer_id": "tcgplayerId",
    "tcgplayer_etched_id": "tcgplayerEtchedId",
    "cardmarket_id": "cardmarketId",
}

# Nested struct field renames (card_faces sub-fields)
CARD_FACE_FIELD_RENAMES: dict[str, str] = {
    "type_line": "typeLine",
    "oracle_text": "oracleText",
    "mana_cost": "manaCost",
    "flavor_text": "flavorText",
    "flavor_name": "flavorName",
    "color_indicator": "colorIndicator",
    "image_uris": "imageUris",
    "illustration_id": "illustrationId",
    "printed_name": "printedName",
    "printed_text": "printedText",
    "printed_type_line": "printedTypeLine",
    "artist_id": "artistId",
    "oracle_id": "oracleId",
}


# =============================================================================
# Schema Inference
# =============================================================================


def infer_card_face_schema(sample_df: DataFrame) -> pl.Struct | None:
    """Infer card_faces struct schema from sample data."""
    if "card_faces" not in sample_df.columns:
        return None

    # Get first non-null card_faces value
    faces_col = sample_df.select("card_faces").drop_nulls()
    if len(faces_col) == 0:
        return None

    # Polars infers struct schema from data
    return sample_df.schema.get("card_faces")  # type: ignore[return-value]


def build_face_rename_expr(face_schema: pl.Struct) -> pl.Expr:
    """Build expression to rename card_faces struct fields."""
    if face_schema is None:
        return pl.col("card_faces")

    # Get field names from struct
    inner_type = face_schema.inner  # type: ignore[attr-defined]
    if not isinstance(inner_type, pl.Struct):
        return pl.col("card_faces")

    field_names = [f.name for f in inner_type.fields]

    # Build struct with renamed fields
    renamed_fields = []
    for name in field_names:
        new_name = CARD_FACE_FIELD_RENAMES.get(name, name)
        renamed_fields.append(pl.element().struct.field(name).alias(new_name))

    return pl.col("card_faces").list.eval(pl.struct(renamed_fields))


# =============================================================================
# Ingestion Functions
# =============================================================================


def read_bulk_json(
    path: str | Path,
    *,
    schema: pl.Schema | None = None,
) -> LazyFrame:
    """
    Read Scryfall bulk JSON file (array of cards).

    Args:
        path: Path to JSON file (e.g., all-cards.json)
        schema: Optional schema override

    Returns:
        LazyFrame with raw Scryfall data
    """
    return pl.scan_ndjson(
        path,
        schema=schema,
        ignore_errors=True,
        infer_schema_length=10000,
    )


def read_bulk_ndjson(
    path: str | Path,
    *,
    schema: pl.Schema | None = None,
) -> LazyFrame:
    """
    Read Scryfall NDJSON file (one card per line).

    Args:
        path: Path to NDJSON file
        schema: Optional schema override

    Returns:
        LazyFrame with raw Scryfall data
    """
    return pl.scan_ndjson(
        path,
        schema=schema,
        ignore_errors=True,
        infer_schema_length=10000,
    )


def load_from_dicts(cards: list[ScryfallCard]) -> DataFrame:
    """
    Load cards from list of TypedDicts.

    Fastest path when you already have parsed dicts (e.g., from orjson).

    Args:
        cards: List of ScryfallCard TypedDicts

    Returns:
        DataFrame with raw Scryfall data
    """
    return pl.DataFrame(cards)


# =============================================================================
# Normalization Pipeline
# =============================================================================


def normalize_column_names(lf: LazyFrame) -> LazyFrame:
    """
    Rename Scryfall snake_case columns to camelCase.

    This produces the column names expected by build_cards().
    """
    schema = lf.collect_schema()
    existing = set(schema.names())

    renames = {old: new for old, new in SCRYFALL_TO_PIPELINE.items() if old in existing}

    return lf.rename(renames)


def normalize_card_faces(lf: LazyFrame) -> LazyFrame:
    """
    Rename fields inside card_faces struct to camelCase.

    The pipeline's explode_card_faces() expects camelCase field names.
    """
    schema = lf.collect_schema()

    if "cardFaces" not in schema.names():
        return lf

    face_type = schema.get("cardFaces")

    # Handle List[Struct] type
    if not isinstance(face_type, pl.List):
        return lf

    inner = face_type.inner
    if not isinstance(inner, pl.Struct):
        return lf

    # Build renamed struct fields
    fields = inner.fields
    renamed_exprs = []

    for field in fields:
        old_name = field.name
        new_name = CARD_FACE_FIELD_RENAMES.get(old_name, old_name)
        renamed_exprs.append(pl.element().struct.field(old_name).alias(new_name))

    return lf.with_columns(
        pl.col("cardFaces").list.eval(pl.struct(renamed_exprs)).alias("cardFaces")
    )


def normalize_all_parts(lf: LazyFrame) -> LazyFrame:
    """
    Rename fields inside allParts struct to camelCase.
    """
    schema = lf.collect_schema()

    if "allParts" not in schema.names():
        return lf

    parts_type = schema.get("allParts")

    if not isinstance(parts_type, pl.List):
        return lf

    inner = parts_type.inner
    if not isinstance(inner, pl.Struct):
        return lf

    # all_parts fields: id, object, component, name, type_line, uri
    field_renames = {
        "type_line": "typeLine",
    }

    fields = inner.fields
    renamed_exprs = []

    for field in fields:
        old_name = field.name
        new_name = field_renames.get(old_name, old_name)
        renamed_exprs.append(pl.element().struct.field(old_name).alias(new_name))

    return lf.with_columns(
        pl.col("allParts").list.eval(pl.struct(renamed_exprs)).alias("allParts")
    )


def add_computed_fields(lf: LazyFrame) -> LazyFrame:
    """
    Add computed fields expected by the pipeline.

    These are fields derived from raw Scryfall data that the
    pipeline expects to exist.
    """
    return lf.with_columns(
        # Uppercase set code (pipeline expects this)
        pl.col("set")
        .str.to_uppercase()
        .alias("set"),
    )


def validate_required_columns(lf: LazyFrame) -> LazyFrame:
    """
    Ensure required columns exist, adding nulls if missing.

    The pipeline expects certain columns to exist even if null.
    """
    schema = lf.collect_schema()
    existing = set(schema.names())

    # Columns the pipeline requires (will error if missing)
    required = {
        "id",
        "name",
        "set",
        "lang",
        "layout",
        "cmc",
        "collectorNumber",
        "rarity",
        "finishes",
    }

    missing = required - existing
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Optional columns to add as null if missing
    optional_nulls: dict[str, pl.DataType] = {
        "cardFaces": pl.List(pl.Struct({})),
        "allParts": pl.List(pl.Struct({})),
        "promoTypes": pl.List(pl.String),
        "frameEffects": pl.List(pl.String),
        "keywords": pl.List(pl.String),
        "games": pl.List(pl.String),
        "multiverseIds": pl.List(pl.Int64),
        "colorIdentity": pl.List(pl.String),
        "colors": pl.List(pl.String),
    }

    add_exprs = []
    for col, dtype in optional_nulls.items():
        if col not in existing:
            add_exprs.append(pl.lit(None).cast(dtype).alias(col))

    if add_exprs:
        lf = lf.with_columns(add_exprs)

    return lf


# =============================================================================
# Full Ingestion Pipeline
# =============================================================================


def ingest_scryfall_bulk(
    path: str | Path,
    *,
    format: str = "auto",
) -> LazyFrame:
    """
    Full ingestion pipeline: Load Scryfall bulk data and normalize for MTGJSON pipeline.

    Args:
        path: Path to Scryfall bulk file
        format: "json", "ndjson", or "auto" (detect from extension)

    Returns:
        LazyFrame ready for build_cards()
    """
    path = Path(path)

    # Detect format
    if format == "auto":
        if path.suffix == ".json":
            format = "json"
        else:
            format = "ndjson"

    # Load raw data
    if format == "json":
        lf = read_bulk_json(path)
    else:
        lf = read_bulk_ndjson(path)

    # Run normalization pipeline
    return (
        lf.pipe(normalize_column_names)
        .pipe(normalize_card_faces)
        .pipe(normalize_all_parts)
        .pipe(add_computed_fields)
        .pipe(validate_required_columns)
    )


def ingest_from_orjson(cards: list[ScryfallCard]) -> LazyFrame:
    """
        Ingest pre-parsed cards from orjson.

        Fastest path when you've already parsed with orjson:
    ```python
        import orjson
        from scryfall._ingest import ingest_from_orjson

        with open("all-cards.json", "rb") as f:
            cards = orjson.loads(f.read())

        lf = ingest_from_orjson(cards)
    ```
    """
    df = load_from_dicts(cards)

    return (
        df.lazy()
        .pipe(normalize_column_names)
        .pipe(normalize_card_faces)
        .pipe(normalize_all_parts)
        .pipe(add_computed_fields)
        .pipe(validate_required_columns)
    )


# =============================================================================
# Context Integration
# =============================================================================


def load_cards_to_context(
    path: str | Path,
    *,
    format: str = "auto",
) -> LazyFrame:
    """
        Load and normalize Scryfall bulk data for PipelineContext.cards_lf.

        Usage:
    ```python
        from scryfall._ingest import load_cards_to_context

        ctx = PipelineContext()
        ctx.cards_lf = load_cards_to_context("scryfall-all-cards.json")
        build_cards(ctx)
    ```
    """
    return ingest_scryfall_bulk(path, format=format)
