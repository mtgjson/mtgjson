"""
MTGJSON Vectorized Card Pipeline

Builds MTGJSON card data using Polars LazyFrame operations instead of
per-card Python object iteration. This provides significant performance
improvements through vectorized operations and lazy evaluation.
"""

import logging
import unicodedata
from typing import List, Optional

import polars as pl
import polars_hash as plh

from mtgjson5.models.projections.mtgjson import ALL_CARD_FIELDS, ATOMIC_EXCLUDE, CARD_DECK_EXCLUDE, CARD_SET_EXCLUDE, LEGALITY_FORMATS, TOKEN_EXCLUDE
from mtgjson5.providers.scryfall.monolith import ScryfallProvider

from . import constants
from . import categoricals
from .cache_builder import GLOBAL_CACHE
from .providers import WhatsInStandardProvider
from .uuid_generator import uuid5_batch, compute_v4_uuid_from_struct

LOGGER = logging.getLogger(__name__)


def _to_ascii_name(name: str) -> str:
    """Convert non-ASCII characters in a card name to ASCII equivalents."""
    # Normalize to decomposed form (separates base chars from accents)
    normalized = unicodedata.normalize("NFD", name)
    # Filter out combining characters (accents, diacritics)
    ascii_chars = []
    for char in normalized:
        if unicodedata.category(char) != "Mn":  # Mn = Mark, Nonspacing
            # Try to get ASCII equivalent
            try:
                ascii_char = char.encode("ascii", "ignore").decode("ascii")
                if ascii_char:
                    ascii_chars.append(ascii_char)
            except UnicodeEncodeError:
                pass
    return "".join(ascii_chars)


def _compute_ascii_names_batch(names: pl.Series) -> pl.Series:
    """
    Batch compute ASCII names for a series of card names.

    Returns None for names that are already ASCII or empty.
    Uses map_batches for efficient bulk processing.
    """
    results = []
    for name in names:
        if name is None or not name or name.isascii():
            results.append(None)
        else:
            results.append(_to_ascii_name(name))
    return pl.Series(results, dtype=pl.String)


# =============================================================================
# Main Pipeline Entry Point
# =============================================================================


def build_set_cards(set_code: str, set_release_date: str = "", scryfall_ids: Optional[List[str]] = None,) -> pl.LazyFrame:
    """
    Build all cards for a set as a LazyFrame.

    This is the main entry point for the vectorized card pipeline.
    It chains together all transformation stages using .pipe().

    :param set_code: Set code to build (e.g., "10E", "MH3")
    :param set_release_date: Original set release date for comparison
    :return: LazyFrame with all card data ready for collection
    """
    if GLOBAL_CACHE.cards_df is None:
        raise RuntimeError("Cache not initialized - call GLOBAL_CACHE.load_all() first")

    LOGGER.info(f"Building cards for {set_code} using vectorized pipeline")

    if scryfall_ids:
        LOGGER.info(f"Filtering to {len(scryfall_ids)} additional cards for {set_code}")
        lf = GLOBAL_CACHE.cards_df.lazy().filter(
                pl.col("id").is_in(scryfall_ids)
            )
    else:
        return (
            GLOBAL_CACHE.cards_df.lazy()
            .filter(
                (pl.col("set") == set_code) & (pl.col("lang") == "en")
            )
            .pipe(explode_card_faces)
            .pipe(add_basic_fields, set_release_date=set_release_date)
            .pipe(join_card_kingdom_data)
            .pipe(add_identifiers_struct)
            .pipe(parse_type_line_expr)
            .pipe(add_mana_info)
            .pipe(add_card_attributes)
            .pipe(filter_keywords_for_face)
            .pipe(add_booster_types)
            .pipe(add_legalities_struct)
            .pipe(add_availability_struct)
            .pipe(join_printings)
            .pipe(join_rulings)
            .pipe(join_foreign_data)
            .pipe(add_uuid_expr)
            .pipe(add_leadership_skills_expr)
            .pipe(add_reverse_related)
            .pipe(
            add_final_fields,
                set_code=set_code,
                set_name=GLOBAL_CACHE.get_set_name(set_code),
                set_release_date=set_release_date,
                
        )
    )


# =============================================================================
# Stage 1: Multi-Face Card Handling
# =============================================================================


def explode_card_faces(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Explode multi-face cards into separate rows per face.

    Single-face cards get face_id=0. Multi-face cards (split, transform,
    modal_dfc, meld, etc.) get one row per face with face_id=0,1,2...

    Uses split/process/concat pattern to avoid list operations on null columns.

    Also adds:
    - _row_id: Original row index for linking faces later
    - face_id: 0-based index of this face
    - side: Letter side identifier ("a", "b", "c", etc.)
    - _face_data: The face struct (for multi-face) or null (for single-face)
    """
    # Must collect to split - then return as lazy
    df = lf.with_row_index("_row_id").collect()

    if "card_faces" not in df.columns:
        return df.with_columns(
            pl.lit(0).cast(pl.Int64).alias("face_id"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).alias("_face_data"),
        ).lazy()

    # Split into cards with faces vs without
    has_faces = df.filter(pl.col("card_faces").is_not_null())
    no_faces = df.filter(pl.col("card_faces").is_null())

    if has_faces.height == 0:
        # All single-faced cards
        return no_faces.with_columns(
            pl.lit(0).cast(pl.Int64).alias("face_id"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).alias("_face_data"),
        ).drop("card_faces").lazy()

    # Process multi-face cards: generate face indices and explode
    # Use int_ranges to generate [0, 1, 2, ...] for each card's face count
    exploded = (
        has_faces.with_columns(
            pl.int_ranges(pl.col("card_faces").list.len()).alias("_face_idx")
        )
        .explode(["card_faces", "_face_idx"])
        .rename({"_face_idx": "face_id", "card_faces": "_face_data"})
        .with_columns(
            # Convert face_id (0,1,2,...) to side letter (a,b,c,...)
            # Using replace_strict with pre-built mapping
            pl.col("face_id")
            .replace_strict(
                {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"},
                default="a",
                return_dtype=pl.String,
            )
            .alias("side")
        )
    )

    # Add columns to no_faces - let diagonal concat fill _face_data with null
    no_faces = no_faces.with_columns(
        pl.lit(0).cast(pl.Int64).alias("face_id"),
        pl.lit(None).cast(pl.String).alias("side"),
    ).drop("card_faces")

    return pl.concat([no_faces, exploded], how="diagonal").lazy()


# =============================================================================
# Stage 2: Basic Fields
# =============================================================================


def _safe_face_get(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    No-op: _face_data is now created directly in explode_card_faces.
    Kept for backwards compatibility in case called elsewhere.
    """
    return lf


def add_basic_fields(lf: pl.LazyFrame, set_release_date: str = "") -> pl.LazyFrame:
    """
    Add basic card fields: name, setCode, language, etc.

    Maps Scryfall column names to MTGJSON names.
    For multi-face cards, the name is the face-specific name.
    """
    lf = _safe_face_get(lf)

    # Check if _face_data has struct type (vs Null when no multi-face cards)
    schema = lf.collect_schema()
    has_face_struct = isinstance(schema.get("_face_data"), pl.Struct)

    if has_face_struct:
        face_data = pl.col("_face_data")
        card_name = pl.coalesce(face_data.struct.field("name"), pl.col("name"))
        face_name_expr = face_data.struct.field("name").alias("faceName")
    else:
        card_name = pl.col("name")
        face_name_expr = pl.lit(None).cast(pl.String).alias("faceName")

    return lf.with_columns(
        card_name.alias("name"),
        pl.col("set").str.to_uppercase().alias("setCode"),
        pl.col("lang")
        .replace_strict(constants.LANGUAGE_MAP, default=pl.lit("Unknown"))
        .alias("language"),
        face_name_expr,
        card_name.map_batches(_compute_ascii_names_batch, return_dtype=pl.String).alias("asciiName"),
        pl.coalesce(pl.col("flavor_name"), pl.col("printed_name")).alias("flavorName"),
        pl.col("printed_name").alias("printedName"),
        pl.col("printed_type_line").alias("printedType"),
        pl.col("printed_text").alias("printedText"),
        pl.when(
            (pl.lit(set_release_date) != "")
            & (pl.col("released_at") != pl.lit(set_release_date))
        )
        .then(pl.col("released_at"))
        .otherwise(pl.lit(None))
        .alias("originalReleaseDate"),
    )


# =============================================================================
# Stage 3: Card Kingdom Data Join
# =============================================================================

CK_URL_PREFIX = "https://www.cardkingdom.com"
MTGJSON_LINKS_PREFIX = "https://mtgjson.com/links/"


def url_keygen_expr(seed_col: str, with_leading: bool = True) -> pl.Expr:
    """
    Vectorized URL key generation using SHA256.

    Replacement for the scalar url_keygen() function.
    Uses polars_hash for native vectorized hashing.

    Args:
        seed_col: Name of the column containing seed strings
        with_leading: Whether to prepend MTGJSON links prefix

    Returns:
        Polars expression that hashes the seed and returns the URL key
    """
    import polars_hash as plh

    hash_expr = plh.col(seed_col).chash.sha256().str.slice(0, 16)

    if with_leading:
        return pl.lit(MTGJSON_LINKS_PREFIX) + hash_expr
    return hash_expr


def join_card_kingdom_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Join pre-pivoted Card Kingdom data to add CK identifiers and URLs.

    The CK data is pivoted during cache loading to one row per scryfall_id.
    Also computes MTGJSON redirect URLs from the CK URL paths.
    """
    if GLOBAL_CACHE.card_kingdom_df is None:
        LOGGER.debug("Card Kingdom DataFrame not loaded, skipping CK data")
        return lf.with_columns(
            pl.lit(None).cast(pl.String).alias("cardKingdomId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
        )

    # Join CK data - note: v2 provider uses 'id' as the join key (was scryfall_id)
    lf = lf.join(
        GLOBAL_CACHE.card_kingdom_df.lazy(),
        on="id",
        how="left",
    )

    # Rename columns from v2 provider to expected names
    # v2 provider columns: card_kingdom_id, card_kingdom_foil_id, card_kingdom_url, card_kingdom_foil_url
    return lf.with_columns([
        pl.col("card_kingdom_id").alias("cardKingdomId"),
        pl.col("card_kingdom_foil_id").alias("cardKingdomFoilId"),
        pl.lit(None).cast(pl.String).alias("cardKingdomEtchedId"),  # v2 doesn't have etched yet
        pl.col("card_kingdom_url").alias("cardKingdomUrl"),
        pl.col("card_kingdom_foil_url").alias("cardKingdomFoilUrl"),
        pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
    ]).drop(["card_kingdom_id", "card_kingdom_foil_id", "card_kingdom_url", "card_kingdom_foil_url"], strict=False)


# =============================================================================
# Stage 4: Identifiers Struct
# =============================================================================


def add_identifiers_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build the identifiers struct from Scryfall columns.

    Creates nested structure matching MTGJSON identifiers format.
    """
    schema = lf.collect_schema()
    has_face_struct = isinstance(schema.get("_face_data"), pl.Struct)

    if has_face_struct:
        face_data = pl.col("_face_data")
        oracle_id_expr = pl.coalesce(face_data.struct.field("oracle_id"), pl.col("oracle_id"))
        illustration_id_expr = pl.coalesce(face_data.struct.field("illustration_id"), pl.col("illustration_id"))
    else:
        oracle_id_expr = pl.col("oracle_id")
        illustration_id_expr = pl.col("illustration_id")

    return lf.with_columns(
        pl.struct(
            scryfallId=pl.col("id"),
            scryfallOracleId=oracle_id_expr,
            scryfallIllustrationId=illustration_id_expr,
            scryfallCardBackId=pl.col("card_back_id"),
            mcmId=pl.col("cardmarket_id").cast(pl.String),
            mtgArenaId=pl.col("arena_id").cast(pl.String),
            mtgoId=pl.col("mtgo_id").cast(pl.String),
            mtgoFoilId=pl.col("mtgo_foil_id").cast(pl.String),
            multiverseId=pl.col("multiverse_ids")
            .list.get(pl.col("face_id"), null_on_oob=True)
            .cast(pl.String),
            tcgplayerProductId=pl.col("tcgplayer_id").cast(pl.String),
            tcgplayerEtchedProductId=pl.col("tcgplayer_etched_id").cast(pl.String),
            # Card Kingdom IDs from join
            cardKingdomId=pl.col("cardKingdomId"),
            cardKingdomFoilId=pl.col("cardKingdomFoilId"),
            cardKingdomEtchedId=pl.col("cardKingdomEtchedId"),
        ).alias("identifiers")
    )


# =============================================================================
# Stage 4: Type Line Parsing
# =============================================================================


def parse_type_line_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Parse type_line into supertypes, types, subtypes using Polars expressions.

    Converts "Legendary Creature - Human Wizard" into:
    - supertypes: ["Legendary"]
    - types: ["Creature"]
    - subtypes: ["Human", "Wizard"]
    """
    schema = lf.collect_schema()
    has_face_struct = isinstance(schema.get("_face_data"), pl.Struct)

    if has_face_struct:
        type_line = pl.coalesce(
            pl.col("_face_data").struct.field("type_line"),
            pl.col("type_line"),
        ).fill_null("Card")
    else:
        type_line = pl.col("type_line").fill_null("Card")

    super_types_list = list(constants.SUPER_TYPES)

    # Split type_line on em-dash
    split_type = type_line.str.split(" \u2014 ")

    return (
        lf.with_columns(
            type_line.alias("type"),
            # Types part is always first element
            split_type.list.first().alias("_types_part"),
            # Subtypes part is second element if it exists
            split_type.list.get(1, null_on_oob=True).alias("_subtypes_part"),
        )
        .with_columns(
            # Split types part into words
            pl.col("_types_part").str.split(" ").alias("_type_words"),
        )
        .with_columns(
            # Supertypes: words that are in SUPER_TYPES constant
            pl.col("_type_words")
            .list.eval(pl.element().filter(pl.element().is_in(super_types_list)))
            .alias("supertypes"),
            # Types: words that are NOT in SUPER_TYPES
            pl.col("_type_words")
            .list.eval(pl.element().filter(~pl.element().is_in(super_types_list)))
            .alias("types"),
            # Subtypes: split the part after em-dash
            pl.when(pl.col("_subtypes_part").is_not_null())
            .then(pl.col("_subtypes_part").str.strip_chars().str.split(" "))
            .otherwise(pl.lit([]).cast(pl.List(pl.String)))
            .alias("subtypes"),
        )
        .drop(["_types_part", "_subtypes_part", "_type_words"])
    )


# =============================================================================
# Stage 5: Mana Info
# =============================================================================


def add_mana_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mana cost, mana value, and colors.

    Handles face-specific mana costs for multi-face cards.
    """
    schema = lf.collect_schema()
    has_face_struct = isinstance(schema.get("_face_data"), pl.Struct)

    if has_face_struct:
        face_data = pl.col("_face_data")
        mana_cost = pl.coalesce(face_data.struct.field("mana_cost"), pl.col("mana_cost"))
        colors = pl.coalesce(face_data.struct.field("colors"), pl.col("colors")).fill_null([])
        face_mana_value = pl.when(face_data.is_not_null()).then(face_data.struct.field("cmc")).otherwise(pl.lit(None))
    else:
        mana_cost = pl.col("mana_cost")
        colors = pl.col("colors").fill_null([])
        face_mana_value = pl.lit(None)

    return lf.with_columns(
        mana_cost.alias("manaCost"),
        colors.alias("colors"),
        pl.col("color_identity").fill_null([]).alias("colorIdentity"),
        pl.col("cmc").fill_null(0.0).alias("manaValue"),
        pl.col("cmc").fill_null(0.0).alias("convertedManaCost"),
        face_mana_value.alias("faceManaValue"),
    )


# =============================================================================
# Stage 6: Card Attributes
# =============================================================================


def add_card_attributes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add card attributes: rarity, frame, finishes, boolean flags, stats.
    """
    schema = lf.collect_schema()
    has_face_struct = isinstance(schema.get("_face_data"), pl.Struct)

    if has_face_struct:
        fd = pl.col("_face_data")
        artist_expr = pl.coalesce(fd.struct.field("artist"), pl.col("artist")).fill_null("")
        watermark_expr = pl.coalesce(fd.struct.field("watermark"), pl.col("watermark"))
        loyalty_expr = pl.coalesce(fd.struct.field("loyalty"), pl.col("loyalty"))
        defense_expr = pl.coalesce(fd.struct.field("defense"), pl.col("defense"))
        power_expr = pl.coalesce(fd.struct.field("power"), pl.col("power"))
        toughness_expr = pl.coalesce(fd.struct.field("toughness"), pl.col("toughness"))
        text_expr = pl.coalesce(fd.struct.field("oracle_text"), pl.col("oracle_text")).fill_null("")
        flavor_expr = pl.coalesce(fd.struct.field("flavor_text"), pl.col("flavor_text"))
    else:
        artist_expr = pl.col("artist").fill_null("")
        watermark_expr = pl.col("watermark")
        loyalty_expr = pl.col("loyalty")
        defense_expr = pl.col("defense")
        power_expr = pl.col("power")
        toughness_expr = pl.col("toughness")
        text_expr = pl.col("oracle_text").fill_null("")
        flavor_expr = pl.col("flavor_text")

    return lf.with_columns(
        pl.col("collector_number").alias("number"),
        pl.col("rarity"),
        pl.col("border_color").alias("borderColor"),
        pl.col("frame").alias("frameVersion"),
        pl.col("frame_effects").fill_null([]).list.sort().alias("frameEffects"),
        pl.col("security_stamp").alias("securityStamp"),
        artist_expr.alias("artist"),
        pl.col("artist_ids").fill_null([]).alias("artistIds"),
        watermark_expr.alias("watermark"),
        pl.col("finishes").fill_null([]).alias("finishes"),
        pl.col("finishes").list.contains("foil").fill_null(False).alias("hasFoil"),
        pl.col("finishes").list.contains("nonfoil").fill_null(False).alias("hasNonFoil"),
        pl.col("content_warning").alias("hasContentWarning"),
        pl.col("full_art").alias("isFullArt"),
        pl.col("digital").alias("isOnlineOnly"),
        pl.col("oversized").alias("isOversized"),
        pl.col("promo").alias("isPromo"),
        pl.col("reprint").alias("isReprint"),
        pl.col("reserved").alias("isReserved"),
        pl.col("story_spotlight").alias("isStorySpotlight"),
        pl.col("textless").alias("isTextless"),
        (pl.col("set_type") == "funny").alias("_is_funny_set"),
        loyalty_expr.alias("loyalty"),
        defense_expr.alias("defense"),
        power_expr.alias("power"),
        toughness_expr.alias("toughness"),
        pl.col("hand_modifier").alias("hand"),
        pl.col("life_modifier").alias("life"),
        pl.col("edhrec_rank").alias("edhrecRank"),
        pl.col("promo_types").fill_null([]).alias("promoTypes"),
        pl.col("booster").alias("_in_booster"),
        pl.col("game_changer").fill_null(False).alias("isGameChanger"),
        pl.col("layout"),
        text_expr.alias("text"),
        flavor_expr.alias("flavorText"),
        pl.col("keywords").fill_null([]).alias("_all_keywords"),
        pl.col("attraction_lights").alias("attractionLights"),
        pl.col("all_parts").fill_null([]).alias("_all_parts"),
    )


def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter keywords to only those that appear in this face's oracle text.

    For multi-face cards, each face should only list keywords that appear
    in that face's text, not all keywords from the entire card.

    Uses explode/filter/aggregate pattern to avoid map_elements.
    """
    # Add row index for re-aggregation
    lf = lf.with_row_index("_kw_row_idx")

    # Lowercase text for case-insensitive matching
    lf = lf.with_columns(pl.col("text").str.to_lowercase().alias("_text_lower"))

    # Collect to DataFrame for split processing
    df = lf.collect()

    # Split: rows with keywords vs without
    has_keywords = df.filter(pl.col("_all_keywords").list.len() > 0)
    no_keywords = df.filter(
        (pl.col("_all_keywords").is_null()) | (pl.col("_all_keywords").list.len() == 0)
    ).with_columns(pl.lit([]).cast(pl.List(pl.String)).alias("keywords"))

    if has_keywords.height > 0:
        # Explode keywords, filter by text containment, re-aggregate
        filtered_keywords = (
            has_keywords.select(["_kw_row_idx", "_all_keywords", "_text_lower"])
            .explode("_all_keywords")
            .filter(
                pl.col("_text_lower").str.contains(
                    pl.col("_all_keywords").str.to_lowercase()
                )
            )
            .group_by("_kw_row_idx")
            .agg(pl.col("_all_keywords").sort().alias("keywords"))
        )

        # Join back
        has_keywords = (
            has_keywords.drop("_all_keywords")
            .join(filtered_keywords, on="_kw_row_idx", how="left")
            .with_columns(pl.col("keywords").fill_null([]))
        )

        df = pl.concat([has_keywords, no_keywords], how="diagonal")
    else:
        df = no_keywords

    return df.drop(["_kw_row_idx", "_text_lower", "_all_keywords"], strict=False).lazy()


def add_booster_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute boosterTypes based on Scryfall booster field and promoTypes.

    - If card is in boosters (booster=True), add "default"
    - If promoTypes contains "starterdeck" or "planeswalkerdeck", add "deck"
    """
    return lf.with_columns(
        pl.when(pl.col("_in_booster").fill_null(False))
        .then(
            pl.when(
                pl.col("promoTypes").list.set_intersection(
                    pl.lit(["starterdeck", "planeswalkerdeck"])
                ).list.len() > 0
            )
            .then(pl.lit(["default", "deck"]))
            .otherwise(pl.lit(["default"]))
        )
        .otherwise(
            pl.when(
                pl.col("promoTypes").list.set_intersection(
                    pl.lit(["starterdeck", "planeswalkerdeck"])
                ).list.len() > 0
            )
            .then(pl.lit(["deck"]))
            .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        )
        .alias("boosterTypes")
    ).drop("_in_booster")


# =============================================================================
# Stage 7: Legalities Struct
# =============================================================================
   
def add_legalities_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Transform Scryfall legalities to MTGJSON format.
    
    - Filters out "not_legal" values
    - Titlecases remaining: "legal" -> "Legal"
    - Memorabilia cards get null legalities
    - Ensures all format fields exist (for schema compliance)
    """
    schema = lf.collect_schema()
    
    if "legalities" not in schema or not isinstance(schema["legalities"], pl.Struct):
        LOGGER.warning("legalities column missing or not a struct")
        return lf.with_columns(pl.lit(None).alias("legalities"))
    
    # Get formats present in source data
    source_formats = {field.name for field in schema["legalities"].fields}
    
    # Unnest to access individual format columns
    unnested = lf.unnest("legalities")
    
    # Build expressions for ALL expected formats
    legality_exprs = []
    for fmt in sorted(LEGALITY_FORMATS):  # Use canonical list, sorted
        if fmt in source_formats:
            # Transform: filter not_legal, titlecase, null for memorabilia
            expr = (
                pl.when(
                    pl.col(fmt).is_not_null()
                    & (pl.col(fmt) != "not_legal")
                    & (pl.col("set_type") != "memorabilia")
                )
                .then(pl.col(fmt).str.to_titlecase())
                .otherwise(pl.lit(None))
            )
        else:
            # Format not in source, add as null
            expr = pl.lit(None).cast(pl.String)
        
        legality_exprs.append(expr.alias(fmt))
    
    return unnested.with_columns(
        pl.struct(legality_exprs).alias("legalities")
    )
    
# =============================================================================
# Stage 8: Availability Struct
# =============================================================================


def add_availability_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build availability struct from games list and IDs.
    """
    if "games" not in lf.columns:
        return lf
    
    schema = lf.collect_schema()
    if not isinstance(schema.get("games"), pl.Struct):
        return lf
    
    platforms = categoricals.STATIC_CATEGORICALS.get("games") or []
    
    return lf.with_columns(
        pl.concat_list([
            pl.when(pl.col("games").struct.field(p).fill_null(False))
            .then(pl.lit(p))
            .otherwise(pl.lit(None))
            for p in platforms
        ])
        .list.drop_nulls()
        .list.sort()
        .alias("availability")
    )


# =============================================================================
# Stage 9: Vectorized Joins
# =============================================================================


def join_printings(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Join printings map - replaces parse_printings() per-card lookups.

    Single join replaces N filter operations.
    """
    if GLOBAL_CACHE.printings_df is None:
        return lf.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias("printings"))

    return lf.join(
        GLOBAL_CACHE.printings_df.lazy(),
        left_on="oracle_id",
        right_on="oracle_id",
        how="left",
    ).with_columns(pl.col("printings").fill_null([]).list.sort())


def join_rulings(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Join rulings map - replaces parse_rulings() per-card lookups.
    """
    if GLOBAL_CACHE.rulings_df is None:
        # Type must match rulings struct: List[{date: String, text: String}]
        return lf.with_columns(
            pl.lit([]).cast(pl.List(pl.Struct({"date": pl.String, "text": pl.String}))).alias("rulings")
        )

    return lf.join(
        GLOBAL_CACHE.rulings_df.lazy(),
        left_on="oracle_id",
        right_on="oracle_id",
        how="left",
    ).with_columns(pl.col("rulings").fill_null([]))


def join_foreign_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Join foreign data with UUIDs."""
    cards = GLOBAL_CACHE.cards_df.lazy()
    
    foreign_struct = (
        cards.filter(pl.col("lang") != "en")
        .select(
            "set",
            "collector_number",
            pl.col("lang")
                .replace_strict(constants.LANGUAGE_MAP, default=pl.col("lang"))
                .alias("language"),
            pl.col("id").alias("scryfall_id"),
            pl.col("multiverse_ids").list.first().alias("multiverse_id"),
            pl.col("name").alias("scryfall_name"),
            pl.coalesce("printed_name", "name").alias("name"),
            pl.col("printed_text").alias("text"),
            pl.col("printed_type_line").alias("type"),
            "flavor_text",
            "card_faces",
        )
        .sort("set", "collector_number", "language")
        .group_by("set", "collector_number")
        .agg(
            pl.struct(
                "language",
                "scryfall_id",
                "multiverse_id",
                "scryfall_name",
                "name",
                "text",
                "type",
                "flavor_text",
                "card_faces",
            ).alias("foreign_data")
        )
    )
    
    GLOBAL_CACHE.foreign_data_df = foreign_struct.collect()
    
    # Join and process in one pass
    return (
        lf.join(
            GLOBAL_CACHE.foreign_data_df.lazy(),
            on=["set", "collector_number"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("foreign_data").list.len() > 0)
            .then(
                pl.col("foreign_data").list.eval(
                    pl.struct(
                        pl.element().struct.field("flavor_text").alias("flavorText"),
                        pl.struct(
                            pl.element().struct.field("scryfall_id").alias("scryfallId"),
                            pl.element().struct.field("multiverse_id").cast(pl.String).alias("multiverseId"),
                        ).alias("identifiers"),
                        pl.element().struct.field("language"),
                        pl.element().struct.field("name"),
                        pl.element().struct.field("text"),
                        pl.element().struct.field("type"),
                    )
                )
            )
            .otherwise(
                pl.lit([]).cast(pl.List(pl.Struct({
                    "flavorText": pl.String,
                    "identifiers": pl.Struct({
                        "scryfallId": pl.String,
                        "multiverseId": pl.String,
                    }),
                    "language": pl.String,
                    "name": pl.String,
                    "text": pl.String,
                    "type": pl.String,
                })))
            )
            .alias("foreignData")
        )
        .drop("foreign_data")
    )

# =============================================================================
# Stage 10: UUID Generation
# =============================================================================


def add_uuid_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Generate MTGJSON UUIDs using vectorized NumPy operations.

    - uuid: UUIDv5 from scryfall_id + side (uses cached UUIDs when available)
    - mtgjsonV4Id: Legacy v4 format added to identifiers struct
    """
    # Join with UUID cache if available
    if GLOBAL_CACHE.uuid_cache_df is not None:
        lf = lf.join(
            GLOBAL_CACHE.uuid_cache_df.lazy(),
            left_on=["id", pl.col("side").fill_null("a")],
            right_on=["scryfall_id", "side"],
            how="left",
        )
        
        # Use cached UUID if available, otherwise generate
        lf = lf.with_columns(
            pl.when(pl.col("cached_uuid").is_not_null())
            .then(pl.col("cached_uuid"))
            .otherwise(
                pl.concat_str([pl.col("id"), pl.col("side").fill_null("a")])
                .map_batches(uuid5_batch, return_dtype=pl.String)
            )
            .alias("uuid")
        ).drop(["scryfall_id_right", "cached_uuid"], strict=False)
    else:
        # No cache - generate all UUIDs
        lf = lf.with_columns(
            pl.concat_str([pl.col("id"), pl.col("side").fill_null("a")])
            .map_batches(uuid5_batch, return_dtype=pl.String)
            .alias("uuid")
        )

    # Generate V4 UUID
    lf = lf.with_columns(
        pl.struct([
            pl.col("id"),
            pl.col("name"),
            pl.col("faceName").alias("face_name"),
            pl.col("types"),
            pl.col("colors"),
            pl.col("power"),
            pl.col("toughness"),
            pl.col("side"),
            pl.col("set"),
        ])
        .map_batches(compute_v4_uuid_from_struct, return_dtype=pl.String)
        .alias("_mtgjsonV4Id"),
    )

    # Add mtgjsonV4Id to the identifiers struct
    return lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            pl.col("_mtgjsonV4Id").alias("mtgjsonV4Id")
        )
    ).drop("_mtgjsonV4Id")


# =============================================================================
# Stage 11: Leadership Skills
# =============================================================================


def add_leadership_skills_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Determine if a card can be a commander/oathbreaker/brawl commander.

    Uses vectorized string operations instead of per-card checks.
    """
    # Override cards that can always be commander
    override_cards = ["Grist, the Hunger Tide"]

    # Commander legal check
    is_legendary = pl.col("type").str.contains("Legendary")
    is_creature = pl.col("type").str.contains("Creature")
    is_vehicle_or_spacecraft = pl.col("type").str.contains("Vehicle|Spacecraft")
    has_power_toughness = pl.col("power").is_not_null() & pl.col("toughness").is_not_null()
    is_front_face = pl.col("side").is_null() | (pl.col("side") == "a")
    can_be_commander_text = pl.col("text").str.contains("can be your commander")
    is_override = pl.col("name").is_in(override_cards)

    is_commander_legal = (
        is_override
        | (
            is_legendary
            & (is_creature | (is_vehicle_or_spacecraft & has_power_toughness))
            & is_front_face
        )
        | can_be_commander_text
    )

    # Oathbreaker legal = is a planeswalker
    is_oathbreaker_legal = pl.col("type").str.contains("Planeswalker")

    # Brawl legal = set is in Standard AND (commander or oathbreaker eligible)
    standard_sets = WhatsInStandardProvider().set_codes
    is_in_standard = pl.col("set").str.to_uppercase().is_in(standard_sets)
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


# =============================================================================
# Stage 12: Reverse Related (for tokens)
# =============================================================================


def add_reverse_related(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Compute reverseRelated for tokens from all_parts.

    For tokens, this lists the names of cards that create/reference this token.
    """
    # Extract names from all_parts where name differs from card name
    # all_parts is List[Struct{name, ...}]
    return lf.with_columns(
        pl.col("_all_parts")
        .list.eval(pl.element().struct.field("name"))
        .list.set_difference(pl.col("name").cast(pl.List(pl.String)))
        .list.sort()
        .alias("reverseRelated")
    ).drop("_all_parts")


# =============================================================================
# Post-Collect Operations (require full DataFrame)
# =============================================================================


def add_other_face_ids(cards_df: pl.DataFrame) -> pl.DataFrame:
    """
    Link multi-face cards via self-join on row ID.

    Must be called after collect() since it requires cross-row lookups.
    """
    # Get all UUIDs for each original card
    face_links = (
        cards_df.select(["_row_id", "uuid"])
        .group_by("_row_id")
        .agg(pl.col("uuid").alias("_all_uuids"))
    )

    return (
        cards_df.join(face_links, on="_row_id", how="left")
        .with_columns(
            # Filter out own UUID from the list
            pl.col("_all_uuids")
            .list.set_difference(pl.col("uuid").cast(pl.List(pl.String)))
            .alias("otherFaceIds")
        )
        .drop("_all_uuids")
    )


def add_variations(cards_df: pl.DataFrame, set_code: str = "") -> pl.DataFrame:
    """
    Identify alternative printings within same set and mark alternatives.

    Cards with the same base name (stripping "(Showcase)" etc.) and face_name
    but different UUIDs are variations. Cards that share the same
    name|border_color|frame_version|frame_effects|side key are alternatives.

    :param cards_df: DataFrame with card data
    :param set_code: Set code for special handling (UNH, 10E include finishes)
    :return: DataFrame with variations and isAlternative columns
    """
    from . import constants

    # Normalize name by stripping " (Showcase)", " (Borderless)", etc.
    df = cards_df.with_columns(
        pl.col("name")
        .str.split(" (")
        .list.first()
        .alias("_base_name")
    )

    # Group by base_name + faceName to find variations
    # faceName can be null, so fill with empty string for grouping
    variation_groups = (
        df.select(["_base_name", "faceName", "uuid", "number"])
        .with_columns(pl.col("faceName").fill_null("").alias("_face_key"))
        .group_by(["_base_name", "_face_key"])
        .agg([
            pl.col("uuid").alias("_group_uuids"),
            pl.col("number").alias("_group_numbers"),
        ])
    )

    df = df.with_columns(pl.col("faceName").fill_null("").alias("_face_key"))
    df = df.join(variation_groups, on=["_base_name", "_face_key"], how="left")

    # Variations: other UUIDs in the group (excluding self, checking different number)
    df = df.with_columns(
        pl.when(pl.col("_group_uuids").list.len() > 1)
        .then(
            pl.col("_group_uuids").list.set_difference(
                pl.col("uuid").cast(pl.List(pl.String))
            )
        )
        .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        .alias("variations")
    )

    # Build distinct printing key for isAlternative detection
    # Key: name|border_color|frame_version|frame_effects|side
    frame_effects_str = (
        pl.col("frameEffects")
        .list.sort()
        .list.join(",")
        .fill_null("")
    )

    key_expr = pl.concat_str([
        pl.col("name"),
        pl.lit("|"),
        pl.col("borderColor").fill_null(""),
        pl.lit("|"),
        pl.col("frameVersion").fill_null(""),
        pl.lit("|"),
        frame_effects_str,
        pl.lit("|"),
        pl.col("side").fill_null(""),
    ])

    # For UNH and 10E, include finishes in the key
    if set_code.upper() in {"UNH", "10E"}:
        finishes_str = pl.col("finishes").list.sort().list.join(",").fill_null("")
        key_expr = pl.concat_str([key_expr, pl.lit("|"), finishes_str])

    df = df.with_columns(key_expr.alias("_printing_key"))

    # Mark as alternative if:
    #   1. Has variations (not singleton)
    #   2. Not a basic land
    #   3. Printing key already seen (duplicate)
    basic_lands = list(constants.BASIC_LAND_NAMES)

    # Find first occurrence of each printing key (non-alternative)
    first_occurrences = (
        df.filter(
            (pl.col("variations").list.len() > 0)
            & ~pl.col("name").is_in(basic_lands)
        )
        .group_by("_printing_key")
        .agg(pl.col("uuid").first().alias("_first_uuid"))
    )

    df = df.join(first_occurrences, on="_printing_key", how="left")

    df = df.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0)
            & ~pl.col("name").is_in(basic_lands)
            & (pl.col("_first_uuid").is_not_null())
            & (pl.col("uuid") != pl.col("_first_uuid"))
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )

    return df.drop([
        "_base_name", "_face_key", "_group_uuids", "_group_numbers",
        "_printing_key", "_first_uuid"
    ])


# =============================================================================
# Post-Collect DataFrame Operations
# =============================================================================


def apply_manual_overrides(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply manual overrides from GLOBAL_CACHE.manual_overrides.

    Overrides are keyed by UUID and can override any field.
    Currently used for Final Fantasy meld cards' other_face_ids.
    """
    overrides = GLOBAL_CACHE.manual_overrides
    if not overrides:
        return df

    # Build override rows: [{uuid, field, value}, ...]
    override_data = []
    for uuid_key, fields in overrides.items():
        for field, value in fields.items():
            if not field.startswith("__"):  # Skip metadata
                override_data.append({"uuid": uuid_key, "field": field, "value": value})

    if not override_data:
        return df

    # For now, only handle otherFaceIds overrides (the only current use case)
    other_face_overrides = {
        row["uuid"]: row["value"]
        for row in override_data
        if row["field"] == "other_face_ids"
    }

    if other_face_overrides and "otherFaceIds" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("uuid").is_in(list(other_face_overrides.keys())))
            .then(
                pl.col("uuid").replace_strict(
                    other_face_overrides,
                    default=pl.col("otherFaceIds"),
                    return_dtype=pl.List(pl.String),
                )
            )
            .otherwise(pl.col("otherFaceIds"))
            .alias("otherFaceIds")
        )

    return df


def add_secret_lair_subsets(df: pl.DataFrame, set_code: str) -> pl.DataFrame:
    """
    Add subsets field for Secret Lair (SLD) cards.

    Maps collector numbers to their Secret Lair drop names via MTG Wiki data.
    Only applies to SLD set.
    """
    if set_code.upper() != "SLD":
        return df

    from .providers import MtgWikiProviderSecretLair

    relation_map = MtgWikiProviderSecretLair().download()
    if not relation_map:
        return df

    # Create lookup DataFrame
    subset_df = pl.DataFrame({
        "number": list(relation_map.keys()),
        "subsets": [[v] for v in relation_map.values()],  # Wrap in list
    })

    df = df.join(subset_df, on="number", how="left")

    return df


def add_meld_card_parts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add cardParts field for meld layout cards.

    Uses meld_triplets.json to determine the [top, bottom, melded] card parts.
    Meld cards don't have faceName (they're single-faced), so we use name.
    """
    import json

    # Only process if there are meld cards
    if "layout" not in df.columns:
        return df

    meld_cards = df.filter(pl.col("layout") == "meld")
    if meld_cards.is_empty():
        return df

    triplets_path = constants.RESOURCE_PATH / "meld_triplets.json"
    if not triplets_path.exists():
        return df

    with triplets_path.open(encoding="utf-8") as f:
        triplets = json.load(f)

    # Build name -> cardParts mapping
    # Each triplet is [card_a, card_b, melded_result]
    # Meld cards use name, not faceName (they're single-faced cards)
    name_to_parts = {}
    for triplet in triplets:
        for card_name in triplet:
            name_to_parts[card_name] = triplet

    if not name_to_parts:
        return df

    # Add cardParts column using name (meld cards don't have faceName)
    df = df.with_columns(
        pl.when(pl.col("layout") == "meld")
        .then(
            pl.col("name").replace_strict(
                name_to_parts,
                default=None,
                return_dtype=pl.List(pl.String),
            )
        )
        .otherwise(pl.lit(None))
        .alias("cardParts")
    )

    return df


def add_rebalanced_linkage(df: pl.DataFrame) -> pl.DataFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.

    Adds:
    - originalPrintings: UUIDs of the original card (on rebalanced cards)
    - rebalancedPrintings: UUIDs of the rebalanced version (on original cards)
    """
    if "name" not in df.columns or "uuid" not in df.columns:
        return df

    # Find rebalanced cards (names starting with "A-")
    rebalanced = df.filter(pl.col("name").str.starts_with("A-"))
    if rebalanced.is_empty():
        return df

    # Create mapping: original_name -> rebalanced UUIDs
    rebalanced_map = (
        rebalanced
        .with_columns(
            pl.col("name").str.replace("A-", "").alias("_original_name")
        )
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_rebalanced_uuids"))
    )

    # Create mapping: rebalanced_name -> original UUIDs
    original_names = rebalanced_map["_original_name"].to_list()
    originals = df.filter(
        pl.col("name").is_in(original_names) & ~pl.col("name").str.starts_with("A-")
    )

    original_map = (
        originals
        .group_by("name")
        .agg(pl.col("uuid").alias("_original_uuids"))
        .rename({"name": "_original_name"})
    )

    # Join rebalanced printings onto originals
    df = df.join(
        rebalanced_map,
        left_on="name",
        right_on="_original_name",
        how="left",
    ).rename({"_rebalanced_uuids": "rebalancedPrintings"})

    # Join original printings onto rebalanced cards
    df = df.with_columns(
        pl.col("name").str.replace("A-", "").alias("_lookup_name")
    )
    df = df.join(
        original_map,
        left_on="_lookup_name",
        right_on="_original_name",
        how="left",
    ).rename({"_original_uuids": "originalPrintings"})

    return df.drop("_lookup_name")


def link_foil_nonfoil_versions(df: pl.DataFrame, set_code: str) -> pl.DataFrame:
    """
    Link foil and non-foil versions that have different card details.

    Only applies to specific sets: CN2, FRF, ONS, 10E, UNH.
    Adds mtgjsonFoilVersionId and mtgjsonNonFoilVersionId to identifiers.
    """
    if set_code.upper() not in {"CN2", "FRF", "ONS", "10E", "UNH"}:
        return df

    if "identifiers" not in df.columns:
        return df

    # Group by illustration_id to find pairs
    illustration_groups = (
        df.select([
            pl.col("identifiers").struct.field("scryfallIllustrationId").alias("_ill_id"),
            pl.col("uuid"),
            pl.col("finishes"),
        ])
        .filter(pl.col("_ill_id").is_not_null())
        .group_by("_ill_id")
        .agg([
            pl.col("uuid"),
            pl.col("finishes"),
        ])
        .filter(pl.col("uuid").list.len() == 2)  # Only pairs
    )

    if illustration_groups.is_empty():
        return df

    # Build mapping for each card in a pair
    foil_version_map = {}
    nonfoil_version_map = {}

    for row in illustration_groups.iter_rows(named=True):
        uuids = row["uuid"]
        finishes_list = row["finishes"]

        if len(uuids) != 2 or len(finishes_list) != 2:
            continue

        uuid1, uuid2 = uuids[0], uuids[1]
        finish1, finish2 = finishes_list[0] or [], finishes_list[1] or []

        # Determine which is foil vs nonfoil
        is_foil1 = "nonfoil" not in finish1
        is_foil2 = "nonfoil" not in finish2

        if is_foil1 and not is_foil2:
            foil_version_map[uuid2] = uuid1
            nonfoil_version_map[uuid1] = uuid2
        elif is_foil2 and not is_foil1:
            foil_version_map[uuid1] = uuid2
            nonfoil_version_map[uuid2] = uuid1

    if not foil_version_map and not nonfoil_version_map:
        return df

    # Add to identifiers struct
    df = df.with_columns([
        pl.col("uuid")
        .replace_strict(foil_version_map, default=None, return_dtype=pl.String)
        .alias("_foil_version"),
        pl.col("uuid")
        .replace_strict(nonfoil_version_map, default=None, return_dtype=pl.String)
        .alias("_nonfoil_version"),
    ])

    df = df.with_columns(
        pl.col("identifiers").struct.with_fields([
            pl.col("_foil_version").alias("mtgjsonFoilVersionId"),
            pl.col("_nonfoil_version").alias("mtgjsonNonFoilVersionId"),
        ])
    ).drop(["_foil_version", "_nonfoil_version"])

    return df


def add_duel_deck_side(df: pl.DataFrame, set_code: str) -> pl.DataFrame:
    """
    Add duelDeck field for Duel Deck (DD*) and GS1 sets.

    Uses precomputed duel_deck_sides.json mapping.
    """
    import json

    if not (set_code.upper().startswith("DD") or set_code.upper() == "GS1"):
        return df

    sides_path = constants.RESOURCE_PATH / "duel_deck_sides.json"
    if not sides_path.exists():
        return df

    with sides_path.open(encoding="utf-8") as f:
        all_sides = json.load(f)

    set_sides = all_sides.get(set_code.upper())
    if not set_sides:
        return df

    # Add duelDeck column using lookup
    df = df.with_columns(
        pl.col("number")
        .replace_strict(set_sides, default=None, return_dtype=pl.String)
        .alias("duelDeck")
    )

    return df


def add_source_products(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add sourceProducts field using GitHubDataProvider.

    Maps card UUIDs to sealed product UUIDs where the card can be found.
    Returns struct with foil/nonfoil/etched product UUID lists.
    """
    return GLOBAL_CACHE.github.join_source_products(df, uuid_col="uuid")


def filter_out_tokens(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Separate tokens from main cards.

    Tokens are identified by:
    - layout in {"token", "double_faced_token", "emblem", "art_series"}
    - type == "Dungeon"
    - "Token" in type string

    Returns:
        Tuple of (cards_df, tokens_df) - cards without tokens, and the filtered tokens
    """
    TOKEN_LAYOUTS = {"token", "double_faced_token", "emblem", "art_series"}

    is_token = (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type") == "Dungeon")
        | pl.col("type").str.contains("Token")
    )

    tokens_df = df.filter(is_token)
    cards_df = df.filter(~is_token)

    return cards_df, tokens_df


def add_multiverse_bridge_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add Cardsphere and Deckbox IDs from MultiverseBridge data.

    Joins on scryfall_id (from identifiers) to add:
    - cardsphereId (non-foil)
    - cardsphereFoilId (foil)
    - deckboxId

    These get merged into the identifiers struct.
    """
    rosetta_cards = GLOBAL_CACHE.multiverse_bridge_cards
    if not rosetta_cards:
        LOGGER.debug("MultiverseBridge cache not loaded, skipping")
        return df

    # Build lookup DataFrame from rosetta data
    # Input format: {scryfall_id: [{cs_id, is_foil, deckbox_id}, ...]}
    records = []
    for scryfall_id, entries in rosetta_cards.items():
        cs_id = None
        cs_foil_id = None
        deckbox_id = None
        for entry in entries:
            if entry.get("is_foil"):
                cs_foil_id = str(entry.get("cs_id", "")) if entry.get("cs_id") else None
            else:
                cs_id = str(entry.get("cs_id", "")) if entry.get("cs_id") else None
            # deckbox_id is the same for foil/non-foil
            if entry.get("deckbox_id") and not deckbox_id:
                deckbox_id = str(entry["deckbox_id"])

        records.append({
            "_mb_scryfall_id": scryfall_id,
            "cardsphereId": cs_id,
            "cardsphereFoilId": cs_foil_id,
            "deckboxId": deckbox_id,
        })

    if not records:
        return df

    rosetta_df = pl.DataFrame(records)

    # Extract scryfall_id from identifiers for join
    df = df.with_columns(
        pl.col("identifiers").struct.field("scryfallId").alias("_scryfall_id_for_mb")
    )

    # Join
    df = df.join(rosetta_df, left_on="_scryfall_id_for_mb", right_on="_mb_scryfall_id", how="left")

    # The identifiers struct will be rebuilt in dataframe_to_card_objects
    # Just keep the columns for now
    return df.drop("_scryfall_id_for_mb")


# =============================================================================
# Purchase URLs
# =============================================================================

MTGJSON_LINKS_PREFIX = "https://mtgjson.com/links/"


def _hash_url_seed(seed_col: str) -> pl.Expr:
    """
    Generate MTGJSON redirect URL from seed string using SHA256.
    
    :param seed_col: Column containing the seed string
    :return: Expression producing the hashed URL
    """
    return (
        pl.lit(MTGJSON_LINKS_PREFIX) 
        + plh.col(seed_col).chash.sha256().str.slice(0, 16)
    )


def add_purchase_urls_struct(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build purchaseUrls struct from TCGPlayer and CardKingdom identifiers.

    TCGPlayer URLs are generated from tcgplayer_id + uuid.
    CardKingdom URLs are generated from CK URL path + uuid.
    CardMarket URLs are generated from mcmId + uuid + buffer + mcmMetaId.

    Note: This runs after select_output_columns, so we extract values from
    the identifiers struct rather than raw columns.
    """
    # Extract TCGPlayer IDs from identifiers struct
    tcg_product_id = pl.col("identifiers").struct.field("tcgplayerProductId")
    tcg_etched_id = pl.col("identifiers").struct.field("tcgplayerEtchedProductId")

    # TCGPlayer seed: product_id + uuid
    df = df.with_columns([
        pl.when(tcg_product_id.is_not_null())
        .then(pl.concat_str([tcg_product_id.cast(pl.String), pl.col("uuid")]))
        .otherwise(pl.lit(None))
        .alias("_tcg_seed"),

        pl.when(tcg_etched_id.is_not_null())
        .then(pl.concat_str([tcg_etched_id.cast(pl.String), pl.col("uuid")]))
        .otherwise(pl.lit(None))
        .alias("_tcg_etched_seed"),
    ])

    # CardKingdom seeds - use internal columns if they exist, otherwise skip
    ck_cols = []
    if "cardKingdomUrl" in df.columns:
        ck_cols.append(
            pl.when(pl.col("cardKingdomUrl").is_not_null())
            .then(pl.concat_str([pl.col("cardKingdomUrl"), pl.col("uuid")]))
            .otherwise(pl.lit(None))
            .alias("_ck_seed")
        )
    else:
        ck_cols.append(pl.lit(None).alias("_ck_seed"))

    if "cardKingdomFoilUrl" in df.columns:
        ck_cols.append(
            pl.when(pl.col("cardKingdomFoilUrl").is_not_null())
            .then(pl.concat_str([pl.col("cardKingdomFoilUrl"), pl.col("uuid")]))
            .otherwise(pl.lit(None))
            .alias("_ck_foil_seed")
        )
    else:
        ck_cols.append(pl.lit(None).alias("_ck_foil_seed"))

    if "cardKingdomEtchedUrl" in df.columns:
        ck_cols.append(
            pl.when(pl.col("cardKingdomEtchedUrl").is_not_null())
            .then(pl.concat_str([pl.col("cardKingdomEtchedUrl"), pl.col("uuid")]))
            .otherwise(pl.lit(None))
            .alias("_ck_etched_seed")
        )
    else:
        ck_cols.append(pl.lit(None).alias("_ck_etched_seed"))

    df = df.with_columns(ck_cols)

    # CardMarket seed: mcmId + uuid + buffer + mcmMetaId
    # Buffer is a constant from constants.py
    from . import constants
    mcm_buffer = getattr(constants, "CARD_MARKET_BUFFER", "")

    mcm_meta_id = pl.col("_mcmMetaId") if "_mcmMetaId" in df.columns else pl.lit("")

    df = df.with_columns(
        pl.when(
            pl.col("identifiers").struct.field("mcmId").is_not_null()
        )
        .then(
            pl.concat_str([
                pl.col("identifiers").struct.field("mcmId"),
                pl.col("uuid"),
                pl.lit(mcm_buffer),
                mcm_meta_id.fill_null(""),
            ])
        )
        .otherwise(pl.lit(None))
        .alias("_mcm_seed")
    )
    
    # Build purchaseUrls struct
    return df.with_columns(
        pl.struct(
            pl.when(pl.col("_tcg_seed").is_not_null())
            .then(_hash_url_seed("_tcg_seed"))
            .otherwise(pl.lit(None))
            .alias("tcgplayer"),
            
            pl.when(pl.col("_tcg_etched_seed").is_not_null())
            .then(_hash_url_seed("_tcg_etched_seed"))
            .otherwise(pl.lit(None))
            .alias("tcgplayerEtched"),
            
            pl.when(pl.col("_ck_seed").is_not_null())
            .then(_hash_url_seed("_ck_seed"))
            .otherwise(pl.lit(None))
            .alias("cardKingdom"),
            
            pl.when(pl.col("_ck_foil_seed").is_not_null())
            .then(_hash_url_seed("_ck_foil_seed"))
            .otherwise(pl.lit(None))
            .alias("cardKingdomFoil"),
            
            pl.when(pl.col("_ck_etched_seed").is_not_null())
            .then(_hash_url_seed("_ck_etched_seed"))
            .otherwise(pl.lit(None))
            .alias("cardKingdomEtched"),
            
            pl.when(pl.col("_mcm_seed").is_not_null())
            .then(_hash_url_seed("_mcm_seed"))
            .otherwise(pl.lit(None))
            .alias("cardmarket"),
        ).alias("purchaseUrls")
    ).drop([
        "_tcg_seed", "_tcg_etched_seed", 
        "_ck_seed", "_ck_foil_seed", "_ck_etched_seed",
        "_mcm_seed"
    ], strict=False)


# =============================================================================
# EDHREC Saltiness
# =============================================================================


def join_edhrec_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Join EDHREC saltiness and rank by oracle_id."""
    edhrec_df = GLOBAL_CACHE.salt_df
    
    if edhrec_df is None or edhrec_df.is_empty():
        return lf.with_columns([
            pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
        ])
    
    return lf.join(
        edhrec_df.lazy().select(["oracle_id", "edhrecSaltiness"]),
        on="oracle_id",
        how="left",
    )

# =============================================================================
# Gatherer Original Text/Type
# =============================================================================


def join_gatherer_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Join Gatherer original text and type by multiverse ID.
    """
    gatherer_map = GLOBAL_CACHE.gatherer_map
    
    if not gatherer_map:
        return lf.with_columns([
            pl.lit(None).cast(pl.String).alias("originalText"),
            pl.lit(None).cast(pl.String).alias("originalType"),
        ])
    
    # Build lookup DataFrame from gatherer_map
    # gatherer_map: {multiverse_id: [{original_text, original_types}, ...]}
    rows = []
    for mv_id, entries in gatherer_map.items():
        if entries:
            entry = entries[0]  # Take first entry
            rows.append({
                "multiverse_id": str(mv_id),
                "originalText": entry.get("original_text"),
                "originalType": entry.get("original_types"),
            })
    
    if not rows:
        return lf.with_columns([
            pl.lit(None).cast(pl.String).alias("originalText"),
            pl.lit(None).cast(pl.String).alias("originalType"),
        ])
    
    gatherer_df = pl.DataFrame(rows)
    
    # Extract multiverse_id from identifiers for join
    lf = lf.with_columns(
        pl.col("identifiers").struct.field("multiverseId").alias("_mv_id_lookup")
    )
    
    lf = lf.join(
        gatherer_df.lazy(),
        left_on="_mv_id_lookup",
        right_on="multiverse_id",
        how="left",
    )
    
    return lf.drop("_mv_id_lookup")


# =============================================================================
# Related Cards (Spellbook)
# =============================================================================


def add_related_cards_struct(lf: pl.LazyFrame, set_type: str = "") -> pl.LazyFrame:
    """
    Build relatedCards struct with spellbook data for Alchemy cards.
    
    reverseRelated is handled separately in add_reverse_related().
    """
    # Only Alchemy sets have spellbook data
    if "alchemy" not in set_type.lower():
        return lf.with_columns(pl.lit(None).alias("relatedCards"))
    
    # Get spellbook data from Scryfall provider
    scryfall = ScryfallProvider()
    alchemy_cards = scryfall.get_alchemy_cards_with_spellbooks()
    
    if not alchemy_cards:
        return lf.with_columns(pl.lit(None).alias("relatedCards"))
    
    # Build spellbook lookup
    spellbook_data = {}
    for card_name in alchemy_cards:
        spellbook = scryfall.get_card_names_in_spellbook(card_name)
        if spellbook:
            spellbook_data[card_name] = sorted(spellbook)
    
    if not spellbook_data:
        return lf.with_columns(pl.lit(None).alias("relatedCards"))
    
    # Create lookup DataFrame
    spellbook_df = pl.DataFrame({
        "name": list(spellbook_data.keys()),
        "spellbook": list(spellbook_data.values()),
    })
    
    lf = lf.join(
        spellbook_df.lazy(),
        on="name",
        how="left",
    )
    
    # Build relatedCards struct only when spellbook exists
    return lf.with_columns(
        pl.when(pl.col("spellbook").is_not_null())
        .then(pl.struct(spellbook=pl.col("spellbook")))
        .otherwise(pl.lit(None))
        .alias("relatedCards")
    ).drop("spellbook", strict=False)


# =============================================================================
# CardMarket Identifiers
# =============================================================================


def join_cardmarket_ids(lf: pl.LazyFrame, set_name: str) -> pl.LazyFrame:
    """
    Join CardMarket mcmId and mcmMetaId by card name/number.
    
    MCM data is matched by:
    1. Exact card name match
    2. Face name match for split cards
    3. Collector number match for disambiguation
    """
    mcm_id = GLOBAL_CACHE.cardmarket.get_set_id(set_name)
    if not mcm_id:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_mcmMetaId"))
    
    mcm_cards = GLOBAL_CACHE.cardmarket.get_mkm_cards(mcm_id)
    if not mcm_cards:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_mcmMetaId"))
    
    # Build lookup: (lowercase_name, number) -> (mcmId, mcmMetaId)
    # MCM uses lowercase keys
    rows = []
    for name_key, variants in mcm_cards.items():
        for variant in variants:
            for number in variant.get("number", ["*"]):
                rows.append({
                    "_mcm_name": name_key,
                    "_mcm_number": number,
                    "_mcmId": str(variant.get("idProduct", "")),
                    "_mcmMetaId": str(variant.get("idMetaproduct", "")),
                })
    
    if not rows:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("_mcmMetaId"))
    
    mcm_df = pl.DataFrame(rows)
    
    # Prepare join keys
    lf = lf.with_columns([
        pl.col("name").str.to_lowercase().alias("_name_lower"),
        pl.coalesce(
            pl.col("faceName").str.to_lowercase(),
            pl.col("name").str.to_lowercase()
        ).alias("_face_name_lower"),
    ])
    
    # Try exact name + number match first
    lf = lf.join(
        mcm_df.lazy(),
        left_on=["_name_lower", "number"],
        right_on=["_mcm_name", "_mcm_number"],
        how="left",
        suffix="_exact"
    )
    
    # Fall back to name-only match if no exact match
    lf = lf.join(
        mcm_df.filter(pl.col("_mcm_number") == "*").lazy(),
        left_on="_name_lower",
        right_on="_mcm_name",
        how="left",
        suffix="_fallback"
    )
    
    # Coalesce results
    lf = lf.with_columns([
        pl.coalesce(pl.col("_mcmId"), pl.col("_mcmId_fallback")).alias("_mcmId_final"),
        pl.coalesce(pl.col("_mcmMetaId"), pl.col("_mcmMetaId_fallback")).alias("_mcmMetaId"),
    ])
    
    # Update identifiers struct with mcmMetaId
    lf = lf.with_columns(
        pl.col("identifiers").struct.with_fields(
            pl.col("_mcmId_final").alias("mcmMetaId")
        )
    )
    
    return lf.drop([
        "_name_lower", "_face_name_lower",
        "_mcmId", "_mcmId_exact", "_mcmId_fallback", "_mcmId_final",
        "_mcmMetaId_exact", "_mcmMetaId_fallback",
        "_mcm_name", "_mcm_number"
    ], strict=False)


# =============================================================================
# Alternative Deck Limit
# =============================================================================


def add_alternative_deck_limit(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Mark cards that don't have the standard 4-copy deck limit.
    
    Uses Scryfall's cards_without_limits list.
    """
    unlimited_cards = ScryfallProvider().cards_without_limits
    
    if not unlimited_cards:
        return lf
    
    return lf.with_columns(
        pl.when(pl.col("name").is_in(list(unlimited_cards)))
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("hasAlternativeDeckLimit")
    )


# =============================================================================
# Funny Set Detection
# =============================================================================


def add_is_funny(lf: pl.LazyFrame, set_type: str, set_code: str) -> pl.LazyFrame:
    """
    Mark cards from "funny" sets (Un-sets, etc.).
    
    For UNF specifically, only acorn-stamped cards are funny.
    """
    if set_type != "funny":
        return lf
    
    if set_code.upper() == "UNF":
        # UNF: only acorn security stamp is funny
        return lf.with_columns(
            pl.when(pl.col("securityStamp") == "acorn")
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
            .alias("isFunny")
        )
    else:
        # All other funny sets: everything is funny
        return lf.with_columns(pl.lit(True).alias("isFunny"))


# =============================================================================
# Timeshifted Detection
# =============================================================================


def add_is_timeshifted(lf: pl.LazyFrame, set_code: str) -> pl.LazyFrame:
    """
    Mark timeshifted cards (future frame or TSB set).
    """
    return lf.with_columns(
        pl.when(
            (pl.col("frameVersion") == "future") | (pl.lit(set_code.upper()) == "TSB")
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isTimeshifted")
    )


# =============================================================================
# Complete Pipeline Stage
# =============================================================================


def add_final_fields(
    lf: pl.LazyFrame,
    set_code: str,
    set_name: str,
    set_type: str,
) -> pl.LazyFrame:
    """
    Add all remaining fields that require external data joins.
    
    This is a convenience function that chains all the final stages.
    Call after UUID generation and before output selection.
    """
    return (
        lf
        .pipe(join_edhrec_data)
        .pipe(join_gatherer_data)
        .pipe(join_cardmarket_ids, set_name=set_name)
        .pipe(add_purchase_urls_struct)
        .pipe(add_related_cards_struct, set_type=set_type)
        .pipe(add_alternative_deck_limit)
        .pipe(add_is_funny, set_type=set_type, set_code=set_code)
        .pipe(add_is_timeshifted, set_code=set_code)
    )


def add_token_signatures(df: pl.DataFrame, set_name: str, set_type: str, set_code: str) -> pl.DataFrame:
    """
    Add signature field and "signed" finish for Art Series and memorabilia cards.

    For Art Series (except MH1): signature = artist
    For memorabilia with gold border: signature from world_championship_signatures.json

    :param df: DataFrame with card data
    :param set_name: Name of the set (e.g., "Modern Horizons Art Series")
    :param set_type: Type of the set (e.g., "memorabilia")
    :param set_code: Set code (e.g., "AMH1")
    :return: DataFrame with signature and updated finishes
    """
    import json

    # Art Series sets (except MH1) - signature = artist
    if set_name.endswith("Art Series") and set_code.upper() != "MH1":
        df = df.with_columns([
            pl.col("artist").alias("signature"),
            pl.when(~pl.col("finishes").list.contains("signed"))
            .then(pl.col("finishes").list.concat(pl.lit(["signed"])))
            .otherwise(pl.col("finishes"))
            .alias("finishes"),
        ])
        return df

    # Memorabilia sets - signature from world_championship_signatures.json for gold border
    if set_type == "memorabilia":
        signatures_path = constants.RESOURCE_PATH / "world_championship_signatures.json"
        if not signatures_path.exists():
            return df

        with signatures_path.open(encoding="utf-8") as f:
            signatures_by_set = json.load(f)

        set_signatures = signatures_by_set.get(set_code.upper())
        if not set_signatures:
            return df

        # Extract prefix and number parts using regex
        # Pattern: ^([^0-9]+)([0-9]+)(.*)
        # Group 1: letters before numbers (prefix)
        # Group 2: the number
        # Group 3: suffix (e.g., "b")
        df = df.with_columns([
            pl.col("number").str.extract(r"^([^0-9]+)", 1).alias("_num_prefix"),
            pl.col("number").str.extract(r"^[^0-9]+([0-9]+)", 1).alias("_num_digits"),
            pl.col("number").str.extract(r"^[^0-9]+[0-9]+(.*)", 1).alias("_num_suffix"),
        ])

        # Apply signature lookup for gold border cards
        # Skip cards where number is "0b" (prefix + "0" + "b")
        df = df.with_columns(
            pl.when(
                (pl.col("borderColor") == "gold")
                & pl.col("_num_prefix").is_not_null()
                & ~((pl.col("_num_digits") == "0") & (pl.col("_num_suffix") == "b"))
            )
            .then(
                pl.col("_num_prefix").replace_strict(
                    set_signatures,
                    default=None,
                    return_dtype=pl.String,
                )
            )
            .otherwise(pl.lit(None))
            .alias("signature")
        )

        # Clean up temp columns
        df = df.drop(["_num_prefix", "_num_digits", "_num_suffix"])

        # Add "signed" to finishes where signature exists
        df = df.with_columns(
            pl.when(
                pl.col("signature").is_not_null()
                & ~pl.col("finishes").list.contains("signed")
            )
            .then(pl.col("finishes").list.concat(pl.lit(["signed"])))
            .otherwise(pl.col("finishes"))
            .alias("finishes")
        )

        return df

    return df


def add_orientations(df: pl.DataFrame, set_code: str, set_name: str) -> pl.DataFrame:
    """
    Add orientation field for Art Series tokens.

    Gets orientation map from ScryfallProviderOrientationDetector and joins by scryfall_id.

    :param df: DataFrame with card data
    :param set_code: Set code
    :param set_name: Set name to check if Art Series
    :return: DataFrame with orientation column
    """
    if "Art Series" not in set_name:
        return df

    from .providers import ScryfallProviderOrientationDetector

    orientation_map = ScryfallProviderOrientationDetector().get_uuid_to_orientation_map(
        set_code
    )

    if not orientation_map:
        return df

    # Create lookup DataFrame
    orientation_df = pl.DataFrame({
        "_scryfall_id": list(orientation_map.keys()),
        "orientation": list(orientation_map.values()),
    })

    # Extract scryfall_id from identifiers for join
    df = df.with_columns(
        pl.col("identifiers").struct.field("scryfallId").alias("_scryfall_id_for_orient")
    )

    # Join and clean up
    df = df.join(
        orientation_df,
        left_on="_scryfall_id_for_orient",
        right_on="_scryfall_id",
        how="left",
    ).drop("_scryfall_id_for_orient")

    return df


# =============================================================================
# Output Column Selection
# =============================================================================


def select_output_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Select and rename columns to camelCase for JSON output.

    This is the final stage before collection. Internal columns (prefixed with _)
    are preserved for post-collect operations and dropped later.
    """
    # Get schema to check which columns exist
    schema = lf.collect_schema()

    # Core columns that should always exist
    core_cols = ALL_CARD_FIELDS

    # Optional columns that may be added during processing
    optional_cols = ["signature", "orientation"]

    # Internal columns needed for post-collect operations (dropped later)
    internal_cols = [
        "_row_id",
        # CardKingdom URLs for purchase URL generation
        "cardKingdomUrl", "cardKingdomFoilUrl", "cardKingdomEtchedUrl",
        # CardMarket meta ID for purchase URL generation
        "_mcmMetaId",
    ]

    # Build selection list
    select_cols = [pl.col(c) for c in core_cols if c in schema]
    for col in optional_cols:
        if col in schema:
            select_cols.append(pl.col(col))
    for col in internal_cols:
        if col in schema:
            select_cols.append(pl.col(col))

    return lf.select(select_cols)


def select_card_set_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Select and order columns for CardSet output."""
    fields = ALL_CARD_FIELDS - CARD_SET_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)


def select_card_token_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Select and order columns for CardToken output."""
    fields = ALL_CARD_FIELDS - TOKEN_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)


def select_card_atomic_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Select and order columns for CardAtomic output."""
    fields = ALL_CARD_FIELDS - ATOMIC_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)


def select_card_deck_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Select and order columns for CardDeck output."""
    fields = (ALL_CARD_FIELDS - CARD_DECK_EXCLUDE) | {"count", "isFoil"}
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)


def prepare_for_json_output(df: pl.DataFrame) -> pl.DataFrame:
    """
    Prepare DataFrame for direct JSON serialization via df.write_json().

    This function:
    1. Drops internal columns (_row_id, etc.)
    2. Consolidates MultiverseBridge IDs into identifiers struct
    3. Ensures schema matches expected JSON output exactly

    After this, use: df.write_json(file, row_oriented=True)

    :param df: DataFrame from the card pipeline
    :return: DataFrame ready for JSON output
    """
    # Drop internal columns
    internal_cols = [c for c in df.columns if c.startswith("_")]
    if internal_cols:
        df = df.drop(internal_cols)

    # Consolidate MultiverseBridge IDs into identifiers struct if present
    mb_cols = ["cardsphereId", "cardsphereFoilId", "deckboxId"]
    has_mb_cols = any(c in df.columns for c in mb_cols)

    if has_mb_cols and "identifiers" in df.columns:
        # Rebuild identifiers struct with MB IDs included
        df = df.with_columns(
            pl.struct(
                pl.col("identifiers").struct.field("scryfallId").alias("scryfallId"),
                pl.col("identifiers").struct.field("scryfallOracleId").alias("scryfallOracleId"),
                pl.col("identifiers").struct.field("scryfallIllustrationId").alias("scryfallIllustrationId"),
                pl.col("identifiers").struct.field("scryfallCardBackId").alias("scryfallCardBackId"),
                pl.col("identifiers").struct.field("mcmId").alias("mcmId"),
                pl.col("identifiers").struct.field("mtgArenaId").alias("mtgArenaId"),
                pl.col("identifiers").struct.field("mtgoId").alias("mtgoId"),
                pl.col("identifiers").struct.field("mtgoFoilId").alias("mtgoFoilId"),
                pl.col("identifiers").struct.field("multiverseId").alias("multiverseId"),
                pl.col("identifiers").struct.field("tcgplayerProductId").alias("tcgplayerProductId"),
                pl.col("identifiers").struct.field("tcgplayerEtchedProductId").alias("tcgplayerEtchedProductId"),
                pl.col("identifiers").struct.field("cardKingdomId").alias("cardKingdomId"),
                pl.col("identifiers").struct.field("cardKingdomFoilId").alias("cardKingdomFoilId"),
                pl.col("identifiers").struct.field("cardKingdomEtchedId").alias("cardKingdomEtchedId"),
                pl.col("identifiers").struct.field("mtgjsonV4Id").alias("mtgjsonV4Id"),
                # Add MultiverseBridge IDs
                pl.col("cardsphereId") if "cardsphereId" in df.columns else pl.lit(None).alias("cardsphereId"),
                pl.col("cardsphereFoilId") if "cardsphereFoilId" in df.columns else pl.lit(None).alias("cardsphereFoilId"),
                pl.col("deckboxId") if "deckboxId" in df.columns else pl.lit(None).alias("deckboxId"),
            ).alias("identifiers")
        )
        # Drop the now-consolidated columns
        df = df.drop([c for c in mb_cols if c in df.columns])

    return df


# =============================================================================
# DataFrame to MtgjsonCardObject Conversion
# =============================================================================

def dataframe_to_card_objects(
    cards_df: pl.DataFrame, set_code: str, is_token: bool = False
) -> List["MtgjsonCardObject"]:  # type: ignore # noqa: F821
    """
    Convert a DataFrame of card data to a list of MtgjsonCardObject instances.

    This bridges the vectorized pipeline output with the existing object-based
    code for compatibility with downstream processing functions.

    :param cards_df: DataFrame with card data from the vectorized pipeline
    :param set_code: Set code for the cards
    :param is_token: Whether these are token cards
    :return: List of MtgjsonCardObject instances
    
    :note:
        We currently serialze through MtgjsonCardObjects because
        the existing codebase relies on these classes for post-processing.
        The current flow is:
            1. Build a DataFrame with all the card data
            2. Iterate through rows in this function to convert to Python objects
            3. Serialize those objects to JSON
        TODO:
        Ideally we would skip the MtgjsonCardObject classes entirely:
            1. Move all post-processing into the DataFrame pipeline
            2. Ensure the DataFrame schema matches the expected JSON schema exactly
            3. Use df.write_json() to serialize directly.
    """
    from .classes.mtgjson_card import MtgjsonCardObject
    from .classes.mtgjson_foreign_data import MtgjsonForeignDataObject
    from .classes.mtgjson_game_formats import MtgjsonGameFormatsObject
    from .classes.mtgjson_identifiers import MtgjsonIdentifiersObject
    from .classes.mtgjson_leadership_skills import MtgjsonLeadershipSkillsObject
    from .classes.mtgjson_legalities import MtgjsonLegalitiesObject
    from .classes.mtgjson_rulings import MtgjsonRulingObject

    cards: List[MtgjsonCardObject] = []

    for row in cards_df.iter_rows(named=True):
        card = MtgjsonCardObject(is_token=is_token)

        # Basic fields
        card.uuid = row.get("uuid", "")
        card.name = row.get("name", "")
        card.ascii_name = row.get("asciiName")
        card.face_name = row.get("faceName")
        card.set_code = row.get("setCode", set_code.upper())
        card.number = row.get("number", "")
        card.side = row.get("side")
        card.layout = row.get("layout", "")
        card.type = row.get("type", "")
        card.supertypes = row.get("supertypes") or []
        card.types = row.get("types") or []
        card.subtypes = row.get("subtypes") or []
        card.keywords = row.get("keywords") or []

        # Mana info
        card.mana_cost = row.get("manaCost") or ""
        card.mana_value = row.get("manaValue") or 0.0
        card.converted_mana_cost = row.get("convertedManaCost") or 0.0
        # Only set face mana value for multi-face cards (split, transform, etc.)
        layout = row.get("layout") or ""
        if layout in ("split", "flip", "transform", "modal_dfc", "meld", "adventure", "reversible_card"):
            card.face_mana_value = row.get("faceManaValue") or 0.0
            card.face_converted_mana_cost = row.get("faceManaValue") or 0.0
        card.colors = row.get("colors") or []
        card.color_identity = row.get("colorIdentity") or []

        # Text fields
        card.text = row.get("text") or ""
        card.flavor_text = row.get("flavorText")

        # Stats
        card.power = row.get("power") or ""
        card.toughness = row.get("toughness") or ""
        card.loyalty = row.get("loyalty")
        card.defense = row.get("defense")
        card.hand = row.get("hand")
        card.life = row.get("life")

        # Appearance
        card.rarity = row.get("rarity", "")
        card.artist = row.get("artist") or ""
        card.artist_ids = row.get("artistIds")
        card.border_color = row.get("borderColor", "")
        card.frame_version = row.get("frameVersion", "")
        card.frame_effects = row.get("frameEffects") or []
        card.security_stamp = row.get("securityStamp")
        card.watermark = row.get("watermark")
        card.finishes = row.get("finishes") or []

        # Boolean flags
        card.has_foil = row.get("hasFoil")
        card.has_non_foil = row.get("hasNonFoil")
        card.has_content_warning = row.get("hasContentWarning")
        card.is_full_art = row.get("isFullArt")
        card.is_online_only = row.get("isOnlineOnly")
        card.is_oversized = row.get("isOversized")
        card.is_promo = row.get("isPromo")
        card.is_reprint = row.get("isReprint")
        card.is_reserved = row.get("isReserved")
        card.is_story_spotlight = row.get("isStorySpotlight")
        card.is_textless = row.get("isTextless")
        card.is_game_changer = row.get("isGameChanger")

        # Lists
        card.promo_types = row.get("promoTypes") or []
        card.booster_types = row.get("boosterTypes") or []
        reverse_related = row.get("reverseRelated") or []
        if reverse_related:
            card.reverse_related = reverse_related
        card.attraction_lights = row.get("attractionLights")
        card.edhrec_rank = row.get("edhrecRank")

        # Printed variants
        card.flavor_name = row.get("flavorName")
        card.printed_name = row.get("printedName")
        card.printed_type = row.get("printedType")
        card.printed_text = row.get("printedText")
        card.original_release_date = row.get("originalReleaseDate")
        card.language = row.get("language", "English")

        # Identifiers struct
        identifiers_data = row.get("identifiers")
        if identifiers_data and isinstance(identifiers_data, dict):
            card.identifiers = MtgjsonIdentifiersObject()
            card.identifiers.scryfall_id = identifiers_data.get("scryfallId")
            card.identifiers.scryfall_oracle_id = identifiers_data.get("scryfallOracleId")
            card.identifiers.scryfall_illustration_id = identifiers_data.get(
                "scryfallIllustrationId"
            )
            card.identifiers.scryfall_card_back_id = identifiers_data.get(
                "scryfallCardBackId"
            )
            card.identifiers.mcm_id = identifiers_data.get("mcmId")
            card.identifiers.mtg_arena_id = identifiers_data.get("mtgArenaId")
            card.identifiers.mtgo_id = identifiers_data.get("mtgoId")
            card.identifiers.mtgo_foil_id = identifiers_data.get("mtgoFoilId")
            card.identifiers.multiverse_id = identifiers_data.get("multiverseId")
            card.identifiers.tcgplayer_product_id = identifiers_data.get(
                "tcgplayerProductId"
            )
            card.identifiers.tcgplayer_etched_product_id = identifiers_data.get(
                "tcgplayerEtchedProductId"
            )
            # Card Kingdom IDs
            card.identifiers.card_kingdom_id = identifiers_data.get("cardKingdomId")
            card.identifiers.card_kingdom_foil_id = identifiers_data.get("cardKingdomFoilId")
            card.identifiers.card_kingdom_etched_id = identifiers_data.get("cardKingdomEtchedId")
            # MTGJSON v4 ID
            card.identifiers.mtgjson_v4_id = identifiers_data.get("mtgjsonV4Id")

            # MultiverseBridge IDs (from separate columns, not identifiers struct)
            cardsphere_id = row.get("cardsphereId")
            if cardsphere_id:
                card.identifiers.cardsphere_id = cardsphere_id
            cardsphere_foil_id = row.get("cardsphereFoilId")
            if cardsphere_foil_id:
                card.identifiers.cardsphere_foil_id = cardsphere_foil_id
            deckbox_id = row.get("deckboxId")
            if deckbox_id:
                card.identifiers.deckbox_id = deckbox_id

        # Legalities struct
        legalities_data = row.get("legalities")
        if legalities_data and isinstance(legalities_data, dict):
            card.legalities = MtgjsonLegalitiesObject()
            for fmt, status in legalities_data.items():
                if status:
                    setattr(card.legalities, fmt, status)

        # Availability struct -> object
        availability_data = row.get("availability")
        if availability_data and isinstance(availability_data, dict):
            card.availability = MtgjsonGameFormatsObject()
            card.availability.arena = availability_data.get("arena", False)
            card.availability.mtgo = availability_data.get("mtgo", False)
            card.availability.paper = availability_data.get("paper", False)
            card.availability.shandalar = availability_data.get("shandalar", False)
            card.availability.dreamcast = availability_data.get("dreamcast", False)

        # Leadership skills struct
        leadership_data = row.get("leadershipSkills")
        if leadership_data and isinstance(leadership_data, dict):
            card.leadership_skills = MtgjsonLeadershipSkillsObject(
                brawl=leadership_data.get("brawl", False),
                commander=leadership_data.get("commander", False),
                oathbreaker=leadership_data.get("oathbreaker", False),
            )

        # Printings and rulings
        card.printings = row.get("printings") or []

        rulings_data = row.get("rulings")
        if rulings_data and isinstance(rulings_data, list):
            card.rulings = []
            for ruling in rulings_data:
                if isinstance(ruling, dict):
                    ruling_obj = MtgjsonRulingObject(
                        date=ruling.get("date", ""),
                        text=ruling.get("text", ""),
                    )
                    card.rulings.append(ruling_obj)

        # Foreign data
        foreign_data_list = row.get("foreignData")
        if foreign_data_list and isinstance(foreign_data_list, list):
            card.foreign_data = []
            for entry in foreign_data_list:
                if isinstance(entry, dict) and entry.get("name"):
                    fd = MtgjsonForeignDataObject()
                    fd.language = entry.get("language", "")
                    fd.uuid = entry.get("uuid", "")
                    fd.identifiers.scryfall_id = entry.get("scryfall_id")
                    multiverse_id = entry.get("multiverse_id")
                    if multiverse_id:
                        fd.multiverse_id = multiverse_id
                        fd.identifiers.multiverse_id = str(multiverse_id)
                    fd.name = entry.get("name")
                    fd.text = entry.get("text")
                    fd.flavor_text = entry.get("flavor_text")
                    fd.type = entry.get("type")
                    fd.face_name = entry.get("face_name")
                    card.foreign_data.append(fd)
        else:
            card.foreign_data = []

        # Other face IDs (populated by add_other_face_ids DataFrame operation)
        card.other_face_ids = row.get("otherFaceIds") or []

        # Variations and alternative status (populated by add_variations)
        variations = row.get("variations")
        if variations:
            card.variations = variations
        card.is_alternative = row.get("isAlternative")

        # Meld card parts (populated by add_meld_card_parts)
        card_parts = row.get("cardParts")
        if card_parts:
            card.card_parts = card_parts

        # Rebalanced linkage (populated by add_rebalanced_linkage)
        rebalanced_printings = row.get("rebalancedPrintings")
        if rebalanced_printings:
            card.rebalanced_printings = rebalanced_printings
        original_printings = row.get("originalPrintings")
        if original_printings:
            card.original_printings = original_printings

        # Secret Lair subsets (populated by add_secret_lair_subsets)
        subsets = row.get("subsets")
        if subsets:
            card.subsets = subsets

        # Duel Deck side (populated by add_duel_deck_side)
        duel_deck = row.get("duelDeck")
        if duel_deck:
            card.duel_deck = duel_deck

        # Source products (populated by add_source_products)
        source_products = row.get("source_products")
        if source_products and isinstance(source_products, dict):
            # Filter out None/empty values
            filtered = {k: v for k, v in source_products.items() if v}
            if filtered:
                card.source_products = filtered

        # Token-specific fields (populated by add_token_signatures and add_orientations)
        signature = row.get("signature")
        if signature:
            card.signature = signature

        orientation = row.get("orientation")
        if orientation:
            card.orientation = orientation

        cards.append(card)

    return cards
