import json
import pathlib
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Optional
import unicodedata
import numpy as np
import orjson
import polars as pl
import polars_hash as plh
from mtgjson5 import constants
from mtgjson5.cache_builder import GLOBAL_CACHE
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.models.schema.scryfall import CardFace
from mtgjson5.models.projections.mtgjson import ALL_CARD_FIELDS, ATOMIC_EXCLUDE, CARD_DECK_EXCLUDE, CARD_SET_EXCLUDE, TOKEN_EXCLUDE
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.providers.scryfall.orientation_detector import ScryfallProviderOrientationDetector
from mtgjson5.utils import LOGGER
from mtgjson5.uuid_generator import compute_v4_uuid_from_struct, uuid5_batch
from mtgjson5.categoricals import discover_categoricals, DynamicCategoricals


@dataclass
class PipelineContext:
    """
    Container for all lookup data needed by the card pipeline.

    Allows pipeline functions to be tested with smaller, controlled datasets
    rather than always pulling from GLOBAL_CACHE.
    """
    # Core DataFrames
    cards_df: Optional[pl.LazyFrame] = None
    sets_df: Optional[pl.DataFrame] = None

    # Lookup DataFrames
    card_kingdom_df: Optional[pl.DataFrame] = None
    mcm_lookup_df: Optional[pl.DataFrame] = None
    printings_df: Optional[pl.DataFrame] = None
    rulings_df: Optional[pl.DataFrame] = None
    salt_df: Optional[pl.DataFrame] = None
    spellbook_df: Optional[pl.DataFrame] = None
    sld_subsets_df: Optional[pl.DataFrame] = None
    uuid_cache_df: Optional[pl.DataFrame] = None

    # Dict lookups
    gatherer_map: dict = field(default_factory=dict)
    meld_triplets: dict = field(default_factory=dict)
    manual_overrides: dict = field(default_factory=dict)
    multiverse_bridge_cards: dict = field(default_factory=dict)

    # Provider accessors
    standard_legal_sets: set[str] = field(default_factory=set)
    unlimited_cards: set[str] = field(default_factory=set)

    # Categoricals
    categoricals: Optional[DynamicCategoricals] = None

    # GitHub data
    card_to_products_df: Optional[pl.DataFrame] = None

    @classmethod
    def from_global_cache(cls) -> "PipelineContext":
        """Create a PipelineContext from the global cache."""
        return cls(
            cards_df=GLOBAL_CACHE.cards_df,
            sets_df=GLOBAL_CACHE.sets_df,
            card_kingdom_df=GLOBAL_CACHE.card_kingdom_df,
            mcm_lookup_df=GLOBAL_CACHE.mcm_lookup_df,
            printings_df=GLOBAL_CACHE.printings_df,
            rulings_df=GLOBAL_CACHE.rulings_df,
            salt_df=GLOBAL_CACHE.salt_df,
            spellbook_df=GLOBAL_CACHE.spellbook_df,
            sld_subsets_df=GLOBAL_CACHE.sld_subsets_df,
            uuid_cache_df=GLOBAL_CACHE.uuid_cache_df,
            gatherer_map=GLOBAL_CACHE.gatherer_map,
            meld_triplets=GLOBAL_CACHE.meld_triplets,
            manual_overrides=GLOBAL_CACHE.manual_overrides,
            multiverse_bridge_cards=GLOBAL_CACHE.multiverse_bridge_cards,
            standard_legal_sets=GLOBAL_CACHE.standard_legal_sets,
            unlimited_cards=GLOBAL_CACHE.scryfall.cards_without_limits if GLOBAL_CACHE._scryfall else set(),
            categoricals=GLOBAL_CACHE.categoricals,
            card_to_products_df=GLOBAL_CACHE.github.card_to_products_df if GLOBAL_CACHE._github else None,
        )
  
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
    
def _ascii_name_expr(expr: pl.Expr) -> pl.Expr:
    """
    Build expression to normalize card name to ASCII.
    Pure Polars - stays lazy.
    """
    return (
        expr
        .str.replace_all("Æ", "AE")
        .str.replace_all("æ", "ae")
        .str.replace_all("Œ", "OE")
        .str.replace_all("œ", "oe")
        .str.replace_all("ß", "ss")
        .str.replace_all("É", "E")
        .str.replace_all("È", "E")
        .str.replace_all("Ê", "E")
        .str.replace_all("Ë", "E")
        .str.replace_all("Á", "A")
        .str.replace_all("À", "A")
        .str.replace_all("Â", "A")
        .str.replace_all("Ä", "A")
        .str.replace_all("Ã", "A")
        .str.replace_all("Í", "I")
        .str.replace_all("Ì", "I")
        .str.replace_all("Î", "I")
        .str.replace_all("Ï", "I")
        .str.replace_all("Ó", "O")
        .str.replace_all("Ò", "O")
        .str.replace_all("Ô", "O")
        .str.replace_all("Ö", "O")
        .str.replace_all("Õ", "O")
        .str.replace_all("Ú", "U")
        .str.replace_all("Ù", "U")
        .str.replace_all("Û", "U")
        .str.replace_all("Ü", "U")
        .str.replace_all("Ý", "Y")
        .str.replace_all("Ñ", "N")
        .str.replace_all("Ç", "C")
        .str.replace_all("é", "e")
        .str.replace_all("è", "e")
        .str.replace_all("ê", "e")
        .str.replace_all("ë", "e")
        .str.replace_all("á", "a")
        .str.replace_all("à", "a")
        .str.replace_all("â", "a")
        .str.replace_all("ä", "a")
        .str.replace_all("ã", "a")
        .str.replace_all("í", "i")
        .str.replace_all("ì", "i")
        .str.replace_all("î", "i")
        .str.replace_all("ï", "i")
        .str.replace_all("ó", "o")
        .str.replace_all("ò", "o")
        .str.replace_all("ô", "o")
        .str.replace_all("ö", "o")
        .str.replace_all("õ", "o")
        .str.replace_all("ú", "u")
        .str.replace_all("ù", "u")
        .str.replace_all("û", "u")
        .str.replace_all("ü", "u")
        .str.replace_all("ý", "y")
        .str.replace_all("ÿ", "y")
        .str.replace_all("ñ", "n")
        .str.replace_all("ç", "c")
    )
    

def _url_hash_batch(series: pl.Series) -> pl.Series:
    """Batch SHA256 hash generation for purchase URLs."""
    import hashlib
    arr = series.to_numpy()
    n = len(arr)
    results = np.empty(n, dtype=object)

    for i in range(n):
        val = arr[i]
        if val is None:
            results[i] = None
        else:
            results[i] = hashlib.sha256(val.encode("utf-8")).hexdigest()[:16]

    return pl.Series(results, dtype=pl.String)


# ---- Stage 1 ---

# 1.1 Explode card faces into separate rows
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
    - _face_data: The face struct (for multi-face) or typed null (for single-face)
    """
    # Get the CardFace struct schema for typed nulls
    face_struct_schema = CardFace.polars_schema()

    # Must collect to split - then return as lazy
    df = lf.with_row_index("_row_id").collect()

    if "card_faces" not in df.columns:
        return df.with_columns(
            pl.lit(0).cast(pl.Int64).alias("face_id"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).cast(face_struct_schema).alias("_face_data"),
        ).lazy()

    # Split into cards with faces vs without
    has_faces = df.filter(pl.col("card_faces").is_not_null())
    no_faces = df.filter(pl.col("card_faces").is_null())

    if has_faces.height == 0:
        # All single-faced cards - use typed null for _face_data
        return no_faces.with_columns(
            pl.lit(0).cast(pl.Int64).alias("face_id"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).cast(face_struct_schema).alias("_face_data"),
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

    # Add columns to no_faces with typed null for _face_data
    no_faces = no_faces.with_columns(
        pl.lit(0).cast(pl.Int64).alias("face_id"),
        pl.lit(None).cast(pl.String).alias("side"),
        pl.lit(None).cast(face_struct_schema).alias("_face_data"),
    ).drop("card_faces")

    # Ensure schema consistency for columns that may have type mismatches
    # between single-face and multi-face cards in Scryfall data
    # Skip _face_data since structs have different schemas by design
    common_cols = (set(no_faces.columns) & set(exploded.columns)) - {"_face_data"}
    for col in common_cols:
        no_dtype = no_faces.schema[col]
        exp_dtype = exploded.schema[col]
        if no_dtype != exp_dtype:
            no_is_list = str(no_dtype).startswith("List")
            exp_is_list = str(exp_dtype).startswith("List")
            if exp_is_list and not no_is_list:
                no_faces = no_faces.with_columns(pl.col(col).cast(exp_dtype))
            elif no_is_list and not exp_is_list:
                exploded = exploded.with_columns(pl.col(col).cast(no_dtype))

    # For _face_data, recreate in no_faces with exploded's schema (no_faces has all nulls anyway)
    if "_face_data" in no_faces.columns and "_face_data" in exploded.columns:
        exp_face_schema = exploded.schema["_face_data"]
        no_faces = no_faces.drop("_face_data").with_columns(
            pl.lit(None).cast(exp_face_schema).alias("_face_data")
        )

    return pl.concat([no_faces, exploded], how="diagonal").lazy()


# Token detection constants
TOKEN_LAYOUTS = {"token", "double_faced_token", "emblem", "art_series"}


def is_token_expr() -> pl.Expr:
    """Expression to detect if a row is a token based on layout/type."""
    return (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type_line").fill_null("") == "Dungeon")
        | pl.col("type_line").fill_null("").str.contains("Token")
    )


# 1.1b: Mark tokens early in pipeline
def mark_tokens(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add _isToken boolean column to identify tokens.

    Should be called early in the pipeline so conditional expressions can use it.
    """
    return lf.with_columns(
        is_token_expr().alias("_isToken")
    )


# 1.1c: Conditional field expressions for tokens vs cards
def token_conditional(
    card_expr: pl.Expr,
    token_expr: pl.Expr | None = None,
    alias: str | None = None,
) -> pl.Expr:
    """
    Return card_expr for cards, token_expr (or null) for tokens.

    Usage:
        .with_columns(
            token_conditional(legalities_struct, alias="legalities"),  # null for tokens
            token_conditional(pl.lit(None), reverse_related_expr, alias="reverseRelated"),  # only for tokens
        )
    """
    expr = pl.when(~pl.col("_isToken")).then(card_expr)
    if token_expr is not None:
        expr = expr.otherwise(token_expr)
    else:
        expr = expr.otherwise(pl.lit(None))
    if alias:
        expr = expr.alias(alias)
    return expr


def card_only(expr: pl.Expr, alias: str | None = None) -> pl.Expr:
    """Shorthand: expression only applies to cards (null for tokens)."""
    return token_conditional(expr, None, alias)


def token_only(expr: pl.Expr, alias: str | None = None) -> pl.Expr:
    """Shorthand: expression only applies to tokens (null for cards)."""
    return token_conditional(pl.lit(None), expr, alias)


# 1.2: Add basic fields
def add_basic_fields(lf: pl.LazyFrame, set_release_date: str = "") -> pl.LazyFrame:
    """
    Add basic card fields: name, setCode, language, etc.

    Maps Scryfall column names to MTGJSON names.
    For multi-face cards, the name is the face-specific name.
    """
    def face_field(field_name: str) -> pl.Expr:
        # For multi-face cards, prefer face-specific data; for single-face, use root field
        # _face_data is now properly typed as a struct (even when null), so coalesce works
        return pl.coalesce(
            pl.col("_face_data").struct.field(field_name),
            pl.col(field_name),
        )

    face_name = face_field("name")
    ascii_name = _ascii_name_expr(face_name)
    return (
        lf
        .rename({
            # Core identifiers (card-level only)
            "id": "scryfallId",
            "oracle_id": "oracleId",
            "set": "setCode",
            "collector_number": "number",
            "card_back_id": "cardBackId",
        })
        .with_columns([
            # Face-aware fields
            face_field("name").alias("name"),
            face_field("mana_cost").alias("manaCost"),
            face_field("type_line").alias("originalType"),
            face_field("oracle_text").alias("text"),
            face_field("flavor_text").alias("flavorText"),
            face_field("power").alias("power"),
            face_field("toughness").alias("toughness"),
            face_field("loyalty").alias("loyalty"),
            face_field("defense").alias("defense"),
            face_field("artist").alias("artist"),
            face_field("watermark").alias("watermark"),
            face_field("illustration_id").alias("illustrationId"),
            face_field("color_indicator").alias("colorIndicator"),
            face_field("colors").alias("colors"),
            face_field("printed_name").alias("faceName"),
            face_field("printed_text").alias("originalText"),
            face_field("printed_type_line").alias("printedType"),
            
            # Card-level fields (not face-specific)
            pl.col("setCode").str.to_uppercase(),
            pl.col("cmc").alias("manaValue"),
            pl.col("color_identity").alias("colorIdentity"),
            pl.col("border_color").alias("borderColor"),
            pl.col("frame").alias("frameVersion"),
            pl.col("frame_effects").alias("frameEffects"),
            pl.col("security_stamp").alias("securityStamp"),
            pl.col("full_art").alias("isFullArt"),
            pl.col("textless").alias("isTextless"),
            pl.col("oversized").alias("isOversized"),
            pl.col("promo").alias("isPromo"),
            pl.col("reprint").alias("isReprint"),
            pl.col("story_spotlight").alias("isStorySpotlight"),
            pl.col("reserved").alias("isReserved"),
            pl.col("foil").alias("hasFoil"),
            pl.col("nonfoil").alias("hasNonFoil"),
            pl.col("flavor_name").alias("flavorName"),
            pl.col("all_parts").alias("allParts"),
            
            # Language mapping
            pl.col("lang").replace_strict(
                {
                    "en": "English",
                    "es": "Spanish",
                    "fr": "French",
                    "de": "German",
                    "it": "Italian",
                    "pt": "Portuguese (Brazil)",
                    "ja": "Japanese",
                    "ko": "Korean",
                    "ru": "Russian",
                    "zhs": "Chinese Simplified",
                    "zht": "Chinese Traditional",
                    "he": "Hebrew",
                    "la": "Latin",
                    "grc": "Ancient Greek",
                    "ar": "Arabic",
                    "sa": "Sanskrit",
                    "ph": "Phyrexian",
                },
                default=pl.col("lang"),
                return_dtype=pl.String,
            ).alias("language"),
        ])
        .with_columns(
            pl.when(ascii_name != face_name)
            .then(ascii_name)
            .otherwise(None)
            .alias("asciiName"),
        )
        .drop(["lang", "frame", "border_color", "full_art", "textless", 
               "oversized", "promo", "reprint", "story_spotlight", "reserved",
               "foil", "nonfoil", "flavor_name", "all_parts", "color_identity",
               "cmc", "frame_effects", "security_stamp"], strict=False)
    )


# 1.3: Parse type_line into supertypes, types, subtypes
def parse_type_line_expr(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Parse type_line into supertypes, types, subtypes using Polars expressions.

    Converts "Legendary Creature - Human Wizard" into:
    - supertypes: ["Legendary"]
    - types: ["Creature"]
    - subtypes: ["Human", "Wizard"]
    """
    super_types_list = list(constants.SUPER_TYPES)
    
    # add_basic_fields already renamed type_line -> originalType
    type_line = pl.col("originalType").fill_null("Card")

    # Split on em-dash
    split_type = type_line.str.split(" — ")

    return (
        lf.with_columns(
            type_line.alias("type"),
            split_type.list.first().alias("_types_part"),
            split_type.list.get(1, null_on_oob=True).alias("_subtypes_part"),
        )
        .with_columns(
            pl.col("_types_part").str.split(" ").alias("_type_words"),
        )
        .with_columns(
            pl.col("_type_words")
            .list.eval(pl.element().filter(pl.element().is_in(super_types_list)))
            .alias("supertypes"),
            pl.col("_type_words")
            .list.eval(pl.element().filter(~pl.element().is_in(super_types_list)))
            .alias("types"),
            pl.when(pl.col("_subtypes_part").is_not_null())
            .then(pl.col("_subtypes_part").str.strip_chars().str.split(" "))
            .otherwise(pl.lit([]).cast(pl.List(pl.String)))
            .alias("subtypes"),
        )
        .drop(["_types_part", "_subtypes_part", "_type_words"])
    )

# 1.4: Add mana cost, mana value, and colors
def add_mana_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mana cost, mana value, and colors.

    Runs after add_basic_fields which renames mana_cost -> manaCost, cmc -> manaValue.
    """
    return lf.with_columns(
        # manaCost already exists from add_basic_fields rename
        pl.col("colors").fill_null([]).alias("colors"),
        pl.col("colorIdentity").fill_null([]),
        # Ensure manaValue stays as Float64
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("manaValue"),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("convertedManaCost"),
        pl.col("manaValue").cast(pl.Float64).fill_null(0.0).alias("faceManaValue"),
    )
    

# 1.5: Add card attributes
def add_card_attributes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add card attributes. Runs after add_basic_fields.
    """
    return lf.with_columns(
        # number already exists from add_basic_fields (collector_number -> number)
        pl.col("rarity"),
        # borderColor already exists
        # frameVersion already exists
        pl.col("frameEffects").fill_null([]).list.sort().alias("frameEffects"),
        # securityStamp already exists
        pl.col("artist").fill_null(""),
        pl.col("artist_ids").fill_null([]).alias("artistIds"),
        pl.col("watermark"),
        pl.col("finishes").fill_null([]).alias("finishes"),
        pl.col("finishes").list.contains("foil").fill_null(False).alias("hasFoil"),
        pl.col("finishes").list.contains("nonfoil").fill_null(False).alias("hasNonFoil"),
        pl.col("content_warning").alias("hasContentWarning"),
        # isFullArt already exists
        pl.col("digital").alias("isOnlineOnly"),
        # isOversized, isPromo, isReprint, isReserved, isStorySpotlight, isTextless already exist
        (pl.col("set_type") == "funny").alias("_is_funny_set"),
        pl.col("loyalty"),
        pl.col("defense"),
        pl.col("power"),
        pl.col("toughness"),
        pl.col("hand_modifier").alias("hand"),
        pl.col("life_modifier").alias("life"),
        pl.col("edhrec_rank").alias("edhrecRank"),
        pl.col("promo_types").fill_null([]).alias("promoTypes"),
        pl.col("booster").alias("_in_booster"),
        pl.col("game_changer").fill_null(False).alias("isGameChanger"),
        pl.col("layout"),
        # text already exists from add_basic_fields (oracle_text -> text)
        # flavorText already exists
        pl.col("keywords").fill_null([]).alias("_all_keywords"),
        pl.col("attraction_lights").alias("attractionLights"),
        # allParts already exists from add_basic_fields rename
        pl.col("allParts").fill_null([]).alias("_all_parts"),
    )

# 1.6: Filter keywords for face
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

    # Collect to LazyFrame for split processing
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


# 1.7: Add booster types
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


# 1.8:
def add_legalities_struct(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Builds legalities struct from Scryfall's legalities column.

    Uses dynamically discovered format names instead of hardcoded list.
    """
    # Unnest the source struct to get individual format columns
    lf = lf.unnest("legalities")

    # Use discovered formats from source data
    categoricals = ctx.categoricals if ctx else GLOBAL_CACHE.categoricals
    formats = categoricals.legality_formats if categoricals else []

    if not formats:
        # Fallback: return empty struct if no formats discovered
        return lf.with_columns(pl.lit(None).alias("legalities"))

    # Build expressions for each format
    struct_fields = []
    for fmt in formats:
        expr = (
            pl.when(
                pl.col(fmt).is_not_null()
                & (pl.col(fmt) != "not_legal")
                & (pl.col("set_type") != "memorabilia")
            )
            .then(pl.col(fmt).str.to_titlecase())
            .otherwise(pl.lit(None))
            .alias(fmt)
        )
        struct_fields.append(expr)

    # Repack into struct and drop the unpacked columns
    return (
        lf
        .with_columns(pl.struct(struct_fields).alias("legalities"))
        .drop(formats, strict=False)
    )



# 1.9:
def add_availability_struct(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Build availability list from games column.

    Uses dynamically discovered game platforms.
    """
    schema = lf.collect_schema()

    if "games" not in schema.names():
        return lf.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias("availability"))

    # Use discovered platforms
    categoricals = ctx.categoricals if ctx else GLOBAL_CACHE.categoricals
    platforms = categoricals.games if categoricals else []
    
    if not platforms:
        # Fallback: just pass through games as availability
        return lf.with_columns(pl.col("games").alias("availability"))

    games_dtype = schema["games"]
    
    # Handle struct format (from parquet) vs list format (from JSON)
    if isinstance(games_dtype, pl.Struct):
        # Struct format: {paper: true, arena: false, mtgo: true}
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
    else:
        # List format: ["paper", "mtgo"]
        return lf.with_columns(
            pl.col("games").list.sort().alias("availability")
        )

# --- Stage 2 ---

# 2.0: join Card Kingdom data
def join_card_kingdom_data(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Join pre-pivoted Card Kingdom data to add CK identifiers and URLs.

    The CK data is pivoted during cache loading to one row per scryfall_id.
    Also computes MTGJSON redirect URLs from the CK URL paths.
    """
    ck_df = ctx.card_kingdom_df if ctx else GLOBAL_CACHE.card_kingdom_df

    if ck_df is None:
        LOGGER.debug("Card Kingdom LazyFrame not loaded, skipping CK data")
        return lf.with_columns(
            pl.lit(None).cast(pl.String).alias("cardKingdomId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedId"),
            pl.lit(None).cast(pl.String).alias("cardKingdomUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomFoilUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
        )

    # Join CK data - v2 provider uses 'id' column, pipeline has 'scryfallId' after rename
    lf = lf.join(
        ck_df.lazy(),
        left_on="scryfallId",
        right_on="id",
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


# 2.1: join Cardmarket IDs
def join_cardmarket_ids(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Vectorized Join using the pre-computed global lookup table.
    """
    mcm_df = ctx.mcm_lookup_df if ctx else GLOBAL_CACHE.mcm_lookup_df
    if mcm_df is None:
        return lf.with_columns([
            pl.lit(None).cast(pl.String).alias("mcmId"),
            pl.lit(None).cast(pl.String).alias("mcmMetaId"),
        ])
    # Ensure the lookup table is available as a LazyFrame
    mcm_lookup = mcm_df.lazy()

    lf = lf.with_columns([
        # Lowercase name for matching
        pl.col("name").str.to_lowercase().alias("_join_name"),

        # Scryfall numbers often have leading zeros (e.g., "001"),
        # while MCM strips them. We strip them here to match.
        pl.col("number").str.strip_chars_start("0").alias("_join_number")
    ])

    # Left join on Set + Name + Number
    lf = lf.join(
        mcm_lookup,
        left_on=["setCode", "_join_name", "_join_number"],
        right_on=["set_code", "name_lower", "number"],
        how="left"
    )

    # Keep mcmId and mcmMetaId columns - they'll be added to identifiers struct later
    lf = lf.drop(["_join_name", "_join_number"])

    return lf


# 2.2: add identifiers struct
def add_identifiers_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build the identifiers struct lazily.
    """
    # For multi-face cards, prefer face-specific IDs; for single-face, use root field
    # _face_data is now properly typed as a struct (even when null), so coalesce works
    return lf.with_columns(
        pl.struct(
            scryfallId=pl.col("scryfallId"),
            scryfallOracleId=pl.coalesce(
                pl.col("_face_data").struct.field("oracle_id"),
                pl.col("oracleId"),
            ),
            scryfallIllustrationId=pl.coalesce(
                pl.col("_face_data").struct.field("illustration_id"),
                pl.col("illustration_id"),
            ),
            scryfallCardBackId=pl.col("cardBackId"),
            # MCM IDs from CardMarket lookup (mcmId, mcmMetaId columns from join_cardmarket_ids)
            mcmId=pl.col("mcmId"),
            mcmMetaId=pl.col("mcmMetaId"),
            mtgArenaId=pl.col("arena_id").cast(pl.String),
            mtgoId=pl.col("mtgo_id").cast(pl.String),
            mtgoFoilId=pl.col("mtgo_foil_id").cast(pl.String),
            multiverseId=pl.col("multiverse_ids")
                .list.get(pl.col("face_id").fill_null(0), null_on_oob=True)
                .cast(pl.String),

            tcgplayerProductId=pl.col("tcgplayer_id").cast(pl.String),
            tcgplayerEtchedProductId=pl.col("tcgplayer_etched_id").cast(pl.String),

            cardKingdomId=pl.col("cardKingdomId"),
            cardKingdomFoilId=pl.col("cardKingdomFoilId"),
            cardKingdomEtchedId=pl.col("cardKingdomEtchedId"),
        ).alias("identifiers")
    ).drop(["mcmId", "mcmMetaId"], strict=False)


# 2.3: join printings map
def join_printings(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Join printings map - replaces parse_printings() per-card lookups.

    Single join replaces N filter operations.
    """
    printings_df = ctx.printings_df if ctx else GLOBAL_CACHE.printings_df
    if printings_df is None:
        return lf.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias("printings"))

    return lf.join(
        printings_df.lazy(),
        left_on="oracleId",
        right_on="oracle_id",
        how="left",
    ).with_columns(pl.col("printings").fill_null([]).list.sort())


# 2.4: join rulings map
def join_rulings(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Join rulings map - replaces parse_rulings() per-card lookups.
    """
    rulings_df = ctx.rulings_df if ctx else GLOBAL_CACHE.rulings_df
    if rulings_df is None:
        return lf.with_columns(pl.lit([]).alias("rulings"))

    return lf.join(
        rulings_df.lazy(),
        left_on="oracleId",
        right_on="oracle_id",
        how="left",
    ).with_columns(pl.col("rulings").fill_null([]))


# 2.5: join foreign data
def join_foreign_data(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """Join foreign data from raw Scryfall bulk data."""
    # Build foreign data from raw cards_df (not from lf which is English-only)
    cards_df = ctx.cards_df if ctx else GLOBAL_CACHE.cards_df
    if cards_df is None:
        return lf.with_columns(pl.lit([]).alias("foreignData"))
    raw_cards = cards_df

    flf = (
        raw_cards.filter(pl.col("lang") != "en")
        .with_columns(pl.col("set").str.to_uppercase().alias("set"))
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

    return (
        lf.join(
            flf,
            left_on=["setCode", "number"],
            right_on=["set", "collector_number"],
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


# 2.6: join EDHREC data
def join_edhrec_data(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """Join EDHREC saltiness and rank by oracle_id."""
    edhrec_df = ctx.salt_df if ctx else GLOBAL_CACHE.salt_df

    if edhrec_df is None or edhrec_df.is_empty():
        return lf.with_columns([
            pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
        ])

    return lf.join(
        edhrec_df.lazy().select(["oracle_id", "edhrecSaltiness"]),
        left_on="oracleId",
        right_on="oracle_id",
        how="left",
    )


# 2.7: join gatherer data
def join_gatherer_data(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Join Gatherer original text and type by multiverse ID.
    """
    gatherer_map = ctx.gatherer_map if ctx else GLOBAL_CACHE.gatherer_map
    
    if not gatherer_map:
        return lf.with_columns([
            pl.lit(None).cast(pl.String).alias("originalText"),
            pl.lit(None).cast(pl.String).alias("originalType"),
        ])
    
    # Build lookup LazyFrame from gatherer_map
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
    
    gatherer_df = pl.LazyFrame(rows)
    
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


# --- Stage 3 ---

# 3.0: add UUIDs
def add_uuid_expr(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Generate MTGJSON UUIDs.

    - Checks legacy cache first
    - Falls back to deterministic UUID5(scryfallId + side)
    - Uses numpy-vectorized batch computation
    """
    cache_df = ctx.uuid_cache_df if ctx else GLOBAL_CACHE.uuid_cache_df

    if cache_df is None:
        # No cache - generate all UUIDs from scratch
        return lf.with_columns(
            pl.concat_str([
                pl.col("scryfallId"),
                pl.col("side").fill_null("a"),
            ])
            .map_batches(uuid5_batch, return_dtype=pl.String)
            .alias("uuid")
        )

    return (
        lf
        .join(
            cache_df.lazy(),
            left_on=["scryfallId", "side"],
            right_on=["scryfall_id", "side"],
            how="left",
        )
        .with_columns(
            pl.coalesce([
                pl.col("cached_uuid"),
                pl.concat_str([
                    pl.col("scryfallId"),
                    pl.col("side").fill_null("a"),
                ])
                .map_batches(uuid5_batch, return_dtype=pl.String),
            ]).alias("uuid")
        )
        .drop("cached_uuid")
    )

# 3.1: add mtgjsonV4Id
def add_identifiers_v4_uuid(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mtgjsonV4Id to identifiers struct.
    
    Uses struct-based batch computation for v4 UUID formula.
    """
    return (
        lf
        .with_columns(
            pl.struct([
                pl.col("scryfallId").alias("id"),
                pl.col("name"),
                pl.col("faceName").alias("face_name"),
                pl.col("types"),
                pl.col("colors"),
                pl.col("power"),
                pl.col("toughness"),
                pl.col("side"),
                pl.col("setCode").alias("set"),
            ])
            .map_batches(compute_v4_uuid_from_struct, return_dtype=pl.String)
            .alias("mtgjsonV4Id")
        )
    )
    

# --- Stage 4 ---

# 4.0: add otherFaceIds
def add_other_face_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link multi-face cards via Scryfall ID.

    Since scryfallId is shared by all faces of a split card/MDFC,
    grouping by scryfallId gathers all sibling UUIDs.
    """
    # Group by Scryfall ID to get list of MTGJSON UUIDs for this object
    face_links = (
        lf.select(["scryfallId", "uuid"])
        .group_by("scryfallId")
        .agg(pl.col("uuid").alias("_all_uuids"))
    )

    return (
        lf.join(face_links, on="scryfallId", how="left")
        .with_columns(
            # Filter out own UUID from the list
            pl.col("_all_uuids")
            .list.set_difference(pl.col("uuid").implode())
            .alias("otherFaceIds")
        )
        .drop("_all_uuids")
    )

# 4.1: add variations and isAlternative
def add_variations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized detection of Variations and Alternatives.
    
    Variations: Cards with the same base name and face name but different UUID
    is_alternative: Within cards sharing a "printing key", only the first is NOT alternative
    """
    
    # Normalize to base name by stripping " (" and beyond
    lf = lf.with_columns(
        pl.col("name")
        .str.split(" (")
        .list.first()
        .alias("_base_name")
    )
    
    # Collect all UUIDs for each (set, base_name, faceName) group
    variation_groups = (
        lf.select(["setCode", "_base_name", "faceName", "uuid"])
        .group_by(["setCode", "_base_name", "faceName"])
        .agg(pl.col("uuid").alias("_group_uuids"))
    )
    
    # Join back to attach the full UUID list to each card
    lf = lf.join(
        variation_groups, 
        on=["setCode", "_base_name", "faceName"], 
        how="left"
    )
    
    # Variations = group UUIDs minus self UUID
    lf = lf.with_columns(
        pl.when(pl.col("_group_uuids").list.len() > 1)
        .then(
            pl.col("_group_uuids")
            .list.set_difference(pl.col("uuid").implode())  # Remove self
            .list.sort()
        )
        .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        .alias("variations")
    )
    
    # Build the "printing key" that defines uniqueness within a set
    frame_effects_str = (
        pl.col("frameEffects")
        .list.sort()
        .list.join(",")
        .fill_null("")
    )
    
    finishes_str = (
        pl.col("finishes")
        .list.sort()
        .list.join(",")
        .fill_null("")
    )
    
    # Base key: name|border|frame|effects|side
    base_key = pl.concat_str([
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
    
    # For UNH/10E, also include finishes in the key
    printing_key = (
        pl.when(pl.col("setCode").is_in(["UNH", "10E"]))
        .then(pl.concat_str([base_key, pl.lit("|"), finishes_str]))
        .otherwise(base_key)
        .alias("_printing_key")
    )
    
    lf = lf.with_columns(printing_key)
    
    # Within each printing key, min(uuid) is the "canonical" first printing
    # All others with the same key are alternatives
    first_uuid_expr = pl.col("uuid").min().over(["setCode", "_printing_key"])
    
    basic_lands = ["Plains", "Island", "Swamp", "Mountain", "Forest",
                   "Snow-Covered Plains", "Snow-Covered Island", 
                   "Snow-Covered Swamp", "Snow-Covered Mountain", 
                   "Snow-Covered Forest", "Wastes"]
    
    lf = lf.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0)      # Has variations
            & (~pl.col("name").is_in(basic_lands))     # Not a basic land
            & (pl.col("uuid") != first_uuid_expr)      # Not the first in group
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )
    # Cleanup temp columns
    return lf.drop(["_base_name", "_group_uuids", "_printing_key"])


# 4.2: add leadership skills
def add_leadership_skills_expr(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
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
    standard_sets = ctx.standard_legal_sets if ctx else GLOBAL_CACHE.standard_legal_sets
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


# 4.4: add relatedCards struct combining spellbook and reverseRelated
def add_related_cards_struct(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Vectorized spellbook logic.
    Prerequisite: 'set_type' column must exist in lf.
    """
    spellbook_df = ctx.spellbook_df if ctx else GLOBAL_CACHE.spellbook_df
    if spellbook_df is None or spellbook_df.is_empty():
        return lf.with_columns(pl.lit(None).alias("relatedCards"))

    # Rename 'spellbook' to '_spellbook_list' for internal use and join on name
    spellbook_renamed = spellbook_df.rename({"spellbook": "_spellbook_list"})
    lf = lf.join(spellbook_renamed.lazy(), on="name", how="left")

    # Build relatedCards struct - include if either spellbook or reverseRelated has data
    has_spellbook = (
        pl.col("set_type").str.to_lowercase().str.contains("alchemy") &
        pl.col("_spellbook_list").is_not_null() &
        (pl.col("_spellbook_list").list.len() > 0)
    )
    has_reverse = (
        pl.col("reverseRelated").is_not_null() &
        (pl.col("reverseRelated").list.len() > 0)
    )

    return lf.with_columns(
        pl.when(has_spellbook | has_reverse)
        .then(
            pl.struct(
                spellbook=pl.col("_spellbook_list"),
                reverseRelated=pl.col("reverseRelated"),
            )
        )
        .otherwise(pl.lit(None))
        .alias("relatedCards")
    ).drop(["_spellbook_list", "reverseRelated"], strict=False)


# 4.5: add_alternative_deck_limit
def add_alternative_deck_limit(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Mark cards that don't have the standard 4-copy deck limit.

    Uses Scryfall's cards_without_limits list.
    """
    unlimited_cards = ctx.unlimited_cards if ctx else GLOBAL_CACHE.scryfall.cards_without_limits

    if not unlimited_cards:
        return lf

    return lf.with_columns(
        pl.when(pl.col("name").is_in(list(unlimited_cards)))
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("hasAlternativeDeckLimit")
    )


# 4.6:
def add_is_funny(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Vectorized 'isFunny' logic.

    Note: This still uses hardcoded "funny" check since it's a semantic
    value not just a categorical enumeration. But we could validate that
    "funny" exists in categoricals.set_types if desired.
    """
    categoricals = ctx.categoricals if ctx else GLOBAL_CACHE.categoricals
    # Validate "funny" is a known set_type (optional sanity check)
    if categoricals is None or "funny" not in categoricals.set_types:
        # No funny sets exist - return all null
        return lf.with_columns(pl.lit(None).cast(pl.Boolean).alias("isFunny"))
    
    return lf.with_columns(
        pl.when(pl.col("set_type") != "funny")
        .then(pl.lit(None))
        .when(pl.col("setCode") == "UNF")
        .then(
            pl.when(pl.col("securityStamp") == "acorn")
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
        )
        .otherwise(pl.lit(True))
        .alias("isFunny")
    )

# 4.7:
def add_is_timeshifted(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized 'isTimeshifted' logic.
    """
    return lf.with_columns(
        pl.when(
            (pl.col("frameVersion") == "future") |
            (pl.col("setCode") == "TSB")
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isTimeshifted")
    )


# 4.8: add purchaseUrls struct
def add_purchase_urls_struct(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Build purchaseUrls struct with SHA256 redirect hashes."""
    base_url = "https://mtgjson.com/links/"

    # Access identifier fields from inside the identifiers struct
    ck_id = pl.col("identifiers").struct.field("cardKingdomId")
    ckf_id = pl.col("identifiers").struct.field("cardKingdomFoilId")
    cke_id = pl.col("identifiers").struct.field("cardKingdomEtchedId")
    mcm_id = pl.col("identifiers").struct.field("mcmId")
    tcg_id = pl.col("identifiers").struct.field("tcgplayerProductId")
    tcge_id = pl.col("identifiers").struct.field("tcgplayerEtchedProductId")

    return (
        lf
        .with_columns([
            pl.concat_str([pl.col("uuid"), pl.lit("cardKingdom")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_ck_hash"),

            pl.concat_str([pl.col("uuid"), pl.lit("cardKingdomFoil")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_ckf_hash"),

            pl.concat_str([pl.col("uuid"), pl.lit("cardKingdomEtched")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_cke_hash"),

            pl.concat_str([pl.col("uuid"), pl.lit("cardmarket")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_cm_hash"),

            pl.concat_str([pl.col("uuid"), pl.lit("tcgplayer")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_tcg_hash"),

            pl.concat_str([pl.col("uuid"), pl.lit("tcgplayerEtched")])
            .map_batches(_url_hash_batch, return_dtype=pl.String)
            .alias("_tcge_hash"),
        ])
        .with_columns(
            pl.struct([
                pl.when(ck_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_ck_hash"))
                .otherwise(None)
                .alias("cardKingdom"),

                pl.when(ckf_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_ckf_hash"))
                .otherwise(None)
                .alias("cardKingdomFoil"),

                pl.when(cke_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_cke_hash"))
                .otherwise(None)
                .alias("cardKingdomEtched"),

                pl.when(mcm_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_cm_hash"))
                .otherwise(None)
                .alias("cardmarket"),

                pl.when(tcg_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_tcg_hash"))
                .otherwise(None)
                .alias("tcgplayer"),

                pl.when(tcge_id.is_not_null())
                .then(pl.lit(base_url) + pl.col("_tcge_hash"))
                .otherwise(None)
                .alias("tcgplayerEtched"),
            ]).alias("purchaseUrls")
        )
        .drop(["_ck_hash", "_ckf_hash", "_cke_hash", "_cm_hash", "_tcg_hash", "_tcge_hash"])
    )


# --- Stage 5 ---

# 5.0: apply manual overrides
def apply_manual_overrides(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Apply manual field overrides keyed by UUID.

    Handles special cases like Final Fantasy meld cards.
    """
    overrides = ctx.manual_overrides if ctx else GLOBAL_CACHE.manual_overrides
    if not overrides:
        return lf

    # Map old field names to new pipeline names
    field_name_map = {
        "collector_number": "number",
        "id": "scryfallId",
        "oracle_id": "oracleId",
        "set": "setCode",
        "card_back_id": "cardBackId",
    }

    # Group overrides by field (using mapped names)
    field_overrides: dict[str, dict[str, Any]] = {}
    for uuid_key, fields in overrides.items():
        for field, value in fields.items():
            if field.startswith("__"):
                continue
            mapped_field = field_name_map.get(field, field)
            if mapped_field not in field_overrides:
                field_overrides[mapped_field] = {}
            field_overrides[mapped_field][uuid_key] = value

    # Get column names once
    schema_names = lf.collect_schema().names()

    # Apply each field's overrides
    for field, uuid_map in field_overrides.items():
        if field not in schema_names:
            continue

        # Determine return dtype from first value
        sample_value = next(iter(uuid_map.values()))
        if isinstance(sample_value, list):
            return_dtype = pl.List(pl.String)
        elif isinstance(sample_value, str):
            return_dtype = pl.String
        else:
            return_dtype = pl.String

        lf = lf.with_columns(
            pl.when(pl.col("uuid").is_in(list(uuid_map.keys())))
            .then(
                pl.col("uuid").replace_strict(
                    uuid_map,
                    default=pl.col(field),
                    return_dtype=return_dtype,
                )
            )
            .otherwise(pl.col(field))
            .alias(field)
        )

    return lf


# 5.1:
def add_meld_card_parts(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Add cardParts for meld cards.

    Uses meld_triplets lookup from cache (name -> [top, bottom, combined]).
    Falls back to computed logic for new meld cards not in resource file.
    """
    meld_triplets = ctx.meld_triplets if ctx else GLOBAL_CACHE.meld_triplets
    
    if not meld_triplets:
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("cardParts"))
    
    # Build lookup DataFrame
    meld_lookup = pl.LazyFrame([
        {"_lookup_name": name, "_resource_parts": parts}
        for name, parts in meld_triplets.items()
    ])
    
    # Join on faceName (or name if faceName is null)
    lf = lf.with_columns(
        pl.coalesce(pl.col("faceName"), pl.col("name")).alias("_meld_key")
    )
    
    lf = lf.join(meld_lookup, left_on="_meld_key", right_on="_lookup_name", how="left")
    
    # Only set cardParts for meld layout cards
    lf = lf.with_columns(
        pl.when(pl.col("layout") == "meld")
        .then(pl.col("_resource_parts"))
        .otherwise(pl.lit(None))
        .alias("cardParts")
    )
    
    return lf.drop(["_meld_key", "_resource_parts"], strict=False)

# 5.2:
def add_rebalanced_linkage(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.
    
    Adds:
    - originalPrintings: UUIDs of the original card (on rebalanced cards)
    - rebalancedPrintings: UUIDs of the rebalanced version (on original cards)
    """
    # Rebalanced cards: names starting with "A-"
    # Original cards: names that match the stripped "A-" version
    
    is_rebalanced = pl.col("name").str.starts_with("A-")
    original_name_expr = pl.col("name").str.replace("^A-", "")
    
    # Build rebalanced -> original name mapping with UUIDs
    rebalanced_map = (
        lf.filter(is_rebalanced)
        .select([
            original_name_expr.alias("_original_name"),
            pl.col("uuid"),
        ])
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_rebalanced_uuids"))
    )
    
    # Build original name -> original UUIDs mapping
    # (cards that DON'T start with A- but whose name matches a rebalanced card's base name)
    original_map = (
        lf.filter(~is_rebalanced)
        .select([
            pl.col("name").alias("_original_name"),
            pl.col("uuid"),
        ])
        .join(
            rebalanced_map.select("_original_name").unique(),
            on="_original_name",
            how="semi"  # Only keep names that have a rebalanced version
        )
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_original_uuids"))
    )
    
    # Join rebalancedPrintings onto original cards (by name)
    lf = lf.join(
        rebalanced_map,
        left_on="name",
        right_on="_original_name",
        how="left",
    ).rename({"_rebalanced_uuids": "rebalancedPrintings"})
    
    # Join originalPrintings onto rebalanced cards (by stripped name)
    lf = lf.join(
        original_map,
        left_on=original_name_expr,
        right_on="_original_name",
        how="left",
    ).rename({"_original_uuids": "originalPrintings"})
    
    return lf


# 5.3:
def link_foil_nonfoil_versions(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link foil and non-foil versions that have different card details.
    
    Only applies to specific sets: CN2, FRF, ONS, 10E, UNH.
    Adds mtgjsonFoilVersionId and mtgjsonNonFoilVersionId to identifiers.
    
    Assumes setCode column exists.
    """
    FOIL_LINK_SETS = {"CN2", "FRF", "ONS", "10E", "UNH"}
    
    in_target_sets = pl.col("setCode").is_in(FOIL_LINK_SETS)
    
    # Extract illustration_id for grouping
    ill_id_expr = pl.col("identifiers").struct.field("scryfallIllustrationId")
    
    # Find pairs: same illustration_id, same set, exactly 2 cards
    pairs = (
        lf.filter(in_target_sets & ill_id_expr.is_not_null())
        .select([
            pl.col("setCode"),
            ill_id_expr.alias("_ill_id"),
            pl.col("uuid"),
            pl.col("finishes"),
        ])
        .group_by(["setCode", "_ill_id"])
        .agg([
            pl.col("uuid"),
            pl.col("finishes"),
        ])
        .filter(pl.col("uuid").list.len() == 2)
    )
    
    # Explode and determine foil status for each card in pair
    pair_cards = (
        pairs
        .with_columns([
            pl.col("uuid").list.get(0).alias("uuid_0"),
            pl.col("uuid").list.get(1).alias("uuid_1"),
            pl.col("finishes").list.get(0).alias("finishes_0"),
            pl.col("finishes").list.get(1).alias("finishes_1"),
        ])
        .with_columns([
            # Card is foil-only if "nonfoil" NOT in its finishes
            ~pl.col("finishes_0").list.contains("nonfoil").alias("_is_foil_0"),
            ~pl.col("finishes_1").list.contains("nonfoil").alias("_is_foil_1"),
        ])
        .filter(
            # Only process pairs where one is foil and one is nonfoil
            pl.col("_is_foil_0") != pl.col("_is_foil_1")
        )
    )
    
    # Build lookup: for each uuid, what's its foil/nonfoil counterpart?
    # If card 0 is foil: card 1's foil_version = card 0, card 0's nonfoil_version = card 1
    foil_map = (
        pair_cards
        .select([
            # For the non-foil card, its foil version is the foil card
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_1"))
            .otherwise(pl.col("uuid_0"))
            .alias("uuid"),
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_0"))
            .otherwise(pl.col("uuid_1"))
            .alias("_foil_version"),
        ])
    )
    
    nonfoil_map = (
        pair_cards
        .select([
            # For the foil card, its nonfoil version is the non-foil card
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_0"))
            .otherwise(pl.col("uuid_1"))
            .alias("uuid"),
            pl.when(pl.col("_is_foil_0"))
            .then(pl.col("uuid_1"))
            .otherwise(pl.col("uuid_0"))
            .alias("_nonfoil_version"),
        ])
    )
    
    # Join mappings back to main LazyFrame
    lf = lf.join(foil_map, on="uuid", how="left")
    lf = lf.join(nonfoil_map, on="uuid", how="left")
    
    # Inject into identifiers struct
    lf = lf.with_columns(
        pl.col("identifiers").struct.with_fields([
            pl.col("_foil_version").alias("mtgjsonFoilVersionId"),
            pl.col("_nonfoil_version").alias("mtgjsonNonFoilVersionId"),
        ])
    ).drop(["_foil_version", "_nonfoil_version"])
    
    return lf


# 5.4:
def add_duel_deck_side(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add duelDeck field for Duel Deck (DD*) and GS1 sets.
    
    Uses duel_deck_sides.json mapping.
    Assumes setCode column exists.
    """
    import json
    
    sides_path = constants.RESOURCE_PATH / "duel_deck_sides.json"
    if not sides_path.exists():
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("duelDeck"))
    
    with sides_path.open(encoding="utf-8") as f:
        all_sides = json.load(f)
    
    # Flatten to DataFrame: setCode, number, duelDeck
    side_rows = []
    for set_code, number_map in all_sides.items():
        for number, side in number_map.items():
            side_rows.append({
                "_dd_set": set_code,
                "_dd_number": number,
                "duelDeck": side,
            })
    
    if not side_rows:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("duelDeck"))
    
    sides_df = pl.LazyFrame(side_rows)
    
    # Only applies to DD* and GS1 sets
    is_duel_deck = (
        pl.col("setCode").str.starts_with("DD") 
        | (pl.col("setCode") == "GS1")
    )
    
    # Join on setCode + number
    lf = lf.join(
        sides_df,
        left_on=["setCode", "number"],
        right_on=["_dd_set", "_dd_number"],
        how="left",
    )
    
    # Null out duelDeck for non-duel-deck sets (in case of number collisions)
    lf = lf.with_columns(
        pl.when(is_duel_deck)
        .then(pl.col("duelDeck"))
        .otherwise(pl.lit(None))
        .alias("duelDeck")
    )
    
    return lf


# 5.5:
def add_secret_lair_subsets(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Add subsets field for Secret Lair (SLD) cards.

    Links collector numbers to drop names.
    """
    sld_df = ctx.sld_subsets_df if ctx else GLOBAL_CACHE.sld_subsets_df
    if sld_df is None or sld_df.is_empty():
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("subsets"))

    # Rename the subsets column before joining to avoid conflicts
    sld_renamed = sld_df.rename({"subsets": "_sld_subsets"})
    lf = lf.join(
        sld_renamed.lazy(),
        on="number",
        how="left",
    )

    return lf.with_columns(
        pl.when(pl.col("setCode") == "SLD")
        .then(pl.col("_sld_subsets"))
        .otherwise(pl.lit(None))
        .alias("subsets")
    ).drop("_sld_subsets", strict=False)


# 5.6:
def add_source_products(lf: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Add sourceProducts field linking cards to sealed products.

    Uses GitHubDataProvider.card_to_products_df for lazy join.
    """
    card_to_products_df = ctx.card_to_products_df if ctx else GLOBAL_CACHE.github.card_to_products_df

    if card_to_products_df is None or card_to_products_df.is_empty():
        return lf.with_columns(
            pl.lit(None).cast(
                pl.Struct({
                    "foil": pl.List(pl.String),
                    "nonfoil": pl.List(pl.String),
                    "etched": pl.List(pl.String),
                })
            ).alias("sourceProducts")
        )

    return (
        lf
        .join(
            card_to_products_df.lazy(),
            on="uuid",
            how="left",
        )
        .with_columns(
            pl.struct([
                pl.col("foil"),
                pl.col("nonfoil"),
                pl.col("etched"),
            ]).alias("sourceProducts")
        )
        .drop(["foil", "nonfoil", "etched"])
    )


# 5.7:
def add_multiverse_bridge_ids(df: pl.LazyFrame, ctx: Optional[PipelineContext] = None) -> pl.LazyFrame:
    """
    Add Cardsphere and Deckbox IDs from MultiverseBridge data.

    Joins on scryfall_id (from identifiers) to add:
    - cardsphereId (non-foil)
    - cardsphereFoilId (foil)
    - deckboxId

    These get merged into the identifiers struct.
    """
    rosetta_cards = ctx.multiverse_bridge_cards if ctx else GLOBAL_CACHE.multiverse_bridge_cards
    if not rosetta_cards:
        LOGGER.debug("MultiverseBridge cache not loaded, skipping")
        return df

    # Build lookup LazyFrame from rosetta data
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

    rosetta_df = pl.LazyFrame(records)

    # Extract scryfall_id from identifiers for join
    df = df.with_columns(
        pl.col("identifiers").struct.field("scryfallId").alias("_scryfall_id_for_mb")
    )

    # Join
    df = df.join(rosetta_df, left_on="_scryfall_id_for_mb", right_on="_mb_scryfall_id", how="left")

    # The identifiers struct will be rebuilt in LazyFrame_to_card_objects
    # Just keep the columns for now
    return df.drop("_scryfall_id_for_mb")


# --- Stage 6 ---

# 6.0:
def add_token_signatures(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add signature field and "signed" finish for Art Series and memorabilia cards.
    
    Assumes set metadata columns: set_name, set_type, setCode
    
    Logic:
    - Art Series (except MH1): signature = artist
    - Memorabilia with gold border: signature from world_championship_signatures.json
    """
    import json
    
    # Condition expressions
    is_art_series = (
        pl.col("set_name").str.ends_with("Art Series") 
        & (pl.col("setCode") != "MH1")
    )
    is_memorabilia = pl.col("set_type") == "memorabilia"
    
    # Load world championship signatures
    signatures_path = constants.RESOURCE_PATH / "world_championship_signatures.json"
    
    if signatures_path.exists():
        with signatures_path.open(encoding="utf-8") as f:
            signatures_by_set = json.load(f)
        
        # Flatten to DataFrame: setCode, prefix, signature_name
        sig_rows = []
        for set_code, prefix_map in signatures_by_set.items():
            for prefix, sig_name in prefix_map.items():
                sig_rows.append({
                    "_sig_set": set_code,
                    "_sig_prefix": prefix,
                    "_sig_name": sig_name,
                })
        
        signatures_df = pl.LazyFrame(sig_rows) if sig_rows else None
    else:
        signatures_df = None
    
    # Extract number prefix for memorabilia lookup
    lf = lf.with_columns([
        pl.col("number").str.extract(r"^([^0-9]+)", 1).alias("_num_prefix"),
        pl.col("number").str.extract(r"^[^0-9]+([0-9]+)", 1).alias("_num_digits"),
        pl.col("number").str.extract(r"^[^0-9]+[0-9]+(.*)", 1).alias("_num_suffix"),
    ])
    
    # Join signatures for memorabilia
    if signatures_df is not None:
        lf = lf.join(
            signatures_df,
            left_on=["setCode", "_num_prefix"],
            right_on=["_sig_set", "_sig_prefix"],
            how="left",
        )
    else:
        lf = lf.with_columns(pl.lit(None).cast(pl.String).alias("_sig_name"))
    
    # Compute signature field
    # Art Series: signature = artist
    # Memorabilia: signature from lookup (if gold border and valid number)
    memorabilia_signature = (
        pl.when(
            (pl.col("borderColor") == "gold")
            & pl.col("_sig_name").is_not_null()
            & ~((pl.col("_num_digits") == "0") & (pl.col("_num_suffix") == "b"))
        )
        .then(pl.col("_sig_name"))
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
        pl.when(
            pl.col("signature").is_not_null()
            & ~pl.col("finishes").list.contains("signed")
        )
        .then(pl.col("finishes").list.concat(pl.lit(["signed"])))
        .otherwise(pl.col("finishes"))
        .alias("finishes")
    )
    
    # Cleanup
    return lf.drop([
        "_num_prefix", "_num_digits", "_num_suffix", "_sig_name"
    ], strict=False)


# 6.1:
def  add_orientations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add orientation field for Art Series tokens.

    Fetches orientation maps for all Art Series sets in one pass,
    then joins to the LazyFrame.

    Assumes set metadata columns: set_name, setCode
    """
    # Identify Art Series sets in the data
    art_series_sets = (
        lf.filter(pl.col("set_name").str.contains("Art Series"))
        .select("setCode")
        .unique()
        .collect()
        .to_series()
        .to_list()
    )

    if not art_series_sets:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("orientation"))

    # Fetch orientation maps for all Art Series sets
    detector = ScryfallProviderOrientationDetector()
    all_orientations = []

    for set_code in art_series_sets:
        orientation_map = detector.get_uuid_to_orientation_map(set_code)
        if orientation_map:
            for scryfall_id, orientation in orientation_map.items():
                all_orientations.append({
                    "_orient_scryfall_id": scryfall_id,
                    "orientation": orientation,
                })

    if not all_orientations:
        return lf.with_columns(pl.lit(None).cast(pl.String).alias("orientation"))

    orientation_df = pl.LazyFrame(all_orientations)

    # Join on scryfall_id from identifiers
    lf = lf.with_columns(
        pl.col("identifiers").struct.field("scryfallId").alias("_scryfall_id_orient")
    )

    lf = lf.join(
        orientation_df,
        left_on="_scryfall_id_orient",
        right_on="_orient_scryfall_id",
        how="left",
    ).drop("_scryfall_id_orient")

    return lf


# 6.2:
def rename_all_the_things(lf: pl.LazyFrame, output_type: str = "card_set") -> pl.LazyFrame:
    """
    Final transformation: Renames internal columns to MTGJSON CamelCase,
    builds nested structs, and selects only fields valid for the output_type.
    """
    
    # Rename Map: Internal (Snake/Scryfall) -> External (MTGJSON CamelCase)
    rename_map = {
        "uuid": "uuid",
        "name": "name",
        "faceName": "faceName",
        "set": "setCode",
        "number": "number",
        "layout": "layout",
        "manaCost": "manaCost",
        "cmc": "convertedManaCost",
        "manaValue": "manaValue",
        "faceManaValue": "faceConvertedManaCost",
        "colors": "colors",
        "colorIdentity": "colorIdentity",
        "text": "text",
        "flavorText": "flavorText",
        "power": "power",
        "toughness": "toughness",
        "loyalty": "loyalty",
        "defense": "defense",
        "rarity": "rarity",
        "artist": "artist",
        "artistIds": "artistIds",
        "borderColor": "borderColor",
        "frameVersion": "frameVersion",
        "frameEffects": "frameEffects",
        "securityStamp": "securityStamp",
        "watermark": "watermark",
        "finishes": "finishes",
        "promoTypes": "promoTypes",
        "boosterTypes": "boosterTypes",
        "edhrecRank": "edhrecRank",
        "foreignData": "foreignData",
        "identifiers": "identifiers",
        "legalities": "legalities",
        "rulings": "rulings",
        "variations": "variations",
        "isAlternative": "isAlternative",
        "source_products": "sourceProducts",
        "relatedCards": "relatedCards",
        "has_foil": "hasFoil",
        "has_non_foil": "hasNonFoil",
        "is_reserved": "isReserved",
        "is_oversized": "isOversized",
        "is_promo": "isPromo",
        "is_reprint": "isReprint",
        "is_online_only": "isOnlineOnly",
        "is_full_art": "isFullArt",
        "is_textless": "isTextless",
        "is_story_spotlight": "isStorySpotlight",
        "is_funny": "isFunny",
        "is_timeshifted": "isTimeshifted"
    }

    # We use strict=False because some source cols might be missing in specific batches
    lf = lf.rename({k: v for k, v in rename_map.items()}, strict=False)

    # Handle Special Logic (Face Mana Value)
    multiface_layouts = ["split", "flip", "transform", "modal_dfc", "meld", "adventure", "reversible_card"]
    
    # Check if we have layout/faceConvertedManaCost columns before trying to use them
    # (LazyFrames don't always know schema until collecting, but we can try/except or assume standard pipeline)
    lf = lf.with_columns([
        pl.when(pl.col("layout").is_in(multiface_layouts))
        .then(pl.col("faceConvertedManaCost"))
        .otherwise(pl.lit(0.0))
        .alias("faceConvertedManaCost")
    ])

    # Convert list ["paper", "mtgo"] -> Struct {paper: true, mtgo: true}
    # This matches the MtgjsonGameFormatsObject
    formats = ["paper", "mtgo", "arena", "shandalar", "dreamcast"]
    
    lf = lf.with_columns(
        pl.struct([
            pl.col("availability").list.contains(fmt).alias(fmt) 
            for fmt in formats
        ]).alias("availability")
    )

    # Get the allowed fields for this specific output type (e.g. 'card_set' vs 'card_token')
    if output_type == "card_set":
        allowed_fields = ALL_CARD_FIELDS - CARD_SET_EXCLUDE
    elif output_type == "card_token":
        allowed_fields = ALL_CARD_FIELDS - TOKEN_EXCLUDE
    elif output_type == "card_atomic":
        allowed_fields = ALL_CARD_FIELDS - ATOMIC_EXCLUDE
    elif output_type == "card_deck":
        allowed_fields = (ALL_CARD_FIELDS - CARD_DECK_EXCLUDE) | {"count", "isFoil"}
    else:
        raise ValueError(f"Unknown output type: {output_type}")

    # Collect to get actual schema, then select only allowed fields that exist
    df = lf.collect()
    existing_cols = set(df.columns)
    final_cols = sorted(existing_cols & allowed_fields)

    return df.select(final_cols).lazy()

# 6.3
def select_card_set_fields(df: pl.LazyFrame) -> pl.LazyFrame:
    """Select and order columns for CardSet output."""
    fields = ALL_CARD_FIELDS - CARD_SET_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)

# 6.4:
def select_card_token_fields(df: pl.LazyFrame) -> pl.LazyFrame:
    """Select and order columns for CardToken output."""
    fields = ALL_CARD_FIELDS - TOKEN_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)

# 6.5:
def select_card_atomic_fields(df: pl.LazyFrame) -> pl.LazyFrame:
    """Select and order columns for CardAtomic output."""
    fields = ALL_CARD_FIELDS - ATOMIC_EXCLUDE
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)

# 6.6:
def select_card_deck_fields(df: pl.LazyFrame) -> pl.LazyFrame:
    """Select and order columns for CardDeck output."""
    fields = (ALL_CARD_FIELDS - CARD_DECK_EXCLUDE) | {"count", "isFoil"}
    existing = sorted(c for c in df.columns if c in fields)
    return df.select(existing)

# 6.7:
def filter_out_tokens(df: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
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

# 6.8:
def build_json_outputs(parquet_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """
    Build all JSON outputs from partitioned parquet.
    
    Runs after pipeline sink completes.
    """
    import orjson
    from datetime import datetime
    
    LOGGER.info("Building individual set files...")
    
    all_sets = {}
    
    for set_folder in sorted(parquet_dir.glob("set=*")):
        set_code = set_folder.name.replace("set=", "")
        
        df = pl.read_parquet(set_folder / "*.parquet")
        
        # Split cards vs tokens
        cards_df, tokens_df = filter_out_tokens(df.lazy())
        cards_df = rename_all_the_things(cards_df, "card_set").collect()
        tokens_df = rename_all_the_things(tokens_df, "card_token").collect()
        
        # Get set metadata
        set_meta = GLOBAL_CACHE.sets_df.filter(pl.col("code") == set_code).to_dicts()
        set_meta = set_meta[0] if set_meta else {}
        
        set_obj = {
            "code": set_code,
            "name": set_meta.get("name"),
            "type": set_meta.get("set_type"),
            "releaseDate": set_meta.get("released_at"),
            "block": set_meta.get("block"),
            "cards": cards_df.to_dicts(),
            "tokens": tokens_df.to_dicts(),
        }
        
        all_sets[set_code] = set_obj
        
        # Write individual set file
        set_path = output_dir / f"{set_code}.json"
        with set_path.open("wb") as f:
            f.write(orjson.dumps({
                "meta": {"date": datetime.now().strftime("%Y-%m-%d"), "version": "5.3.0"},
                "data": set_obj,
            }, option=orjson.OPT_INDENT_2))
        
        LOGGER.info(f"  {set_code}: {len(cards_df)} cards, {len(tokens_df)} tokens")
    
    # AllPrintings.json
    LOGGER.info("Writing AllPrintings.json...")
    with (output_dir / "AllPrintings.json").open("wb") as f:
        f.write(orjson.dumps({
            "meta": {"date": datetime.now().strftime("%Y-%m-%d"), "version": "5.3.0"},
            "data": all_sets,
        }))
    
    # AllIdentifiers.json
    LOGGER.info("Writing AllIdentifiers.json...")
    df = pl.scan_parquet(parquet_dir / "**/*.parquet").select(["uuid", "identifiers"]).collect()
    identifiers_map = {row["uuid"]: row["identifiers"] for row in df.to_dicts()}
    with (output_dir / "AllIdentifiers.json").open("wb") as f:
        f.write(orjson.dumps({"meta": {}, "data": identifiers_map}))
    
    # AtomicCards.json
    LOGGER.info("Writing AtomicCards.json...")
    df = (
        pl.scan_parquet(parquet_dir / "**/*.parquet")
        .filter(pl.col("oracle_id").is_not_null())
        .unique(subset=["oracle_id"], keep="first")
        .pipe(lambda x: rename_all_the_things(x, "card_atomic"))
        .collect()
    )
    atomic_map: dict[str, list] = {}
    for row in df.to_dicts():
        name = row.get("name", "Unknown")
        atomic_map.setdefault(name, []).append(row)
    with (output_dir / "AtomicCards.json").open("wb") as f:
        f.write(orjson.dumps({"meta": {}, "data": atomic_map}))
    
    LOGGER.info("JSON outputs complete.")


def build_cards(
    set_codes: list[str] | None = None,
    skip_tokens: bool = False,
    ctx: Optional[PipelineContext] = None,
) -> None:
    """
    Build all cards using fully vectorized Polars pipeline.

    All operations run on the complete LazyFrame before a single sink.
    No per-set iteration until final JSON output.

    Args:
        set_codes: Optional list of set codes to filter. If None, builds all sets.
        skip_tokens: If True, skip token-type cards.
        ctx: Optional PipelineContext with lookup data. If None, uses GLOBAL_CACHE.
    """
    # Use provided context or create from global cache
    if ctx is None:
        if GLOBAL_CACHE.cards_df is None:
            raise RuntimeError("Cache not loaded. Call GLOBAL_CACHE.load_all() first.")
        ctx = PipelineContext.from_global_cache()

    if ctx.cards_df is None or ctx.sets_df is None:
        raise RuntimeError("PipelineContext missing required cards_df or sets_df")

    output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = output_dir / "_parquet"

    LOGGER.info("Building card pipeline...")

    # =========================================================================
    # Set Metadata Join
    # =========================================================================
    sets_lf = ctx.sets_df.lazy().select([
        pl.col("code").str.to_uppercase().alias("set"),
        pl.col("name").alias("set_name"),
        pl.col("set_type"),
        pl.col("card_count"),
        pl.col("released_at").alias("set_release_date"),
        pl.col("parent_set_code"),
        pl.col("block"),
        pl.col("block_code"),
    ])

    lf = (
        ctx.cards_df
        .filter(pl.col("lang") == "en")
        .with_columns(pl.col("set").str.to_uppercase())
        .join(sets_lf, on="set", how="left")
    )
    
    if set_codes:
        lf = lf.filter(pl.col("set").is_in([s.upper() for s in set_codes]))
    
    if skip_tokens:
        lf = lf.filter(~pl.col("layout").is_in(["token", "double_faced_token", "emblem"]))
    
    # =========================================================================
    # PHASE 1: Core Transformations
    # =========================================================================
    lf = (
        lf
        .pipe(explode_card_faces)
        .pipe(add_basic_fields)
        .pipe(parse_type_line_expr)
        .pipe(add_mana_info)
        .pipe(add_card_attributes)
        .pipe(filter_keywords_for_face)
        .pipe(add_booster_types)
        .pipe(partial(add_legalities_struct, ctx=ctx))
        .pipe(partial(add_availability_struct, ctx=ctx))
    )

    # =========================================================================
    # PHASE 2: External Data Joins
    # =========================================================================
    lf = (
        lf
        .pipe(partial(join_card_kingdom_data, ctx=ctx))
        .pipe(partial(join_cardmarket_ids, ctx=ctx))
        .pipe(add_identifiers_struct)
        .pipe(partial(join_printings, ctx=ctx))
        .pipe(partial(join_rulings, ctx=ctx))
        .pipe(partial(join_foreign_data, ctx=ctx))
        .pipe(partial(join_edhrec_data, ctx=ctx))
        .pipe(partial(join_gatherer_data, ctx=ctx))
    )

    # =========================================================================
    # PHASE 3: UUID Generation
    # =========================================================================
    lf = lf.pipe(partial(add_uuid_expr, ctx=ctx))

    # =========================================================================
    # PHASE 4: UUID-Dependent Derivations
    # =========================================================================
    lf = (
        lf
        .pipe(add_other_face_ids)
        .pipe(add_variations)
        .pipe(partial(add_leadership_skills_expr, ctx=ctx))
        .pipe(add_reverse_related)
        .pipe(partial(add_related_cards_struct, ctx=ctx))
        .pipe(partial(add_alternative_deck_limit, ctx=ctx))
        .pipe(partial(add_is_funny, ctx=ctx))
        .pipe(add_is_timeshifted)
        .pipe(add_purchase_urls_struct)
    )

    # =========================================================================
    # PHASE 5: Cross-Card Linkages (all lazy, no per-set iteration)
    # =========================================================================
    lf = (
        lf
        .pipe(partial(apply_manual_overrides, ctx=ctx))
        .pipe(partial(add_meld_card_parts, ctx=ctx))
        .pipe(add_rebalanced_linkage)
        .pipe(link_foil_nonfoil_versions)
        .pipe(add_duel_deck_side)
        .pipe(partial(add_secret_lair_subsets, ctx=ctx))
        .pipe(partial(add_source_products, ctx=ctx))
        .pipe(partial(add_multiverse_bridge_ids, ctx=ctx))
    )
    
    # =========================================================================
    # PHASE 6: Token-Specific (still lazy, conditional on set_type/set_name)
    # =========================================================================
    lf = (
        lf
        .pipe(add_token_signatures)
        .pipe(add_orientations)
    )
    
    # =========================================================================
    # PHASE 7: Final Selection & Sink
    # =========================================================================
    LOGGER.info("Sinking to hive-partitioned parquet...")

    final_lf = lf.pipe(rename_all_the_things, output_type="card_set")
    final_lf.sink_parquet(
        pl.PartitionByKey(
            parquet_dir,
            by=["setCode"],
            include_key=True,
        ),
        mkdir=True,
    )
    LOGGER.info(f"Wrote hive-partitioned parquet to {parquet_dir}")


def build_tokens(
    set_codes: list[str] | None = None,
    ctx: Optional[PipelineContext] = None,
) -> None:
    """
    Build all tokens using the same vectorized pipeline as cards.

    Tokens are filtered by layout (token, double_faced_token, emblem, art_series).
    Output is written to _parquet_tokens/ partitioned by setCode.
    """
    if ctx is None:
        if GLOBAL_CACHE.cards_df is None:
            raise RuntimeError("Cache not loaded. Call GLOBAL_CACHE.load_all() first.")
        ctx = PipelineContext.from_global_cache()

    output_dir = MtgjsonConfig().output_path
    token_parquet_dir = output_dir / "_parquet_tokens"

    LOGGER.info("Building tokens pipeline...")

    sets_lf = ctx.sets_df.lazy().select([
        pl.col("code").str.to_uppercase().alias("set"),
        pl.col("name").alias("set_name"),
        pl.col("set_type"),
        pl.col("card_count"),
        pl.col("released_at").alias("set_release_date"),
        pl.col("parent_set_code"),
        pl.col("block"),
        pl.col("block_code"),
    ])

    # Filter to tokens only
    lf = (
        ctx.cards_df
        .filter(
            (pl.col("lang") == "en") &
            pl.col("layout").is_in(TOKEN_LAYOUTS)
        )
        .with_columns(pl.col("set").str.to_uppercase())
        .join(sets_lf, on="set", how="left")
    )

    if set_codes:
        lf = lf.filter(pl.col("set").is_in([s.upper() for s in set_codes]))

    # Apply same transformations as cards (tokens need similar processing)
    lf = (
        lf
        .pipe(explode_card_faces)
        .pipe(add_basic_fields)
        .pipe(parse_type_line_expr)
        .pipe(add_mana_info)
        .pipe(add_card_attributes)
        .pipe(filter_keywords_for_face)
        .pipe(partial(add_legalities_struct, ctx=ctx))
        .pipe(partial(add_availability_struct, ctx=ctx))
        .pipe(partial(join_card_kingdom_data, ctx=ctx))
        .pipe(add_identifiers_struct)
        .pipe(partial(add_uuid_expr, ctx=ctx))
        .pipe(add_reverse_related)
        .pipe(add_token_signatures)
        .pipe(add_orientations)
    )

    LOGGER.info("Sinking tokens to hive-partitioned parquet...")

    final_lf = lf.pipe(rename_all_the_things, output_type="token")
    final_lf.sink_parquet(
        pl.PartitionByKey(
            token_parquet_dir,
            by=["setCode"],
            include_key=True,
        ),
        mkdir=True,
    )
    LOGGER.info(f"Wrote tokens parquet to {token_parquet_dir}")


def build_decks_df(set_code: str | None = None) -> pl.DataFrame:
    """
    Build decks DataFrame with full card lists.

    Fetches raw deck data and transforms to MTGJSON format with
    mainBoard, sideBoard, commander as lists of {count, uuid}.

    Args:
        set_code: Optional set code filter. If None, returns all sets.

    Returns:
        DataFrame with full deck structure including card lists.
    """
    import requests

    # Fetch raw deck data from GitHub
    url = "https://github.com/taw/magic-preconstructed-decks-data/blob/master/decks_v2.json?raw=true"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        raw_decks = r.json()
    except Exception as e:
        LOGGER.warning(f"Failed to fetch deck data: {e}")
        return pl.DataFrame()

    def transform_card_list(cards: list[dict]) -> list[dict]:
        """Transform raw card list to {count, uuid} format."""
        return [
            {"count": c.get("count", 1), "uuid": c.get("mtgjson_uuid")}
            for c in cards
            if c.get("mtgjson_uuid")
        ]

    decks = []
    for deck in raw_decks:
        deck_set_code = deck.get("set_code", "").upper()
        if set_code and deck_set_code != set_code.upper():
            continue

        decks.append({
            "setCode": deck_set_code,
            "code": deck_set_code,
            "name": deck.get("name"),
            "type": deck.get("type"),
            "releaseDate": deck.get("release_date"),
            "sourceSetCodes": [s.upper() for s in deck.get("sourceSetCodes", [])],
            "sealedProductUuids": None,  # Would need deck_map.json lookup
            "mainBoard": transform_card_list(deck.get("cards", [])),
            "sideBoard": transform_card_list(deck.get("sideboard", [])),
            "commander": transform_card_list(deck.get("commander", [])),
            "displayCommander": transform_card_list(deck.get("displayCommander", [])),
            "planes": transform_card_list(deck.get("planarDeck", [])),
            "schemes": transform_card_list(deck.get("schemeDeck", [])),
            "tokens": transform_card_list(deck.get("tokens", [])),
        })

    if not decks:
        return pl.DataFrame()

    return pl.DataFrame(decks)


def build_sealed_products_df(set_code: str | None = None) -> pl.DataFrame:
    """
    Build sealed products DataFrame with contents struct.

    Joins github_sealed_products with github_sealed_contents
    and aggregates contents by type (card, sealed, other).

    Args:
        set_code: Optional set code filter. If None, returns all sets.

    Returns:
        DataFrame with columns: setCode, name, category, subtype, releaseDate,
        identifiers (struct), contents (struct), uuid
    """
    import uuid as uuid_module

    products_df = pl.read_parquet(GLOBAL_CACHE.CACHE_DIR / "github_sealed_products.parquet")
    contents_df = pl.read_parquet(GLOBAL_CACHE.CACHE_DIR / "github_sealed_contents.parquet")

    # Normalize set codes
    products_df = products_df.with_columns(
        pl.col("set_code").str.to_uppercase().alias("setCode")
    )
    contents_df = contents_df.with_columns(
        pl.col("set_code").str.to_uppercase().alias("setCode")
    )

    if set_code:
        products_df = products_df.filter(pl.col("setCode") == set_code.upper())
        contents_df = contents_df.filter(pl.col("setCode") == set_code.upper())

    # Aggregate contents by product and content_type
    # Each content type becomes a list of structs
    card_contents = (
        contents_df
        .filter(pl.col("content_type") == "card")
        .group_by(["setCode", "product_name"])
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
        contents_df
        .filter(pl.col("content_type") == "sealed")
        .group_by(["setCode", "product_name"])
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
        contents_df
        .filter(pl.col("content_type") == "other")
        .group_by(["setCode", "product_name"])
        .agg(
            pl.struct(
                name=pl.col("name"),
            ).alias("_other_list")
        )
    )

    # Join contents to products
    result = (
        products_df
        .join(card_contents, on=["setCode", "product_name"], how="left")
        .join(sealed_contents, on=["setCode", "product_name"], how="left")
        .join(other_contents, on=["setCode", "product_name"], how="left")
    )

    # Build contents struct
    result = result.with_columns(
        pl.struct(
            card=pl.col("_card_list"),
            sealed=pl.col("_sealed_list"),
            other=pl.col("_other_list"),
        ).alias("contents")
    ).drop(["_card_list", "_sealed_list", "_other_list"])

    # Clean up identifiers - remove null values
    # identifiers is already a dict/struct from the parquet

    # Generate UUID for each product
    result = result.with_columns(
        pl.col("product_name").map_elements(
            lambda name: str(uuid_module.uuid5(uuid_module.NAMESPACE_DNS, name)),
            return_dtype=pl.String
        ).alias("uuid")
    )

    # Rename and select final columns
    result = result.select([
        "setCode",
        pl.col("product_name").alias("name"),
        pl.col("category").str.to_lowercase(),
        pl.col("subtype").str.to_lowercase(),
        pl.col("release_date").alias("releaseDate"),
        "identifiers",
        "contents",
        "uuid",
    ])

    return result


def build_set_metadata_df(ctx: Optional[PipelineContext] = None) -> pl.DataFrame:
    """
    Build a DataFrame containing all set-level metadata.

    Includes: code, name, releaseDate, type, mcmId, tcgplayerGroupId,
    booster configs, translations, etc.
    """
    if ctx is None:
        ctx = PipelineContext.from_global_cache()

    sets_df = ctx.sets_df

    # Load booster configs
    booster_df = pl.read_parquet(GLOBAL_CACHE.CACHE_DIR / "github_booster.parquet")

    # Load translations
    translations_path = constants.RESOURCE_PATH / "mkm_set_name_translations.json"
    if translations_path.exists():
        with translations_path.open(encoding="utf-8") as f:
            translations_data = json.load(f)
    else:
        translations_data = {}

    # Build set metadata DataFrame
    set_meta = (
        sets_df
        .with_columns([
            pl.col("code").str.to_uppercase().alias("code"),
            pl.col("name"),
            pl.col("released_at").alias("releaseDate"),
            pl.col("set_type").alias("type"),
            pl.col("mtgo_code").str.to_uppercase().alias("mtgoCode"),
            pl.col("tcgplayer_id").alias("tcgplayerGroupId"),
            pl.col("digital").alias("isOnlineOnly"),
            pl.col("foil_only").alias("isFoilOnly"),
            pl.col("nonfoil_only").alias("isNonFoilOnly"),
            pl.col("parent_set_code").str.to_uppercase().alias("parentCode"),
            pl.col("block"),
            # Keyrune code from icon URL
            pl.col("icon_svg_uri")
                .str.extract(r"/([^/]+)\.svg", 1)
                .str.to_uppercase()
                .alias("keyruneCode"),
            # Token set code
            pl.when(pl.col("code").str.starts_with("T"))
                .then(pl.col("code").str.to_uppercase())
                .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase())
                .alias("tokenSetCode"),
        ])
        .join(
            booster_df.with_columns(pl.col("set_code").str.to_uppercase().alias("code")),
            on="code",
            how="left",
        )
        .rename({"config": "booster"})
    )

    return set_meta


def assemble_json_outputs(
    set_codes: list[str] | None = None,
    pretty_print: bool = False,
) -> None:
    """
    Read parquet partitions and assemble final JSON files per set.

    Combines cards, tokens, boosters, sealed products, decks into
    the full MTGJSON set structure.
    """
    output_dir = MtgjsonConfig().output_path
    parquet_dir = output_dir / "_parquet"
    token_parquet_dir = output_dir / "_parquet_tokens"

    LOGGER.info("Assembling JSON outputs from parquet...")

    # Get list of sets to process
    if set_codes:
        sets_to_process = [s.upper() for s in set_codes]
    else:
        # Get all set codes from parquet partitions
        sets_to_process = [
            p.name.replace("setCode=", "")
            for p in parquet_dir.iterdir()
            if p.is_dir() and p.name.startswith("setCode=")
        ]

    # Load set metadata
    set_meta_df = build_set_metadata_df()
    set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

    # Load sealed products with proper contents structure
    sealed_products_df = build_sealed_products_df()

    # Load decks
    decks_df = pl.read_parquet(GLOBAL_CACHE.CACHE_DIR / "github_decks.parquet")

    # Build meta object
    meta = MtgjsonMetaObject()
    meta_dict = {"date": meta.date, "version": meta.version}

    for set_code in sets_to_process:
        LOGGER.info(f"Assembling {set_code}...")

        # Read cards for this set
        cards_path = parquet_dir / f"setCode={set_code}"
        if not cards_path.exists():
            LOGGER.warning(f"No cards found for {set_code}")
            continue

        cards_df = pl.read_parquet(cards_path / "*.parquet")
        cards = cards_df.to_dicts()

        # Read tokens for this set
        tokens = []
        tokens_path = token_parquet_dir / f"setCode=T{set_code}"
        if tokens_path.exists():
            tokens_df = pl.read_parquet(tokens_path / "*.parquet")
            tokens = tokens_df.to_dicts()

        # Get set metadata
        meta_row = set_meta.get(set_code, {})

        # Get sealed products for this set
        set_sealed = sealed_products_df.filter(
            pl.col("setCode") == set_code
        ).drop("setCode").to_dicts()

        # Get decks for this set
        set_decks = decks_df.filter(
            pl.col("set_code").str.to_uppercase() == set_code
        ).to_dicts()

        # Get booster config
        booster = meta_row.get("booster")
        if booster:
            # Filter out null booster types
            booster = {k: v for k, v in booster.items() if v is not None}

        # Assemble final set object
        set_data = {
            "baseSetSize": len([c for c in cards if not c.get("isReprint")]),
            "cards": cards,
            "code": set_code,
            "name": set_meta.get("name"),
            "type": set_meta.get("set_type"),
            "releaseDate": set_meta.get("released_at"),
            "block": set_meta.get("block"),
            "cards": cards_list,
            "tokens": [],
        }

        set_path = output_dir / f"{set_code}.json"
        with set_path.open("wb") as f:
            f.write(orjson.dumps(
                {
                    "meta": {"date": datetime.now().strftime("%Y-%m-%d"), "version": "5.3.0"},
                    "data": set_obj,
                },
                option=orjson.OPT_INDENT_2,
            ))
        LOGGER.info(f"Wrote {set_path} ({len(cards_list)} cards)")

    LOGGER.info("Full vectorized build complete.")