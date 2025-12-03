from dataclasses import dataclass, field
import polars as pl

from mtgjson5 import constants
from mtgjson5.cache import GLOBAL_CACHE
from mtgjson5.models.providers.scryfall import CardFace


TOKEN_LAYOUTS={"token", "double_faced_token", "emblem", "art_series"}

@dataclass
class PipelineContext:
    """
    Container for all lookup data needed by the card pipeline.
    """

    # Core DataFrames
    cards_df: pl.LazyFrame|None = None
    sets_df: pl.DataFrame|None = None

    # Lookup DataFrames
    card_kingdom_df: pl.DataFrame|None = None
    mcm_lookup_df: pl.DataFrame|None = None
    printings_df: pl.DataFrame|None = None
    rulings_df: pl.DataFrame|None = None
    salt_df: pl.DataFrame|None = None
    spellbook_df: pl.DataFrame|None = None
    sld_subsets_df: pl.DataFrame|None = None
    uuid_cache_df: pl.DataFrame|None = None

    # Dict lookups
    gatherer_map: dict = field(default_factory=dict)
    meld_triplets: dict = field(default_factory=dict)
    manual_overrides: dict = field(default_factory=dict)
    multiverse_bridge_cards: dict = field(default_factory=dict)

    # Provider accessors
    standard_legal_sets: set[str] = field(default_factory=set)
    unlimited_cards: set[str] = field(default_factory=set)

    # GitHub data
    card_to_products_df: pl.DataFrame|None = None
   
   
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
            unlimited_cards=GLOBAL_CACHE.scryfall.cards_without_limits
            if GLOBAL_CACHE._scryfall
            else set(),
            categoricals=GLOBAL_CACHE.categoricals,
            card_to_products_df=GLOBAL_CACHE.github.card_to_products_df
            if GLOBAL_CACHE._github
            else None,
        )

 
def _ascii_name_expr(expr: pl.Expr) -> pl.Expr:
    """
    Build expression to normalize card name to ASCII.
    Pure Polars - stays lazy.
    """
    return (
        expr.str.replace_all("Æ", "AE")
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
    

def is_token_expr() -> pl.Expr:
    """
    Expression to detect if a row is a token based on layout/type.

    Returns:
        pl.Expr: Boolean expression for token detection.
    """
    return (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type_line").fill_null("") == "Dungeon")
        | pl.col("type_line").fill_null("").str.contains("Token")
    )
    
    
def mark_tokens(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add _isToken boolean column to identify tokens.

    Should be called early in the pipeline so conditional expressions can use it.
    """
    return lf.with_columns(is_token_expr().alias("_isToken"))


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


# Core transformations
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
    face_struct_schema = CardFace.get_schema()
    lf = lf.with_row_index("_row_id")

    schema = lf.collect_schema()
    if "card_faces" not in schema.names():
        return lf.with_columns(
            pl.lit(0).alias("face_id"),
            pl.lit(None).cast(pl.String).alias("side"),
            pl.lit(None).cast(face_struct_schema).alias("_face_data"),
        )

    lf = lf.with_columns(
        pl.int_ranges(pl.col("card_faces").list.len()).alias("_face_idx")
    )

    lf = lf.explode(["card_faces", "_face_idx"])

    return lf.with_columns(
        pl.col("card_faces").alias("_face_data"),
        pl.col("_face_idx").fill_null(0).alias("face_id"),
        pl.col("_face_idx")
        .fill_null(0)
        .replace_strict(
            {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"},
            default="a",
            return_dtype=pl.String,
        )
        .alias("side"),
    ).drop(["card_faces", "_face_idx"])


def add_basic_fields(lf: pl.LazyFrame) -> pl.LazyFrame:
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
        lf.rename(
            {
                # Core identifiers (card-level only)
                "id": "scryfallId",
                "oracle_id": "oracleId",
                "set": "setCode",
                "collector_number": "number",
                "card_back_id": "cardBackId",
            }
        )
        .with_columns(
            [
                # Card-level name (full name with // for multi-face)
                # Scryfall's root "name" has the full name like "Invasion of Ravnica // Guildpact Paragon"
                pl.col("name").alias("name"),
                # Face-specific name (only for multi-face cards)
                # faceName is the individual face's name
                pl.when(
                    pl.col("layout").is_in(
                        [
                            "transform",
                            "modal_dfc",
                            "meld",
                            "reversible_card",
                            "flip",
                            "split",
                            "adventure",
                            "battle",
                        ]
                    )
                )
                .then(face_field("name"))
                .otherwise(pl.lit(None).cast(pl.String))
                .alias("faceName"),
                # Face-aware fields
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
                pl.col("lang")
                .replace_strict(
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
                )
                .alias("language"),
            ]
        )
        .with_columns(
            pl.when(ascii_name != face_name)
            .then(ascii_name)
            .otherwise(None)
            .alias("asciiName"),
        )
        .drop(
            [
                "lang",
                "frame",
                "border_color",
                "full_art",
                "textless",
                "oversized",
                "promo",
                "reprint",
                "story_spotlight",
                "reserved",
                "foil",
                "nonfoil",
                "flavor_name",
                "all_parts",
                "color_identity",
                "cmc",
                "frame_effects",
                "security_stamp",
            ],
            strict=False,
        )
    )


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
        pl.col("finishes")
        .list.contains("nonfoil")
        .fill_null(False)
        .alias("hasNonFoil"),
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


def filter_keywords_for_face(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = lf.with_row_index("_kw_row_idx")
    lf = lf.with_columns(
        pl.col("text").fill_null("").str.to_lowercase().alias("_text_lower"),
        pl.col("_all_keywords").fill_null([]),
    )

    # Process only rows that have keywords
    keywords_filtered = (
        lf.select("_kw_row_idx", "_text_lower", "_all_keywords")
        .filter(pl.col("_all_keywords").list.len() > 0)
        .explode("_all_keywords")
        .filter(
            pl.col("_text_lower").str.contains(
                pl.col("_all_keywords").str.to_lowercase()
            )
        )
        .group_by("_kw_row_idx")
        .agg(pl.col("_all_keywords").sort().alias("keywords"))
    )

    # Left join preserves rows with no keywords; fill_null gives them []
    return (
        lf.join(keywords_filtered, on="_kw_row_idx", how="left")
        .with_columns(pl.col("keywords").fill_null([]))
        .drop(["_kw_row_idx", "_text_lower", "_all_keywords"])
    )


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
                pl.col("promoTypes")
                .list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"]))
                .list.len()
                > 0
            )
            .then(pl.lit(["default", "deck"]))
            .otherwise(pl.lit(["default"]))
        )
        .otherwise(
            pl.when(
                pl.col("promoTypes")
                .list.set_intersection(pl.lit(["starterdeck", "planeswalkerdeck"]))
                .list.len()
                > 0
            )
            .then(pl.lit(["deck"]))
            .otherwise(pl.lit([]).cast(pl.List(pl.String)))
        )
        .alias("boosterTypes")
    ).drop("_in_booster")


