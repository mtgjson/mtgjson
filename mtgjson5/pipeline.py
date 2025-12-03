from dataclasses import dataclass, field
import polars as pl

from mtgjson5 import constants
from mtgjson5.cache import GLOBAL_CACHE
from mtgjson5.models.providers.scryfall import CardFace
from mtgjson5.utils import LOGGER


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


def add_legalities_struct(lf: pl.LazyFrame, ctx: PipelineContext = None ) -> pl.LazyFrame:
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
    return lf.with_columns(pl.struct(struct_fields).alias("legalities")).drop(
        formats, strict=False
    )


def add_availability_struct(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Build availability list from games column.

    Uses dynamically discovered game platforms.
    """
    schema = lf.collect_schema()

    if "games" not in schema.names():
        return lf.with_columns(
            pl.lit([]).cast(pl.List(pl.String)).alias("availability")
        )

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
            pl.concat_list(
                [
                    pl.when(pl.col("games").struct.field(p).fill_null(False))
                    .then(pl.lit(p))
                    .otherwise(pl.lit(None))
                    for p in platforms
                ]
            )
            .list.drop_nulls()
            .list.sort()
            .alias("availability")
        )
    else:
        # List format: ["paper", "mtgo"]
        return lf.with_columns(pl.col("games").list.sort().alias("availability"))


def join_card_kingdom_data(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
    return lf.with_columns(
        [
            pl.col("card_kingdom_id").alias("cardKingdomId"),
            pl.col("card_kingdom_foil_id").alias("cardKingdomFoilId"),
            pl.lit(None)
            .cast(pl.String)
            .alias("cardKingdomEtchedId"),  # v2 doesn't have etched yet
            pl.col("card_kingdom_url").alias("cardKingdomUrl"),
            pl.col("card_kingdom_foil_url").alias("cardKingdomFoilUrl"),
            pl.lit(None).cast(pl.String).alias("cardKingdomEtchedUrl"),
        ]
    ).drop(
        [
            "card_kingdom_id",
            "card_kingdom_foil_id",
            "card_kingdom_url",
            "card_kingdom_foil_url",
        ],
        strict=False,
    )


def join_cardmarket_ids(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Vectorized Join using the pre-computed global lookup table.
    """
    mcm_df = ctx.mcm_lookup_df if ctx else GLOBAL_CACHE.mcm_lookup_df
    if mcm_df is None:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("mcmId"),
                pl.lit(None).cast(pl.String).alias("mcmMetaId"),
            ]
        )
    # Ensure the lookup table is available as a LazyFrame
    mcm_lookup = mcm_df.lazy()

    lf = lf.with_columns(
        [
            # Lowercase name for matching
            pl.col("name").str.to_lowercase().alias("_join_name"),
            # Scryfall numbers often have leading zeros (e.g., "001"),
            # while MCM strips them. We strip them here to match.
            pl.col("number").str.strip_chars_start("0").alias("_join_number"),
        ]
    )

    # Left join on Set + Name + Number
    lf = lf.join(
        mcm_lookup,
        left_on=["setCode", "_join_name", "_join_number"],
        right_on=["set_code", "name_lower", "number"],
        how="left",
    )

    # Keep mcmId and mcmMetaId columns - they'll be added to identifiers struct later
    lf = lf.drop(["_join_name", "_join_number"])

    return lf


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


def join_printings(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Join printings map - replaces parse_printings() per-card lookups.

    Single join replaces N filter operations.
    Also computes originalReleaseDate (first printing date) for reprints.
    """
    printings_df = ctx.printings_df if ctx else GLOBAL_CACHE.printings_df
    sets_df = ctx.sets_df if ctx else GLOBAL_CACHE.sets_df

    if printings_df is None:
        return lf.with_columns(
            [
                pl.lit([]).cast(pl.List(pl.String)).alias("printings"),
                pl.lit(None).cast(pl.String).alias("originalReleaseDate"),
            ]
        )

    lf = lf.join(
        printings_df.lazy(),
        left_on="oracleId",
        right_on="oracle_id",
        how="left",
    ).with_columns(pl.col("printings").fill_null([]).list.sort())

    # Compute originalReleaseDate: earliest release date among all printings
    # Only set for reprints (cards where current set isn't the first printing)
    if sets_df is not None:
        # Build set_code -> release_date lookup
        set_dates = sets_df.select(
            [
                pl.col("code").str.to_uppercase().alias("set_code"),
                pl.col("released_at").alias("release_date"),
            ]
        ).lazy()

        # Explode printings to join with set dates
        first_printing = (
            lf.select(["oracleId", "printings"])
            .explode("printings")
            .join(set_dates, left_on="printings", right_on="set_code", how="left")
            .group_by("oracleId")
            .agg(pl.col("release_date").min().alias("_first_release_date"))
        )

        lf = lf.join(first_printing, on="oracleId", how="left")

        # Only populate originalReleaseDate for reprints
        # (current set's release_date > first_release_date)
        lf = lf.with_columns(
            pl.when(pl.col("isReprint"))
            .then(pl.col("_first_release_date"))
            .otherwise(pl.lit(None).cast(pl.String))
            .alias("originalReleaseDate")
        ).drop("_first_release_date", strict=False)
    else:
        lf = lf.with_columns(pl.lit(None).cast(pl.String).alias("originalReleaseDate"))

    return lf


def join_rulings(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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


def join_foreign_data(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
                            pl.element()
                            .struct.field("scryfall_id")
                            .alias("scryfallId"),
                            pl.element()
                            .struct.field("multiverse_id")
                            .cast(pl.String)
                            .alias("multiverseId"),
                        ).alias("identifiers"),
                        pl.element().struct.field("language"),
                        pl.element().struct.field("name"),
                        pl.element().struct.field("text"),
                        pl.element().struct.field("type"),
                    )
                )
            )
            .otherwise(
                pl.lit([]).cast(
                    pl.List(
                        pl.Struct(
                            {
                                "flavorText": pl.String,
                                "identifiers": pl.Struct(
                                    {
                                        "scryfallId": pl.String,
                                        "multiverseId": pl.String,
                                    }
                                ),
                                "language": pl.String,
                                "name": pl.String,
                                "text": pl.String,
                                "type": pl.String,
                            }
                        )
                    )
                )
            )
            .alias("foreignData")
        )
        .drop("foreign_data")
    )


def join_edhrec_data(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """Join EDHREC saltiness and rank by oracle_id."""
    edhrec_df = ctx.salt_df if ctx else GLOBAL_CACHE.salt_df

    if edhrec_df is None or edhrec_df.is_empty():
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.Float64).alias("edhrecSaltiness"),
            ]
        )

    return lf.join(
        edhrec_df.lazy().select(["oracle_id", "edhrecSaltiness"]),
        left_on="oracleId",
        right_on="oracle_id",
        how="left",
    )


def join_gatherer_data(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Join Gatherer original text and type by multiverse ID.
    """
    gatherer_map = ctx.gatherer_map if ctx else GLOBAL_CACHE.gatherer_map

    if not gatherer_map:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("originalText"),
                pl.lit(None).cast(pl.String).alias("originalType"),
            ]
        )

    # Build lookup LazyFrame from gatherer_map
    # gatherer_map: {multiverse_id: [{original_text, original_types}, ...]}
    rows = []
    for mv_id, entries in gatherer_map.items():
        if entries:
            entry = entries[0]  # Take first entry
            rows.append(
                {
                    "multiverse_id": str(mv_id),
                    "originalText": entry.get("original_text"),
                    "originalType": entry.get("original_types"),
                }
            )

    if not rows:
        return lf.with_columns(
            [
                pl.lit(None).cast(pl.String).alias("originalText"),
                pl.lit(None).cast(pl.String).alias("originalType"),
            ]
        )

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

def add_uuid_expr(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
            pl.concat_str(
                [
                    pl.col("scryfallId"),
                    pl.col("side").fill_null("a"),
                ]
            )
            .map_batches(uuid5_batch, return_dtype=pl.String)
            .alias("uuid")
        )

    return (
        lf.join(
            cache_df.lazy(),
            left_on=["scryfallId", "side"],
            right_on=["scryfall_id", "side"],
            how="left",
        )
        .with_columns(
            pl.coalesce(
                [
                    pl.col("cached_uuid"),
                    pl.concat_str(
                        [
                            pl.col("scryfallId"),
                            pl.col("side").fill_null("a"),
                        ]
                    ).map_batches(uuid5_batch, return_dtype=pl.String),
                ]
            ).alias("uuid")
        )
        .drop("cached_uuid")
    )


def add_identifiers_v4_uuid(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add mtgjsonV4Id to identifiers struct.

    Uses struct-based batch computation for v4 UUID formula.
    """
    return lf.with_columns(
        pl.struct(
            [
                pl.col("scryfallId").alias("id"),
                pl.col("name"),
                pl.col("faceName").alias("face_name"),
                pl.col("types"),
                pl.col("colors"),
                pl.col("power"),
                pl.col("toughness"),
                pl.col("side"),
                pl.col("setCode").alias("set"),
            ]
        )
        .map_batches(compute_v4_uuid_from_struct, return_dtype=pl.String)
        .alias("mtgjsonV4Id")
    )


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
            # Cast scalar uuid to List(String) for set_difference
            pl.col("_all_uuids")
            .list.set_difference(pl.col("uuid").cast(pl.List(pl.String)))
            .alias("otherFaceIds")
        )
        .drop("_all_uuids")
    )


def add_variations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized detection of Variations and Alternatives.

    Variations: Cards with the same base name and face name but different UUID
    is_alternative: Within cards sharing a "printing key", only the first is NOT alternative
    """

    # Normalize to base name by stripping " (" and beyond
    lf = lf.with_columns(
        pl.col("name").str.split(" (").list.first().alias("_base_name")
    )

    # Collect all UUIDs for each (set, base_name, faceName) group
    variation_groups = (
        lf.select(["setCode", "_base_name", "faceName", "uuid"])
        .group_by(["setCode", "_base_name", "faceName"])
        .agg(pl.col("uuid").alias("_group_uuids"))
    )

    # Join back to attach the full UUID list to each card
    lf = lf.join(variation_groups, on=["setCode", "_base_name", "faceName"], how="left")

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

    basic_lands = [
        "Plains",
        "Island",
        "Swamp",
        "Mountain",
        "Forest",
        "Snow-Covered Plains",
        "Snow-Covered Island",
        "Snow-Covered Swamp",
        "Snow-Covered Mountain",
        "Snow-Covered Forest",
        "Wastes",
    ]

    lf = lf.with_columns(
        pl.when(
            (pl.col("variations").list.len() > 0)  # Has variations
            & (~pl.col("name").is_in(basic_lands))  # Not a basic land
            & (pl.col("uuid") != first_uuid_expr)  # Not the first in group
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isAlternative")
    )
    # Cleanup temp columns
    return lf.drop(["_base_name", "_group_uuids", "_printing_key"])


def add_leadership_skills_expr(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
    has_power_toughness = (
        pl.col("power").is_not_null() & pl.col("toughness").is_not_null()
    )
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


def add_related_cards_struct(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
        pl.col("set_type").str.to_lowercase().str.contains("alchemy")
        & pl.col("_spellbook_list").is_not_null()
        & (pl.col("_spellbook_list").list.len() > 0)
    )
    has_reverse = pl.col("reverseRelated").is_not_null() & (
        pl.col("reverseRelated").list.len() > 0
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


def add_alternative_deck_limit(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Mark cards that don't have the standard 4-copy deck limit.

    Uses Scryfall's cards_without_limits list (12 cards total like Seven Dwarves, Rat Colony, etc).
    """
    unlimited_cards = (
        ctx.unlimited_cards if ctx else GLOBAL_CACHE.scryfall.cards_without_limits
    )

    if not unlimited_cards:
        return lf.with_columns(
            pl.lit(None).cast(pl.Boolean).alias("hasAlternativeDeckLimit")
        )

    return lf.with_columns(
        pl.when(pl.col("name").is_in(list(unlimited_cards)))
        .then(pl.lit(True))
        .otherwise(pl.lit(None).cast(pl.Boolean))
        .alias("hasAlternativeDeckLimit")
    )


def add_is_funny(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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


def add_is_timeshifted(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Vectorized 'isTimeshifted' logic.
    """
    return lf.with_columns(
        pl.when((pl.col("frameVersion") == "future") | (pl.col("setCode") == "TSB"))
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("isTimeshifted")
    )


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
        lf.with_columns(
            [
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
            ]
        )
        .with_columns(
            pl.struct(
                [
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
                ]
            ).alias("purchaseUrls")
        )
        .drop(
            [
                "_ck_hash",
                "_ckf_hash",
                "_cke_hash",
                "_cm_hash",
                "_tcg_hash",
                "_tcge_hash",
            ]
        )
    )


def apply_manual_overrides(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
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
        for field_name, value in fields.items():
            if field_name.startswith("__"):
                continue
            mapped_field = field_name_map.get(field_name, field_name)
            if mapped_field not in field_overrides:
                field_overrides[mapped_field] = {}
            field_overrides[mapped_field][uuid_key] = value

    # Get column names once
    schema_names = lf.collect_schema().names()

    # Apply each field's overrides
    for field_name, uuid_map in field_overrides.items():
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

def add_meld_card_parts(lf: pl.LazyFrame, ctx: PipelineContext = None) -> pl.LazyFrame:
    """
    Add cardParts for meld cards.

    Uses meld_triplets lookup from cache (name -> [top, bottom, combined]).
    Falls back to computed logic for new meld cards not in resource file.
    """
    meld_triplets = ctx.meld_triplets if ctx else GLOBAL_CACHE.meld_triplets

    if not meld_triplets:
        return lf.with_columns(pl.lit(None).cast(pl.List(pl.String)).alias("cardParts"))

    # Build lookup DataFrame
    meld_lookup = pl.LazyFrame(
        [
            {"_lookup_name": name, "_resource_parts": parts}
            for name, parts in meld_triplets.items()
        ]
    )

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


def add_rebalanced_linkage(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Link rebalanced cards (A-Name) to their original printings and vice versa.

    Adds:
    - isRebalanced: True for Alchemy rebalanced cards (A-Name or promo_types contains 'rebalanced')
    - originalPrintings: UUIDs of the original card (on rebalanced cards)
    - rebalancedPrintings: UUIDs of the rebalanced version (on original cards)
    """
    # Rebalanced cards: names starting with "A-" or promo_types contains 'rebalanced'
    # Original cards: names that match the stripped "A-" version

    is_rebalanced = pl.col("name").str.starts_with("A-") | pl.col(
        "promoTypes"
    ).list.contains("rebalanced")

    # Add isRebalanced boolean (True for rebalanced, null otherwise)
    lf = lf.with_columns(
        pl.when(is_rebalanced)
        .then(pl.lit(True))
        .otherwise(pl.lit(None).cast(pl.Boolean))
        .alias("isRebalanced")
    )

    is_rebalanced = pl.col("name").str.starts_with("A-")
    original_name_expr = pl.col("name").str.replace("^A-", "")

    # Build rebalanced -> original name mapping with UUIDs
    rebalanced_map = (
        lf.filter(is_rebalanced)
        .select(
            [
                original_name_expr.alias("_original_name"),
                pl.col("uuid"),
            ]
        )
        .group_by("_original_name")
        .agg(pl.col("uuid").alias("_rebalanced_uuids"))
    )

    # Build original name -> original UUIDs mapping
    # (cards that DON'T start with A- but whose name matches a rebalanced card's base name)
    original_map = (
        lf.filter(~is_rebalanced)
        .select(
            [
                pl.col("name").alias("_original_name"),
                pl.col("uuid"),
            ]
        )
        .join(
            rebalanced_map.select("_original_name").unique(),
            on="_original_name",
            how="semi",  # Only keep names that have a rebalanced version
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

