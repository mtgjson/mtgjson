"""Scryfall data models and schemas."""

from datetime import date
from enum import StrEnum
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

if TYPE_CHECKING:
    import polars as pl


class Color(StrEnum):
    """Magic color symbols."""

    WHITE = "W"
    BLUE = "U"
    BLACK = "B"
    RED = "R"
    GREEN = "G"


class Rarity(StrEnum):
    """Card rarity levels."""

    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"
    MYTHIC = "mythic"
    SPECIAL = "special"
    BONUS = "bonus"


class BorderColor(StrEnum):
    """Physical card border colors."""

    BLACK = "black"
    WHITE = "white"
    BORDERLESS = "borderless"
    SILVER = "silver"
    GOLD = "gold"


class Layout(StrEnum):
    """Card layout types determining face structure and rendering."""

    NORMAL = "normal"
    SPLIT = "split"
    FLIP = "flip"
    TRANSFORM = "transform"
    MODAL_DFC = "modal_dfc"
    MELD = "meld"
    LEVELER = "leveler"
    CLASS = "class"
    CASE = "case"
    SAGA = "saga"
    ADVENTURE = "adventure"
    MUTATE = "mutate"
    PROTOTYPE = "prototype"
    BATTLE = "battle"
    PLANAR = "planar"
    SCHEME = "scheme"
    VANGUARD = "vanguard"
    TOKEN = "token"
    DOUBLE_FACED_TOKEN = "double_faced_token"
    EMBLEM = "emblem"
    AUGMENT = "augment"
    HOST = "host"
    ART_SERIES = "art_series"
    REVERSIBLE_CARD = "reversible_card"


class Frame(StrEnum):
    """Card frame versions by year of introduction."""

    FRAME_1993 = "1993"
    FRAME_1997 = "1997"
    FRAME_2003 = "2003"
    FRAME_2015 = "2015"
    FUTURE = "future"


class SecurityStamp(StrEnum):
    """Holographic security stamp shapes."""

    OVAL = "oval"
    TRIANGLE = "triangle"
    ACORN = "acorn"
    CIRCLE = "circle"
    ARENA = "arena"
    HEART = "heart"


class ImageStatus(StrEnum):
    """Scryfall image availability status."""

    MISSING = "missing"
    PLACEHOLDER = "placeholder"
    LOWRES = "lowres"
    HIGHRES_SCAN = "highres_scan"


class Legality(StrEnum):
    """Format legality status values."""

    LEGAL = "legal"
    NOT_LEGAL = "not_legal"
    RESTRICTED = "restricted"
    BANNED = "banned"


class Finish(StrEnum):
    """Physical card finish types."""

    FOIL = "foil"
    NONFOIL = "nonfoil"
    ETCHED = "etched"


class Game(StrEnum):
    """Game platforms where a card exists."""

    PAPER = "paper"
    ARENA = "arena"
    MTGO = "mtgo"


class Component(StrEnum):
    """Role a related card plays in a relationship."""

    TOKEN = "token"
    MELD_PART = "meld_part"
    MELD_RESULT = "meld_result"
    COMBO_PIECE = "combo_piece"


class ImageUris(BaseModel):
    """Available imagery for a card."""

    small: HttpUrl | None = Field(
        default=None,
        description="A small full card image (146×204 pixels).",
    )
    normal: HttpUrl | None = Field(
        default=None,
        description="A medium-sized full card image (488×680 pixels).",
    )
    large: HttpUrl | None = Field(
        default=None,
        description="A large full card image (672×936 pixels).",
    )
    png: HttpUrl | None = Field(
        default=None,
        description="A transparent PNG of the card (745×1040 pixels).",
    )
    art_crop: HttpUrl | None = Field(
        default=None,
        description="A crop of the card art (variable dimensions).",
    )
    border_crop: HttpUrl | None = Field(
        default=None,
        description="A crop of the card including border (480×680 pixels).",
    )


class SetMetadata(BaseModel):
    """
    Scryfall Set object.

    A Set object represents a group of related Magic cards. All Card objects
    on Scryfall belong to exactly one set.
    """

    object: Literal["set"] = Field(
        default="set",
        description="A content type for this object, always 'set'.",
    )
    id: UUID = Field(
        description="A unique ID for this set on Scryfall that will not change.",
    )
    code: str = Field(
        description="The unique three to five-letter code for this set.",
    )
    mtgo_code: str | None = Field(
        default=None,
        description="The unique code for this set on MTGO, which may differ from the regular code.",
    )
    arena_code: str | None = Field(
        default=None,
        description="The unique code for this set on Arena, which may differ from the regular code.",
    )
    tcgplayer_id: int | None = Field(
        default=None,
        description="The unique identifier for this set on TCGplayer, also known as the groupId.",
    )
    name: str = Field(
        description="The English name of the set.",
    )
    uri: HttpUrl = Field(
        description="A link to this set object on Scryfall's API.",
    )
    scryfall_uri: HttpUrl = Field(
        description="A link to this set's permapage on Scryfall's website.",
    )
    search_uri: HttpUrl = Field(
        description="A Scryfall API URI that you can request to begin paginating over the cards in this set.",
    )
    released_at: date | None = Field(
        default=None,
        description="The date the set was released (in GMT-8 Pacific time). Not all sets have a known release date.",
    )
    set_type: str = Field(
        description=(
            "A computer-readable classification for this set. See the set type documentation. "
            "Examples: core, expansion, masters, masterpiece, from_the_vault, spellbook, premium_deck, "
            "duel_deck, draft_innovation, treasure_chest, commander, planechase, archenemy, vanguard, "
            "funny, starter, box, promo, token, memorabilia, minigame, alchemy."
        ),
    )
    card_count: int = Field(
        description="The number of cards in this set.",
    )
    printed_size: int | None = Field(
        default=None,
        description=(
            "The denominator for the set's printed collector numbers. "
            "Usually equal to card_count, but may differ for sets with gaps or special numbering."
        ),
    )
    digital: bool = Field(
        description="True if this set was only released in a video game.",
    )
    nonfoil_only: bool = Field(
        description="True if this set contains only nonfoil cards.",
    )
    foil_only: bool = Field(
        description="True if this set contains only foil cards.",
    )
    block_code: str | None = Field(
        default=None,
        description="The block code for this set, if any.",
    )
    block: str | None = Field(
        default=None,
        description="The block name for this set, if any.",
    )
    icon_svg_uri: HttpUrl = Field(
        description="A URI to an SVG file for this set's icon on Scryfall's CDN. Hotlinking this image isn't recommended.",
    )

    @classmethod
    def polars_schema(cls) -> "pl.Struct":
        """Generate a Polars struct schema matching this model."""
        return pl.Struct(
            {
                "object": pl.String,
                "id": pl.String,
                "code": pl.String,
                "mtgo_code": pl.String,
                "arena_code": pl.String,
                "tcgplayer_id": pl.Int64,
                "name": pl.String,
                "uri": pl.String,
                "scryfall_uri": pl.String,
                "search_uri": pl.String,
                "released_at": pl.Date,
                "set_type": pl.String,
                "card_count": pl.Int64,
                "printed_size": pl.Int64,
                "digital": pl.Boolean,
                "nonfoil_only": pl.Boolean,
                "foil_only": pl.Boolean,
                "block_code": pl.String,
                "block": pl.String,
                "icon_svg_uri": pl.String,
            }
        )


class Prices(BaseModel):
    """Daily price information for a card."""

    usd: str | None = Field(
        default=None,
        description="The price in US dollars.",
    )
    usd_foil: str | None = Field(
        default=None,
        description="The foil price in US dollars.",
    )
    usd_etched: str | None = Field(
        default=None,
        description="The etched foil price in US dollars.",
    )
    eur: str | None = Field(
        default=None,
        description="The price in Euros.",
    )
    eur_foil: str | None = Field(
        default=None,
        description="The foil price in Euros.",
    )
    eur_etched: str | None = Field(
        default=None,
        description="The etched foil price in Euros.",
    )
    tix: str | None = Field(
        default=None,
        description="The price in MTGO event tickets.",
    )


class Legalities(BaseModel):
    """Legality of a card across play formats."""

    standard: Legality = Field(
        description="Legality in Standard format.",
    )
    future: Legality = Field(
        description="Legality in Future Standard format.",
    )
    historic: Legality = Field(
        description="Legality in Historic format (Arena).",
    )
    timeless: Legality = Field(
        description="Legality in Timeless format (Arena).",
    )
    gladiator: Legality = Field(
        description="Legality in Gladiator format.",
    )
    pioneer: Legality = Field(
        description="Legality in Pioneer format.",
    )
    explorer: Legality = Field(
        description="Legality in Explorer format (Arena).",
    )
    modern: Legality = Field(
        description="Legality in Modern format.",
    )
    legacy: Legality = Field(
        description="Legality in Legacy format.",
    )
    pauper: Legality = Field(
        description="Legality in Pauper format.",
    )
    vintage: Legality = Field(
        description="Legality in Vintage format.",
    )
    penny: Legality = Field(
        description="Legality in Penny Dreadful format.",
    )
    commander: Legality = Field(
        description="Legality in Commander format.",
    )
    oathbreaker: Legality = Field(
        description="Legality in Oathbreaker format.",
    )
    standardbrawl: Legality = Field(
        description="Legality in Standard Brawl format.",
    )
    brawl: Legality = Field(
        description="Legality in Brawl format.",
    )
    alchemy: Legality = Field(
        description="Legality in Alchemy format (Arena).",
    )
    paupercommander: Legality = Field(
        description="Legality in Pauper Commander format.",
    )
    duel: Legality = Field(
        description="Legality in Duel Commander format.",
    )
    oldschool: Legality = Field(
        description="Legality in Old School 93/94 format.",
    )
    premodern: Legality = Field(
        description="Legality in Premodern format.",
    )
    predh: Legality = Field(
        description="Legality in Pre-EDH format (Commander with pre-2012 rules).",
    )


class PurchaseUris(BaseModel):
    """URIs to a card's listing on major marketplaces."""

    tcgplayer: HttpUrl | None = Field(
        default=None,
        description="A link to purchase this card on TCGplayer.",
    )
    cardmarket: HttpUrl | None = Field(
        default=None,
        description="A link to purchase this card on Cardmarket.",
    )
    cardhoarder: HttpUrl | None = Field(
        default=None,
        description="A link to purchase this card on Cardhoarder.",
    )


class RelatedUris(BaseModel):
    """URIs to a card's listing on other Magic resources."""

    gatherer: HttpUrl | None = Field(
        default=None,
        description="A link to this card on Gatherer.",
    )
    tcgplayer_infinite_articles: HttpUrl | None = Field(
        default=None,
        description="A link to TCGplayer Infinite articles about this card.",
    )
    tcgplayer_infinite_decks: HttpUrl | None = Field(
        default=None,
        description="A link to TCGplayer Infinite decks featuring this card.",
    )
    edhrec: HttpUrl | None = Field(
        default=None,
        description="A link to this card on EDHREC.",
    )


class Preview(BaseModel):
    """Preview information for a newly spoiled card."""

    source: str | None = Field(
        default=None,
        description="The name of the source that previewed this card.",
    )
    source_uri: HttpUrl | None = Field(
        default=None,
        description="A link to the preview for this card.",
    )
    previewed_at: date | None = Field(
        default=None,
        description="The date this card was previewed.",
    )


class CardFace(BaseModel):
    """
    A single face of a multiface card.

    Multiface cards have a card_faces property containing at least two
    Card Face objects.
    """

    object: Literal["card_face"] = Field(
        default="card_face",
        description="A content type for this object, always card_face.",
    )
    name: str = Field(
        description="The name of this particular face.",
    )
    mana_cost: str = Field(
        description=(
            "The mana cost for this face. This value will be any empty string "
            "if the cost is absent. Remember that per the game rules, a missing "
            "mana cost and a mana cost of {0} are different values."
        ),
    )
    type_line: str | None = Field(
        default=None,
        description="The type line of this particular face, if the card is reversible.",
    )
    oracle_text: str | None = Field(
        default=None,
        description="The Oracle text for this face, if any.",
    )
    colors: list[Color] | None = Field(
        default=None,
        description=("This face's colors, if the game defines colors for the individual face of this card."),
    )
    color_indicator: list[Color] | None = Field(
        default=None,
        description="The colors in this face's color indicator, if any.",
    )
    power: str | None = Field(
        default=None,
        description=("This face's power, if any. Note that some cards have powers that are not numeric, such as *."),
    )
    toughness: str | None = Field(
        default=None,
        description=(
            "This face's toughness, if any. Note that some cards have toughnesses that are not numeric, such as *."
        ),
    )
    defense: str | None = Field(
        default=None,
        description="This face's defense, if any.",
    )
    loyalty: str | None = Field(
        default=None,
        description="This face's loyalty, if any.",
    )
    flavor_text: str | None = Field(
        default=None,
        description="The flavor text printed on this face, if any.",
    )
    illustration_id: UUID | None = Field(
        default=None,
        description=(
            "A unique identifier for the card face artwork that remains consistent "
            "across reprints. Newly spoiled cards may not have this field yet."
        ),
    )
    image_uris: ImageUris | None = Field(
        default=None,
        description=(
            "An object providing URIs to imagery for this face, if this is a "
            "double-sided card. If this card is not double-sided, then the "
            "image_uris property will be part of the parent object instead."
        ),
    )
    artist: str | None = Field(
        default=None,
        description=("The name of the illustrator of this card face. Newly spoiled cards may not have this field yet."),
    )
    artist_id: UUID | None = Field(
        default=None,
        description=("The ID of the illustrator of this card face. Newly spoiled cards may not have this field yet."),
    )
    watermark: str | None = Field(
        default=None,
        description="The watermark on this particular card face, if any.",
    )
    printed_name: str | None = Field(
        default=None,
        description="The localized name printed on this face, if any.",
    )
    printed_text: str | None = Field(
        default=None,
        description="The localized text printed on this face, if any.",
    )
    printed_type_line: str | None = Field(
        default=None,
        description="The localized type line printed on this face, if any.",
    )
    cmc: float | None = Field(
        default=None,
        description="The mana value of this particular face, if the card is reversible.",
    )
    oracle_id: UUID | None = Field(
        default=None,
        description="The Oracle ID of this particular face, if the card is reversible.",
    )
    layout: Layout | None = Field(
        default=None,
        description="The layout of this card face, if the card is reversible.",
    )

    @classmethod
    def polars_schema(cls) -> "pl.Struct":
        """Generate a Polars struct schema matching this model."""
        import polars as pl

        return pl.Struct(
            {
                "object": pl.String,
                "name": pl.String,
                "mana_cost": pl.String,
                "type_line": pl.String,
                "oracle_text": pl.String,
                "colors": pl.List(pl.String),
                "color_indicator": pl.List(pl.String),
                "power": pl.String,
                "toughness": pl.String,
                "defense": pl.String,
                "loyalty": pl.String,
                "flavor_text": pl.String,
                "illustration_id": pl.String,
                "image_uris": pl.Struct(
                    {
                        "small": pl.String,
                        "normal": pl.String,
                        "large": pl.String,
                        "png": pl.String,
                        "art_crop": pl.String,
                        "border_crop": pl.String,
                    }
                ),
                "artist": pl.String,
                "artist_id": pl.String,
                "watermark": pl.String,
                "printed_name": pl.String,
                "printed_text": pl.String,
                "printed_type_line": pl.String,
                "cmc": pl.String,
                "oracle_id": pl.String,
                "layout": pl.String,
                "flavor_name": pl.String,
            }
        )


class RelatedCard(BaseModel):
    """
    A card closely related to another card.

    Cards that are closely related to other cards (because they call them by
    name, or generate a token, or meld, etc) have an all_parts property that
    contains Related Card objects.
    """

    object: Literal["related_card"] = Field(
        default="related_card",
        description="A content type for this object, always related_card.",
    )
    id: UUID = Field(
        description="A unique ID for this card in Scryfall's database.",
    )
    component: Component = Field(
        description=(
            "A field explaining what role this card plays in this relationship, "
            "one of token, meld_part, meld_result, or combo_piece."
        ),
    )
    name: str = Field(
        description="The name of this particular related card.",
    )
    type_line: str = Field(
        description="The type line of this card.",
    )
    uri: HttpUrl = Field(
        description=("A URI where you can retrieve a full object describing this card on Scryfall's API."),
    )


class ScryfallCard(BaseModel):
    """
    Scryfall Card object.

    Card objects represent individual Magic: The Gathering cards that players
    could obtain and add to their collection. Cards are the API's most complex
    object.

    Magic cards can have multiple faces or multiple cards printed on one card
    stock. Scryfall represents multi-face cards as a single object with a
    card_faces array describing the distinct faces.
    """

    object: Literal["card"] = Field(
        default="card",
        description="A content type for this object, always card.",
    )
    id: UUID = Field(
        description="A unique ID for this card in Scryfall's database.",
    )
    oracle_id: UUID | None = Field(
        default=None,
        description=(
            "A unique ID for this card's oracle identity. This value is consistent "
            "across reprinted card editions, and unique among different cards with "
            "the same name (tokens, Unstable variants, etc). Always present except "
            "for the reversible_card layout where it will be absent; oracle_id will "
            "be found on each face instead."
        ),
    )
    multiverse_ids: list[int] | None = Field(
        default=None,
        description=("This card's multiverse IDs on Gatherer, if any, as an array of integers."),
    )
    mtgo_id: int | None = Field(
        default=None,
        description=("This card's Magic Online ID (also known as the Catalog ID), if any."),
    )
    mtgo_foil_id: int | None = Field(
        default=None,
        description=("This card's foil Magic Online ID (also known as the Catalog ID), if any."),
    )
    tcgplayer_id: int | None = Field(
        default=None,
        description=("This card's ID on TCGplayer's API, also known as the productId."),
    )
    tcgplayer_etched_id: int | None = Field(
        default=None,
        description=(
            "This card's ID on TCGplayer's API, for its etched version if that version is a separate product."
        ),
    )
    cardmarket_id: int | None = Field(
        default=None,
        description=("This card's ID on Cardmarket's API, colliquially named mkmId"),
    )
    arena_id: int | None = Field(
        default=None,
        description=(
            "This card's Arena ID, if any. A large percentage of cards are not available on Arena and do not have this ID."
        ),
    )
    resource_id: str | None = Field(
        default=None,
        description="This card's Resource ID on Gatherer, if any.",
    )
    lang: str = Field(
        description="A language code for this printing.",
    )
    prints_search_uri: HttpUrl = Field(
        description=("A link to where you can begin paginating all re/prints for this card on Scryfall's API."),
    )
    rulings_uri: HttpUrl = Field(
        description="A link to this card's rulings list on Scryfall's API.",
    )
    scryfall_uri: HttpUrl = Field(
        description="A link to this card's permapage on Scryfall's website.",
    )
    uri: HttpUrl = Field(
        description="A link to this card object on Scryfall's API.",
    )
    all_parts: list[RelatedCard] | None = Field(
        default=None,
        description=(
            "If this card is closely related to other cards, this property will be an array with Related Card Objects."
        ),
    )
    card_faces: list[CardFace] | None = Field(
        default=None,
        description="An array of Card Face objects, if this card is multifaced.",
    )
    cmc: float = Field(
        description=("The card's mana value. Note that some funny cards have fractional mana costs."),
    )
    color_identity: list[Color] = Field(
        description="This card's color identity.",
    )
    color_indicator: list[Color] | None = Field(
        default=None,
        description=(
            "The colors in this card's color indicator, if any. A null value for this field indicates the card does not have one."
        ),
    )
    colors: list[Color] | None = Field(
        default=None,
        description=(
            "This card's colors, if the overall card has colors defined by the rules. Otherwise the colors will be on the card_faces objects."
        ),
    )
    defense: str | None = Field(
        default=None,
        description="This face's defense, if any.",
    )
    edhrec_rank: int | None = Field(
        default=None,
        description=("This card's overall rank/popularity on EDHREC. Not all cards are ranked."),
    )
    game_changer: bool | None = Field(
        default=None,
        description="True if this card is on the Commander Game Changer list.",
    )
    hand_modifier: str | None = Field(
        default=None,
        description=("This card's hand modifier, if it is Vanguard card. This value will contain a delta, such as -1."),
    )
    keywords: list[str] = Field(
        description=("An array of keywords that this card uses, such as 'Flying' and 'Cumulative upkeep'."),
    )
    layout: Layout = Field(
        description="A code for this card's layout.",
    )
    legalities: Legalities | None = Field(
        default=None,
        description=(
            "An object describing the legality of this card across play formats. Possible legalities are legal, not_legal, restricted, and banned."
        ),
    )
    life_modifier: str | None = Field(
        default=None,
        description=("This card's life modifier, if it is Vanguard card. This value will contain a delta, such as +2."),
    )
    loyalty: str | None = Field(
        default=None,
        description=("This loyalty if any. Note that some cards have loyalties that are not numeric, such as X."),
    )
    mana_cost: str | None = Field(
        default=None,
        description=(
            "The mana cost for this card. This value will be any empty string '' if the cost is absent. Multi-faced cards will report this value in card faces."
        ),
    )
    name: str = Field(
        description=(
            "The name of this card. If this card has multiple faces, this field will contain both names separated by ' // '."
        ),
    )
    oracle_text: str | None = Field(
        default=None,
        description="The Oracle text for this card, if any.",
    )
    penny_rank: int | None = Field(
        default=None,
        description=("This card's rank/popularity on Penny Dreadful. Not all cards are ranked."),
    )
    power: str | None = Field(
        default=None,
        description=("This card's power, if any. Note that some cards have powers that are not numeric, such as *."),
    )
    produced_mana: list[Color] | None = Field(
        default=None,
        description="Colors of mana that this card could produce.",
    )
    reserved: bool = Field(
        description="True if this card is on the Reserved List.",
    )
    toughness: str | None = Field(
        default=None,
        description=(
            "This card's toughness, if any. Note that some cards have toughnesses that are not numeric, such as *."
        ),
    )
    type_line: str = Field(
        description="The type line of this card.",
    )

    artist: str | None = Field(
        default=None,
        description=("The name of the illustrator of this card. Newly spoiled cards may not have this field yet."),
    )
    artist_ids: list[UUID] | None = Field(
        default=None,
        description=(
            "The IDs of the artists that illustrated this card. Newly spoiled cards may not have this field yet."
        ),
    )
    attraction_lights: list[int] | None = Field(
        default=None,
        description="The list of Unfinity attractions lights on this card, if any.",
    )
    booster: bool = Field(
        default=True,
        description="Whether this card is found in boosters.",
    )
    border_color: BorderColor = Field(
        description=("This card's border color: black, white, borderless, silver, or gold."),
    )
    card_back_id: UUID = Field(
        description="The Scryfall ID for the card back design present on this card.",
    )
    collector_number: str = Field(
        description=(
            "This card's collector 'number'. Note that collector numbers can contain non-numeric characters, such as letters or ★."
        ),
    )
    content_warning: bool | None = Field(
        default=None,
        description=("True if you should consider avoiding use of this print downstream."),
    )
    digital: bool = Field(
        description="True if this card was only released in a video game.",
    )
    finishes: list[Finish | None] = Field(
        description=(
            "An array of computer-readable flags that indicate if this card can come in foil, nonfoil, or etched finishes."
        ),
    )
    flavor_name: str | None = Field(
        default=None,
        description=("The just-for-fun name printed on the card (such as for Godzilla series cards)."),
    )
    flavor_text: str | None = Field(
        default=None,
        description="The flavor text, if any.",
    )
    frame_effects: list[str] | None = Field(
        default=None,
        description="This card's frame effects, if any.",
    )
    frame: Frame = Field(
        description="This card's frame layout.",
    )
    full_art: bool = Field(
        description="True if this card's artwork is larger than normal.",
    )
    games: list[Game] | None = Field(
        description=("A list of games that this card print is available in, paper, arena, and/or mtgo."),
    )
    highres_image: bool = Field(
        description="True if this card's imagery is high resolution.",
    )
    illustration_id: UUID | None = Field(
        default=None,
        description=(
            "A unique identifier for the card artwork that remains consistent across reprints. Newly spoiled cards may not have this field yet."
        ),
    )
    image_status: ImageStatus = Field(
        description=(
            "A computer-readable indicator for the state of this card's image, one of missing, placeholder, lowres, or highres_scan."
        ),
    )
    image_uris: ImageUris | None = Field(
        default=None,
        description=("An object listing available imagery for this card."),
    )
    oversized: bool = Field(
        description="True if this card is oversized.",
    )
    prices: Prices = Field(
        description=("An object containing daily price information for this card, if any is available."),
    )
    printed_name: str | None = Field(
        default=None,
        description="The localized name printed on this card, if any.",
    )
    printed_text: str | None = Field(
        default=None,
        description="The localized text printed on this card, if any.",
    )
    printed_type_line: str | None = Field(
        default=None,
        description="The localized type line printed on this card, if any.",
    )
    promo: bool = Field(
        description="True if this card is a promotional print.",
    )
    promo_types: list[str] | None = Field(
        default=None,
        description=("An array of strings describing what categories of promo cards this card falls into."),
    )
    purchase_uris: PurchaseUris | None = Field(
        default=None,
        description=(
            "An object providing URIs to this card's listing on major marketplaces. Omitted if the card is unpurchaseable."
        ),
    )
    rarity: Rarity = Field(
        description=("This card's rarity. One of common, uncommon, rare, special, mythic, or bonus."),
    )
    related_uris: RelatedUris = Field(
        description=("An object providing URIs to this card's listing on other Magic: The Gathering online resources."),
    )
    released_at: date = Field(
        description="The date this card was first released.",
    )
    reprint: bool = Field(
        description="True if this card is a reprint.",
    )
    scryfall_set_uri: HttpUrl = Field(
        description="A link to this card's set on Scryfall's website.",
    )
    set_name: str = Field(
        description="This card's full set name.",
    )
    set_search_uri: HttpUrl = Field(
        description=("A link to where you can begin paginating this card's set on the Scryfall API."),
    )
    set_type: str = Field(
        description="The type of set this printing is in.",
    )
    set_uri: HttpUrl = Field(
        description="A link to this card's set object on Scryfall's API.",
    )
    set: str = Field(
        description="This card's set code.",
    )
    set_id: UUID = Field(
        description="This card's Set object UUID.",
    )
    story_spotlight: bool = Field(
        description="True if this card is a Story Spotlight.",
    )
    textless: bool = Field(
        description="True if the card is printed without text.",
    )
    variation: bool = Field(
        description="Whether this card is a variation of another printing.",
    )
    variation_of: UUID | None = Field(
        default=None,
        description=("The printing ID of the printing this card is a variation of."),
    )
    security_stamp: SecurityStamp | None = Field(
        default=None,
        description=("The security stamp on this card, if any. One of oval, triangle, acorn, circle, arena, or heart."),
    )
    watermark: str | None = Field(
        default=None,
        description="This card's watermark, if any.",
    )
    preview: Preview | None = Field(
        default=None,
        description="Preview information for this card, if it was previewed.",
    )
