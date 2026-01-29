"""Scryfall data models and schemas."""

from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

if TYPE_CHECKING:
    import polars as pl


class Color(str, Enum):
    """Magic color symbols."""

    WHITE = "W"
    BLUE = "U"
    BLACK = "B"
    RED = "R"
    GREEN = "G"


class ManaColor(str, Enum):
    """Mana colors including colorless (for produced_mana)."""

    WHITE = "W"
    BLUE = "U"
    BLACK = "B"
    RED = "R"
    GREEN = "G"
    COLORLESS = "C"


class Rarity(str, Enum):
    """Card rarity levels."""

    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"
    MYTHIC = "mythic"
    SPECIAL = "special"
    BONUS = "bonus"


class BorderColor(str, Enum):
    """Physical card border colors."""

    BLACK = "black"
    WHITE = "white"
    BORDERLESS = "borderless"
    SILVER = "silver"
    GOLD = "gold"


class Layout(str, Enum):
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


class Frame(str, Enum):
    """Card frame versions by year of introduction."""

    FRAME_1993 = "1993"
    FRAME_1997 = "1997"
    FRAME_2003 = "2003"
    FRAME_2015 = "2015"
    FUTURE = "future"


class SecurityStamp(str, Enum):
    """Holographic security stamp shapes."""

    OVAL = "oval"
    TRIANGLE = "triangle"
    ACORN = "acorn"
    CIRCLE = "circle"
    ARENA = "arena"
    HEART = "heart"


class ImageStatus(str, Enum):
    """Scryfall image availability status."""

    MISSING = "missing"
    PLACEHOLDER = "placeholder"
    LOWRES = "lowres"
    HIGHRES_SCAN = "highres_scan"


class Legality(str, Enum):
    """Format legality status values."""

    LEGAL = "legal"
    NOT_LEGAL = "not_legal"
    RESTRICTED = "restricted"
    BANNED = "banned"


class Finish(str, Enum):
    """Physical card finish types."""

    FOIL = "foil"
    NONFOIL = "nonfoil"
    ETCHED = "etched"
    GLOSSY = "glossy"


class Game(str, Enum):
    """Game platforms where a card exists."""

    PAPER = "paper"
    ARENA = "arena"
    MTGO = "mtgo"
    # Legacy platforms (may appear in older data)
    ASTRAL = "astral"
    SEGA = "sega"


class Component(str, Enum):
    """Role a related card plays in a relationship."""

    TOKEN = "token"
    MELD_PART = "meld_part"
    MELD_RESULT = "meld_result"
    COMBO_PIECE = "combo_piece"


class ImageUris(BaseModel):
    """Available imagery for a card."""

    small: HttpUrl | None = Field(
        default=None,
        description="A small full card image (146x204 pixels).",
    )
    normal: HttpUrl | None = Field(
        default=None,
        description="A medium-sized full card image (488x680 pixels).",
    )
    large: HttpUrl | None = Field(
        default=None,
        description="A large full card image (672x936 pixels).",
    )
    png: HttpUrl | None = Field(
        default=None,
        description="A transparent PNG of the card (745x1040 pixels).",
    )
    art_crop: HttpUrl | None = Field(
        default=None,
        description="A crop of the card art (variable dimensions).",
    )
    border_crop: HttpUrl | None = Field(
        default=None,
        description="A crop of the card including border (480x680 pixels).",
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
        description="A computer-readable classification for this set.",
    )
    card_count: int = Field(
        description="The number of cards in this set.",
    )
    printed_size: int | None = Field(
        default=None,
        description="The denominator for the set's printed collector numbers.",
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
    parent_set_code: str | None = Field(
        default=None,
        description="The set code for the parent set, if any. Promo and token sets often have a parent set.",
    )
    icon_svg_uri: HttpUrl = Field(
        description="A URI to an SVG file for this set's icon on Scryfall's CDN.",
    )

    @classmethod
    def polars_schema(cls) -> "pl.Struct":
        """Generate a Polars struct schema matching this model."""
        import polars as pl

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
                "parent_set_code": pl.String,
                "icon_svg_uri": pl.String,
            }
        )


class Prices(BaseModel):
    """Daily price information for a card."""

    usd: str | None = Field(default=None, description="The price in US dollars.")
    usd_foil: str | None = Field(
        default=None, description="The foil price in US dollars."
    )
    usd_etched: str | None = Field(
        default=None, description="The etched foil price in US dollars."
    )
    eur: str | None = Field(default=None, description="The price in Euros.")
    eur_foil: str | None = Field(default=None, description="The foil price in Euros.")
    eur_etched: str | None = Field(
        default=None, description="The etched foil price in Euros."
    )
    tix: str | None = Field(
        default=None, description="The price in MTGO event tickets."
    )


class Legalities(BaseModel):
    """Legality of a card across play formats."""

    standard: Legality = Field(description="Legality in Standard format.")
    future: Legality = Field(description="Legality in Future Standard format.")
    historic: Legality = Field(description="Legality in Historic format (Arena).")
    timeless: Legality = Field(description="Legality in Timeless format (Arena).")
    gladiator: Legality = Field(description="Legality in Gladiator format.")
    pioneer: Legality = Field(description="Legality in Pioneer format.")
    explorer: Legality = Field(description="Legality in Explorer format (Arena).")
    modern: Legality = Field(description="Legality in Modern format.")
    legacy: Legality = Field(description="Legality in Legacy format.")
    pauper: Legality = Field(description="Legality in Pauper format.")
    vintage: Legality = Field(description="Legality in Vintage format.")
    penny: Legality = Field(description="Legality in Penny Dreadful format.")
    commander: Legality = Field(description="Legality in Commander format.")
    oathbreaker: Legality = Field(description="Legality in Oathbreaker format.")
    standardbrawl: Legality = Field(description="Legality in Standard Brawl format.")
    brawl: Legality = Field(description="Legality in Brawl format.")
    alchemy: Legality = Field(description="Legality in Alchemy format (Arena).")
    paupercommander: Legality = Field(
        description="Legality in Pauper Commander format."
    )
    duel: Legality = Field(description="Legality in Duel Commander format.")
    oldschool: Legality = Field(description="Legality in Old School 93/94 format.")
    premodern: Legality = Field(description="Legality in Premodern format.")
    predh: Legality = Field(description="Legality in Pre-EDH format.")
    historicbrawl: Legality = Field(
        description="Legality in Historic Brawl format (Arena)."
    )


class PurchaseUris(BaseModel):
    """URIs to a card's listing on major marketplaces."""

    tcgplayer: HttpUrl | None = Field(
        default=None, description="A link to purchase this card on TCGplayer."
    )
    cardmarket: HttpUrl | None = Field(
        default=None, description="A link to purchase this card on Cardmarket."
    )
    cardhoarder: HttpUrl | None = Field(
        default=None, description="A link to purchase this card on Cardhoarder."
    )


class RelatedUris(BaseModel):
    """URIs to a card's listing on other Magic resources."""

    gatherer: HttpUrl | None = Field(
        default=None, description="A link to this card on Gatherer."
    )
    tcgplayer_infinite_articles: HttpUrl | None = Field(
        default=None, description="A link to TCGplayer Infinite articles."
    )
    tcgplayer_infinite_decks: HttpUrl | None = Field(
        default=None, description="A link to TCGplayer Infinite decks."
    )
    edhrec: HttpUrl | None = Field(
        default=None, description="A link to this card on EDHREC."
    )


class Preview(BaseModel):
    """Preview information for a newly spoiled card."""

    source: str | None = Field(
        default=None, description="The name of the source that previewed this card."
    )
    source_uri: HttpUrl | None = Field(
        default=None, description="A link to the preview for this card."
    )
    previewed_at: date | None = Field(
        default=None, description="The date this card was previewed."
    )


class CardFace(BaseModel):
    """A single face of a multiface card."""

    object: Literal["card_face"] = Field(
        default="card_face", description="A content type for this object."
    )
    name: str = Field(description="The name of this particular face.")
    mana_cost: str = Field(description="The mana cost for this face.")
    type_line: str | None = Field(
        default=None, description="The type line of this particular face."
    )
    oracle_text: str | None = Field(
        default=None, description="The Oracle text for this face."
    )
    colors: list[Color] | None = Field(default=None, description="This face's colors.")
    color_indicator: list[Color] | None = Field(
        default=None, description="The colors in this face's color indicator."
    )
    power: str | None = Field(default=None, description="This face's power.")
    toughness: str | None = Field(default=None, description="This face's toughness.")
    defense: str | None = Field(default=None, description="This face's defense.")
    loyalty: str | None = Field(default=None, description="This face's loyalty.")
    flavor_text: str | None = Field(
        default=None, description="The flavor text printed on this face."
    )
    flavor_name: str | None = Field(
        default=None,
        description="The flavor name for this face (e.g., Godzilla series).",
    )
    illustration_id: UUID | None = Field(
        default=None, description="A unique identifier for the card face artwork."
    )
    image_uris: ImageUris | None = Field(
        default=None, description="URIs to imagery for this face."
    )
    artist: str | None = Field(
        default=None, description="The name of the illustrator of this card face."
    )
    artist_id: UUID | None = Field(
        default=None, description="The ID of the illustrator of this card face."
    )
    watermark: str | None = Field(
        default=None, description="The watermark on this particular card face."
    )
    printed_name: str | None = Field(
        default=None, description="The localized name printed on this face."
    )
    printed_text: str | None = Field(
        default=None, description="The localized text printed on this face."
    )
    printed_type_line: str | None = Field(
        default=None, description="The localized type line printed on this face."
    )
    cmc: float | None = Field(
        default=None, description="The mana value of this particular face."
    )
    oracle_id: UUID | None = Field(
        default=None, description="The Oracle ID of this particular face."
    )
    layout: Layout | None = Field(
        default=None, description="The layout of this card face."
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
                "flavor_name": pl.String,
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
                "cmc": pl.Float64,
                "oracle_id": pl.String,
                "layout": pl.String,
            }
        )


class RelatedCard(BaseModel):
    """A card closely related to another card."""

    object: Literal["related_card"] = Field(
        default="related_card", description="A content type for this object."
    )
    id: UUID = Field(description="A unique ID for this card in Scryfall's database.")
    component: Component = Field(
        description="A field explaining what role this card plays in this relationship."
    )
    name: str = Field(description="The name of this particular related card.")
    type_line: str = Field(description="The type line of this card.")
    uri: HttpUrl = Field(
        description="A URI where you can retrieve a full object describing this card."
    )


class ScryfallCard(BaseModel):
    """Scryfall Card object."""

    object: Literal["card"] = Field(
        default="card", description="A content type for this object."
    )
    id: UUID = Field(description="A unique ID for this card in Scryfall's database.")
    oracle_id: UUID | None = Field(
        default=None, description="A unique ID for this card's oracle identity."
    )
    multiverse_ids: list[int] | None = Field(
        default=None, description="This card's multiverse IDs on Gatherer."
    )
    mtgo_id: int | None = Field(
        default=None, description="This card's Magic Online ID."
    )
    mtgo_foil_id: int | None = Field(
        default=None, description="This card's foil Magic Online ID."
    )
    tcgplayer_id: int | None = Field(
        default=None, description="This card's ID on TCGplayer's API."
    )
    tcgplayer_etched_id: int | None = Field(
        default=None, description="This card's etched version ID on TCGplayer."
    )
    cardmarket_id: int | None = Field(
        default=None, description="This card's ID on Cardmarket's API."
    )
    arena_id: int | None = Field(default=None, description="This card's Arena ID.")
    resource_id: str | None = Field(
        default=None, description="This card's Resource ID on Gatherer."
    )
    lang: str = Field(description="A language code for this printing.")
    prints_search_uri: HttpUrl = Field(
        description="A link to paginate all re/prints for this card."
    )
    rulings_uri: HttpUrl = Field(description="A link to this card's rulings list.")
    scryfall_uri: HttpUrl = Field(
        description="A link to this card's permapage on Scryfall."
    )
    uri: HttpUrl = Field(description="A link to this card object on Scryfall's API.")
    all_parts: list[RelatedCard] | None = Field(
        default=None, description="Related cards if closely related."
    )
    card_faces: list[CardFace] | None = Field(
        default=None, description="Card Face objects if multifaced."
    )
    cmc: float = Field(description="The card's mana value.")
    color_identity: list[Color] = Field(description="This card's color identity.")
    color_indicator: list[Color] | None = Field(
        default=None, description="The colors in this card's color indicator."
    )
    colors: list[Color] | None = Field(default=None, description="This card's colors.")
    defense: str | None = Field(default=None, description="This face's defense.")
    edhrec_rank: int | None = Field(
        default=None, description="This card's rank/popularity on EDHREC."
    )
    game_changer: bool | None = Field(
        default=None, description="True if on the Commander Game Changer list."
    )
    hand_modifier: str | None = Field(
        default=None, description="This card's hand modifier if Vanguard."
    )
    keywords: list[str] = Field(description="Keywords that this card uses.")
    layout: Layout = Field(description="A code for this card's layout.")
    legalities: Legalities | None = Field(
        default=None, description="Legality across play formats."
    )
    life_modifier: str | None = Field(
        default=None, description="This card's life modifier if Vanguard."
    )
    loyalty: str | None = Field(default=None, description="This loyalty if any.")
    mana_cost: str | None = Field(
        default=None, description="The mana cost for this card."
    )
    name: str = Field(description="The name of this card.")
    oracle_text: str | None = Field(
        default=None, description="The Oracle text for this card."
    )
    penny_rank: int | None = Field(
        default=None, description="This card's rank on Penny Dreadful."
    )
    power: str | None = Field(default=None, description="This card's power.")
    produced_mana: list[ManaColor] | None = Field(
        default=None, description="Colors of mana this card could produce."
    )
    reserved: bool = Field(description="True if this card is on the Reserved List.")
    toughness: str | None = Field(default=None, description="This card's toughness.")
    type_line: str = Field(description="The type line of this card.")
    artist: str | None = Field(default=None, description="The name of the illustrator.")
    artist_ids: list[UUID] | None = Field(
        default=None, description="The IDs of the artists."
    )
    attraction_lights: list[int] | None = Field(
        default=None, description="Unfinity attraction lights."
    )
    booster: bool = Field(
        default=True, description="Whether this card is found in boosters."
    )
    border_color: BorderColor = Field(description="This card's border color.")
    card_back_id: UUID = Field(description="The Scryfall ID for the card back design.")
    collector_number: str = Field(description="This card's collector number.")
    content_warning: bool | None = Field(
        default=None, description="True if avoiding use is recommended."
    )
    digital: bool = Field(description="True if only released in a video game.")
    finishes: list[Finish] = Field(
        description="Available finishes: foil, nonfoil, etched, glossy."
    )
    flavor_name: str | None = Field(
        default=None, description="The flavor name (e.g., Godzilla series)."
    )
    flavor_text: str | None = Field(default=None, description="The flavor text.")
    frame_effects: list[str] | None = Field(
        default=None, description="This card's frame effects."
    )
    frame: Frame = Field(description="This card's frame layout.")
    full_art: bool = Field(
        description="True if this card's artwork is larger than normal."
    )
    games: list[Game] | None = Field(description="Game platforms: paper, arena, mtgo.")
    highres_image: bool = Field(description="True if imagery is high resolution.")
    illustration_id: UUID | None = Field(
        default=None, description="A unique identifier for the card artwork."
    )
    image_status: ImageStatus = Field(description="The state of this card's image.")
    image_uris: ImageUris | None = Field(
        default=None, description="Available imagery for this card."
    )
    oversized: bool = Field(description="True if this card is oversized.")
    prices: Prices = Field(description="Daily price information for this card.")
    printed_name: str | None = Field(
        default=None, description="The localized name printed on this card."
    )
    printed_text: str | None = Field(
        default=None, description="The localized text printed on this card."
    )
    printed_type_line: str | None = Field(
        default=None, description="The localized type line printed on this card."
    )
    promo: bool = Field(description="True if this card is a promotional print.")
    promo_types: list[str] | None = Field(
        default=None, description="Categories of promo cards this falls into."
    )
    purchase_uris: PurchaseUris | None = Field(
        default=None, description="URIs to marketplace listings."
    )
    rarity: Rarity = Field(description="This card's rarity.")
    related_uris: RelatedUris = Field(description="URIs to other Magic resources.")
    released_at: date = Field(description="The date this card was first released.")
    reprint: bool = Field(description="True if this card is a reprint.")
    scryfall_set_uri: HttpUrl = Field(
        description="A link to this card's set on Scryfall."
    )
    set_name: str = Field(description="This card's full set name.")
    set_search_uri: HttpUrl = Field(description="A link to paginate this card's set.")
    set_type: str = Field(description="The type of set this printing is in.")
    set_uri: HttpUrl = Field(description="A link to this card's set object.")
    set: str = Field(description="This card's set code.")
    set_id: UUID = Field(description="This card's Set object UUID.")
    story_spotlight: bool = Field(description="True if this card is a Story Spotlight.")
    textless: bool = Field(description="True if the card is printed without text.")
    variation: bool = Field(
        description="Whether this card is a variation of another printing."
    )
    variation_of: UUID | None = Field(
        default=None, description="The printing ID this card is a variation of."
    )
    security_stamp: SecurityStamp | None = Field(
        default=None, description="The security stamp on this card."
    )
    watermark: str | None = Field(default=None, description="This card's watermark.")
    preview: Preview | None = Field(
        default=None, description="Preview information for this card."
    )
