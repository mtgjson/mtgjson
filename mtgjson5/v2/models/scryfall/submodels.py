"""
Scryfall TypedDict definitions for fast parsing.

Use these for bulk data ingestion where validation overhead matters.
For validated parsing, use the Pydantic models with TypeAdapters.
"""

from __future__ import annotations

from typing import Literal, Required, TypedDict

from .literals import (
    BorderColor,
    Color,
    Component,
    Finish,
    Frame,
    FrameEffect,
    Game,
    ImageStatus,
    Layout,
    LegalityStatus,
    ManaColor,
    Rarity,
    SecurityStamp,
)


class ScryfallImageUris(TypedDict, total=False):
    """Available imagery for a card."""

    small: str
    normal: str
    large: str
    png: str
    art_crop: str
    border_crop: str


class ScryfallPrices(TypedDict, total=False):
    """Daily price information."""

    usd: str | None
    usd_foil: str | None
    usd_etched: str | None
    eur: str | None
    eur_foil: str | None
    eur_etched: str | None
    tix: str | None


class ScryfallLegalities(TypedDict, total=False):
    """Format legalities."""

    alchemy: LegalityStatus
    brawl: LegalityStatus
    commander: LegalityStatus
    duel: LegalityStatus
    explorer: LegalityStatus
    future: LegalityStatus
    gladiator: LegalityStatus
    historic: LegalityStatus
    historicbrawl: LegalityStatus
    legacy: LegalityStatus
    modern: LegalityStatus
    oathbreaker: LegalityStatus
    oldschool: LegalityStatus
    pauper: LegalityStatus
    paupercommander: LegalityStatus
    penny: LegalityStatus
    pioneer: LegalityStatus
    predh: LegalityStatus
    premodern: LegalityStatus
    standard: LegalityStatus
    standardbrawl: LegalityStatus
    timeless: LegalityStatus
    vintage: LegalityStatus


class ScryfallPurchaseUris(TypedDict, total=False):
    """Purchase URLs."""

    tcgplayer: str
    cardmarket: str
    cardhoarder: str


class ScryfallRelatedUris(TypedDict, total=False):
    """Related resource URLs."""

    gatherer: str
    tcgplayer_infinite_articles: str
    tcgplayer_infinite_decks: str
    edhrec: str


class ScryfallPreview(TypedDict, total=False):
    """Preview/spoiler information."""

    source: str
    source_uri: str
    previewed_at: str


class ScryfallRelatedCard(TypedDict):
    """Related card reference."""

    object: Required[Literal["related_card"]]
    id: Required[str]
    component: Required[Component]
    name: Required[str]
    type_line: Required[str]
    uri: Required[str]


class ScryfallCardFace(TypedDict, total=False):
    """Single face of a multi-face card."""

    object: Literal["card_face"]
    name: Required[str]
    mana_cost: Required[str]
    type_line: str
    oracle_text: str
    colors: list[Color]
    color_indicator: list[Color]
    power: str
    toughness: str
    defense: str
    loyalty: str
    flavor_text: str
    flavor_name: str
    illustration_id: str
    image_uris: ScryfallImageUris
    artist: str
    artist_id: str
    watermark: str
    printed_name: str
    printed_text: str
    printed_type_line: str
    cmc: float
    oracle_id: str
    layout: Layout


class ScryfallSet(TypedDict, total=False):
    """Scryfall Set object."""

    object: Required[Literal["set"]]
    id: Required[str]
    code: Required[str]
    name: Required[str]
    set_type: Required[str]
    card_count: Required[int]
    digital: Required[bool]
    foil_only: Required[bool]
    nonfoil_only: Required[bool]
    scryfall_uri: Required[str]
    uri: Required[str]
    icon_svg_uri: Required[str]
    search_uri: Required[str]
    # Optional
    mtgo_code: str
    arena_code: str
    tcgplayer_id: int
    released_at: str
    printed_size: int
    block_code: str
    block: str
    parent_set_code: str


class ScryfallCard(TypedDict, total=False):
    """Scryfall Card object."""

    # Required core
    object: Required[Literal["card"]]
    id: Required[str]
    name: Required[str]
    lang: Required[str]
    layout: Required[Layout]
    uri: Required[str]
    scryfall_uri: Required[str]
    prints_search_uri: Required[str]
    rulings_uri: Required[str]

    # Required gameplay
    cmc: Required[float]
    type_line: Required[str]
    color_identity: Required[list[Color]]
    keywords: Required[list[str]]
    reserved: Required[bool]

    # Required print
    set: Required[str]
    set_id: Required[str]
    set_name: Required[str]
    set_type: Required[str]
    set_uri: Required[str]
    set_search_uri: Required[str]
    scryfall_set_uri: Required[str]
    collector_number: Required[str]
    rarity: Required[Rarity]
    released_at: Required[str]
    reprint: Required[bool]
    digital: Required[bool]

    # Required visual
    finishes: Required[list[Finish]]
    frame: Required[Frame]
    border_color: Required[BorderColor]
    image_status: Required[ImageStatus]
    highres_image: Required[bool]
    card_back_id: Required[str]

    # Required flags
    booster: Required[bool]
    full_art: Required[bool]
    textless: Required[bool]
    variation: Required[bool]
    oversized: Required[bool]
    promo: Required[bool]
    story_spotlight: Required[bool]

    # Required nested
    prices: Required[ScryfallPrices]
    related_uris: Required[ScryfallRelatedUris]

    # Optional gameplay
    oracle_id: str
    mana_cost: str
    oracle_text: str
    colors: list[Color]
    color_indicator: list[Color]
    produced_mana: list[ManaColor]
    power: str
    toughness: str
    loyalty: str
    defense: str
    hand_modifier: str
    life_modifier: str
    legalities: ScryfallLegalities

    # Optional multi-face
    card_faces: list[ScryfallCardFace]
    all_parts: list[ScryfallRelatedCard]

    # Optional print details
    variation_of: str
    promo_types: list[str]
    frame_effects: list[FrameEffect]
    security_stamp: SecurityStamp
    games: list[Game]

    # Optional IDs
    multiverse_ids: list[int]
    mtgo_id: int
    mtgo_foil_id: int
    tcgplayer_id: int
    tcgplayer_etched_id: int
    cardmarket_id: int
    arena_id: int

    # Optional imagery
    image_uris: ScryfallImageUris
    illustration_id: str

    # Optional flavor
    artist: str
    artist_ids: list[str]
    flavor_name: str
    flavor_text: str
    watermark: str
    attraction_lights: list[int]

    # Optional localized
    printed_name: str
    printed_text: str
    printed_type_line: str

    # Optional misc
    purchase_uris: ScryfallPurchaseUris
    content_warning: bool
    edhrec_rank: int
    penny_rank: int
    game_changer: bool
    preview: ScryfallPreview
    resource_id: str


class ScryfallRuling(TypedDict):
    """Scryfall ruling entry."""

    object: Literal["ruling"]
    oracle_id: str
    source: str
    published_at: str
    comment: str


class ScryfallList(TypedDict, total=False):
    """Scryfall paginated list response."""

    object: Required[Literal["list"]]
    has_more: Required[bool]
    data: Required[list]  # ScryfallCard | ScryfallSet | ScryfallRuling
    total_cards: int
    next_page: str
    warnings: list[str]


class ScryfallBulkData(TypedDict):
    """Scryfall bulk data metadata."""

    object: Literal["bulk_data"]
    id: str
    type: str
    updated_at: str
    uri: str
    name: str
    description: str
    size: int
    download_uri: str
    content_type: str
    content_encoding: str


# =============================================================================
# Registry
# =============================================================================

SCRYFALL_TYPEDDICT_REGISTRY: list[type] = [
    ScryfallImageUris,
    ScryfallPrices,
    ScryfallLegalities,
    ScryfallPurchaseUris,
    ScryfallRelatedUris,
    ScryfallPreview,
    ScryfallRelatedCard,
    ScryfallCardFace,
    ScryfallSet,
    ScryfallCard,
    ScryfallRuling,
    ScryfallList,
    ScryfallBulkData,
]


# =============================================================================
# Field Mappings (Scryfall -> MTGJSON)
# =============================================================================

# Scryfall snake_case -> MTGJSON camelCase
SCRYFALL_TO_MTGJSON_FIELDS: dict[str, str] = {
    "card_faces": "cardFaces",
    "all_parts": "allParts",
    "color_identity": "colorIdentity",
    "color_indicator": "colorIndicator",
    "produced_mana": "producedMana",
    "mana_cost": "manaCost",
    "oracle_text": "oracleText",
    "type_line": "typeLine",
    "collector_number": "number",
    "set": "setCode",
    "set_name": "setName",
    "set_type": "setType",
    "released_at": "releaseDate",
    "frame_effects": "frameEffects",
    "border_color": "borderColor",
    "security_stamp": "securityStamp",
    "flavor_text": "flavorText",
    "flavor_name": "flavorName",
    "image_uris": "imageUris",
    "image_status": "imageStatus",
    "highres_image": "highresImage",
    "card_back_id": "cardBackId",
    "full_art": "isFullArt",
    "story_spotlight": "isStorySpotlight",
    "promo_types": "promoTypes",
    "artist_ids": "artistIds",
    "multiverse_ids": "multiverseIds",
    "mtgo_id": "mtgoId",
    "mtgo_foil_id": "mtgoFoilId",
    "tcgplayer_id": "tcgplayerId",
    "tcgplayer_etched_id": "tcgplayerEtchedId",
    "cardmarket_id": "cardmarketId",
    "arena_id": "arenaId",
    "oracle_id": "scryfallOracleId",
    "illustration_id": "scryfallIllustrationId",
    "edhrec_rank": "edhrecRank",
    "penny_rank": "pennyRank",
    "hand_modifier": "hand",
    "life_modifier": "life",
    "parent_set_code": "parentCode",
    "printed_size": "printedSize",
    "foil_only": "isFoilOnly",
    "nonfoil_only": "isNonFoilOnly",
    "content_warning": "hasContentWarning",
    "game_changer": "isGameChanger",
    "attraction_lights": "attractionLights",
}

# Scryfall ruling fields -> MTGJSON ruling fields
SCRYFALL_RULING_ALIASES: dict[tuple[str, str], str] = {
    ("ScryfallRuling", "published_at"): "date",
    ("ScryfallRuling", "comment"): "text",
}
