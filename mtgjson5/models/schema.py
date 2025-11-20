from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel


class MtgjsonBaseModel(BaseModel):
    """
    Base model configuration to automatically map camelCase JSON 
    to snake_case Python attributes.
    """
    model_config = ConfigDict(
        alias_generator=to_camel, 
        populate_by_name=True,
        arbitrary_types_allowed=True
    )


class MtgjsonIdentifiersObject(MtgjsonBaseModel):
    abu_id: str | None = None
    card_kingdom_etched_id: str | None = None
    card_kingdom_foil_id: str | None = None
    card_kingdom_id: str | None = None
    cardsphere_id: str | None = None
    cardsphere_foil_id: str | None = None
    cardtrader_id: str | None = None
    csi_id: str | None = None
    mcm_id: str | None = None
    mcm_meta_id: str | None = None
    miniaturemarket_id: str | None = None
    mtg_arena_id: str | None = None
    mtgjson_foil_version_id: str | None = None
    mtgjson_non_foil_version_id: str | None = None
    mtgjson_v4_id: str | None = None
    mtgo_foil_id: str | None = None
    mtgo_id: str | None = None
    multiverse_id: str | None = None
    scg_id: str | None = None
    scryfall_id: str | None = None
    scryfall_card_back_id: str | None = None
    scryfall_oracle_id: str | None = None
    scryfall_illustration_id: str | None = None
    tcgplayer_product_id: str | None = None
    tcgplayer_etched_product_id: str | None = None
    tnt_id: str | None = None


class MtgjsonPurchaseUrlsObject(MtgjsonBaseModel):
    card_kingdom: str | None = None
    card_kingdom_etched: str | None = None
    card_kingdom_foil: str | None = None
    cardmarket: str | None = None
    tcgplayer: str | None = None
    tcgplayer_etched: str | None = None


class MtgjsonRelatedCardsObject(MtgjsonBaseModel):
    reverse_related: List[str] = Field(default_factory=list)
    spellbook: List[str] = Field(default_factory=list)


class MtgjsonRulingsObject(MtgjsonBaseModel):
    date: str
    text: str


class MtgjsonLeadershipSkillsObject(MtgjsonBaseModel):
    brawl: bool = False
    commander: bool = False
    oathbreaker: bool = False


class MtgjsonLegalitiesObject(MtgjsonBaseModel):
    alchemy: str | None = None
    brawl: str | None = None
    commander: str | None = None
    duel: str | None = None
    explorer: str | None = None
    future: str | None = None
    gladiator: str | None = None
    historic: str | None = None
    historicbrawl: str | None = None
    legacy: str | None = None
    modern: str | None = None
    oathbreaker: str | None = None
    oldschool: str | None = None
    pauper: str | None = None
    paupercommander: str | None = None
    penny: str | None = None
    pioneer: str | None = None
    predh: str | None = None
    premodern: str | None = None
    standard: str | None = None
    standardbrawl: str | None = None
    timeless: str | None = None
    vintage: str | None = None


class MtgjsonForeignDataObject(MtgjsonBaseModel):
    face_name: str | None = None
    flavor_text: str | None = None
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    language: str
    name: str
    text: str | None = None
    type: str | None = None
    uuid: str


class MtgjsonTranslationsObject(MtgjsonBaseModel):
    # Using alias to handle keys with spaces
    ancient_greek: str | None = Field(default=None, alias="Ancient Greek")
    arabic: str | None = Field(default=None, alias="Arabic")
    chinese_simplified: str | None = Field(default=None, alias="Chinese Simplified")
    chinese_traditional: str | None = Field(default=None, alias="Chinese Traditional")
    french: str | None = Field(default=None, alias="French")
    german: str | None = Field(default=None, alias="German")
    hebrew: str | None = Field(default=None, alias="Hebrew")
    italian: str | None = Field(default=None, alias="Italian")
    japanese: str | None = Field(default=None, alias="Japanese")
    korean: str | None = Field(default=None, alias="Korean")
    latin: str | None = Field(default=None, alias="Latin")
    phyrexian: str | None = Field(default=None, alias="Phyrexian")
    portuguese_brazil: str | None = Field(default=None, alias="Portuguese (Brazil)")
    russian: str | None = Field(default=None, alias="Russian")
    sanskrit: str | None = Field(default=None, alias="Sanskrit")
    spanish: str | None = Field(default=None, alias="Spanish")


class MtgjsonKeywordsObject(MtgjsonBaseModel):
    ability_words: List[str] = Field(default_factory=list)
    keyword_abilities: List[str] = Field(default_factory=list)
    keyword_actions: List[str] = Field(default_factory=list)


class MtgjsonSourceProductsObject(MtgjsonBaseModel):
    etched: List[str] = Field(default_factory=list)
    foil: List[str] = Field(default_factory=list)
    nonfoil: List[str] = Field(default_factory=list)


class MtgjsonTcgplayerSkusObject(MtgjsonBaseModel):
    condition: str
    finish: str
    language: str
    printing: str
    product_id: str
    sku_id: str


class MtgjsonMetaObject(MtgjsonBaseModel):
    date: str
    version: str


class MtgjsonPricePointsObject(MtgjsonBaseModel):
    etched: Dict[str, float] = Field(default_factory=dict)
    foil: Dict[str, float] = Field(default_factory=dict)
    normal: Dict[str, float] = Field(default_factory=dict)


class MtgjsonPriceListObject(MtgjsonBaseModel):
    buylist: MtgjsonPricePointsObject | None = None
    currency: str
    retail: MtgjsonPricePointsObject | None = None


class MtgjsonPriceFormatsObject(MtgjsonBaseModel):
    mtgo: Dict[str, MtgjsonPriceListObject] = Field(default_factory=dict)
    paper: Dict[str, MtgjsonPriceListObject] = Field(default_factory=dict)


class MtgjsonBoosterPackObject(MtgjsonBaseModel):
    contents: Dict[str, int] = Field(default_factory=dict)
    weight: float


class MtgjsonBoosterSheetObject(MtgjsonBaseModel):
    allow_duplicates: bool | None = None
    balance_colors: bool | None = None
    cards: Dict[str, int] = Field(default_factory=dict)
    foil: bool
    fixed: bool | None = None
    total_weight: float


class MtgjsonBoosterConfigObject(MtgjsonBaseModel):
    boosters: List[MtgjsonBoosterPackObject] = Field(default_factory=list)
    boosters_total_weight: float
    name: str | None = None
    sheets: Dict[str, MtgjsonBoosterSheetObject] = Field(default_factory=dict)
    source_set_codes: List[str] = Field(default_factory=list)


# --- Sealed Products ---

class MtgjsonSealedProductCardObject(MtgjsonBaseModel):
    foil: bool | None = None
    name: str
    number: str
    set: str
    uuid: str


class MtgjsonSealedProductDeckObject(MtgjsonBaseModel):
    name: str
    set: str


class MtgjsonSealedProductOtherObject(MtgjsonBaseModel):
    name: str


class MtgjsonSealedProductPackObject(MtgjsonBaseModel):
    code: str
    set: str


class MtgjsonSealedProductSealedObject(MtgjsonBaseModel):
    count: int
    name: str
    set: str
    uuid: str


class MtgjsonSealedProductContentsObject(MtgjsonBaseModel):
    card: List[MtgjsonSealedProductCardObject] = Field(default_factory=list)
    deck: List[MtgjsonSealedProductDeckObject] = Field(default_factory=list)
    other: List[MtgjsonSealedProductOtherObject] = Field(default_factory=list)
    pack: List[MtgjsonSealedProductPackObject] = Field(default_factory=list)
    sealed: List[MtgjsonSealedProductSealedObject] = Field(default_factory=list)
    # Recursive structure definition
    variable: List[Dict[str, List[MtgjsonSealedProductContentsObject]]] = Field(default_factory=list)


class MtgjsonSealedProductObject(MtgjsonBaseModel):
    card_count: int | None = None
    category: str | None = None
    contents: MtgjsonSealedProductContentsObject | None = None
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    name: str
    product_size: int | None = None
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(default_factory=MtgjsonPurchaseUrlsObject)
    release_date: str | None = None
    subtype: str | None = None
    uuid: str


# --- Card Types ---

class MtgjsonCardTypeObject(MtgjsonBaseModel):
    sub_types: List[str] = Field(default_factory=list)
    super_types: List[str] = Field(default_factory=list)


class MtgjsonCardTypesObject(MtgjsonBaseModel):
    artifact: MtgjsonCardTypeObject
    battle: MtgjsonCardTypeObject
    conspiracy: MtgjsonCardTypeObject
    creature: MtgjsonCardTypeObject
    enchantment: MtgjsonCardTypeObject
    instant: MtgjsonCardTypeObject
    land: MtgjsonCardTypeObject
    phenomenon: MtgjsonCardTypeObject
    plane: MtgjsonCardTypeObject
    planeswalker: MtgjsonCardTypeObject
    scheme: MtgjsonCardTypeObject
    sorcery: MtgjsonCardTypeObject
    tribal: MtgjsonCardTypeObject
    vanguard: MtgjsonCardTypeObject


# --- Cards ---

class MtgjsonCardAtomicObject(MtgjsonBaseModel):
    ascii_name: str | None = None
    attraction_lights: List[int] | None = None
    color_identity: List[str] = Field(default_factory=list)
    color_indicator: List[str] | None = None
    colors: List[str] = Field(default_factory=list)
    converted_mana_cost: float
    defense: str | None = None
    edhrec_rank: int | None = None
    edhrec_saltiness: float | None = None
    face_converted_mana_cost: float | None = None
    face_mana_value: float | None = None
    face_name: str | None = None
    first_printing: str | None = None
    foreign_data: List[MtgjsonForeignDataObject] = Field(default_factory=list)
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    is_funny: bool | None = None
    is_game_changer: bool | None = None
    is_reserved: bool | None = None
    keywords: List[str] = Field(default_factory=list)
    layout: str
    leadership_skills: MtgjsonLeadershipSkillsObject | None = None
    legalities: MtgjsonLegalitiesObject = Field(default_factory=MtgjsonLegalitiesObject)
    life: str | None = None
    loyalty: str | None = None
    mana_cost: str | None = None
    mana_value: float
    name: str
    power: str | None = None
    printings: List[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(default_factory=MtgjsonPurchaseUrlsObject)
    related_cards: MtgjsonRelatedCardsObject = Field(default_factory=MtgjsonRelatedCardsObject)
    rulings: List[MtgjsonRulingsObject] = Field(default_factory=list)
    side: str | None = None
    subsets: List[str] = Field(default_factory=list)
    subtypes: List[str] = Field(default_factory=list)
    supertypes: List[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: List[str] = Field(default_factory=list)


class MtgjsonCardDeckObject(MtgjsonBaseModel):
    artist: str | None = None
    artist_ids: List[str] = Field(default_factory=list)
    ascii_name: str | None = None
    attraction_lights: List[int] | None = None
    availability: List[str] = Field(default_factory=list)
    booster_types: List[str] = Field(default_factory=list)
    border_color: str
    card_parts: List[str] = Field(default_factory=list)
    color_identity: List[str] = Field(default_factory=list)
    color_indicator: List[str] | None = None
    colors: List[str] = Field(default_factory=list)
    converted_mana_cost: float
    count: int
    defense: str | None = None
    duel_deck: str | None = None
    edhrec_rank: int | None = None
    edhrec_saltiness: float | None = None
    face_converted_mana_cost: float | None = None
    face_flavor_name: str | None = None
    face_mana_value: float | None = None
    face_name: str | None = None
    finishes: List[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    foreign_data: List[MtgjsonForeignDataObject] = Field(default_factory=list)
    frame_effects: List[str] = Field(default_factory=list)
    frame_version: str
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    has_content_warning: bool | None = None
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    is_alternative: bool | None = None
    is_foil: bool
    is_full_art: bool | None = None
    is_funny: bool | None = None
    is_game_changer: bool | None = None
    is_online_only: bool | None = None
    is_oversized: bool | None = None
    is_promo: bool | None = None
    is_rebalanced: bool | None = None
    is_reprint: bool | None = None
    is_reserved: bool | None = None
    is_starter: bool | None = None
    is_story_spotlight: bool | None = None
    is_textless: bool | None = None
    is_timeshifted: bool | None = None
    keywords: List[str] = Field(default_factory=list)
    language: str
    layout: str
    leadership_skills: MtgjsonLeadershipSkillsObject | None = None
    legalities: MtgjsonLegalitiesObject = Field(default_factory=MtgjsonLegalitiesObject)
    life: str | None = None
    loyalty: str | None = None
    mana_cost: str | None = None
    mana_value: float
    name: str
    number: str
    original_printings: List[str] = Field(default_factory=list)
    original_release_date: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: List[str] = Field(default_factory=list)
    power: str | None = None
    printings: List[str] = Field(default_factory=list)
    promo_types: List[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(default_factory=MtgjsonPurchaseUrlsObject)
    rarity: str
    related_cards: MtgjsonRelatedCardsObject | None = None
    rebalanced_printings: List[str] = Field(default_factory=list)
    rulings: List[MtgjsonRulingsObject] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    source_products: List[str] = Field(default_factory=list)
    subsets: List[str] = Field(default_factory=list)
    subtypes: List[str] = Field(default_factory=list)
    supertypes: List[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: List[str] = Field(default_factory=list)
    uuid: str
    variations: List[str] = Field(default_factory=list)
    watermark: str | None = None


class MtgjsonCardSetDeckObject(MtgjsonBaseModel):
    count: int
    is_foil: bool | None = None
    uuid: str


class MtgjsonCardSetObject(MtgjsonBaseModel):
    artist: str | None = None
    artist_ids: List[str] = Field(default_factory=list)
    ascii_name: str | None = None
    attraction_lights: List[int] | None = None
    availability: List[str] = Field(default_factory=list)
    booster_types: List[str] = Field(default_factory=list)
    border_color: str
    card_parts: List[str] = Field(default_factory=list)
    color_identity: List[str] = Field(default_factory=list)
    color_indicator: List[str] | None = None
    colors: List[str] = Field(default_factory=list)
    converted_mana_cost: float
    defense: str | None = None
    duel_deck: str | None = None
    edhrec_rank: int | None = None
    edhrec_saltiness: float | None = None
    face_converted_mana_cost: float | None = None
    face_flavor_name: str | None = None
    face_mana_value: float | None = None
    face_name: str | None = None
    finishes: List[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    foreign_data: List[MtgjsonForeignDataObject] = Field(default_factory=list)
    frame_effects: List[str] = Field(default_factory=list)
    frame_version: str
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    has_content_warning: bool | None = None
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    is_alternative: bool | None = None
    is_full_art: bool | None = None
    is_funny: bool | None = None
    is_game_changer: bool | None = None
    is_online_only: bool | None = None
    is_oversized: bool | None = None
    is_promo: bool | None = None
    is_rebalanced: bool | None = None
    is_reprint: bool | None = None
    is_reserved: bool | None = None
    is_starter: bool | None = None
    is_story_spotlight: bool | None = None
    is_textless: bool | None = None
    is_timeshifted: bool | None = None
    keywords: List[str] = Field(default_factory=list)
    language: str
    layout: str
    leadership_skills: MtgjsonLeadershipSkillsObject | None = None
    legalities: MtgjsonLegalitiesObject = Field(default_factory=MtgjsonLegalitiesObject)
    life: str | None = None
    loyalty: str | None = None
    mana_cost: str | None = None
    mana_value: float
    name: str
    number: str
    original_printings: List[str] = Field(default_factory=list)
    original_release_date: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: List[str] = Field(default_factory=list)
    power: str | None = None
    printings: List[str] = Field(default_factory=list)
    promo_types: List[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(default_factory=MtgjsonPurchaseUrlsObject)
    rarity: str
    related_cards: MtgjsonRelatedCardsObject | None = None
    rebalanced_printings: List[str] = Field(default_factory=list)
    rulings: List[MtgjsonRulingsObject] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    source_products: MtgjsonSourceProductsObject | None = None
    subsets: List[str] = Field(default_factory=list)
    subtypes: List[str] = Field(default_factory=list)
    supertypes: List[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: List[str] = Field(default_factory=list)
    uuid: str
    variations: List[str] = Field(default_factory=list)
    watermark: str | None = None


class MtgjsonCardTokenObject(MtgjsonBaseModel):
    artist: str | None = None
    artist_ids: List[str] = Field(default_factory=list)
    ascii_name: str | None = None
    availability: List[str] = Field(default_factory=list)
    booster_types: List[str] = Field(default_factory=list)
    border_color: str
    card_parts: List[str] = Field(default_factory=list)
    color_identity: List[str] = Field(default_factory=list)
    color_indicator: List[str] | None = None
    colors: List[str] = Field(default_factory=list)
    edhrec_saltiness: float | None = None
    face_name: str | None = None
    face_flavor_name: str | None = None
    finishes: List[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    frame_effects: List[str] = Field(default_factory=list)
    frame_version: str
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(default_factory=MtgjsonIdentifiersObject)
    is_full_art: bool | None = None
    is_funny: bool | None = None
    is_online_only: bool | None = None
    is_oversized: bool | None = None
    is_promo: bool | None = None
    is_reprint: bool | None = None
    is_textless: bool | None = None
    keywords: List[str] = Field(default_factory=list)
    language: str
    layout: str
    loyalty: str | None = None
    mana_cost: str | None = None
    name: str
    number: str
    orientation: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: List[str] = Field(default_factory=list)
    power: str | None = None
    promo_types: List[str] = Field(default_factory=list)
    related_cards: MtgjsonRelatedCardsObject | None = None
    reverse_related: List[str] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    source_products: List[str] = Field(default_factory=list)
    subsets: List[str] = Field(default_factory=list)
    subtypes: List[str] = Field(default_factory=list)
    supertypes: List[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: List[str] = Field(default_factory=list)
    uuid: str
    watermark: str | None = None


# --- Decks ---

class MtgjsonDeckObject(MtgjsonBaseModel):
    code: str
    commander: List[MtgjsonCardDeckObject] = Field(default_factory=list)
    main_board: List[MtgjsonCardDeckObject] = Field(default_factory=list)
    name: str
    release_date: str | None = None
    sealed_product_uuids: str
    side_board: List[MtgjsonCardDeckObject] = Field(default_factory=list)
    tokens: List[MtgjsonCardDeckObject] = Field(default_factory=list)
    type: str


class MtgjsonDeckListObject(MtgjsonBaseModel):
    code: str
    file_name: str
    name: str
    release_date: str | None = None
    type: str


class MtgjsonDeckSetObject(MtgjsonBaseModel):
    code: str
    commander: List[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    main_board: List[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    name: str
    release_date: str | None = None
    sealed_product_uuids: List[str] | None = None
    side_board: List[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    type: str


# --- Sets ---

class MtgjsonSetObject(MtgjsonBaseModel):
    base_set_size: int
    block: str | None = None
    booster: Dict[str, MtgjsonBoosterConfigObject] = Field(default_factory=dict)
    cards: List[MtgjsonCardSetObject] = Field(default_factory=list)
    cardsphere_set_id: int | None = None
    code: str
    code_v3: str | None = None
    decks: List[MtgjsonDeckSetObject] = Field(default_factory=list)
    is_foreign_only: bool | None = None
    is_foil_only: bool
    is_non_foil_only: bool | None = None
    is_online_only: bool
    is_paper_only: bool | None = None
    is_partial_preview: bool | None = None
    keyrune_code: str
    languages: List[str] = Field(default_factory=list)
    mcm_id: int | None = None
    mcm_id_extras: int | None = None
    mcm_name: str | None = None
    mtgo_code: str | None = None
    name: str
    parent_code: str | None = None
    release_date: str
    sealed_product: List[MtgjsonSealedProductObject] = Field(default_factory=list)
    tcgplayer_group_id: int | None = None
    tokens: List[MtgjsonCardTokenObject] = Field(default_factory=list)
    token_set_code: str | None = None
    total_set_size: int
    translations: MtgjsonTranslationsObject = Field(default_factory=MtgjsonTranslationsObject)
    type: str


class MtgjsonSetListObject(MtgjsonBaseModel):
    base_set_size: int
    block: str | None = None
    cardsphere_set_id: int | None = None
    code: str
    code_v3: str | None = None
    decks: List[MtgjsonDeckSetObject] = Field(default_factory=list)
    is_foreign_only: bool | None = None
    is_foil_only: bool
    is_non_foil_only: bool | None = None
    is_online_only: bool
    is_paper_only: bool | None = None
    is_partial_preview: bool | None = None
    keyrune_code: str
    languages: List[str] = Field(default_factory=list)
    mcm_id: int | None = None
    mcm_id_extras: int | None = None
    mcm_name: str | None = None
    mtgo_code: str | None = None
    name: str
    parent_code: str | None = None
    release_date: str
    sealed_product: List[MtgjsonSealedProductObject] = Field(default_factory=list)
    tcgplayer_group_id: int | None = None
    total_set_size: int
    token_set_code: str | None = None
    translations: MtgjsonTranslationsObject = Field(default_factory=MtgjsonTranslationsObject)
    type: str

# Rebuild models to ensure all recursive references are resolved
MtgjsonSealedProductContentsObject.model_rebuild()