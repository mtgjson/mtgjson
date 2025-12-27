"""MTGJSON Pydantic data models for cards, sets, decks, and related objects."""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable
from typing import Any, ClassVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    model_serializer,
)
from pydantic.alias_generators import to_camel
from pydantic_core import core_schema


_CAMEL_TO_SNAKE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_TO_SNAKE_2 = re.compile(r"([a-z0-9])([A-Z])")


class MtgjsonBaseModel(BaseModel):
    """
    Base model configuration to automatically map camelCase JSON
    to snake_case Python attributes.
    """

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, arbitrary_types_allowed=True
    )


class MTGJsonModel(BaseModel):
    """
    Base for all MTGJSON models with custom serialization logic.
    """

    @field_serializer("*", when_used="json", check_fields=False)
    def serialize_dates_and_sets(self, value: Any) -> Any:
        """field serializer for datetime and set types."""
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, set):
            return list(value)
        return value

    def build_keys_to_skip(self) -> set[str]:
        """Override to define fields to exclude dynamically."""
        return set()

    @model_serializer(mode="wrap")
    def serialize_model(
        self,
        serializer: Callable[[Any], dict[str, Any]],
        _info: core_schema.SerializationInfo,
    ) -> dict[str, Any]:
        """Custom serialization respecting build_keys_to_skip()."""
        data = serializer(self)
        skip_keys = self.build_keys_to_skip()

        if not skip_keys:
            return data

        result = {}
        for field_name, value in data.items():
            snake_case_field = self._to_snake_case(field_name)
            if snake_case_field not in skip_keys:
                result[field_name] = value

        return result

    @staticmethod
    def _to_snake_case(camel_str: str) -> str:
        """regex camelCase -> snake_case conversion."""
        # module level regex compilation for a little speed boost
        s1 = _CAMEL_TO_SNAKE_1.sub(r"\1_\2", camel_str)
        return _CAMEL_TO_SNAKE_2.sub(r"\1_\2", s1).lower()

    def to_json(self) -> dict[str, Any]:
        """
        Backward compatibility with existing to_json() calls.
        Uses by_alias=True so alias_generator=to_camel converts all fields to camelCase.
        Fields with validation_alias (not alias) will use the generated camelCase name.
        """
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="ignore",
        use_enum_values=True,
        validate_assignment=True,
        validate_default=False,
        revalidate_instances="never",
        from_attributes=True,
    )


class MTGJsonCardModel(MTGJsonModel):
    """
    Extended Base for all MTGJSON Card models with dynamic field exclusion.
    """

    _allow_if_falsey: ClassVar[set[str]] = {
        # Required fields that must always be present
        "uuid",
        "set_code",
        "text",
        "type",
        "layout",
        "frame_version",
        "language",
        # List fields that should be present even if empty
        "supertypes",
        "types",
        "subtypes",
        "booster_types",
        "finishes",
        "printings",
        "variations",
        "rulings",
        # Numeric/boolean fields that should be present even if 0/false
        "has_foil",
        "has_non_foil",
        "color_identity",
        "colors",
        "converted_mana_cost",
        "mana_value",
        "face_converted_mana_cost",
        "face_mana_value",
        # Other fields
        "foreign_data",
        "reverse_related",
    }

    _exclude_for_tokens: ClassVar[set[str]] = {
        "rulings",
        "rarity",
        "prices",
        "purchase_urls",
        "printings",
        "converted_mana_cost",
        "mana_value",
        "foreign_data",
        "legalities",
        "leadership_skills",
    }

    _exclude_for_cards: ClassVar[set[str]] = {
        "reverse_related",
        "ascii_name",  # Only in atomic cards
        "count",  # Only in deck lists
        "face_mana_value",  # Only for multi-face cards
        "prices",  # Excluded from card output
    }

    _atomic_keys: list[str] = [
        "ascii_name",
        "color_identity",
        "color_indicator",
        "colors",
        "converted_mana_cost",
        "count",
        "defense",
        "edhrec_rank",
        "edhrec_saltiness",
        "face_converted_mana_cost",
        "face_mana_value",
        "face_name",
        "foreign_data",
        "hand",
        "has_alternative_deck_limit",
        "identifiers",
        "is_funny",
        "is_reserved",
        "keywords",
        "layout",
        "leadership_skills",
        "legalities",
        "life",
        "loyalty",
        "mana_cost",
        "mana_value",
        "name",
        "power",
        "printings",
        "purchase_urls",
        "rulings",
        "scryfall_oracle_id",
        "side",
        "subtypes",
        "supertypes",
        "text",
        "toughness",
        "type",
        "types",
    ]

    uuid: str = Field(default="", exclude=False)
    is_token: bool = Field(default=False, exclude=True)

    def build_keys_to_skip(self) -> set[str]:
        """Dynamic field exclusion for cards."""
        if self.is_token:
            excluded_keys = self._exclude_for_tokens.copy()
        else:
            excluded_keys = self._exclude_for_cards.copy()

        # Exclude empty fields unless specifically allowed
        for field_name, field_value in self.__dict__.items():
            if not field_value and field_name not in self._allow_if_falsey:
                excluded_keys.add(field_name)

        return excluded_keys

    def to_json(self) -> dict[str, Any]:
        """
        Custom JSON serialization that filters out empty values
        :return: JSON object
        """
        skip_keys = self.build_keys_to_skip()
        return self.model_dump(
            by_alias=True, exclude=skip_keys, exclude_none=True, mode="json"
        )


class MTGJsonSetModel(MTGJsonModel):
    """
    Extended Base for all MTGJSON Set models with custom Windows-safe set code.
    """

    _BAD_FILE_NAMES: ClassVar[set[str]] = {"CON", "PRN", "AUX", "NUL", "COM1", "LPT1"}

    name: str = Field(default="")
    code: str = Field(default="")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def windows_set_code(self) -> str:
        """Set code safe for Windows filesystem.

        appends underscore to reserved names.
        computed property is automatically serialized and cached.
        """
        if self.code in self._BAD_FILE_NAMES:
            return self.code + "_"
        return self.code

    def get_windows_safe_set_code(self) -> str:
        """Handle Windows-incompatible file names.

        Deprecated: Use windows_safe_code property instead.
        Kept for backward compatibility.
        """
        return self.windows_set_code


class MTGJsonCompiledModel(MTGJsonModel):
    """MTGJSON Compiled Base model"""


class MtgjsonIdentifiersObject(MtgjsonBaseModel):
    """Data model for card identifiers across different platforms."""

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

    def is_foreign(self) -> list[str]:
        """If being used for MtgjsonForeignDataObject, return subset of ids"""
        result: list[str] = []
        if self.scryfall_id:
            result.append(self.scryfall_id)
        if self.multiverse_id:
            result.append(self.multiverse_id)
        if self.mtgjson_v4_id:
            result.append(self.mtgjson_v4_id)
        return result


class MtgjsonPurchaseUrlsObject(MtgjsonBaseModel):
    """Data model for purchase URLs from various retailers."""

    card_kingdom: str | None = None
    card_kingdom_etched: str | None = None
    card_kingdom_foil: str | None = None
    cardmarket: str | None = None
    tcgplayer: str | None = None
    tcgplayer_etched: str | None = None


class MtgjsonRelatedCardsObject(MtgjsonBaseModel):
    """Data model for related cards and card relationships."""

    reverse_related: list[str] = Field(default_factory=list)
    spellbook: list[str] = Field(default_factory=list)


class MtgjsonRulingsObject(MtgjsonBaseModel):
    """Data model for card rulings."""

    date: str
    text: str


class MtgjsonLeadershipSkillsObject(MtgjsonBaseModel):
    """Data model for card leadership skills in various formats."""

    brawl: bool = False
    commander: bool = False
    oathbreaker: bool = False


class MtgjsonLegalitiesObject(MtgjsonBaseModel):
    """Data model for card legality across different formats."""

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


class ForeignDataIdentifiersObject(MtgjsonBaseModel):
    """Data model for foreign data identifiers subset."""

    multiverse_id: str | None = None
    scryfall_id: str | None = None


class MtgjsonForeignDataObject(MtgjsonBaseModel):
    """Data model for card information in foreign languages."""

    face_name: str | None = None
    identifiers: ForeignDataIdentifiersObject = Field(
        default_factory=ForeignDataIdentifiersObject
    )
    language: str
    multiverse_id: int | None = None
    name: str
    text: str | None = None
    type: str | None = None
    uuid: UUID


class MtgjsonTranslationsObject(MtgjsonBaseModel):
    """Data model for set name translations across languages."""

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
    """Data model for Magic keywords and abilities."""

    ability_words: list[str] = Field(default_factory=list)
    keyword_abilities: list[str] = Field(default_factory=list)
    keyword_actions: list[str] = Field(default_factory=list)


class MtgjsonSourceProductsObject(MtgjsonBaseModel):
    """Data model for source products by finish type."""

    etched: list[str] = Field(default_factory=list)
    foil: list[str] = Field(default_factory=list)
    nonfoil: list[str] = Field(default_factory=list)


class MtgjsonTcgplayerSkusObject(MtgjsonBaseModel):
    """Data model for TCGplayer SKU information."""

    condition: str
    finish: str
    language: str
    printing: str
    product_id: str
    sku_id: str


class MtgjsonMetaObject(MtgjsonBaseModel):
    """Data model for metadata information."""

    date: str
    version: str


class MtgjsonPricePointsObject(MtgjsonBaseModel):
    """Data model for price points by finish type."""

    etched: dict[str, float] = Field(default_factory=dict)
    foil: dict[str, float] = Field(default_factory=dict)
    normal: dict[str, float] = Field(default_factory=dict)


class MtgjsonPriceListObject(MtgjsonBaseModel):
    """Data model for price lists with buylist and retail prices."""

    buylist: MtgjsonPricePointsObject | None = None
    currency: str
    retail: MtgjsonPricePointsObject | None = None


class MtgjsonPriceFormatsObject(MtgjsonBaseModel):
    """Data model for prices across different formats (MTGO, paper)."""

    mtgo: dict[str, MtgjsonPriceListObject] = Field(default_factory=dict)
    paper: dict[str, MtgjsonPriceListObject] = Field(default_factory=dict)


class MtgjsonBoosterPackObject(MtgjsonBaseModel):
    """Data model for booster pack contents and weight."""

    contents: dict[str, int] = Field(default_factory=dict)
    weight: float


class MtgjsonBoosterSheetObject(MtgjsonBaseModel):
    """Data model for booster sheet configuration."""

    allow_duplicates: bool | None = None
    balance_colors: bool | None = None
    cards: dict[str, int] = Field(default_factory=dict)
    foil: bool
    fixed: bool | None = None
    total_weight: float


class MtgjsonBoosterConfigObject(MtgjsonBaseModel):
    """Data model for booster configuration."""

    boosters: list[MtgjsonBoosterPackObject] = Field(default_factory=list)
    boosters_total_weight: float
    name: str | None = None
    sheets: dict[str, MtgjsonBoosterSheetObject] = Field(default_factory=dict)
    source_set_codes: list[str] = Field(default_factory=list)


# --- Sealed Products ---


class MtgjsonSealedProductCardObject(MtgjsonBaseModel):
    """Data model for cards in sealed products."""

    foil: bool | None = None
    name: str
    number: str
    set: str
    uuid: str


class MtgjsonSealedProductDeckObject(MtgjsonBaseModel):
    """Data model for decks in sealed products."""

    name: str
    set: str


class MtgjsonSealedProductOtherObject(MtgjsonBaseModel):
    """Data model for other items in sealed products."""

    name: str


class MtgjsonSealedProductPackObject(MtgjsonBaseModel):
    """Data model for booster packs in sealed products."""

    code: str
    set: str


class MtgjsonSealedProductSealedObject(MtgjsonBaseModel):
    """Data model for nested sealed products."""

    count: int
    name: str
    set: str
    uuid: str


class MtgjsonSealedProductContentsObject(MtgjsonBaseModel):
    """Data model for sealed product contents."""

    card: list[MtgjsonSealedProductCardObject] = Field(default_factory=list)
    deck: list[MtgjsonSealedProductDeckObject] = Field(default_factory=list)
    other: list[MtgjsonSealedProductOtherObject] = Field(default_factory=list)
    pack: list[MtgjsonSealedProductPackObject] = Field(default_factory=list)
    sealed: list[MtgjsonSealedProductSealedObject] = Field(default_factory=list)
    variable: list[dict[str, list[MtgjsonSealedProductContentsObject]]] = Field(
        default_factory=list
    )


class MtgjsonSealedProductObject(MtgjsonBaseModel):
    """Data model for sealed products."""

    card_count: int | None = None
    category: str | None = None
    contents: MtgjsonSealedProductContentsObject | None = None
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
    name: str
    product_size: int | None = None
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject
    )
    release_date: str | None = None
    subtype: str | None = None
    uuid: str


class MtgjsonCardTypeObject(MtgjsonBaseModel):
    """Data model for card type with supertypes and subtypes."""

    sub_types: list[str] = Field(default_factory=list)
    super_types: list[str] = Field(default_factory=list)


class MtgjsonCardTypesObject(MtgjsonBaseModel):
    """Data model for all card types in Magic."""

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
    """Data model for atomic card objects."""

    ascii_name: str | None = None
    attraction_lights: list[int] | None = None
    color_identity: list[str] = Field(default_factory=list)
    color_indicator: list[str] | None = None
    colors: list[str] = Field(default_factory=list)
    converted_mana_cost: float
    defense: str | None = None
    edhrec_rank: int | None = None
    edhrec_saltiness: float | None = None
    face_converted_mana_cost: float | None = None
    face_mana_value: float | None = None
    face_name: str | None = None
    first_printing: str | None = None
    foreign_data: list[MtgjsonForeignDataObject] = Field(default_factory=list)
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
    is_funny: bool | None = None
    is_game_changer: bool | None = None
    is_reserved: bool | None = None
    keywords: list[str] = Field(default_factory=list)
    layout: str
    leadership_skills: MtgjsonLeadershipSkillsObject | None = None
    legalities: MtgjsonLegalitiesObject = Field(default_factory=MtgjsonLegalitiesObject)
    life: str | None = None
    loyalty: str | None = None
    mana_cost: str | None = None
    mana_value: float
    name: str
    power: str | None = None
    printings: list[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject
    )
    related_cards: MtgjsonRelatedCardsObject = Field(
        default_factory=MtgjsonRelatedCardsObject
    )
    rulings: list[MtgjsonRulingsObject] = Field(default_factory=list)
    side: str | None = None
    subsets: list[str] = Field(default_factory=list)
    subtypes: list[str] = Field(default_factory=list)
    supertypes: list[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: list[str] = Field(default_factory=list)


class MtgjsonCardDeckObject(MtgjsonBaseModel):
    """Data model for card objects in decks."""

    artist: str | None = None
    artist_ids: list[str] = Field(default_factory=list)
    ascii_name: str | None = None
    attraction_lights: list[int] | None = None
    availability: list[str] = Field(default_factory=list)
    booster_types: list[str] = Field(default_factory=list)
    border_color: str
    card_parts: list[str] = Field(default_factory=list)
    color_identity: list[str] = Field(default_factory=list)
    color_indicator: list[str] | None = None
    colors: list[str] = Field(default_factory=list)
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
    finishes: list[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    foreign_data: list[MtgjsonForeignDataObject] = Field(default_factory=list)
    frame_effects: list[str] = Field(default_factory=list)
    frame_version: str
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    has_content_warning: bool | None = None
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
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
    keywords: list[str] = Field(default_factory=list)
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
    original_printings: list[str] = Field(default_factory=list)
    original_release_date: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: list[str] = Field(default_factory=list)
    power: str | None = None
    printings: list[str] = Field(default_factory=list)
    promo_types: list[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject
    )
    rarity: str
    related_cards: MtgjsonRelatedCardsObject | None = None
    rebalanced_printings: list[str] = Field(default_factory=list)
    rulings: list[MtgjsonRulingsObject] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    source_products: list[str] = Field(default_factory=list)
    subsets: list[str] = Field(default_factory=list)
    subtypes: list[str] = Field(default_factory=list)
    supertypes: list[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: list[str] = Field(default_factory=list)
    uuid: str
    variations: list[str] = Field(default_factory=list)
    watermark: str | None = None


class MtgjsonCardSetDeckObject(MtgjsonBaseModel):
    """Data model for card references in set decks."""

    count: int
    is_foil: bool | None = None
    uuid: str


class MtgjsonCardSetObject(MtgjsonBaseModel):
    """Data model for card objects in sets."""

    artist: str | None = None
    artist_ids: list[str] = Field(default_factory=list)
    ascii_name: str | None = None
    attraction_lights: list[int] | None = None
    availability: list[str] = Field(default_factory=list)
    booster_types: list[str] = Field(default_factory=list)
    border_color: str
    card_parts: list[str] = Field(default_factory=list)
    color_identity: list[str] = Field(default_factory=list)
    color_indicator: list[str] | None = None
    colors: list[str] = Field(default_factory=list)
    converted_mana_cost: float
    defense: str | None = None
    duel_deck: str | None = None
    edhrec_rank: int | None = None
    edhrec_saltiness: float | None = None
    face_converted_mana_cost: float | None = None
    face_flavor_name: str | None = None
    face_mana_value: float | None = None
    face_name: str | None = None
    finishes: list[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    foreign_data: list[MtgjsonForeignDataObject] = Field(default_factory=list)
    frame_effects: list[str] = Field(default_factory=list)
    frame_version: str
    hand: str | None = None
    has_alternative_deck_limit: bool | None = None
    has_content_warning: bool | None = None
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
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
    keywords: list[str] = Field(default_factory=list)
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
    original_printings: list[str] = Field(default_factory=list)
    original_release_date: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: list[str] = Field(default_factory=list)
    power: str | None = None
    printings: list[str] = Field(default_factory=list)
    promo_types: list[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject
    )
    rarity: str
    related_cards: MtgjsonRelatedCardsObject | None = None
    rebalanced_printings: list[str] = Field(default_factory=list)
    rulings: list[MtgjsonRulingsObject] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    source_products: MtgjsonSourceProductsObject | None = None
    subsets: list[str] = Field(default_factory=list)
    subtypes: list[str] = Field(default_factory=list)
    supertypes: list[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: list[str] = Field(default_factory=list)
    uuid: str
    variations: list[str] = Field(default_factory=list)
    watermark: str | None = None


class MtgjsonCardTokenObject(MtgjsonBaseModel):
    """Data model for token card objects."""

    artist: str | None = None
    artist_ids: list[str] = Field(default_factory=list)
    ascii_name: str | None = None
    availability: list[str] = Field(default_factory=list)
    booster_types: list[str] = Field(default_factory=list)
    border_color: str
    card_parts: list[str] = Field(default_factory=list)
    color_identity: list[str] = Field(default_factory=list)
    color_indicator: list[str] | None = None
    colors: list[str] = Field(default_factory=list)
    edhrec_saltiness: float | None = None
    face_name: str | None = None
    face_flavor_name: str | None = None
    finishes: list[str] = Field(default_factory=list)
    flavor_name: str | None = None
    flavor_text: str | None = None
    frame_effects: list[str] = Field(default_factory=list)
    frame_version: str
    has_foil: bool
    has_non_foil: bool
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
    is_full_art: bool | None = None
    is_funny: bool | None = None
    is_online_only: bool | None = None
    is_oversized: bool | None = None
    is_promo: bool | None = None
    is_reprint: bool | None = None
    is_textless: bool | None = None
    keywords: list[str] = Field(default_factory=list)
    language: str
    layout: str
    loyalty: str | None = None
    mana_cost: str | None = None
    name: str
    number: str
    orientation: str | None = None
    original_text: str | None = None
    original_type: str | None = None
    other_face_ids: list[str] = Field(default_factory=list)
    power: str | None = None
    promo_types: list[str] = Field(default_factory=list)
    related_cards: MtgjsonRelatedCardsObject | None = None
    reverse_related: list[str] = Field(default_factory=list)
    security_stamp: str | None = None
    set_code: str
    side: str | None = None
    signature: str | None = None
    subsets: list[str] = Field(default_factory=list)
    subtypes: list[str] = Field(default_factory=list)
    supertypes: list[str] = Field(default_factory=list)
    text: str | None = None
    toughness: str | None = None
    type: str
    types: list[str] = Field(default_factory=list)
    uuid: str
    watermark: str | None = None


# --- Decks ---


class MtgjsonDeckObject(MtgjsonBaseModel):
    """Data model for deck objects with all required fields."""

    code: str
    commander: list[MtgjsonCardDeckObject] = Field(default_factory=list)
    display_commander: list[str] = Field(default_factory=list)
    main_board: list[MtgjsonCardDeckObject] = Field(default_factory=list)
    name: str
    planes: list[str] = Field(default_factory=list)
    release_date: str | None = None
    schemes: list[str] = Field(default_factory=list)
    sealed_product_uuids: list[str] = Field(default_factory=list)
    side_board: list[MtgjsonCardDeckObject] = Field(default_factory=list)
    tokens: list[MtgjsonCardDeckObject] = Field(default_factory=list)
    type: str


class MtgjsonDeckListObject(MtgjsonBaseModel):
    """Data model for deck list metadata."""

    code: str
    file_name: str
    name: str
    release_date: str | None = None
    type: str


class MtgjsonDeckSetObject(MtgjsonBaseModel):
    """Data model for deck objects in sets with all 11 required fields."""

    code: str
    commander: list[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    display_commander: list[str] = Field(default_factory=list)
    main_board: list[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    name: str
    planes: list[str] = Field(default_factory=list)
    release_date: str | None = None
    schemes: list[str] = Field(default_factory=list)
    sealed_product_uuids: list[str] = Field(default_factory=list)
    side_board: list[MtgjsonCardSetDeckObject] = Field(default_factory=list)
    type: str


# --- Sets ---


class MtgjsonSetObject(MtgjsonBaseModel):
    """Data model for complete set objects."""

    base_set_size: int
    block: str | None = None
    booster: dict[str, MtgjsonBoosterConfigObject] = Field(default_factory=dict)
    cards: list[MtgjsonCardSetObject] = Field(default_factory=list)
    cardsphere_set_id: int | None = None
    code: str
    code_v3: str | None = None
    decks: list[MtgjsonDeckSetObject] = Field(default_factory=list)
    is_foreign_only: bool | None = None
    is_foil_only: bool
    is_non_foil_only: bool | None = None
    is_online_only: bool
    is_paper_only: bool | None = None
    is_partial_preview: bool | None = None
    keyrune_code: str
    languages: list[str] = Field(default_factory=list)
    mcm_id: int | None = None
    mcm_id_extras: int | None = None
    mcm_name: str | None = None
    mtgo_code: str | None = None
    name: str
    parent_code: str | None = None
    release_date: str
    sealed_product: list[MtgjsonSealedProductObject] = Field(default_factory=list)
    tcgplayer_group_id: int | None = None
    tokens: list[MtgjsonCardTokenObject] = Field(default_factory=list)
    token_set_code: str | None = None
    total_set_size: int
    translations: MtgjsonTranslationsObject = Field(
        default_factory=MtgjsonTranslationsObject
    )
    type: str


class MtgjsonSetListObject(MtgjsonBaseModel):
    """Data model for set list metadata."""

    base_set_size: int
    block: str | None = None
    cardsphere_set_id: int | None = None
    code: str
    code_v3: str | None = None
    decks: list[MtgjsonDeckSetObject] = Field(default_factory=list)
    is_foreign_only: bool | None = None
    is_foil_only: bool
    is_non_foil_only: bool | None = None
    is_online_only: bool
    is_paper_only: bool | None = None
    is_partial_preview: bool | None = None
    keyrune_code: str
    languages: list[str] = Field(default_factory=list)
    mcm_id: int | None = None
    mcm_id_extras: int | None = None
    mcm_name: str | None = None
    mtgo_code: str | None = None
    name: str
    parent_code: str | None = None
    release_date: str
    sealed_product: list[MtgjsonSealedProductObject] = Field(default_factory=list)
    tcgplayer_group_id: int | None = None
    total_set_size: int
    token_set_code: str | None = None
    translations: MtgjsonTranslationsObject = Field(
        default_factory=MtgjsonTranslationsObject
    )
    type: str


# Rebuild models to ensure all recursive references are resolved
MtgjsonSealedProductContentsObject.model_rebuild()
