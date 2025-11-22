"""MTGJSON Card Object model for individual MTG card data."""

import json
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import Field, PrivateAttr

from ... import constants
from ..mtgjson_base import MTGJsonCardModel
from .mtgjson_foreign_data import MtgjsonForeignDataObject
from .mtgjson_game_formats import MtgjsonGameFormatsObject
from .mtgjson_identifiers import MtgjsonIdentifiersObject
from .mtgjson_leadership_skills import MtgjsonLeadershipSkillsObject
from .mtgjson_legalities import MtgjsonLegalitiesObject
from .mtgjson_prices import MtgjsonPricesObject
from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject
from .mtgjson_related_cards import MtgjsonRelatedCardsObject
from .mtgjson_rulings import MtgjsonRulingObject


class MtgjsonCardObject(MTGJsonCardModel):
    """
    MTGJSON Singular Card Object
    """

    # Configure field exclusion rules (class variables)
    _allow_if_falsey: ClassVar[Set[str]] = {
        "supertypes",
        "types",
        "subtypes",
        "has_foil",
        "has_non_foil",
        "color_identity",
        "colors",
        "converted_mana_cost",
        "mana_value",
        "face_converted_mana_cost",
        "face_mana_value",
        "foreign_data",
        "reverse_related",
    }
    _exclude_for_tokens: ClassVar[Set[str]] = {
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
    _exclude_for_cards: ClassVar[Set[str]] = {"reverse_related"}

    # Atomic keys list for cards that don't change between printings
    _atomic_keys: List[str] = [
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

    # Required fields with defaults
    artist: str = ""
    border_color: str = ""
    colors: List[str] = Field(default_factory=list)
    converted_mana_cost: float = 0.0
    count: int = 1
    face_converted_mana_cost: float = 0.0
    face_mana_value: float = 0.0
    finishes: List[str] = Field(default_factory=list)
    foreign_data: List[MtgjsonForeignDataObject] = Field(default_factory=list)
    frame_effects: List[str] = Field(default_factory=list)
    frame_version: str = ""
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
    keywords: List[str] = Field(default_factory=list)
    language: str = ""
    layout: str = ""
    mana_cost: str = ""
    mana_value: float = 0.0
    name: str = ""
    number: str = "0"
    other_face_ids: List[str] = Field(default_factory=list)
    power: str = ""
    printings: List[str] = Field(default_factory=list)
    promo_types: List[str] = Field(default_factory=list)
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject
    )
    rarity: str = ""
    rebalanced_printings: List[str] = Field(default_factory=list)
    subtypes: List[str] = Field(default_factory=list)
    supertypes: List[str] = Field(default_factory=list)
    text: str = ""
    toughness: str = ""
    type: str = ""
    types: List[str] = Field(default_factory=list)
    uuid: str = ""
    variations: List[str] = Field(default_factory=list)

    # Fields that require complex types with defaults
    availability: MtgjsonGameFormatsObject = Field(
        default_factory=MtgjsonGameFormatsObject
    )
    booster_types: List[str] = Field(default_factory=list)
    card_parts: List[str] = Field(default_factory=list)
    color_identity: List[str] = Field(default_factory=list)
    legalities: MtgjsonLegalitiesObject = Field(default_factory=MtgjsonLegalitiesObject)
    original_printings: List[str] = Field(default_factory=list)
    prices: MtgjsonPricesObject = Field(default_factory=MtgjsonPricesObject)

    # Outside entities, not published
    set_code: str = ""
    is_token: bool = Field(default=False, exclude=True)
    raw_purchase_urls: Dict[str, str] = Field(default_factory=dict, exclude=True)

    # Optional fields
    artist_ids: Optional[List[str]] = None
    ascii_name: Optional[str] = None
    attraction_lights: Optional[List[str]] = None
    color_indicator: Optional[List[str]] = None
    defense: Optional[str] = None
    duel_deck: Optional[str] = None
    edhrec_rank: Optional[int] = None
    edhrec_saltiness: Optional[float] = None
    face_flavor_name: Optional[str] = None
    face_name: Optional[str] = None
    first_printing: Optional[str] = None
    flavor_name: Optional[str] = None
    flavor_text: Optional[str] = None
    hand: Optional[str] = None
    has_alternative_deck_limit: Optional[bool] = None
    has_content_warning: Optional[bool] = None
    has_foil: Optional[bool] = None
    has_non_foil: Optional[bool] = None
    is_alternative: Optional[bool] = None
    is_foil: Optional[bool] = None
    is_full_art: Optional[bool] = None
    is_funny: Optional[bool] = None
    is_game_changer: Optional[bool] = None
    is_online_only: Optional[bool] = None
    is_oversized: Optional[bool] = None
    is_promo: Optional[bool] = None
    is_rebalanced: Optional[bool] = None
    is_reprint: Optional[bool] = None
    is_reserved: Optional[bool] = None
    is_starter: Optional[bool] = None
    is_story_spotlight: Optional[bool] = None
    is_textless: Optional[bool] = None
    is_timeshifted: Optional[bool] = None
    leadership_skills: Optional[MtgjsonLeadershipSkillsObject] = None
    life: Optional[str] = None
    loyalty: Optional[str] = None
    orientation: Optional[str] = None
    original_release_date: Optional[str] = None
    original_text: Optional[str] = None
    original_type: Optional[str] = None
    related_cards: Optional[MtgjsonRelatedCardsObject] = None
    reverse_related: Optional[List[str]] = None
    rulings: Optional[List[MtgjsonRulingObject]] = None
    security_stamp: Optional[str] = None
    side: Optional[str] = None
    signature: Optional[str] = None
    source_products: Optional[Dict[str, List[str]]] = None
    subsets: Optional[List[str]] = None
    watermark: Optional[str] = None
    printed_name: Optional[str] = None
    printed_type: Optional[str] = None
    printed_text: Optional[str] = None
    face_printed_name: Optional[str] = None
    is_etched: Optional[bool] = None

    # Private fields (excluded from serialization)
    _names: Optional[List[str]] = PrivateAttr(default=None)
    _illustration_ids: List[str] = PrivateAttr(default_factory=list)
    _watermark_resource: Dict[str, List[Any]] = PrivateAttr(default_factory=dict)

    def __eq__(self, other: Any) -> bool:
        """
        Determine if two MTGJSON Card Objects are equal
        First comparison: Card Number
        Second comparison: Side Letter
        :param other: Other card
        :return: Same object or not
        """
        if not isinstance(other, MtgjsonCardObject):
            return False
        return bool(
            self.number == other.number and (self.side or "") == (other.side or "")
        )

    def __lt__(self, other: Any) -> bool:
        """
        Less than operation
        First comparison: Card Number
        Second comparison: Side Letter
        :param other: Other card
        :return: Less than or not
        """
        if not isinstance(other, MtgjsonCardObject):
            return NotImplemented

        self_side = self.side or ""
        other_side = other.side or ""

        if self.number == other.number:
            return self_side < other_side

        self_number_clean = "".join(x for x in self.number if x.isdigit()) or "100000"
        self_number_clean_int = int(self_number_clean)

        other_number_clean = "".join(x for x in other.number if x.isdigit()) or "100000"
        other_number_clean_int = int(other_number_clean)

        if self.number == self_number_clean and other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                if len(self_number_clean) != len(other_number_clean):
                    return len(self_number_clean) < len(other_number_clean)
                return self_side < other_side
            return self_number_clean_int < other_number_clean_int

        if self.number == self_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return True
            return self_number_clean_int < other_number_clean_int

        if other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return False
            return self_number_clean_int < other_number_clean_int

        if self_number_clean == other_number_clean:
            if not self_side and not other_side:
                return bool(self.number < other.number)
            return self_side < other_side

        if self_number_clean_int == other_number_clean_int:
            if len(self_number_clean) != len(other_number_clean):
                return len(self_number_clean) < len(other_number_clean)
            return self_side < other_side

        return self_number_clean_int < other_number_clean_int

    def set_illustration_ids(self, illustration_ids: List[str]) -> None:
        """
        Set internal illustration IDs for this card to
        better identify what side we're working on,
        especially for Art and Token cards
        :param illustration_ids: Illustration IDs of the card faces
        """
        self._illustration_ids = illustration_ids

    def get_illustration_ids(self) -> List[str]:
        """
        Get the internal illustration IDs roster for this card
        to better identify the sides for Art and Token cards
        :return: Illustration IDs
        """
        return self._illustration_ids

    def get_names(self) -> List[str]:
        """
        Get internal names array for this card
        :return: Names array or empty list
        """
        return self._names or []

    def set_names(self, names: Optional[List[str]]) -> None:
        """
        Set internal names array for this card
        :param names: Names list (optional)
        """
        self._names = list(map(str.strip, names)) if names else None

    def append_names(self, name: str) -> None:
        """
        Append to internal names array for this card
        :param name: Name to append
        """
        if self._names:
            self._names.append(name)
        else:
            self.set_names([name])

    def set_watermark(self, watermark: Optional[str]) -> None:
        """
        Watermarks sometimes aren't specific enough, so we
        must manually update them. This only applies if the
        watermark is "set" and then we will append the actual
        set code to the watermark.
        :param watermark: Current watermark
        """
        if not watermark:
            return

        if not self._watermark_resource:
            with constants.RESOURCE_PATH.joinpath("set_code_watermarks.json").open(
                encoding="utf-8"
            ) as f:
                self._watermark_resource = json.load(f)

        if watermark == "set":
            for card in self._watermark_resource.get(self.set_code.upper(), []):
                if self.name in card["name"].split(" // "):
                    watermark = str(card["watermark"])
                    break

        self.watermark = watermark

    def get_atomic_keys(self) -> List[str]:
        """
        Get attributes of a card that don't change
        from printing to printing
        :return: Keys that are atomic
        """
        return self._atomic_keys

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return: What keys to skip over
        """
        if self.is_token:
            excluded_keys = self._exclude_for_tokens.copy()
        else:
            excluded_keys = self._exclude_for_cards.copy()

        excluded_keys = excluded_keys.union({"is_token", "raw_purchase_urls"})

        for key, value in self.__dict__.items():
            if not value:
                if key not in self._allow_if_falsey:
                    excluded_keys.add(key)

        return excluded_keys
