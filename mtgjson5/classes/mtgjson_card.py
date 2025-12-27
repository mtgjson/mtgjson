"""
MTGJSON Singular Card Object
"""

import json
from collections.abc import Iterable
from typing import Any

from .. import constants
from ..classes.mtgjson_foreign_data import MtgjsonForeignDataObject
from ..classes.mtgjson_game_formats import MtgjsonGameFormatsObject
from ..classes.mtgjson_identifiers import MtgjsonIdentifiersObject
from ..classes.mtgjson_leadership_skills import MtgjsonLeadershipSkillsObject
from ..classes.mtgjson_legalities import MtgjsonLegalitiesObject
from ..classes.mtgjson_prices import MtgjsonPricesObject
from ..classes.mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject
from ..classes.mtgjson_related_cards import MtgjsonRelatedCardsObject
from ..classes.mtgjson_rulings import MtgjsonRulingObject
from .json_object import JsonObject


class MtgjsonCardObject(JsonObject):
    """
    MTGJSON Singular Card Object
    """

    artist: str
    artist_ids: list[str] | None
    ascii_name: str | None
    attraction_lights: list[str] | None
    availability: MtgjsonGameFormatsObject
    booster_types: list[str]
    border_color: str
    card_parts: list[str]
    color_identity: list[str]
    color_indicator: list[str] | None
    colors: list[str]
    converted_mana_cost: float
    count: int
    defense: str | None
    duel_deck: str | None
    edhrec_rank: int | None
    edhrec_saltiness: float | None
    face_converted_mana_cost: float
    face_flavor_name: str | None
    face_mana_value: float
    face_name: str | None
    face_printed_name: str | None
    finishes: list[str]
    first_printing: str | None
    flavor_name: str | None
    flavor_text: str | None
    foreign_data: list[MtgjsonForeignDataObject]
    frame_effects: list[str]
    frame_version: str
    hand: str | None
    has_alternative_deck_limit: bool | None
    has_content_warning: bool | None
    has_foil: bool | None  # Deprecated - Remove in 5.3.0
    has_non_foil: bool | None  # Deprecated - Remove in 5.3.0
    identifiers: MtgjsonIdentifiersObject
    is_alternative: bool | None
    is_foil: bool | None
    is_etched: bool | None
    is_full_art: bool | None
    is_funny: bool | None
    is_game_changer: bool | None
    is_online_only: bool | None
    is_oversized: bool | None
    is_promo: bool | None
    is_rebalanced: bool | None
    is_reprint: bool | None
    is_reserved: bool | None
    is_starter: bool | None  # Deprecated - Remove in 5.3.0
    is_story_spotlight: bool | None
    is_textless: bool | None
    is_timeshifted: bool | None
    keywords: list[str]
    language: str
    layout: str
    leadership_skills: MtgjsonLeadershipSkillsObject | None
    legalities: MtgjsonLegalitiesObject
    life: str | None
    loyalty: str | None
    mana_cost: str
    mana_value: float
    name: str
    number: str
    orientation: str | None
    original_printings: list[str]
    original_release_date: str | None
    original_text: str | None
    original_type: str | None
    other_face_ids: list[str]
    power: str
    prices: MtgjsonPricesObject
    printed_name: str | None
    printed_type: str | None
    printed_text: str | None
    printings: list[str]
    promo_types: list[str]
    purchase_urls: MtgjsonPurchaseUrlsObject
    rarity: str
    rebalanced_printings: list[str]
    related_cards: MtgjsonRelatedCardsObject | None
    reverse_related: list[str] | None
    rulings: list[MtgjsonRulingObject] | None
    security_stamp: str | None
    side: str | None
    signature: str | None
    source_products: dict[str, list[str]] | None
    subsets: list[str] | None
    subtypes: list[str]
    supertypes: list[str]
    text: str
    toughness: str
    type: str
    types: list[str]
    uuid: str
    variations: list[str]
    watermark: str | None

    # Outside entities, not published
    set_code: str
    is_token: bool
    raw_purchase_urls: dict[str, str]
    __names: list[str] | None
    __illustration_ids: list[str]
    __watermark_resource: dict[str, list[Any]]

    __allow_if_falsey = {
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

    __remove_for_tokens = {
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

    __remove_for_cards = {"reverse_related"}

    __atomic_keys = [
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

    def __init__(self, is_token: bool = False) -> None:
        """
        Initializer for MTGJSON Singular Card Object
        """
        self.is_token = is_token
        self.colors = []
        self.artist = ""
        self.artist_ids = None
        self.layout = ""
        self.watermark = None
        self.__watermark_resource = {}
        self.__names = []
        self.__illustration_ids = []
        self.purchase_urls = MtgjsonPurchaseUrlsObject()
        self.side = None
        self.face_name = None
        self.raw_purchase_urls = {}
        self.identifiers = MtgjsonIdentifiersObject()

    def __eq__(self, other: Any) -> bool:
        """
        Determine if two MTGJSON Card Objects are equal
        First comparison: Card Number
        Second comparison: Side Letter
        :param other: Other card
        :return: Same object or not
        """
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

    def set_illustration_ids(self, illustration_ids: list[str]) -> None:
        """
        Set internal illustration IDs for this card to
        better identify what side we're working on,
        especially for Art and Token cards
        :param illustration_ids: Illustration IDs of the card faces
        """
        self.__illustration_ids = illustration_ids

    def get_illustration_ids(self) -> list[str]:
        """
        Get the internal illustration IDs roster for this card
        to better identify the sides for Art and Token cards
        """
        return self.__illustration_ids

    def get_names(self) -> list[str]:
        """
        Get internal names array for this card
        :return Names array or None
        """
        return self.__names or []

    def set_names(self, names: list[str] | None) -> None:
        """
        Set internal names array for this card
        :param names: Names list (optional)
        """
        self.__names = list(map(str.strip, names)) if names else None

    def append_names(self, name: str) -> None:
        """
        Append to internal names array for this card
        :param name: Name to append
        """
        if self.__names:
            self.__names.append(name)
        else:
            self.set_names([name])

    def set_watermark(self, watermark: str | None) -> None:
        """
        Watermarks sometimes aren't specific enough, so we
        must manually update them. This only applies if the
        watermark is "set" and then we will append the actual
        set code to the watermark.
        :param watermark: Current watermark
        :return optional value
        """
        if not watermark:
            return

        if not self.__watermark_resource:
            with constants.RESOURCE_PATH.joinpath("set_code_watermarks.json").open(
                encoding="utf-8"
            ) as f:
                self.__watermark_resource = json.load(f)

        if watermark == "set":
            for card in self.__watermark_resource.get(self.set_code.upper(), []):
                if self.name in card["name"].split(" // "):
                    watermark = str(card["watermark"])
                    break

        self.watermark = watermark

    def get_atomic_keys(self) -> list[str]:
        """
        Get attributes of a card that don't change
        from printing to printing
        :return: Keys that are atomic
        """
        return self.__atomic_keys

    def build_keys_to_skip(self) -> Iterable[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return What keys to skip over
        """
        if self.is_token:
            excluded_keys = self.__remove_for_tokens.copy()
        else:
            excluded_keys = self.__remove_for_cards.copy()

        excluded_keys = excluded_keys.union({"is_token", "raw_purchase_urls"})

        for key, value in self.__dict__.items():
            if not value:
                if key not in self.__allow_if_falsey:
                    excluded_keys.add(key)

        return excluded_keys
