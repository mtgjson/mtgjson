"""
MTGJSON Singular Card Object
"""
import json
from typing import Any, Dict, List, Optional, Set

from ..classes.mtgjson_foreign_data import MtgjsonForeignDataObject
from ..classes.mtgjson_game_formats import MtgjsonGameFormatsObject
from ..classes.mtgjson_identifiers import MtgjsonIdentifiersObject
from ..classes.mtgjson_leadership_skills import MtgjsonLeadershipSkillsObject
from ..classes.mtgjson_legalities import MtgjsonLegalitiesObject
from ..classes.mtgjson_prices import MtgjsonPricesObject
from ..classes.mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject
from ..classes.mtgjson_rulings import MtgjsonRulingObject
from ..consts import RESOURCE_PATH
from ..utils import to_camel_case


class MtgjsonCardObject:
    """
    MTGJSON Singular Card Object
    """

    artist: str
    ascii_name: Optional[str]
    availability: MtgjsonGameFormatsObject
    border_color: str
    color_identity: List[str]
    color_indicator: Optional[List[str]]
    colors: List[str]
    converted_mana_cost: float
    count: int
    duel_deck: Optional[str]
    edhrec_rank: Optional[int]
    face_converted_mana_cost: float
    face_name: Optional[str]
    flavor_name: Optional[str]
    flavor_text: Optional[str]
    foreign_data: List[MtgjsonForeignDataObject]
    frame_effects: List[str]
    frame_version: str
    hand: Optional[str]
    has_alternative_deck_limit: Optional[bool]
    has_content_warning: Optional[bool]
    has_foil: Optional[bool]
    has_non_foil: Optional[bool]
    identifiers: MtgjsonIdentifiersObject
    is_alternative: Optional[bool]
    is_full_art: Optional[bool]
    is_online_only: Optional[bool]
    is_oversized: Optional[bool]
    is_promo: Optional[bool]
    is_reprint: Optional[bool]
    is_reserved: Optional[bool]
    is_starter: Optional[bool]
    is_story_spotlight: Optional[bool]
    is_textless: Optional[bool]
    is_timeshifted: Optional[bool]
    keywords: List[str]
    layout: str
    leadership_skills: Optional[MtgjsonLeadershipSkillsObject]
    legalities: MtgjsonLegalitiesObject
    life: Optional[str]
    loyalty: Optional[str]
    mana_cost: str
    name: str
    number: str
    original_release_date: Optional[str]
    original_text: Optional[str]
    original_type: Optional[str]
    other_face_ids: List[str]
    power: str
    prices: MtgjsonPricesObject
    printings: List[str]
    promo_types: List[str]
    purchase_urls: MtgjsonPurchaseUrlsObject
    rarity: str
    reverse_related: Optional[List[str]]
    rulings: List[MtgjsonRulingObject]
    side: Optional[str]
    subtypes: List[str]
    supertypes: List[str]
    text: str
    toughness: str
    type: str
    types: List[str]
    uuid: str
    variations: List[str]
    watermark: Optional[str]

    # Outside entities, not published
    set_code: str
    is_token: bool
    raw_purchase_urls: Dict[str, str]
    __names: Optional[List[str]]
    __illustration_ids: List[str]
    __watermark_resource: Dict[str, List[Any]]

    __allow_if_falsey = {
        "supertypes",
        "types",
        "subtypes",
        "has_foil",
        "has_non_foil",
        "color_identity",
        "colors",
        "rulings",
        "converted_mana_cost",
        "face_converted_mana_cost",
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
        "edhrec_rank",
        "face_converted_mana_cost",
        "face_name",
        "foreign_data",
        "hand",
        "has_alternative_deck_limit",
        "identifiers",
        "is_reserved",
        "layout",
        "leadership_skills",
        "legalities",
        "life",
        "loyalty",
        "mana_cost",
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
        :param other: Other card
        :return: Same object or not
        """
        return bool(self.number == other.number)

    def __lt__(self, other: Any) -> bool:
        """
        Determine if this card object is "less than" another
        :param other: Other card
        :return: Less than or not
        """
        try:
            return int(self.number) < int(other.number)
        except ValueError:
            return bool(self.number < other.number)

    def set_illustration_ids(self, illustration_ids: List[str]) -> None:
        """
        Set internal illustration IDs for this card to
        better identify what side we're working on,
        especially for Art and Token cards
        :param illustration_ids: Illustration IDs of the card faces
        """
        self.__illustration_ids = illustration_ids

    def get_illustration_ids(self) -> List[str]:
        """
        Get the internal illustration IDs roster for this card
        to better identify the sides for Art and Token cards
        """
        return self.__illustration_ids

    def get_names(self) -> List[str]:
        """
        Get internal names array for this card
        :return Names array or None
        """
        return self.__names or []

    def set_names(self, names: Optional[List[str]]) -> None:
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

    def set_watermark(self, watermark: Optional[str]) -> None:
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
            with RESOURCE_PATH.joinpath("set_code_watermarks.json").open(
                encoding="utf-8"
            ) as f:
                self.__watermark_resource = json.load(f)

        if watermark == "set":
            for card in self.__watermark_resource.get(self.set_code.upper(), []):
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
        return self.__atomic_keys

    def build_keys_to_skip(self) -> Set[str]:
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

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        skip_keys = self.build_keys_to_skip()

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and key not in skip_keys
        }
