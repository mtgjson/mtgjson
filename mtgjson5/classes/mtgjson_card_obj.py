"""
MTGJSON container for holding an individual card
"""
from typing import Any, Dict, List, Optional, Set

from ..classes.mtgjson_foreign_data_obj import MtgjsonForeignDataObject
from ..classes.mtgjson_leadership_skills_obj import MtgjsonLeadershipSkillsObject
from ..classes.mtgjson_legalities_obj import MtgjsonLegalitiesObject
from ..classes.mtgjson_prices_obj import MtgjsonPricesObject
from ..classes.mtgjson_purchase_urls_obj import MtgjsonPurchaseUrlsObject
from ..classes.mtgjson_rulings_obj import MtgjsonRulingObject
from ..utils import to_camel_case


class MtgjsonCardObject:
    """
    MTGJSON's container for a card
    """

    artist: str
    border_color: str
    color_identity: List[str]
    color_indicator: Optional[List[str]]
    colors: List[str]
    converted_mana_cost: float
    count: int
    duel_deck: Optional[str]
    edhrec_rank: Optional[int]
    face_converted_mana_cost: float
    flavor_text: Optional[str]
    foreign_data: List[MtgjsonForeignDataObject]
    frame_effect: str  # DEPRECATED
    frame_effects: List[str]
    frame_version: str
    hand: Optional[str]
    has_foil: Optional[bool]
    has_no_deck_limit: Optional[bool]  # DEPRECATED
    has_non_foil: Optional[bool]
    is_alternative: Optional[bool]
    is_arena: Optional[bool]
    is_buy_a_box: Optional[bool]
    is_date_stamped: Optional[bool]
    is_full_art: Optional[bool]
    is_mtgo: Optional[bool]
    is_online_only: Optional[bool]
    is_oversized: Optional[bool]
    is_paper: Optional[bool]
    is_promo: Optional[bool]
    is_reprint: Optional[bool]
    is_reserved: Optional[bool]
    is_starter: Optional[bool]
    is_story_spotlight: Optional[bool]
    is_textless: Optional[bool]
    is_timeshifted: Optional[bool]
    layout: str
    leadership_skills: Optional[MtgjsonLeadershipSkillsObject]
    legalities: MtgjsonLegalitiesObject
    life: Optional[str]
    loyalty: Optional[str]
    mana_cost: str
    mcm_id: int
    mcm_meta_id: int
    mtg_arena_id: Optional[int]
    mtgo_foil_id: Optional[int]
    mtgo_id: Optional[int]
    mtgstocks_id: int
    multiverse_id: int
    name: str
    names: Optional[List[str]]
    number: str
    original_text: Optional[str]
    original_type: Optional[str]
    other_face_ids: List[str]
    power: str
    prices: MtgjsonPricesObject
    printings: List[str]
    purchase_urls: MtgjsonPurchaseUrlsObject
    rarity: str
    reverse_related: Optional[List[str]]
    rulings: List[MtgjsonRulingObject]
    set_code: str
    scryfall_id: str
    scryfall_oracle_id: str
    scryfall_illustration_id: Optional[str]
    side: Optional[str]
    subtypes: List[str]
    supertypes: List[str]
    tcgplayer_product_id: int
    text: str
    toughness: str
    type: str
    types: List[str]
    uuid: str
    variations: List[str]
    watermark: Optional[str]

    is_token: bool

    def __init__(self, is_token: bool = False) -> None:
        # These values are tested against at some point
        # So we need a default value
        self.colors = []
        self.artist = ""
        self.layout = ""
        self.watermark = None
        self.names = []
        self.multiverse_id = 0
        self.purchase_urls = MtgjsonPurchaseUrlsObject()
        self.prices = MtgjsonPricesObject("")
        self.is_token = is_token

    def __eq__(self, other: Any) -> bool:
        """
        Determine if two card objects are equal
        :param other: Other card
        :return: Same object or not
        """
        return bool(self.number == other.number)

    def __lt__(self, other: Any) -> bool:
        """
        Determine if this card object is less than another
        :param other: Other card
        :return: Less than or not
        """
        try:
            return int(self.number) < int(other.number)
        except ValueError:
            return bool(self.number < other.number)

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return What keys to skip over
        """
        allow_if_empty = {
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

        remove_for_tokens = {
            "rulings",
            "rarity",
            "prices",
            "purchase_urls",
            "printings",
            "converted_mana_cost",
            "foreign_data",
            "legalities",
            "leadership_skills",
            "names",
        }

        remove_for_cards = {"reverse_related"}

        excluded_keys: Set[str] = set()

        if self.is_token:
            excluded_keys.update(remove_for_tokens)
        else:
            excluded_keys.update(remove_for_cards)

        for key, value in self.__dict__.items():
            if not value:
                if key not in allow_if_empty:
                    excluded_keys.add(key)

        return excluded_keys

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys = self.build_keys_to_skip().union({"set_code", "is_token"})

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value) and key not in skip_keys
        }
