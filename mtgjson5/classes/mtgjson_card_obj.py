"""
MTGJSON container for holding an individual card
"""
from typing import List, Dict, Any, Optional

from mtgjson5.classes.mtgjson_foreign_data_obj import MtgjsonForeignDataObject
from mtgjson5.classes.mtgjson_leadership_skills_obj import MtgjsonLeadershipSkillsObject
from mtgjson5.classes.mtgjson_legalities_obj import MtgjsonLegalitiesObject
from mtgjson5.classes.mtgjson_prices_obj import MtgjsonPricesObject
from mtgjson5.classes.mtgjson_purchase_urls_obj import MtgjsonPurchaseUrlsObject
from mtgjson5.classes.mtgjson_rulings_obj import MtgjsonRulingObject
from mtgjson5.globals import to_camel_case


class MtgjsonCardObject:
    """
    MTGJSON's container for a card
    """

    artist: Optional[str]
    border_color: str
    color_identity: List[str]
    color_indicator: List[str]
    colors: List[str]
    converted_mana_cost: float
    count: int
    duel_deck: str
    edhrec_rank: int
    face_converted_mana_cost: float
    flavor_text: str
    foreign_data: List[MtgjsonForeignDataObject]
    frame_effect: str  # DEPRECATED
    frame_effects: List[str]
    frame_version: str
    hand: str
    has_foil: bool
    has_no_deck_limit: bool  # DEPRECATED
    has_non_foil: bool
    is_alternative: bool
    is_arena: bool
    is_full_art: bool
    is_mtgo: bool
    is_online_only: bool
    is_oversized: bool
    is_paper: bool
    is_promo: bool
    is_reprint: bool
    is_reserved: bool
    is_starter: bool
    is_story_spotlight: bool
    is_textless: bool
    is_timeshifted: bool
    layout: Optional[str]
    leadership_skills: MtgjsonLeadershipSkillsObject
    legalities: MtgjsonLegalitiesObject
    life: str
    loyalty: str
    mana_cost: str
    mcm_id: int
    mcm_meta_id: int
    mtg_arena_id: int
    mtgo_foil_id: int
    mtgo_id: int
    mtgstocks_id: int
    multiverse_id: int
    name: str
    names: List[str]
    number: str
    original_text: str
    original_type: str
    power: str
    prices: MtgjsonPricesObject
    printings: List[str]
    purchase_urls: MtgjsonPurchaseUrlsObject
    rarity: str
    reverse_related: List[str]
    rulings: List[MtgjsonRulingObject]
    set_code: str
    scryfall_id: str
    scryfall_oracle_id: str
    scryfall_illustration_id: str
    side: str
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

    def __init__(self):

        # These values are tested against at some point
        # So we need a default value
        self.colors = []
        self.artist = None
        self.layout = None
        self.watermark = None
        self.names = []
        self.multiverse_id = 0

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
            and not callable(value)
            and key not in {"set_code"}  # CUSTOM LIST OF EXCEPTIONS
        }
