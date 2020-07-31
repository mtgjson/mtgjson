"""
MTGJSON Singular Card.Identifiers Object
"""
from typing import Dict, List, Optional

from mtgjson5.utils import to_camel_case


class MtgjsonIdentifiersObject:
    """
    MTGJSON Singular Card.Identifiers Object
    """

    card_kingdom_foil_id: Optional[str]
    card_kingdom_id: Optional[str]
    mcm_id: Optional[str]
    mcm_meta_id: Optional[str]
    mtg_arena_id: Optional[str]
    mtgo_foil_id: Optional[str]
    mtgo_id: Optional[str]
    multiverse_id: Optional[str]
    scryfall_id: Optional[str]
    scryfall_illustration_id: Optional[str]
    scryfall_oracle_id: Optional[str]
    tcgplayer_product_id: Optional[str]
    mtgjson_v4_id: Optional[str]
    tcgplayer_sku_ids: List[Dict[str, str]]

    def __init__(self) -> None:
        """
        Empty initializer
        """

        self.multiverse_id = ""
        self.tcgplayer_product_id = None

    def to_json(self) -> Dict[str, str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and value
        }
