"""
MTGJSON Singular Card.PurchaseURLs Object
"""
from typing import Any, Dict, Set

from ..utils import to_camel_case


class MtgjsonPurchaseUrlsObject:
    """
    MTGJSON Singular Card.PurchaseURLs Object
    """

    card_kingdom: str
    card_kingdom_foil: str
    cardmarket: str
    tcgplayer: str

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return What keys to skip over
        """
        excluded_keys = set()

        for _, value in self.__dict__.items():
            if not value:
                excluded_keys.add(value)

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
