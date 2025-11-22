"""MTGJSON Purchase URLs Object model for vendor purchase links."""

from typing import Set

from ..mtgjson_base import MTGJsonModel


class MtgjsonPurchaseUrlsObject(MTGJsonModel):
    """
    MTGJSON Singular Card.PurchaseURLs Object
    """

    card_kingdom: str = ""
    card_kingdom_etched: str = ""
    card_kingdom_foil: str = ""
    cardmarket: str = ""
    tcgplayer: str = ""
    tcgplayer_etched: str = ""

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return: Keys to skip over
        """
        excluded_keys: Set[str] = set()

        for key, value in self.__dict__.items():
            if not value:
                excluded_keys.add(key)

        return excluded_keys
