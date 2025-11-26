"""MTGJSON Purchase URLs Object model for vendor purchase links."""

from typing import Set

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonPurchaseUrlsObject(MTGJsonModel):
    """
    The Purchase Urls Data Model describes the properties of links to purchase a product from a marketplace.
    """

    card_kingdom: str | None = Field(
        default=None, description="The URL to purchase a product on Card Kingdom."
    )
    card_kingdom_etched: str | None = Field(
        default=None,
        description="The URL to purchase an etched product on Card Kingdom.",
    )
    card_kingdom_foil: str | None = Field(
        default=None, description="The URL to purchase a foil product on Card Kingdom."
    )
    cardmarket: str | None = Field(
        default=None, description="The URL to purchase a product on Cardmarket."
    )
    tcgplayer: str | None = Field(
        default=None, description="The URL to purchase a product on TCGplayer."
    )
    tcgplayer_etched: str | None = Field(
        default=None, description="The URL to purchase an etched product on TCGplayer."
    )

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
