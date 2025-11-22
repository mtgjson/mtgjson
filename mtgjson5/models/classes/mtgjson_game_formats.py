"""MTGJSON Game Formats Object model for card availability by platform."""

from typing import List

from ..mtgjson_base import MTGJsonModel


class MtgjsonGameFormatsObject(MTGJsonModel):
    """
    MTGJSON Singular Card.GameFormats Object
    """

    paper: bool = False
    mtgo: bool = False
    arena: bool = False
    shandalar: bool = False
    dreamcast: bool = False

    def to_json(self) -> List[str]:
        """
        Custom JSON serialization that returns list of format names where value is True
        :return: List of available format names
        """
        parent = super().to_json()
        return [key for key, value in parent.items() if value]
