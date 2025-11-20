"""
MTGJSON Singular Card.Legalities Object
"""

from ..mtgjson_base import MTGJsonModel


class MtgjsonLegalitiesObject(MTGJsonModel):
    """
    MTGJSON Singular Card.Legalities Object
    """

    brawl: str
    commander: str
    duel: str
    future: str
    frontier: str
    legacy: str
    modern: str
    pauper: str
    penny: str
    pioneer: str
    standard: str
    vintage: str
