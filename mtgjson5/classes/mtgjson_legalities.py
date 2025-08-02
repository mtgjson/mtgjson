"""
MTGJSON Singular Card.Legalities Object
"""

from .json_object import JsonObject


class MtgjsonLegalitiesObject(JsonObject):
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
