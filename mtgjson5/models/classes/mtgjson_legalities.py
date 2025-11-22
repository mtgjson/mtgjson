"""
MTGJSON Singular Card.Legalities Object
"""

from ..mtgjson_base import MTGJsonModel


class MtgjsonLegalitiesObject(MTGJsonModel):
    """
    MTGJSON Singular Card.Legalities Object
    """

    alchemy: str = ""
    brawl: str = ""
    commander: str = ""
    duel: str = ""
    explorer: str = ""
    future: str = ""
    frontier: str = ""
    gladiator: str = ""
    historic: str = ""
    legacy: str = ""
    modern: str = ""
    oathbreaker: str = ""
    oldschool: str = ""
    pauper: str = ""
    paupercommander: str = ""
    penny: str = ""
    pioneer: str = ""
    predh: str = ""
    premodern: str = ""
    standard: str = ""
    standardbrawl: str = ""
    timeless: str = ""
    vintage: str = ""
