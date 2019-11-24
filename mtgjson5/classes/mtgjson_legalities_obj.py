"""
MTGJSON Legalities container
"""


class MtgjsonLegalitiesObject:
    """
    Legalities container for cards
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

    def __init__(self):
        pass
