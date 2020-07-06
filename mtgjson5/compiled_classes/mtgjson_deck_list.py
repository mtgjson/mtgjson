"""
MTGJSON DeckList Object
"""
from typing import List

from ..classes import MtgjsonDeckHeaderObject


class MtgjsonDeckListObject:
    """
    MTGJSON DeckList Object
    """

    decks: List[MtgjsonDeckHeaderObject]

    def __init__(self, deck_headers: List[MtgjsonDeckHeaderObject]) -> None:
        """
        Initializer to build up the object
        """
        self.decks = deck_headers

    def to_json(self) -> List[MtgjsonDeckHeaderObject]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.decks
