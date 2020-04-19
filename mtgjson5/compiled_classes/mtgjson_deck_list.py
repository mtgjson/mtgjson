"""
MTGJSON DeckList Object
"""
from typing import Any, Dict, List

from ..classes import MtgjsonDeckHeaderObject
from ..utils import to_camel_case


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

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
