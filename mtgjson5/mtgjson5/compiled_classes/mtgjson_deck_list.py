"""
MTGJSON DeckList container
"""
from typing import Any, Dict, List

from ..classes import MtgjsonDeckHeaderObject, MtgjsonMetaObject
from ..utils import to_camel_case


class MtgjsonDeckListObject:
    """
    DeckList container
    """

    decks: List[MtgjsonDeckHeaderObject]
    meta: MtgjsonMetaObject

    def __init__(self, deck_headers: List[MtgjsonDeckHeaderObject]) -> None:
        self.decks = deck_headers
        self.meta = MtgjsonMetaObject()

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
