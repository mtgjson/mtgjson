"""
MTGJSON Singular Deck Header Object
"""
from typing import Any, Dict

from ..classes.mtgjson_deck import MtgjsonDeckObject
from ..utils import to_camel_case


class MtgjsonDeckHeaderObject:
    """
    MTGJSON Singular Deck Header Object
    """

    code: str
    file_name: str
    name: str
    release_date: str
    type: str

    def __init__(self, output_deck: MtgjsonDeckObject) -> None:
        """
        Initialize the header given a deck
        """
        self.code = output_deck.code
        self.file_name = output_deck.file_name
        self.name = output_deck.name
        self.release_date = output_deck.release_date
        self.type = output_deck.type

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
