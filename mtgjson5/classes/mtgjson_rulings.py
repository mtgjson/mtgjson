"""
MTGJSON Singular Card.Rulings Object
"""
from typing import Any, Dict

from ..utils import to_camel_case


class MtgjsonRulingObject:
    """
    MTGJSON Singular Card.Rulings Object
    """

    date: str
    text: str

    def __init__(self, date: str, text: str) -> None:
        """
        Set the ruling date and text
        """
        self.date = date
        self.text = text

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
