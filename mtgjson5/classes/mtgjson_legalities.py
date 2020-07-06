"""
MTGJSON Singular Card.Legalities Object
"""
from typing import Any, Dict

from ..utils import to_camel_case


class MtgjsonLegalitiesObject:
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
