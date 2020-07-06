"""
MTGJSON Singular Card.GameFormats Object
"""
from typing import List

from mtgjson5.utils import to_camel_case


class MtgjsonGameFormatsObject:
    """
    MTGJSON Singular Card.GameFormats Object
    """

    paper: bool
    mtgo: bool
    arena: bool
    shandalar: bool
    dreamcast: bool

    def __init__(self) -> None:
        """
        Empty initializer
        """

    def to_json(self) -> List[str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return [
            to_camel_case(key)
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and value
        ]
