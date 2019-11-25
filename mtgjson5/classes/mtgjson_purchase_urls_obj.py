"""
MTGJSON container for Purchase URLs
"""
from typing import Dict, Any

from mtgjson5.globals import to_camel_case


class MtgjsonPurchaseUrlsObject:
    """
    Container for purchase affiliate URLs
    """

    cardmarket: str
    tcgplayer: str
    mtgstocks: str

    def __init__(self):
        pass

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
