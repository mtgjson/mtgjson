"""
MTGJSON container for pricing data
"""
from typing import Any, Dict

from ..utils import to_camel_case


class MtgjsonPricesObject:
    """
    Pricing Container
    """

    uuid: str
    paper: Dict[str, float]
    paper_foil: Dict[str, float]
    mtgo: Dict[str, float]
    mtgo_foil: Dict[str, float]

    def __init__(self, uuid: str) -> None:
        self.uuid = uuid
        self.paper = {}
        self.paper_foil = {}
        self.mtgo = {}
        self.mtgo_foil = {}

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
