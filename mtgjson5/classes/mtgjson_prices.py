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
    __parent_is_card_object: bool = True

    def __init__(self, uuid: str, entries: Dict[str, Dict[str, float]] = None) -> None:
        if entries is None:
            entries = {}
            self.__parent_is_card_object = False

        self.uuid = uuid
        self.paper = entries.get("paper", {})
        self.paper_foil = entries.get("paperFoil", {})
        self.mtgo = entries.get("mtgo", {})
        self.mtgo_foil = entries.get("mtgoFoil", {})

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys = set()

        if self.__parent_is_card_object:
            skip_keys.add("uuid")

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value) and key not in skip_keys
        }
