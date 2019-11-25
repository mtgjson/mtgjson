"""
MTGJSON container for pricing data
"""
from typing import Dict, Any


class MtgjsonPricesObject:
    """
    Pricing Container
    """

    paper: Dict[str, float]
    paper_foil: Dict[str, float]
    mtgo: Dict[str, float]
    mtgo_foil: Dict[str, float]

    def __init__(self):
        pass

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
