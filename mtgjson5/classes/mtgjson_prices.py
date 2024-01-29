"""
MTGJSON Singular Prices.Card Object
"""
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from mtgjson5.classes.json_object import JsonObject


class MtgjsonPricesObject(JsonObject):
    """
    MTGJSON Singular Prices.Card Object
    """

    source: str
    provider: str
    date: str
    currency: str
    buy_normal: Optional[float]
    buy_foil: Optional[float]
    buy_etched: Optional[float]
    sell_normal: Optional[float]
    sell_foil: Optional[float]
    sell_etched: Optional[float]

    def __init__(
        self,
        source: str,
        provider: str,
        date: str,
        currency: str,
        buy_normal: Optional[float] = None,
        buy_foil: Optional[float] = None,
        buy_etched: Optional[float] = None,
        sell_normal: Optional[float] = None,
        sell_foil: Optional[float] = None,
        sell_etched: Optional[float] = None,
    ) -> None:
        """
        Initializer for Pricing Container
        """
        self.source = source
        self.provider = provider
        self.date = date
        self.currency = currency
        self.buy_normal = buy_normal
        self.buy_foil = buy_foil
        self.buy_etched = buy_etched
        self.sell_normal = sell_normal
        self.sell_foil = sell_foil
        self.sell_etched = sell_etched

    def items(self) -> List[Tuple[str, Optional[float]]]:
        """
        Override dict iterator
        :return: List of entities
        """
        return [
            (key, value)
            for key, value in vars(self).items()
            if not callable(getattr(self, key)) and not key.startswith("__")
        ]

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        buy_sell_option: Dict[str, Any] = {
            "buylist": defaultdict(dict),
            "retail": defaultdict(dict),
            "currency": self.currency,
        }

        if self.buy_normal is not None:
            buy_sell_option["buylist"]["normal"][self.date] = self.buy_normal
        if self.buy_foil is not None:
            buy_sell_option["buylist"]["foil"][self.date] = self.buy_foil
        if self.buy_etched is not None:
            buy_sell_option["buylist"]["etched"][self.date] = self.buy_etched
        if self.sell_normal is not None:
            buy_sell_option["retail"]["normal"][self.date] = self.sell_normal
        if self.sell_foil is not None:
            buy_sell_option["retail"]["foil"][self.date] = self.sell_foil
        if self.sell_etched is not None:
            buy_sell_option["retail"]["etched"][self.date] = self.sell_etched

        return {self.source: {self.provider: buy_sell_option}}
