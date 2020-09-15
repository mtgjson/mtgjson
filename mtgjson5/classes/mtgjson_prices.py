"""
MTGJSON Singular Prices.Card Object
"""
from typing import Any, Dict, Optional


class MtgjsonPricesObject:
    """
    MTGJSON Singular Prices.Card Object
    """

    source: str
    provider: str
    date: str
    currency: str
    buy_normal: Optional[float]
    buy_foil: Optional[float]
    sell_normal: Optional[float]
    sell_foil: Optional[float]

    def __init__(self, source: str, provider: str, date: str, currency: str) -> None:
        """
        Initializer for Pricing Container
        """
        self.source = source
        self.provider = provider
        self.date = date
        self.currency = currency
        self.buy_normal = None
        self.buy_foil = None
        self.sell_normal = None
        self.sell_foil = None

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        buy_sell_option: Dict[str, Any] = {}
        if (self.buy_normal is not None) or (self.buy_foil is not None):
            buy_sell_option["buylist"] = {"normal": {}, "foil": {}}
            if self.buy_normal is not None:
                buy_sell_option["buylist"]["normal"][self.date] = self.buy_normal
            if self.buy_foil is not None:
                buy_sell_option["buylist"]["foil"][self.date] = self.buy_foil

        if (self.sell_normal is not None) or (self.sell_foil is not None):
            buy_sell_option["retail"] = {"normal": {}, "foil": {}}
            if self.sell_normal is not None:
                buy_sell_option["retail"]["normal"][self.date] = self.sell_normal
            if self.sell_foil is not None:
                buy_sell_option["retail"]["foil"][self.date] = self.sell_foil
        buy_sell_option["currency"] = self.currency
        return_object: Dict[str, Any] = {self.source: {self.provider: buy_sell_option}}

        return return_object
