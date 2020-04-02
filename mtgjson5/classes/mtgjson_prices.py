"""
MTGJSON container for pricing data
"""
from typing import Any, Dict, Optional


class MtgjsonPricesObject:
    """
    Pricing Container
    """

    source: str
    provider: str
    date: str
    buy_normal: Optional[float]
    buy_foil: Optional[float]
    sell_normal: Optional[float]
    sell_foil: Optional[float]

    def __init__(self, source: str, provider: str, date: str) -> None:
        self.source = source
        self.provider = provider
        self.date = date
        self.buy_normal = None
        self.buy_foil = None
        self.sell_normal = None
        self.sell_foil = None

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        buy_sell_option: Dict[str, Dict[str, Dict[str, float]]] = {}
        if (self.buy_normal is not None) or (self.buy_foil is not None):
            buy_sell_option["buy"] = {"normal": {}, "foil": {}}
            if self.buy_normal is not None:
                buy_sell_option["buy"]["normal"][self.date] = self.buy_normal
            if self.buy_foil is not None:
                buy_sell_option["buy"]["foil"][self.date] = self.buy_foil

        if (self.sell_normal is not None) or (self.sell_foil is not None):
            buy_sell_option["sell"] = {"normal": {}, "foil": {}}
            if self.sell_normal is not None:
                buy_sell_option["sell"]["normal"][self.date] = self.sell_normal
            if self.sell_foil is not None:
                buy_sell_option["sell"]["foil"][self.date] = self.sell_foil

        return_object: Dict[str, Any] = {self.source: {self.provider: buy_sell_option}}

        return return_object
