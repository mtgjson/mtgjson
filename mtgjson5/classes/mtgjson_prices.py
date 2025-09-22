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
    
    # Enhanced pricing fields for more granular price points
    sell_normal_low: Optional[float]
    sell_normal_mid: Optional[float]
    sell_normal_high: Optional[float]
    sell_normal_market: Optional[float]
    sell_normal_direct: Optional[float]
    
    sell_foil_low: Optional[float]
    sell_foil_mid: Optional[float]
    sell_foil_high: Optional[float]
    sell_foil_market: Optional[float]
    sell_foil_direct: Optional[float]
    
    sell_etched_low: Optional[float]
    sell_etched_mid: Optional[float]
    sell_etched_high: Optional[float]
    sell_etched_market: Optional[float]
    sell_etched_direct: Optional[float]

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
        # Enhanced pricing parameters
        sell_normal_low: Optional[float] = None,
        sell_normal_mid: Optional[float] = None,
        sell_normal_high: Optional[float] = None,
        sell_normal_market: Optional[float] = None,
        sell_normal_direct: Optional[float] = None,
        sell_foil_low: Optional[float] = None,
        sell_foil_mid: Optional[float] = None,
        sell_foil_high: Optional[float] = None,
        sell_foil_market: Optional[float] = None,
        sell_foil_direct: Optional[float] = None,
        sell_etched_low: Optional[float] = None,
        sell_etched_mid: Optional[float] = None,
        sell_etched_high: Optional[float] = None,
        sell_etched_market: Optional[float] = None,
        sell_etched_direct: Optional[float] = None,
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
        
        # Enhanced pricing fields
        self.sell_normal_low = sell_normal_low
        self.sell_normal_mid = sell_normal_mid
        self.sell_normal_high = sell_normal_high
        self.sell_normal_market = sell_normal_market
        self.sell_normal_direct = sell_normal_direct
        
        self.sell_foil_low = sell_foil_low
        self.sell_foil_mid = sell_foil_mid
        self.sell_foil_high = sell_foil_high
        self.sell_foil_market = sell_foil_market
        self.sell_foil_direct = sell_foil_direct
        
        self.sell_etched_low = sell_etched_low
        self.sell_etched_mid = sell_etched_mid
        self.sell_etched_high = sell_etched_high
        self.sell_etched_market = sell_etched_market
        self.sell_etched_direct = sell_etched_direct

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

        # Buylist prices (existing)
        if self.buy_normal is not None:
            buy_sell_option["buylist"]["normal"][self.date] = self.buy_normal
        if self.buy_foil is not None:
            buy_sell_option["buylist"]["foil"][self.date] = self.buy_foil
        if self.buy_etched is not None:
            buy_sell_option["buylist"]["etched"][self.date] = self.buy_etched
            
        # Basic retail prices (existing, for backward compatibility)
        if self.sell_normal is not None:
            buy_sell_option["retail"]["normal"][self.date] = self.sell_normal
        if self.sell_foil is not None:
            buy_sell_option["retail"]["foil"][self.date] = self.sell_foil
        if self.sell_etched is not None:
            buy_sell_option["retail"]["etched"][self.date] = self.sell_etched

        # Enhanced retail pricing structure with multiple price points
        retail_enhanced = defaultdict(lambda: defaultdict(dict))
        
        # Normal enhanced prices
        if self.sell_normal_low is not None:
            retail_enhanced["normal"]["low"][self.date] = self.sell_normal_low
        if self.sell_normal_mid is not None:
            retail_enhanced["normal"]["mid"][self.date] = self.sell_normal_mid
        if self.sell_normal_high is not None:
            retail_enhanced["normal"]["high"][self.date] = self.sell_normal_high
        if self.sell_normal_market is not None:
            retail_enhanced["normal"]["market"][self.date] = self.sell_normal_market
        if self.sell_normal_direct is not None:
            retail_enhanced["normal"]["direct"][self.date] = self.sell_normal_direct
        
        # Foil enhanced prices
        if self.sell_foil_low is not None:
            retail_enhanced["foil"]["low"][self.date] = self.sell_foil_low
        if self.sell_foil_mid is not None:
            retail_enhanced["foil"]["mid"][self.date] = self.sell_foil_mid
        if self.sell_foil_high is not None:
            retail_enhanced["foil"]["high"][self.date] = self.sell_foil_high
        if self.sell_foil_market is not None:
            retail_enhanced["foil"]["market"][self.date] = self.sell_foil_market
        if self.sell_foil_direct is not None:
            retail_enhanced["foil"]["direct"][self.date] = self.sell_foil_direct
        
        # Etched enhanced prices
        if self.sell_etched_low is not None:
            retail_enhanced["etched"]["low"][self.date] = self.sell_etched_low
        if self.sell_etched_mid is not None:
            retail_enhanced["etched"]["mid"][self.date] = self.sell_etched_mid
        if self.sell_etched_high is not None:
            retail_enhanced["etched"]["high"][self.date] = self.sell_etched_high
        if self.sell_etched_market is not None:
            retail_enhanced["etched"]["market"][self.date] = self.sell_etched_market
        if self.sell_etched_direct is not None:
            retail_enhanced["etched"]["direct"][self.date] = self.sell_etched_direct
        
        # Add enhanced pricing if we have any enhanced data
        if retail_enhanced:
            buy_sell_option["retail_enhanced"] = dict(retail_enhanced)

        return {self.source: {self.provider: buy_sell_option}}
