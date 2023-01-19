"""
MTGJSON Sales History Container
"""
from typing import Any, Dict, Optional

from mtgjson5.utils import to_camel_case


class MtgjsonSalesHistoryObject:
    """
    MTGJSON Sales History Container
    """

    quantity: int
    custom_listing_id: Optional[str]
    purchase_price: float
    shipping_price: float
    order_date: str

    def __lt__(self, other: "MtgjsonSalesHistoryObject") -> bool:
        return self.order_date < other.order_date

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
