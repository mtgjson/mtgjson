"""
MTGJSON Singular Sealed Product Object
"""
from typing import Any, Dict, Optional

from ..utils import to_camel_case
from .mtgjson_identifiers import MtgjsonIdentifiersObject
from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject


class MtgjsonSealedProductObject:
    """
    MTGJSON Singular Sealed Product Object
    """

    name: str
    uuid: str
    identifiers: MtgjsonIdentifiersObject
    purchase_urls: MtgjsonPurchaseUrlsObject
    raw_purchase_urls: Dict[str, str]
    release_date: Optional[str]
    __skip_keys = [
        "raw_purchase_urls",
    ]

    def __init__(self) -> None:
        self.identifiers = MtgjsonIdentifiersObject()
        self.purchase_urls = MtgjsonPurchaseUrlsObject()
        self.raw_purchase_urls = {}

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        skip_keys = self.__skip_keys

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and key not in skip_keys
        }
