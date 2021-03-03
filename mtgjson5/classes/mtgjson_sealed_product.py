"""
MTGJSON Singular Sealed Product Object
"""
from typing import Any, Dict

from mtgjson5.utils import to_camel_case

from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject
from .mtgjson_identifiers import MtgjsonIdentifiersObject


class MtgjsonSealedProductObject:
    """
    MTGJSON Singular Sealed Product Object
    """

    name: str
    uuid: str
    identifiers: MtgjsonIdentifiersObject
    purchase_urls: MtgjsonPurchaseUrlsObject
    raw_purchase_urls: Dict[str, str]
    release_date: str

    def __init__(self) -> None:
        self.purchase_urls = MtgjsonPurchaseUrlsObject()
        self.raw_purchase_urls = {}

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
