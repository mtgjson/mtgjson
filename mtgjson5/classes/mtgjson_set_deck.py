"""
MTGJSON Set Deck Object
"""
import re
from typing import Any, Dict, List, Optional

from ..utils import to_camel_case
from .mtgjson_sealed_product import MtgjsonSealedProductObject


class MtgjsonSetDeckObject:
    """
    MTGJSON Set Deck Object
    """

    class MtgjsonSetDeckCardObject:
        """
        MTGJSON Set Deck Card Object
        """

        uuid: str
        count: int
        finish: str
        tags: Optional[List[str]]

        def __init__(self, uuid: str, count: int, finish: str, tags: List[str]) -> None:
            self.uuid = uuid
            self.count = count
            self.finish = finish
            self.tags = tags

        def to_json(self) -> Dict[str, Any]:
            """
            Support json.dump()
            :return: JSON serialized object
            """
            return {
                to_camel_case(key): value
                for key, value in self.__dict__.items()
                if "__" not in key and not callable(value)
                and value
            }

    name: str
    sealed_product_uuids: Optional[List[str]]
    cards: List[MtgjsonSetDeckCardObject]
    __alpha_numeric_name: str

    def __init__(self, name: str, sealed_product_uuids: Optional[List[str]]) -> None:
        self.name = name
        self.sealed_product_uuids = sealed_product_uuids
        self.cards = []
        self.__alpha_numeric_name = re.sub(r"[^A-Za-z0-9 ]+", "", self.name).lower()

    def add_sealed_product_uuids(
        self, mtgjson_set_sealed_products: List[MtgjsonSealedProductObject]
    ) -> None:
        """
        Update the UUID for the deck to link back to sealed product, if able
        :param mtgjson_set_sealed_products MTGJSON Set Sealed Products for this Set
        """
        if not self.sealed_product_uuids:
            for sealed_product_entry in mtgjson_set_sealed_products:
                sealed_name = sealed_product_entry.name.lower()
                if self.__alpha_numeric_name in sealed_name:
                    self.sealed_product_uuids = [sealed_product_entry.uuid]
                    break

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
