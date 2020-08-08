"""
MTGJSON Singular Card.TcgplayerSkus Object
"""
from typing import Dict

from mtgjson5.utils import to_camel_case


class MtgjsonTcgplayerSkusObject:
    """
    MTGJSON Singular Card.TcgplayerSkus Object
    """

    sku_id: str
    language: str
    condition: str
    is_foil: bool

    def __init__(
        self, sku_id: str, language: str, condition: str, is_foil: bool
    ) -> None:
        """
        Setup values
        """
        self.sku_id = sku_id
        self.language = language
        self.condition = condition
        self.is_foil = is_foil

    def to_json(self) -> Dict[str, str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and value
        }
