"""
MTGJSON container for Set Translations
"""
from typing import Any, Dict

from mtgjson5.globals import to_camel_case


class MtgjsonTranslationsObject:
    """
    Structure to hold translations for an individual set
    """

    chinese_simplified: str
    chinese_traditional: str
    french: str
    german: str
    italian: str
    japanese: str
    korean: str
    portuguese_brazil: str
    russian: str
    spanish: str

    def __init__(self) -> None:
        pass

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
