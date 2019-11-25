"""
MTGJSON container for Set Translations
"""
from typing import Dict, Any


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

    def __init__(self):
        pass

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
