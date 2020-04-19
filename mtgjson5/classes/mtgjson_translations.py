"""
MTGJSON Set.Translations Object
"""
from typing import Any, Dict, Optional


class MtgjsonTranslationsObject:
    """
    MTGJSON Set.Translations Object
    """

    chinese_simplified: Optional[str]
    chinese_traditional: Optional[str]
    french: Optional[str]
    german: Optional[str]
    italian: Optional[str]
    japanese: Optional[str]
    korean: Optional[str]
    portuguese_ob_brazil_cb: Optional[str]
    russian: Optional[str]
    spanish: Optional[str]

    def __init__(self, active_dict: Dict[str, str] = None) -> None:
        """
        Initializer, for each language, given the contents
        """
        if not active_dict:
            return

        self.chinese_simplified = active_dict.get("Chinese Simplified")
        self.chinese_traditional = active_dict.get("Chinese Traditional")
        self.french = active_dict.get("French")
        self.german = active_dict.get("German")
        self.italian = active_dict.get("Italian")
        self.japanese = active_dict.get("Japanese")
        self.korean = active_dict.get("Korean")
        self.portuguese_ob_brazil_cb = active_dict.get("Portuguese (Brazil)")
        self.russian = active_dict.get("Russian")
        self.spanish = active_dict.get("Spanish")

    @staticmethod
    def parse_key(key: str) -> str:
        """
        Custom parsing of translation keys
        :param key: Key to translate
        :return: Translated key for JSON
        """
        key = key.replace("ob_", "(").replace("_cb", ")")
        components = key.split("_")
        return " ".join(x.title() for x in components)

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            self.parse_key(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
