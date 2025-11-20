from typing import Any, Dict, Optional

from ..mtgjson_base import MTGJsonModel


class MtgjsonTranslationsObject(MTGJsonModel):
    """
    MTGJSON Set.Translations Object
    """
    chinese_simplified: Optional[str] = None
    chinese_traditional: Optional[str] = None
    french: Optional[str] = None
    german: Optional[str] = None
    italian: Optional[str] = None
    japanese: Optional[str] = None
    korean: Optional[str] = None
    portuguese_ob_brazil_cb: Optional[str] = None
    russian: Optional[str] = None
    spanish: Optional[str] = None

    def __init__(self, active_dict: Optional[Dict[str, str]] = None, **data):
        """
        Initializer, for each language, given the contents
        :param active_dict: Dictionary mapping language names to translations
        """
        if active_dict:
            data.update({
                'chinese_simplified': active_dict.get("Chinese Simplified"),
                'chinese_traditional': active_dict.get("Chinese Traditional"),
                'french': active_dict.get("French", active_dict.get("fr")),
                'german': active_dict.get("German", active_dict.get("de")),
                'italian': active_dict.get("Italian", active_dict.get("it")),
                'japanese': active_dict.get("Japanese"),
                'korean': active_dict.get("Korean"),
                'portuguese_ob_brazil_cb': active_dict.get("Portuguese (Brazil)"),
                'russian': active_dict.get("Russian"),
                'spanish': active_dict.get("Spanish", active_dict.get("es")),
            })
        super().__init__(**data)

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
        Custom JSON serialization with key parsing
        :return: JSON object with parsed keys
        """
        return {
            self.parse_key(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
