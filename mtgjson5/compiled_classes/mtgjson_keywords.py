"""
MTGJSON Keywords Object
"""
from typing import Any, Dict, List

from ..providers.scryfall import ScryfallProvider
from ..utils import to_camel_case


class MtgjsonKeywordsObject:
    """
    MTGJSON Keywords Object
    """

    ability_words: List[str]
    keyword_actions: List[str]
    keyword_abilities: List[str]

    def __init__(self) -> None:
        """
        Initializer to build up the object
        """
        self.ability_words = ScryfallProvider().get_catalog_entry("keyword-abilities")
        self.keyword_actions = ScryfallProvider().get_catalog_entry("keyword-actions")
        self.keyword_abilities = ScryfallProvider().get_catalog_entry(
            "keyword-abilities"
        )

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
