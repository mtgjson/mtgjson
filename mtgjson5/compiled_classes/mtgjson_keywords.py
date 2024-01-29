"""
MTGJSON Keywords Object
"""
from typing import List

from ..classes.json_object import JsonObject
from ..providers.scryfall.monolith import ScryfallProvider


class MtgjsonKeywordsObject(JsonObject):
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
        self.ability_words = ScryfallProvider().get_catalog_entry("ability-words")
        self.keyword_actions = ScryfallProvider().get_catalog_entry("keyword-actions")
        self.keyword_abilities = ScryfallProvider().get_catalog_entry(
            "keyword-abilities"
        )
