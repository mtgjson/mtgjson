"""
MTGJSON Keywords Object
"""

from ..classes.json_object import JsonObject
from ..providers.scryfall.monolith import ScryfallProvider


class MtgjsonKeywordsObject(JsonObject):
    """
    MTGJSON Keywords Object
    """

    ability_words: list[str]
    keyword_actions: list[str]
    keyword_abilities: list[str]

    def __init__(self) -> None:
        """
        Initializer to build up the object
        """
        self.ability_words = ScryfallProvider().get_catalog_entry("ability-words")
        self.keyword_actions = ScryfallProvider().get_catalog_entry("keyword-actions")
        self.keyword_abilities = ScryfallProvider().get_catalog_entry(
            "keyword-abilities"
        )
