"""MTGJSON Keywords compiled model for card keyword abilities."""

from typing import Any, List

from pydantic import Field

from ... import providers
from ..mtgjson_base import MTGJsonCompiledModel

ScryfallProvider = providers.scryfall.monolith.ScryfallProvider


class MtgjsonKeywordsObject(MTGJsonCompiledModel):
    """
    MTGJSON Keywords Object
    """

    ability_words: List[str] = Field(default_factory=list)
    keyword_actions: List[str] = Field(default_factory=list)
    keyword_abilities: List[str] = Field(default_factory=list)

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializer to build up the object
        """
        super().__init__(**kwargs)

        self.ability_words = ScryfallProvider().get_catalog_entry("ability-words")
        self.keyword_actions = ScryfallProvider().get_catalog_entry("keyword-actions")
        self.keyword_abilities = ScryfallProvider().get_catalog_entry(
            "keyword-abilities"
        )
