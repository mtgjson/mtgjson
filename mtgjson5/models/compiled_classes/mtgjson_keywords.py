"""MTGJSON Keywords compiled model for card keyword abilities."""

from typing import Any

from pydantic import Field

from ... import providers
from ..mtgjson_base import MTGJsonCompiledModel

ScryfallProvider = providers.scryfall.monolith.ScryfallProvider


class MtgjsonKeywordsObject(MTGJsonCompiledModel):
    """
    The Keywords Data Model describes the properties of keywords available to any card.
    """

    ability_words: list[str] = Field(
        default_factory=list,
        description="A list of ability words found in rules text on cards.",
    )
    keyword_actions: list[str] = Field(
        default_factory=list,
        description="A list of keyword actions found in rules text on cards.",
    )
    keyword_abilities: list[str] = Field(
        default_factory=list,
        description="A list of keyword abilities found in rules text on cards.",
    )

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
