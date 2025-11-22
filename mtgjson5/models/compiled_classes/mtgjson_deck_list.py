"""MTGJSON Deck List compiled model for pre-constructed deck collection."""

from typing import Any

from pydantic import Field

from ..classes.mtgjson_deck_header import MtgjsonDeckHeaderObject
from ..mtgjson_base import MTGJsonModel


class MtgjsonDeckListObject(MTGJsonModel):
    """
    The Deck List compiled output containing metadata for all pre-constructed decks.
    """

    decks: list[MtgjsonDeckHeaderObject] = Field(
        default_factory=list,
        description="A list of deck header objects containing metadata about each deck.",
    )

    def __init__(
        self, deck_headers: list[MtgjsonDeckHeaderObject] | None = None, **data: Any
    ) -> None:
        """
        Initialize deck list
        :param deck_headers: List of deck headers
        """
        if deck_headers:
            data["decks"] = deck_headers
        super().__init__(**data)

    def to_list(self) -> list[MtgjsonDeckHeaderObject]:
        """
        Support json.dump()
        :return: List serialized object (list of deck headers)
        """
        return self.decks
