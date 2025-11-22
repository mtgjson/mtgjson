"""MTGJSON Deck List compiled model for pre-constructed deck collection."""

from typing import Any, List, Optional

from pydantic import Field

from ..classes.mtgjson_deck_header import MtgjsonDeckHeaderObject
from ..mtgjson_base import MTGJsonModel


class MtgjsonDeckListObject(MTGJsonModel):
    """
    MTGJSON DeckList Object
    """

    decks: List[MtgjsonDeckHeaderObject] = Field(default_factory=list)

    def __init__(
        self, deck_headers: Optional[List[MtgjsonDeckHeaderObject]] = None, **data: Any
    ) -> None:
        """
        Initialize deck list
        :param deck_headers: List of deck headers
        """
        if deck_headers:
            data["decks"] = deck_headers
        super().__init__(**data)

    def to_json(self) -> List[MtgjsonDeckHeaderObject]:
        """
        Support json.dump()
        :return: JSON serialized object (list of deck headers)
        """
        return self.decks
