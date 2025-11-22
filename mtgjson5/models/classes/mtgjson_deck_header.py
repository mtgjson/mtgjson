"""MTGJSON Deck Header Object model for deck metadata."""

from __future__ import annotations

from typing import Any, Optional

from ..mtgjson_base import MTGJsonModel
from .mtgjson_deck import MtgjsonDeckObject


class MtgjsonDeckHeaderObject(MTGJsonModel):
    """
    MTGJSON Singular Deck Header Object
    """

    code: str = ""
    file_name: str = ""
    name: str = ""
    release_date: str = ""
    type: str = ""

    def __init__(
        self, output_deck: Optional[MtgjsonDeckObject] = None, **data: Any
    ) -> None:
        """
        Initialize deck header from deck object
        :param output_deck: Deck object to extract header from
        """
        if output_deck:
            data.update(
                {
                    "code": output_deck.code,
                    "file_name": output_deck.file_name,
                    "name": output_deck.name,
                    "release_date": output_deck.release_date,
                    "type": output_deck.type,
                }
            )
        super().__init__(**data)

    @classmethod
    def from_deck(cls, deck: MtgjsonDeckObject) -> MtgjsonDeckHeaderObject:
        """
        Create deck header from deck object.
        :param deck: Deck object to extract header from
        :return: New MtgjsonDeckHeaderObject
        """
        return cls(output_deck=deck)
