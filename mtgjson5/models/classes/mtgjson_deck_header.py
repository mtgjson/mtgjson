"""MTGJSON Deck Header Object model for deck metadata."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from ..mtgjson_base import MTGJsonModel
from .mtgjson_deck import MtgjsonDeckObject


class MtgjsonDeckHeaderObject(MTGJsonModel):
    """
    The Deck List Data Model describes the meta data properties of an individual Deck.
    """

    code: str = Field(description="The printing deck code for the deck.")
    file_name: str = Field(description="The file name for the deck.")
    name: str = Field(description="The name of the deck.")
    release_date: str | None = Field(
        default=None, description="The release date in ISO 8601 format for the set."
    )
    type: str = Field(description="The type of the deck.")

    def __init__(
        self, output_deck: MtgjsonDeckObject | None = None, **data: Any
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
