"""
MTGJSON deck models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, Field

from .base import PolarsMixin

if TYPE_CHECKING:
    from .cards import CardDeck, CardToken


class DeckList(PolarsMixin, BaseModel):
    """Deck list summary (without cards, for DeckList.json)."""

    model_config = {"populate_by_name": True}

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Deck List"
    __doc_desc__: ClassVar[str] = (
        "The Deck List Data Model describes the meta data properties of an individual [Deck](/data-models/deck/)."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent file:** [DeckList](/downloads/all-files/#decklist)\n- **Parent property:** `data`"
    )
    __doc_enum__: ClassVar[str] = "deck"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, deck list"

    code: str = Field(
        description="The printing deck code for the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    name: str = Field(
        description="The name of the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    file_name: str = Field(
        alias="fileName",
        description=(
            "The file name for the deck. Combines the `name` and `code` properties "
            "to avoid namespace collisions and are given a `_` delimiter."
        ),
        json_schema_extra={"introduced": "v4.3.0"},
    )
    type: str = Field(
        description="The type of the deck.",
        json_schema_extra={"introduced": "v5.1.0", "enum_key": "type"},
    )
    release_date: str | None = Field(
        default=None,
        alias="releaseDate",
        description=(
            "The release date in "
            "[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) "
            "format for the set. Returns `null` if the deck was not formally released as a product."
        ),
        json_schema_extra={"introduced": "v4.3.0"},
    )


class Deck(PolarsMixin, BaseModel):
    """Full deck with expanded cards."""

    model_config = {"populate_by_name": True}

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Deck"
    __doc_desc__: ClassVar[str] = "The Deck Data Model describes the properties of an individual Deck."
    __doc_parent__: ClassVar[str] = "**Parent file:** [All Decks](/downloads/all-decks/)\n- **Parent property:** `data`"
    __doc_enum__: ClassVar[str] = "deck"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, deck"

    code: str = Field(
        description="The printing set code for the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    name: str = Field(
        description="The name of the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    type: str = Field(
        description="The type of deck.",
        json_schema_extra={"introduced": "v4.3.0", "enum_key": "type"},
    )
    release_date: str | None = Field(
        default=None,
        alias="releaseDate",
        description=(
            "The release date in "
            "[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) "
            "format for the set. Returns `null` if the deck was not formally released as a product."
        ),
        json_schema_extra={"introduced": "v4.3.0"},
    )
    sealed_product_uuids: list[str] | None = Field(
        default=None,
        alias="sealedProductUuids",
        description="A cross-reference identifier to determine which sealed products contain this deck.",
        json_schema_extra={"introduced": "v5.2.2"},
    )
    main_board: list[CardDeck] = Field(
        default_factory=list,
        alias="mainBoard",
        description="The cards in the main-board. See the [Card (Deck)](/data-models/card/card-deck/) Data Model.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    side_board: list[CardDeck] = Field(
        default_factory=list,
        alias="sideBoard",
        description="The cards in the side-board. See the [Card (Deck)](/data-models/card/card-deck/) Data Model.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    commander: list[CardDeck] | None = Field(
        default=None,
        description="The card that is the Commander in this deck. See the [Card (Deck)](/data-models/card/card-deck/) Data Model.",
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )
    display_commander: list[CardDeck] | None = Field(default=None, alias="displayCommander")
    planes: list[CardDeck] | None = None
    schemes: list[CardDeck] | None = None
    tokens: list[CardToken] | None = Field(
        default=None,
        description="The tokens included with the product. See the [Card (Deck)](/data-models/card/card-deck/) Data Model.",
        json_schema_extra={"introduced": "v5.2.2"},
    )
    source_set_codes: list[str] | None = Field(default=None, alias="sourceSetCodes")


class Decks:
    """Namespace for all deck models."""

    DeckList = DeckList
    Deck = Deck


DECK_MODEL_REGISTRY: list[type[BaseModel]] = [
    DeckList,
    Deck,
]

__all__ = [
    "DECK_MODEL_REGISTRY",
    "Decks",
]
