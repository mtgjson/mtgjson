"""
MTGJSON deck models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from .base import PolarsMixin


if TYPE_CHECKING:
    from .cards import CardDeck, CardToken


# =============================================================================
# Deck Models
# =============================================================================

class DeckList(PolarsMixin, BaseModel):
    """Deck list summary (without cards, for DeckList.json)."""
    model_config = {"populate_by_name": True}

    code: str
    name: str
    file_name: str = Field(alias="fileName")
    type: str
    release_date: str | None = Field(default=None, alias="releaseDate")


class Deck(PolarsMixin, BaseModel):
    """Full deck with expanded cards."""
    model_config = {"populate_by_name": True}

    code: str
    name: str
    type: str
    release_date: str | None = Field(default=None, alias="releaseDate")
    sealed_product_uuids: list[str] | None = Field(default=None, alias="sealedProductUuids")
    main_board: list[CardDeck] = Field(default_factory=list, alias="mainBoard")
    side_board: list[CardDeck] = Field(default_factory=list, alias="sideBoard")
    commander: list[CardDeck] | None = None
    tokens: list[CardToken] = Field(default_factory=list)


# =============================================================================
# Registry
# =============================================================================

DECK_MODEL_REGISTRY: list[type[BaseModel]] = [
    DeckList,
    Deck,
]
