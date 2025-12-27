"""
MTGJSON set models.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .base import PolarsMixin
from .cards import CardSet, CardSetDeck, CardToken
from .sealed import SealedProduct
from .submodels import (
    BoosterConfig,
    Translations,
)


# =============================================================================
# Deck Models (minimal, for Set.decks)
# =============================================================================

class DeckSet(PolarsMixin, BaseModel):
    """Deck with minimal card references (as in Set.decks)."""
    model_config = {"populate_by_name": True}

    code: str
    name: str
    type: str
    release_date: str | None = Field(default=None, alias="releaseDate")
    sealed_product_uuids: list[str] | None = Field(default=None, alias="sealedProductUuids")
    main_board: list[CardSetDeck] = Field(default_factory=list, alias="mainBoard")
    side_board: list[CardSetDeck] = Field(default_factory=list, alias="sideBoard")
    commander: list[CardSetDeck] | None = None


# =============================================================================
# Set Models
# =============================================================================

class SetList(PolarsMixin, BaseModel):
    """Set summary (without cards, for SetList.json)."""
    model_config = {"populate_by_name": True}

    code: str
    name: str
    type: str
    release_date: str = Field(alias="releaseDate")
    base_set_size: int = Field(alias="baseSetSize")
    total_set_size: int = Field(alias="totalSetSize")
    keyrune_code: str = Field(alias="keyruneCode")
    translations: Translations = Field(default_factory=dict)

    # Optional fields
    block: str | None = None
    parent_code: str | None = Field(default=None, alias="parentCode")
    mtgo_code: str | None = Field(default=None, alias="mtgoCode")
    token_set_code: str | None = Field(default=None, alias="tokenSetCode")

    # External IDs
    mcm_id: int | None = Field(default=None, alias="mcmId")
    mcm_id_extras: int | None = Field(default=None, alias="mcmIdExtras")
    mcm_name: str | None = Field(default=None, alias="mcmName")
    tcgplayer_group_id: int | None = Field(default=None, alias="tcgplayerGroupId")
    cardsphere_set_id: int | None = Field(default=None, alias="cardsphereSetId")

    # Flags
    is_foil_only: bool = Field(default=False, alias="isFoilOnly")
    is_non_foil_only: bool | None = Field(default=None, alias="isNonFoilOnly")
    is_online_only: bool = Field(default=False, alias="isOnlineOnly")
    is_paper_only: bool | None = Field(default=None, alias="isPaperOnly")
    is_foreign_only: bool | None = Field(default=None, alias="isForeignOnly")
    is_partial_preview: bool | None = Field(default=None, alias="isPartialPreview")

    # Languages
    languages: list[str] | None = None


class MtgSet(SetList):
    """Full set with cards, tokens, decks, etc."""

    cards: list[CardSet] = Field(default_factory=list)
    tokens: list[CardToken] = Field(default_factory=list)
    booster: dict[str, BoosterConfig] | None = None
    decks: list[DeckSet] | None = None
    sealed_product: list[SealedProduct] | None = Field(default=None, alias="sealedProduct")


# =============================================================================
# Registry
# =============================================================================

SET_MODEL_REGISTRY: list[type[BaseModel]] = [
    DeckSet,
    SetList,
    MtgSet,
]
