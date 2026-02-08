"""
MTGJSON card models.

Inheritance hierarchy:
- CardBase: Common fields for all cards
  - CardAtomicBase: Oracle/evergreen properties
    - CardAtomic: Final atomic card model
  - CardPrintingBase: Printing-specific properties
    - CardPrintingFull: Full printing (atomic + printing)
      - CardSet: Card in a set
      - CardDeck: Card in a deck (with count/foil)
    - CardToken: Token card
- CardSetDeck: Minimal card reference in deck
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .base import PolarsMixin
from .submodels import (
    ForeignData,
    Identifiers,
    LeadershipSkills,
    Legalities,
    PurchaseUrls,
    RelatedCards,
    Rulings,
    SourceProducts,
)

# Layouts that use WUBRG color order instead of alphabetical
_WUBRG_COLOR_LAYOUTS = frozenset({"split", "adventure"})
_WUBRG_ORDER = {"W": 0, "U": 1, "B": 2, "R": 3, "G": 4}


class CardBase(PolarsMixin, BaseModel):
    """Base fields shared by all card types."""

    model_config = {"populate_by_name": True}

    # Identity
    name: str = Field(description="The name of the card.")
    ascii_name: str | None = Field(default=None, alias="asciiName")
    face_name: str | None = Field(default=None, alias="faceName")

    # Type line
    type: str = Field(description="The type line of the card.")
    types: list[str] = Field(default_factory=list, description="Card types.")
    subtypes: list[str] = Field(default_factory=list)
    supertypes: list[str] = Field(default_factory=list)

    # Colors
    colors: list[str] = Field(default_factory=list)
    color_identity: list[str] = Field(default_factory=list, alias="colorIdentity")
    color_indicator: list[str] | None = Field(default=None, alias="colorIndicator")
    produced_mana: list[str] | None = Field(default=None, alias="producedMana")

    # Mana
    mana_cost: str | None = Field(default=None, alias="manaCost")

    # Text
    text: str | None = Field(default=None)

    # Layout
    layout: str = Field(description="The card layout type.")
    side: str | None = Field(default=None)

    # Stats
    power: str | None = Field(default=None)
    toughness: str | None = Field(default=None)
    loyalty: str | None = Field(default=None)

    # Keywords
    keywords: list[str] | None = Field(default=None)

    # Identifiers
    identifiers: Identifiers = Field(default_factory=dict)  # type: ignore[assignment]

    # Flags
    is_funny: bool | None = Field(default=None, alias="isFunny")

    # EDHREC
    edhrec_saltiness: float | None = Field(default=None, alias="edhrecSaltiness")

    # Subsets
    subsets: list[str] | None = Field(default=None)

    def to_polars_dict(
        self,
        use_alias: bool = True,
        sort_keys: bool = True,
        sort_lists: bool = True,
        exclude_none: bool = False,
        keep_empty_lists: bool = False,
    ) -> dict[str, Any]:
        """Convert to dict, preserving WUBRG color order for split/adventure layouts."""
        result = super().to_polars_dict(use_alias, sort_keys, sort_lists, exclude_none, keep_empty_lists)

        if self.layout in _WUBRG_COLOR_LAYOUTS and "colors" in result and result["colors"]:
            result["colors"] = sorted(result["colors"], key=lambda c: _WUBRG_ORDER.get(c, 99))

        return result


class CardAtomicBase(CardBase):
    """Base for atomic (oracle-like) card properties."""

    # Mana values
    converted_mana_cost: float = Field(alias="convertedManaCost")
    mana_value: float = Field(alias="manaValue")
    face_converted_mana_cost: float | None = Field(default=None, alias="faceConvertedManaCost")
    face_mana_value: float | None = Field(default=None, alias="faceManaValue")

    # Battle
    defense: str | None = Field(default=None)

    # Vanguard
    hand: str | None = Field(default=None)
    life: str | None = Field(default=None)

    # EDHREC
    edhrec_rank: int | None = Field(default=None, alias="edhrecRank")

    # Foreign
    foreign_data: list[ForeignData] | None = Field(default=None, alias="foreignData")

    # Rules
    legalities: Legalities = Field(default_factory=dict)  # type: ignore[assignment]
    leadership_skills: LeadershipSkills | None = Field(default=None, alias="leadershipSkills")
    rulings: list[Rulings] | None = Field(default=None)

    # Deck limits
    has_alternative_deck_limit: bool | None = Field(default=None, alias="hasAlternativeDeckLimit")

    # Reserved
    is_reserved: bool | None = Field(default=None, alias="isReserved")
    is_game_changer: bool | None = Field(default=None, alias="isGameChanger")

    # Printings
    printings: list[str] | None = Field(default=None)

    # Purchase
    purchase_urls: PurchaseUrls | None = Field(default=None, alias="purchaseUrls")
    related_cards: RelatedCards | None = Field(default=None, alias="relatedCards")


class CardPrintingBase(CardBase):
    """Base for printing-specific card properties."""

    # Printing identity
    uuid: str = Field(description="MTGJSON unique identifier.")
    set_code: str = Field(alias="setCode")
    number: str = Field(description="Card collector number.")

    def __lt__(self, other: object) -> bool:
        """Sort by collector number, then side.

        Mirrors MtgjsonCardObject.__lt__ logic for consistency.
        """
        if not isinstance(other, CardPrintingBase):
            return NotImplemented

        self_side = self.side or ""
        other_side = other.side or ""

        if self.number == other.number:
            return self_side < other_side

        self_number_clean = "".join(x for x in self.number if x.isdigit()) or "100000"
        self_number_clean_int = int(self_number_clean)

        other_number_clean = "".join(x for x in other.number if x.isdigit()) or "100000"
        other_number_clean_int = int(other_number_clean)

        if self.number == self_number_clean and other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                if len(self_number_clean) != len(other_number_clean):
                    return len(self_number_clean) < len(other_number_clean)
                return self_side < other_side
            return self_number_clean_int < other_number_clean_int

        if self.number == self_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return True
            return self_number_clean_int < other_number_clean_int

        if other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return False
            return self_number_clean_int < other_number_clean_int

        if self_number_clean == other_number_clean:
            # Same numeric part - compare by side first
            if self_side != other_side:
                return self_side < other_side
            # Tiebreaker: multi-face cards sort by name, then by collector number
            if self_side and self.name != other.name:
                return self.name < other.name
            return bool(self.number < other.number)

        if self_number_clean_int == other_number_clean_int:
            if len(self_number_clean) != len(other_number_clean):
                return len(self_number_clean) < len(other_number_clean)
            if self_side != other_side:
                return self_side < other_side
            # Tiebreaker: multi-face cards sort by name, then by collector number
            if self_side and self.name != other.name:
                return self.name < other.name
            return bool(self.number < other.number)

        return self_number_clean_int < other_number_clean_int

    # Visual
    artist: str | None = Field(default=None)
    artist_ids: list[str] | None = Field(default=None, alias="artistIds")
    border_color: str = Field(alias="borderColor")
    frame_version: str = Field(alias="frameVersion")
    frame_effects: list[str] | None = Field(default=None, alias="frameEffects")
    watermark: str | None = Field(default=None)
    signature: str | None = Field(default=None)
    security_stamp: str | None = Field(default=None, alias="securityStamp")

    # Flavor
    flavor_text: str | None = Field(default=None, alias="flavorText")
    flavor_name: str | None = Field(default=None, alias="flavorName")
    face_flavor_name: str | None = Field(default=None, alias="faceFlavorName")

    # Original text (Gatherer)
    original_text: str | None = Field(default=None, alias="originalText")
    original_type: str | None = Field(default=None, alias="originalType")

    # Printed text (localized content for non-English cards)
    printed_name: str | None = Field(default=None, alias="printedName")
    printed_text: str | None = Field(default=None, alias="printedText")
    printed_type: str | None = Field(default=None, alias="printedType")
    face_printed_name: str | None = Field(default=None, alias="facePrintedName")

    # Availability
    availability: list[str] = Field(default_factory=list)
    booster_types: list[str] | None = Field(default=None, alias="boosterTypes")
    finishes: list[str] = Field(default_factory=list)
    promo_types: list[str] | None = Field(default=None, alias="promoTypes")

    # Un-sets
    attraction_lights: list[int] | None = Field(default=None, alias="attractionLights")

    # Flags
    is_full_art: bool | None = Field(default=None, alias="isFullArt")
    is_online_only: bool | None = Field(default=None, alias="isOnlineOnly")
    is_oversized: bool | None = Field(default=None, alias="isOversized")
    is_promo: bool | None = Field(default=None, alias="isPromo")
    is_reprint: bool | None = Field(default=None, alias="isReprint")
    is_textless: bool | None = Field(default=None, alias="isTextless")

    # Multi-face
    other_face_ids: list[str] | None = Field(default=None, alias="otherFaceIds")
    card_parts: list[str] | None = Field(default=None, alias="cardParts")

    # Language
    language: str = Field(default="English")

    # Source
    source_products: list[str] | None = Field(default=None, alias="sourceProducts")


class CardPrintingFull(CardPrintingBase, CardAtomicBase):
    """Full printing with all atomic + printing fields."""

    # Rarity
    rarity: str = Field(description="Card rarity.")

    # Duel deck
    duel_deck: str | None = Field(default=None, alias="duelDeck")

    # Rebalanced
    is_rebalanced: bool | None = Field(default=None, alias="isRebalanced")
    original_printings: list[str] | None = Field(default=None, alias="originalPrintings")
    rebalanced_printings: list[str] | None = Field(default=None, alias="rebalancedPrintings")
    original_release_date: str | None = Field(default=None, alias="originalReleaseDate")

    # Flags
    is_alternative: bool | None = Field(default=None, alias="isAlternative")
    is_starter: bool | None = Field(default=None, alias="isStarter")
    is_story_spotlight: bool | None = Field(default=None, alias="isStorySpotlight")
    is_timeshifted: bool | None = Field(default=None, alias="isTimeshifted")
    has_content_warning: bool | None = Field(default=None, alias="hasContentWarning")

    # Variations
    variations: list[str] | None = Field(default=None)


# =============================================================================
# Final Card Models
# =============================================================================


class CardAtomic(CardAtomicBase):
    """Oracle-like card with evergreen properties (no printing info)."""

    first_printing: str | None = Field(default=None, alias="firstPrinting")


class CardSet(CardPrintingFull):
    """Card as it appears in a set."""

    source_products: SourceProducts | None = Field(default=None, alias="sourceProducts")  # type: ignore


class CardDeck(CardPrintingFull):
    """Card in a deck with count and foil info."""

    count: int = Field(description="Number of copies in deck.")
    is_foil: bool | None = Field(default=None, alias="isFoil")
    is_etched: bool | None = Field(default=None, alias="isEtched")
    source_products: SourceProducts | None = Field(default=None, alias="sourceProducts")  # type: ignore
    original_release_date: str | None = Field(default=None, alias="originalReleaseDate")


class CardToken(CardPrintingBase):
    """Token card."""

    orientation: str | None = Field(default=None)
    reverse_related: list[str] | None = Field(default=None, alias="reverseRelated")
    related_cards: RelatedCards | None = Field(default=None, alias="relatedCards")
    edhrec_saltiness: float | None = Field(default=None, alias="edhrecSaltiness")
    source_products: SourceProducts | None = Field(default=None, alias="sourceProducts")  # type: ignore
    token_products: list[Any] | None = Field(default=None, alias="tokenProducts")


class CardSetDeck(PolarsMixin, BaseModel):
    """Minimal card reference in a deck (used in Set.decks)."""

    model_config = {"populate_by_name": True}

    count: int = Field(description="Number of copies.")
    is_foil: bool | None = Field(default=None, alias="isFoil")
    uuid: str = Field(description="MTGJSON uuid.")


class Cards:
    """Namespace for all card models."""

    CardAtomic = CardAtomic
    CardSet = CardSet
    CardDeck = CardDeck
    CardToken = CardToken
    CardSetDeck = CardSetDeck


CARD_MODEL_REGISTRY: list[type[BaseModel]] = [
    CardSetDeck,
    CardToken,
    CardAtomic,
    CardSet,
    CardDeck,
]

__all__ = [
    "CARD_MODEL_REGISTRY",
    "Cards",
]
