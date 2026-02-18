"""
Card layout constants and classifications.

Single source of truth for layout-related logic throughout the pipeline.
"""

from __future__ import annotations

from enum import Enum
from typing import Final


class LayoutVariant(Enum):
    """Scryfall/MTGJSON card layouts."""

    NORMAL = "normal"
    SPLIT = "split"
    FLIP = "flip"
    TRANSFORM = "transform"
    MODAL_DFC = "modal_dfc"
    MELD = "meld"
    LEVELER = "leveler"
    CLASS = "class"
    CASE = "case"
    SAGA = "saga"
    ADVENTURE = "adventure"
    MUTATE = "mutate"
    PROTOTYPE = "prototype"
    BATTLE = "battle"
    PLANAR = "planar"
    SCHEME = "scheme"
    VANGUARD = "vanguard"
    TOKEN = "token"
    DOUBLE_FACED_TOKEN = "double_faced_token"
    EMBLEM = "emblem"
    AUGMENT = "augment"
    HOST = "host"
    ART_SERIES = "art_series"
    REVERSIBLE_CARD = "reversible_card"
    AFTERMATH = "aftermath"


MULTIFACE_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.SPLIT.value,
        LayoutVariant.AFTERMATH.value,
        LayoutVariant.FLIP.value,
        LayoutVariant.TRANSFORM.value,
        LayoutVariant.MODAL_DFC.value,
        LayoutVariant.MELD.value,
        LayoutVariant.ADVENTURE.value,
        LayoutVariant.REVERSIBLE_CARD.value,
        LayoutVariant.BATTLE.value,
        LayoutVariant.DOUBLE_FACED_TOKEN.value,
    }
)

FACE_NAME_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.TRANSFORM.value,
        LayoutVariant.MODAL_DFC.value,
        LayoutVariant.MELD.value,
        LayoutVariant.REVERSIBLE_CARD.value,
        LayoutVariant.FLIP.value,
        LayoutVariant.SPLIT.value,
        LayoutVariant.AFTERMATH.value,
        LayoutVariant.ADVENTURE.value,
        LayoutVariant.BATTLE.value,
        LayoutVariant.DOUBLE_FACED_TOKEN.value,
        LayoutVariant.ART_SERIES.value,
    }
)

FACE_MANA_VALUE_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.SPLIT.value,
        LayoutVariant.AFTERMATH.value,
        LayoutVariant.FLIP.value,
        LayoutVariant.TRANSFORM.value,
        LayoutVariant.MODAL_DFC.value,
        LayoutVariant.MELD.value,
        LayoutVariant.ADVENTURE.value,
        LayoutVariant.REVERSIBLE_CARD.value,
    }
)

TOKEN_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.TOKEN.value,
        LayoutVariant.DOUBLE_FACED_TOKEN.value,
        LayoutVariant.EMBLEM.value,
        LayoutVariant.ART_SERIES.value,
    }
)

SPLIT_WATERMARK_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.SPLIT.value,
        LayoutVariant.AFTERMATH.value,
    }
)

SPLIT_COLOR_LAYOUTS: Final[frozenset[str]] = frozenset(
    {
        LayoutVariant.SPLIT.value,
        LayoutVariant.AFTERMATH.value,
    }
)


def is_multiface(layout: str) -> bool:
    """Check if layout has multiple faces."""
    return layout in MULTIFACE_LAYOUTS


def is_token_layout(layout: str) -> bool:
    """Check if layout is a token type."""
    return layout in TOKEN_LAYOUTS
