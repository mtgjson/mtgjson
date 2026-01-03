"""
Set-related constants.

Special set codes, set types, and set-specific behaviors.
"""

from __future__ import annotations

from enum import Enum
from typing import Final


class SetType(Enum):
    """Scryfall set types."""
    CORE = "core"
    EXPANSION = "expansion"
    MASTERS = "masters"
    ALCHEMY = "alchemy"
    MASTERPIECE = "masterpiece"
    ARSENAL = "arsenal"
    FROM_THE_VAULT = "from_the_vault"
    SPELLBOOK = "spellbook"
    PREMIUM_DECK = "premium_deck"
    DUEL_DECK = "duel_deck"
    DRAFT_INNOVATION = "draft_innovation"
    TREASURE_CHEST = "treasure_chest"
    COMMANDER = "commander"
    PLANECHASE = "planechase"
    ARCHENEMY = "archenemy"
    VANGUARD = "vanguard"
    FUNNY = "funny"
    STARTER = "starter"
    BOX = "box"
    PROMO = "promo"
    TOKEN = "token"
    MEMORABILIA = "memorabilia"
    MINIGAME = "minigame"


# Set-specific behaviors

DUEL_DECK_SETS: Final[frozenset[str]] = frozenset({
    "DD1", "DD2", "DDC", "DDD", "DDE", "DDF", "DDG", "DDH", "DDI",
    "DDJ", "DDK", "DDL", "DDM", "DDN", "DDO", "DDP", "DDQ", "DDR",
    "DDS", "DDT", "DDU", "GS1",
})
"""Sets that use duelDeck field (DD* prefix + GS1)."""

FOIL_NONFOIL_LINK_SETS: Final[frozenset[str]] = frozenset({
    "CN2", "FRF", "ONS", "10E", "UNH",
})
"""Sets where foil/non-foil versions have different card details."""

FUNNY_SETS_WITH_ACORN: Final[frozenset[str]] = frozenset({
    "UNF",
})
"""Funny sets where isFunny depends on securityStamp=acorn."""

class Expansion:
    """
    Umbrella class for expansion-related constants.
    """
    SetType: Final[Enum] = SetType

    DUEL_DECK_SETS: Final[frozenset[str]] = DUEL_DECK_SETS

    FOIL_NONFOIL_LINK_SETS: Final[frozenset[str]] = FOIL_NONFOIL_LINK_SETS

    FUNNY_SETS_WITH_ACORN: Final[frozenset[str]] = FUNNY_SETS_WITH_ACORN

