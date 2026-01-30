"""
Card-related constants.

Supertypes, basic land names, and other card classification constants.
"""

from __future__ import annotations

from typing import Final

SUPER_TYPES: Final[frozenset[str]] = frozenset(
    {"Basic", "Host", "Legendary", "Ongoing", "Snow", "World"}
)
"""Valid MTG supertypes."""

BASIC_LAND_NAMES: Final[frozenset[str]] = frozenset(
    {"Plains", "Island", "Swamp", "Mountain", "Forest"}
)
"""The five basic land card names."""

MULTI_WORD_SUB_TYPES: Final[frozenset[str]] = frozenset({"Time Lord"})
"""Subtypes that contain spaces."""
