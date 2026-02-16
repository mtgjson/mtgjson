"""
Set-related constants.

Special set codes, set types, and set-specific behaviors.
"""

from __future__ import annotations

from typing import Final

# Set-specific behaviors

FUNNY_SETS_WITH_ACORN: Final[frozenset[str]] = frozenset(
    {
        "UNF",
    }
)
"""Funny sets where isFunny depends on securityStamp=acorn."""

SUPPORTED_SET_TYPES: Final[frozenset[str]] = frozenset(
    {
        "expansion",
        "core",
        "draft_innovation",
        "commander",
        "masters",
    }
)
"""Set types that are considered 'supported' for format legality checks."""
