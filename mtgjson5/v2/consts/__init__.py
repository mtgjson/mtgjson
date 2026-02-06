"""
MTGJSON Constants Module.

Centralized constants for layouts, fields, expansions, languages, finishes, and mappings.
This is the single source of truth - other modules should import from here.

Usage:
    from mtgjson5.v2.consts import LayoutVariant, MULTIFACE_LAYOUTS
    from mtgjson5.v2.consts.fields import SORTED_LIST_FIELDS
"""

from __future__ import annotations

# Card constants
from .cards import (
    BASIC_LAND_NAMES,
    MULTI_WORD_SUB_TYPES,
    SUPER_TYPES,
)

# Expansion/Set constants
from .expansions import (
    DUEL_DECK_SETS,
    FOIL_NONFOIL_LINK_SETS,
    FUNNY_SETS_WITH_ACORN,
    SUPPORTED_SET_TYPES,
    Expansion,
    SetType,
)

# Field constants
from .fields import (
    ALLOW_IF_FALSEY,
    EXCLUDE_FROM_OUTPUT,
    IDENTIFIERS_FIELD_SOURCES,
    OMIT_EMPTY_LIST_FIELDS,
    OMIT_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_CARD_LIST_FIELDS,
    REQUIRED_DECK_LIST_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SCRYFALL_COLUMNS_TO_DROP,
    SORTED_LIST_FIELDS,
)

# Finish constants
from .finishes import FINISH_ORDER

# Language constants
from .languages import LANGUAGE_MAP

# Layout constants
from .layouts import (
    FACE_MANA_VALUE_LAYOUTS,
    FACE_NAME_LAYOUTS,
    MULTIFACE_LAYOUTS,
    SPLIT_COLOR_LAYOUTS,
    SPLIT_WATERMARK_LAYOUTS,
    TOKEN_LAYOUTS,
    LayoutVariant,
    is_multiface,
    is_token_layout,
)

# Mapping constants
from .mappings import CARD_MARKET_BUFFER, TYPEDDICT_FIELD_ALIASES

# Output constants
from .outputs import (
    ALL_CSVS_DIRECTORY,
    ALL_DECKS_DIRECTORY,
    ALL_PARQUETS_DIRECTORY,
    ALL_SETS_DIRECTORY,
)

__all__ = [
    "ALLOW_IF_FALSEY",
    # Output directories
    "ALL_CSVS_DIRECTORY",
    "ALL_DECKS_DIRECTORY",
    "ALL_PARQUETS_DIRECTORY",
    "ALL_SETS_DIRECTORY",
    # Cards
    "BASIC_LAND_NAMES",
    "CARD_MARKET_BUFFER",
    "DUEL_DECK_SETS",
    "EXCLUDE_FROM_OUTPUT",
    "FACE_MANA_VALUE_LAYOUTS",
    "FACE_NAME_LAYOUTS",
    # Finishes
    "FINISH_ORDER",
    "FOIL_NONFOIL_LINK_SETS",
    "FUNNY_SETS_WITH_ACORN",
    "IDENTIFIERS_FIELD_SOURCES",
    # Languages
    "LANGUAGE_MAP",
    "MULTIFACE_LAYOUTS",
    "MULTI_WORD_SUB_TYPES",
    "OMIT_EMPTY_LIST_FIELDS",
    "OMIT_FIELDS",
    "OPTIONAL_BOOL_FIELDS",
    "OTHER_OPTIONAL_FIELDS",
    "REQUIRED_CARD_LIST_FIELDS",
    "REQUIRED_DECK_LIST_FIELDS",
    "REQUIRED_LIST_FIELDS",
    "REQUIRED_SET_BOOL_FIELDS",
    # Fields
    "SCRYFALL_COLUMNS_TO_DROP",
    "SORTED_LIST_FIELDS",
    "SPLIT_COLOR_LAYOUTS",
    "SPLIT_WATERMARK_LAYOUTS",
    "SUPER_TYPES",
    "SUPPORTED_SET_TYPES",
    "TOKEN_LAYOUTS",
    # Mappings
    "TYPEDDICT_FIELD_ALIASES",
    # Expansions
    "Expansion",
    # Layouts
    "LayoutVariant",
    "SetType",
    "is_multiface",
    "is_token_layout",
]
