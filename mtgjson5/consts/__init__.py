"""
MTGJSON Constants Module.

Centralized constants for layouts, fields, expansions, languages, finishes, and mappings.
This is the single source of truth - other modules should import from here.

Usage:
    from mtgjson5.consts import LayoutVariant, MULTIFACE_LAYOUTS
    from mtgjson5.consts.fields import SORTED_LIST_FIELDS
"""

from __future__ import annotations

# Expansion/Set constants
from mtgjson5.consts.expansions import (
	DUEL_DECK_SETS,
	FOIL_NONFOIL_LINK_SETS,
	FUNNY_SETS_WITH_ACORN,
	Expansion,
	SetType,
)

# Field constants
from mtgjson5.consts.fields import (
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
	SORTED_LIST_FIELDS,
	SCRYFALL_COLUMNS_TO_DROP
)

# Finish constants
from mtgjson5.consts.finishes import FINISH_ORDER

# Language constants
from mtgjson5.consts.languages import LANGUAGE_MAP

# Layout constants
from mtgjson5.consts.layouts import (
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
from mtgjson5.consts.mappings import TYPEDDICT_FIELD_ALIASES


__all__ = [
	"ALLOW_IF_FALSEY",
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
