"""
MTGJSON Constants Module.

Centralized constants for layouts, fields, expansions, languages, finishes, and mappings.
This is the single source of truth - other modules should import from here.

Usage:
    from mtgjson5.consts import LayoutVariant, MULTIFACE_LAYOUTS
    from mtgjson5.consts.fields import SORTED_LIST_FIELDS
"""

from __future__ import annotations

# Layout constants
from mtgjson5.consts.layouts import (
	LayoutVariant,
	MULTIFACE_LAYOUTS,
	FACE_NAME_LAYOUTS,
	FACE_MANA_VALUE_LAYOUTS,
	TOKEN_LAYOUTS,
	SPLIT_WATERMARK_LAYOUTS,
	SPLIT_COLOR_LAYOUTS,
	is_multiface,
	is_token_layout,
)

# Expansion/Set constants
from mtgjson5.consts.expansions import (
	Expansion,
	SetType,
	DUEL_DECK_SETS,
	FOIL_NONFOIL_LINK_SETS,
	FUNNY_SETS_WITH_ACORN,
)

# Field constants
from mtgjson5.consts.fields import (
	SORTED_LIST_FIELDS,
	REQUIRED_CARD_LIST_FIELDS,
	REQUIRED_DECK_LIST_FIELDS,
	REQUIRED_LIST_FIELDS,
	OMIT_EMPTY_LIST_FIELDS,
	OPTIONAL_BOOL_FIELDS,
	REQUIRED_SET_BOOL_FIELDS,
	OTHER_OPTIONAL_FIELDS,
	EXCLUDE_FROM_OUTPUT,
	OMIT_FIELDS,
	ALLOW_IF_FALSEY,
	IDENTIFIERS_FIELD_SOURCES,
)

# Language constants
from mtgjson5.consts.languages import LANGUAGE_MAP

# Finish constants
from mtgjson5.consts.finishes import FINISH_ORDER

# Mapping constants
from mtgjson5.consts.mappings import TYPEDDICT_FIELD_ALIASES


__all__ = [
	# Layouts
	"LayoutVariant",
	"MULTIFACE_LAYOUTS",
	"FACE_NAME_LAYOUTS",
	"FACE_MANA_VALUE_LAYOUTS",
	"TOKEN_LAYOUTS",
	"SPLIT_WATERMARK_LAYOUTS",
	"SPLIT_COLOR_LAYOUTS",
	"is_multiface",
	"is_token_layout",
	# Expansions
	"Expansion",
	"SetType",
	"DUEL_DECK_SETS",
	"FOIL_NONFOIL_LINK_SETS",
	"FUNNY_SETS_WITH_ACORN",
	# Fields
	"SORTED_LIST_FIELDS",
	"REQUIRED_CARD_LIST_FIELDS",
	"REQUIRED_DECK_LIST_FIELDS",
	"REQUIRED_LIST_FIELDS",
	"OMIT_EMPTY_LIST_FIELDS",
	"OPTIONAL_BOOL_FIELDS",
	"REQUIRED_SET_BOOL_FIELDS",
	"OTHER_OPTIONAL_FIELDS",
	"EXCLUDE_FROM_OUTPUT",
	"OMIT_FIELDS",
	"ALLOW_IF_FALSEY",
	"IDENTIFIERS_FIELD_SOURCES",
	# Languages
	"LANGUAGE_MAP",
	# Finishes
	"FINISH_ORDER",
	# Mappings
	"TYPEDDICT_FIELD_ALIASES",
]
