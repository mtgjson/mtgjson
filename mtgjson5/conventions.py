"""
MTGJSON field conventions and constants.

BACKWARD COMPATIBILITY SHIM - All constants are now defined in mtgjson5.consts.
This module re-exports them for backward compatibility. New code should import
directly from mtgjson5.consts or its submodules.

Example:
    # Preferred (new code)
    from mtgjson5.consts.fields import SORTED_LIST_FIELDS

    # Acceptable (backward compatible)
    from mtgjson5.conventions import SORTED_LIST_FIELDS
"""

from __future__ import annotations

# Re-export all field constants from mtgjson5.consts.fields
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
)

# Re-export language constants
from mtgjson5.consts.languages import LANGUAGE_MAP

# Re-export finish constants
from mtgjson5.consts.finishes import FINISH_ORDER

# Re-export mapping constants
from mtgjson5.consts.mappings import TYPEDDICT_FIELD_ALIASES


__all__ = [
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
	"LANGUAGE_MAP",
	"FINISH_ORDER",
	"TYPEDDICT_FIELD_ALIASES",
]
