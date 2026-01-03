"""
Field mapping constants.

Defines field name transformations between source data and MTGJSON output.
"""

from __future__ import annotations

from typing import Final


# TypedDict field aliases for pipeline -> model conversion
# Maps (TypedDict_name, source_field) -> target_field
TYPEDDICT_FIELD_ALIASES: Final[dict[tuple[str, str], str]] = {
	# Rulings: Scryfall uses publishedAt/comment, MTGJSON uses date/text
	("Rulings", "publishedAt"): "date",
	("Rulings", "comment"): "text",
}
"""Maps (TypedDict name, source field) to target MTGJSON field name."""
