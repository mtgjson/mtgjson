"""
Language constants and mappings.

Maps Scryfall language codes to human-readable names.
"""

from __future__ import annotations

from typing import Final


LANGUAGE_MAP: Final[dict[str, str]] = {
	"en": "English",
	"es": "Spanish",
	"fr": "French",
	"de": "German",
	"it": "Italian",
	"pt": "Portuguese (Brazil)",
	"ja": "Japanese",
	"ko": "Korean",
	"ru": "Russian",
	"zhs": "Chinese Simplified",
	"zht": "Chinese Traditional",
	"he": "Hebrew",
	"la": "Latin",
	"grc": "Ancient Greek",
	"ar": "Arabic",
	"sa": "Sanskrit",
	"ph": "Phyrexian",
}
"""Maps Scryfall language codes to full language names."""
