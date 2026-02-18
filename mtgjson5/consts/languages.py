"""
Language constants and mappings.

Maps Scryfall language codes to human-readable names.
"""

from __future__ import annotations

from typing import Final

LANGUAGE_MAP: Final[dict[str, str]] = {
    "grc": "Ancient Greek",
    "ar": "Arabic",
    "zhs": "Chinese Simplified",
    "zht": "Chinese Traditional",
    "en": "English",
    "fr": "French",
    "de": "German",
    "he": "Hebrew",
    "it": "Italian",
    "ja": "Japanese",
    "ko": "Korean",
    "la": "Latin",
    "ph": "Phyrexian",
    "px": "Phyrexian",  # Alternate code
    "pt": "Portuguese (Brazil)",
    "qya": "Quenya",
    "ru": "Russian",
    "sa": "Sanskrit",
    "es": "Spanish",
}
"""Maps Scryfall language codes to full language names."""
