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

ASCII_REPLACEMENTS: dict[str, str] = {
    "Æ": "AE",
    "æ": "ae",
    "Œ": "OE",
    "œ": "oe",
    "ß": "ss",
    "É": "E",
    "È": "E",
    "Ê": "E",
    "Ë": "E",
    "Á": "A",
    "À": "A",
    "Â": "A",
    "Ä": "A",
    "Ã": "A",
    "Í": "I",
    "Ì": "I",
    "Î": "I",
    "Ï": "I",
    "Ó": "O",
    "Ò": "O",
    "Ô": "O",
    "Ö": "O",
    "Õ": "O",
    "Ú": "U",
    "Ù": "U",
    "Û": "U",
    "Ü": "U",
    "Ý": "Y",
    "Ñ": "N",
    "Ç": "C",
    "é": "e",
    "è": "e",
    "ê": "e",
    "ë": "e",
    "á": "a",
    "à": "a",
    "â": "a",
    "ä": "a",
    "ã": "a",
    "í": "i",
    "ì": "i",
    "î": "i",
    "ï": "i",
    "ó": "o",
    "ò": "o",
    "ô": "o",
    "ö": "o",
    "õ": "o",
    "ú": "u",
    "ù": "u",
    "û": "u",
    "ü": "u",
    "ý": "y",
    "ÿ": "y",
    "ñ": "n",
    "ç": "c",
    "꞉": "",  # U+A789 modifier letter colon (ACR cards - Ratonhnhake:ton)
    "Š": "S",  # WC97/WC99 tokens (Šlemr)
    "š": "s",
    "®": "",  # UGL card (trademark symbol)
}

# CardMarket ID buffer for identifier generation
CARD_MARKET_BUFFER: Final[str] = "10101"
"""Buffer value used in CardMarket identifier calculations."""
