"""
V2 Pipeline Utilities.

Preprocessing and categorical discovery for the Polars pipeline.
"""

from mtgjson5.constants import BAD_FILE_NAMES

from .categoricals import DynamicCategoricals, discover_categoricals
from .preprocess import (
    ingest_from_orjson,
    ingest_scryfall_bulk,
    load_cards_to_context,
    normalize_column_names,
)


def get_windows_safe_set_code(code: str) -> str:
    """Return Windows-safe set code (appends _ to reserved names).

    Windows reserved names (CON, PRN, AUX, NUL, COM1-9, LPT1-9) cannot
    be used for files/directories. This function appends an underscore
    to make them safe.

    Args:
        code: The set code to sanitize

    Returns:
        The original code if safe, or code with underscore appended if reserved
    """
    return f"{code}_" if code in BAD_FILE_NAMES else code


__all__ = [
    "DynamicCategoricals",
    "discover_categoricals",
    "get_windows_safe_set_code",
    "ingest_from_orjson",
    "ingest_scryfall_bulk",
    "load_cards_to_context",
    "normalize_column_names",
]
