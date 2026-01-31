"""
V2 Pipeline Utilities.

Preprocessing and categorical discovery for the Polars pipeline.
"""

from .categoricals import DynamicCategoricals, discover_categoricals
from .preprocess import (
    ingest_from_orjson,
    ingest_scryfall_bulk,
    load_cards_to_context,
    normalize_column_names,
)

__all__ = [
    "DynamicCategoricals",
    "discover_categoricals",
    "ingest_from_orjson",
    "ingest_scryfall_bulk",
    "load_cards_to_context",
    "normalize_column_names",
]
