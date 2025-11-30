"""
Fast JSON serialization for MTGJSON data.

Uses Polars to_dicts() + orjson for ~500 MB/s throughput.
"""

from pathlib import Path
from typing import Any, FrozenSet

import orjson
import polars as pl

# Fields where empty list should be present in output
REQUIRED_LIST_FIELDS: FrozenSet[str] = frozenset({
    "availability", "boosterTypes", "colorIdentity", "colors", "finishes",
    "frameEffects", "keywords", "printings", "promoTypes", "subtypes",
    "supertypes", "types",
})

# Fields where empty list should be OMITTED
OMIT_EMPTY_LIST_FIELDS: FrozenSet[str] = frozenset({
    "artistIds", "attractionLights", "cardParts", "foreignData",
    "originalPrintings", "otherFaceIds", "rebalancedPrintings",
    "reverseRelated", "rulings", "subsets", "variations",
})

# Optional boolean fields - omit unless True
OPTIONAL_BOOL_FIELDS: FrozenSet[str] = frozenset({
    "hasAlternativeDeckLimit", "hasContentWarning", "isAlternative",
    "isFullArt", "isFunny", "isGameChanger", "isOnlineOnly", "isOversized",
    "isPromo", "isRebalanced", "isReprint", "isReserved", "isStarter",
    "isStorySpotlight", "isTextless", "isTimeshifted",
})


def _clean_struct(d: dict[str, Any] | None) -> dict[str, Any] | None:
    """Remove None values from a struct dict."""
    if not d:
        return None
    cleaned = {k: v for k, v in d.items() if v is not None}
    return cleaned if cleaned else None


def _clean_foreign_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Clean a foreignData entry, including nested identifiers."""
    result = {}
    for k, v in entry.items():
        if v is None:
            continue
        if k == "identifiers":
            cleaned_ids = _clean_struct(v)
            if cleaned_ids:
                result[k] = cleaned_ids
        else:
            result[k] = v
    return result


def clean_card(card: dict[str, Any]) -> dict[str, Any]:
    """
    Clean a single card dict for JSON output.
    
    - Omits None values
    - Handles empty list semantics
    - Cleans nested structs
    - Handles optional booleans
    """
    result: dict[str, Any] = {}
    
    for key, value in card.items():
        # Skip internal columns
        if key.startswith("_"):
            continue
        
        # Handle None
        if value is None:
            if key in REQUIRED_LIST_FIELDS:
                result[key] = []
            # Everything else omitted
            continue
        
        # Handle lists
        if isinstance(value, list):
            if len(value) == 0:
                if key in REQUIRED_LIST_FIELDS:
                    result[key] = []
                continue
            
            # Special handling for foreignData
            if key == "foreignData":
                result[key] = [_clean_foreign_entry(e) for e in value]
            else:
                result[key] = value
            continue
        
        # Handle dicts (structs)
        if isinstance(value, dict):
            if key in {"identifiers", "legalities", "purchaseUrls"}:
                cleaned = _clean_struct(value)
                if cleaned:
                    result[key] = cleaned
            elif key == "leadershipSkills":
                # Include if any value is True
                if any(value.values()):
                    result[key] = value
            elif key == "sourceProducts":
                # Remove empty arrays
                cleaned = {k: v for k, v in value.items() if v}
                if cleaned:
                    result[key] = cleaned
            elif key == "relatedCards":
                cleaned = {k: v for k, v in value.items() if v}
                if cleaned:
                    result[key] = cleaned
            else:
                cleaned = _clean_struct(value)
                if cleaned:
                    result[key] = cleaned
            continue
        
        # Handle booleans
        if key in OPTIONAL_BOOL_FIELDS:
            if value is True:
                result[key] = True
            continue
        
        if key in {"hasFoil", "hasNonFoil"}:
            result[key] = bool(value)
            continue
        
        # Default: include non-None values
        result[key] = value
    
    return result


def dataframe_to_cards_list(
    df: pl.DataFrame,
    sort_by: tuple[str, ...] = ("number", "side"),
) -> list[dict[str, Any]]:
    """
    Convert cards DataFrame to cleaned list of dicts.
    
    This replaces dataframe_to_card_objects entirely.
    """
    # Sort for consistent output
    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        if "number" in sort_cols:
            # Natural sort for collector numbers
            df = df.with_columns(
                pl.col("number").str.zfill(10).alias("_sort_num")
            )
            sort_cols = ["_sort_num" if c == "number" else c for c in sort_cols]
        df = df.sort(sort_cols, nulls_last=True)
        if "_sort_num" in df.columns:
            df = df.drop("_sort_num")
    
    # Fast path: Polars to_dicts() is implemented in Rust
    raw_cards = df.to_dicts()
    
    # Clean each card
    return [clean_card(card) for card in raw_cards]


def write_json(
    data: Any,
    path: Path,
    indent: bool = True,
    sort_keys: bool = True,
) -> None:
    """Write data to JSON using orjson."""
    opts = 0
    if indent:
        opts |= orjson.OPT_INDENT_2
    if sort_keys:
        opts |= orjson.OPT_SORT_KEYS
    
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(data, option=opts))


def build_set_dict(
    cards_df: pl.DataFrame,
    tokens_df: pl.DataFrame,
    set_code: str,
    set_name: str,
    set_type: str,
    release_date: str,
    base_set_size: int,
    keyrune_code: str,
    **extra: Any,
) -> dict[str, Any]:
    """Build complete set dict ready for JSON serialization."""
    cards = dataframe_to_cards_list(cards_df)
    tokens = dataframe_to_cards_list(tokens_df) if not tokens_df.is_empty() else []
    
    set_dict: dict[str, Any] = {
        "baseSetSize": base_set_size,
        "cards": cards,
        "code": set_code.upper(),
        "isFoilOnly": extra.pop("is_foil_only", False),
        "isOnlineOnly": extra.pop("is_online_only", False),
        "keyruneCode": keyrune_code,
        "name": set_name,
        "releaseDate": release_date,
        "tokens": tokens,
        "totalSetSize": len(cards),
        "type": set_type,
    }
    
    # Add optional fields
    if extra.pop("is_non_foil_only", False):
        set_dict["isNonFoilOnly"] = True
    
    # Add remaining extras (translations, booster, etc.)
    for key, value in extra.items():
        if value is not None and value != "" and value != []:
            set_dict[key] = value
    
    return dict(sorted(set_dict.items()))