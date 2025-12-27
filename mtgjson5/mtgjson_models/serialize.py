"""
MTGJSON serialization utilities.

Provides DataFrame-level cleaning and conversion to JSON-compatible formats.
Replaces the legacy serialize.py with model-aware serialization.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .constants import (
    EXCLUDE_FROM_OUTPUT,
    OMIT_EMPTY_LIST_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_LIST_FIELDS,
    REQUIRED_SET_BOOL_FIELDS,
    SORTED_LIST_FIELDS,
)


if TYPE_CHECKING:
    from polars import DataFrame, Expr

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore


# =============================================================================
# DataFrame Preparation
# =============================================================================

def prepare_cards_for_json(df: DataFrame) -> DataFrame:
    """
    Clean DataFrame for JSON serialization.

    Applies MTGJSON conventions:
    - Fill null with [] for required list fields
    - Nullify empty lists for omit fields
    - Nullify False for optional bool fields
    - Nullify empty strings/zero values for optional fields
    - Drop internal columns (prefixed with _)

    Args:
        df: Cards DataFrame

    Returns:
        Cleaned DataFrame ready for serialization
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars required")

    expressions: list[Expr] = []

    # Fill null with [] for required list fields
    for field in REQUIRED_LIST_FIELDS:
        if field in df.columns:
            expressions.append(pl.col(field).fill_null([]).alias(field))

    # Nullify empty lists for omit fields
    for field in OMIT_EMPTY_LIST_FIELDS:
        if field in df.columns:
            expressions.append(
                pl.when(pl.col(field).list.len() == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )

    # Nullify False for optional bool fields
    for field in OPTIONAL_BOOL_FIELDS:
        if field in df.columns:
            expressions.append(
                pl.when(pl.col(field) == True)  # noqa: E712
                .then(True)
                .otherwise(None)
                .alias(field)
            )

    # Handle other optional fields by type
    for field in OTHER_OPTIONAL_FIELDS:
        if field not in df.columns:
            continue

        col_type = df.schema[field]

        if col_type == pl.String or col_type == pl.Utf8:
            # Empty strings to null
            expressions.append(
                pl.when(pl.col(field) == "")
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )
        elif isinstance(col_type, pl.List):
            # Empty lists to null
            expressions.append(
                pl.when(pl.col(field).list.len() == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )
        elif isinstance(col_type, pl.Struct):
            # Clean struct sub-fields
            struct_fields = col_type.fields
            cleaned_fields = []

            for sf in struct_fields:
                sf_expr = pl.col(field).struct.field(sf.name)

                if sf.dtype == pl.String or sf.dtype == pl.Utf8:
                    cleaned_fields.append(
                        pl.when(sf_expr == "")
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf.name)
                    )
                elif isinstance(sf.dtype, pl.List):
                    cleaned_fields.append(
                        pl.when(sf_expr.list.len() == 0)
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf.name)
                    )
                else:
                    cleaned_fields.append(sf_expr.alias(sf.name))

            # Rebuild struct, preserve null if original was null
            expressions.append(
                pl.when(pl.col(field).is_null())
                .then(None)
                .otherwise(pl.struct(cleaned_fields))
                .alias(field)
            )
        elif col_type in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            # Nullify zero values
            expressions.append(
                pl.when(pl.col(field) == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )

    # Apply all transformations
    if expressions:
        df = df.with_columns(expressions)

    # Drop internal columns
    df = df.select([c for c in df.columns if not c.startswith("_")])

    return df


# =============================================================================
# DataFrame to List Conversion
# =============================================================================

def dataframe_to_cards_list(
    df: DataFrame,
    sort_by: tuple[str, ...] = ("number", "side"),
    use_model: type | None = None,
) -> list[dict[str, Any]]:
    """
    Convert cards DataFrame to cleaned list of dicts.

    Args:
        df: Cards DataFrame
        sort_by: Columns to sort by (default: number, side)
        use_model: Optional Pydantic model class for serialization.
                   If provided, uses model's to_polars_dict method.

    Returns:
        List of cleaned card dictionaries
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars required")

    # Sort for consistent output
    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        if "number" in sort_cols:
            df = df.with_columns(pl.col("number").str.zfill(10).alias("_sort_num"))
            sort_cols = ["_sort_num" if c == "number" else c for c in sort_cols]
        df = df.sort(sort_cols, nulls_last=True)
        if "_sort_num" in df.columns:
            df = df.drop("_sort_num")

    # Use model serialization if provided
    if use_model is not None:
        models = use_model.from_dataframe(df)
        return [m.to_polars_dict(exclude_none=True) for m in models]

    # Otherwise use DataFrame-level cleaning + clean_nested
    df = prepare_cards_for_json(df)
    raw_dicts = df.to_dicts()
    return [clean_nested(card, omit_empty=True) for card in raw_dicts]


# =============================================================================
# Recursive Cleaning
# =============================================================================

def clean_nested(
    obj: Any,
    omit_empty: bool = True,
    field_handlers: dict[str, Callable[[Any], Any]] | None = None,
    current_path: str = "",
) -> Any:
    """
    Recursively clean any nested structure.

    Args:
        obj: Any Python object to clean
        omit_empty: If True, omit empty lists/dicts/None values
        field_handlers: Dict of field_path -> handler_function
        current_path: Internal tracking of nested path

    Returns:
        Cleaned version of the input object
    """
    if obj is None:
        return None

    # Handle dictionaries
    if isinstance(obj, dict):
        result: dict[str, Any] = {}

        for key, value in sorted(obj.items()):  # Sort keys
            # Skip excluded fields
            if key in EXCLUDE_FROM_OUTPUT:
                continue

            field_path = f"{current_path}.{key}" if current_path else key

            # Custom handler
            if field_handlers and field_path in field_handlers:
                cleaned_value = field_handlers[field_path](value)
            else:
                cleaned_value = clean_nested(
                    value,
                    omit_empty=omit_empty,
                    field_handlers=field_handlers,
                    current_path=field_path,
                )

            # Omit None, but keep required list fields as []
            if cleaned_value is None and omit_empty:
                if key in REQUIRED_LIST_FIELDS:
                    result[key] = []
                # Keep legalities as {} instead of None
                elif key == "legalities":
                    result[key] = {}
                continue

            # Omit False for optional bool fields (except required set-level booleans)
            if (omit_empty and key in OPTIONAL_BOOL_FIELDS
                and cleaned_value is False and key not in REQUIRED_SET_BOOL_FIELDS):
                continue

            # Omit empty collections (except required list fields and legalities)
            if omit_empty and isinstance(cleaned_value, dict | list) and not cleaned_value:
                if isinstance(cleaned_value, list) and key in REQUIRED_LIST_FIELDS:
                    pass  # Keep empty required list
                elif isinstance(cleaned_value, dict) and key == "legalities":
                    result[key] = {}  # Keep empty legalities as {}
                else:
                    continue

            result[key] = cleaned_value

        return result if result or not omit_empty else None

    # Handle lists
    if isinstance(obj, list):
        result_list: list[Any] = []

        for item in obj:
            cleaned_item = clean_nested(
                item,
                omit_empty=omit_empty,
                field_handlers=field_handlers,
                current_path=current_path,
            )

            if cleaned_item is None and omit_empty:
                continue

            result_list.append(cleaned_item)

        # Sort list if it's a sortable field
        field_name = current_path.split(".")[-1] if current_path else ""
        if field_name in SORTED_LIST_FIELDS and result_list:
            with contextlib.suppress(TypeError):
                result_list = sorted(result_list)
        # Sort rulings by date (desc), then text
        elif field_name == "rulings" and result_list and isinstance(result_list[0], dict):
            result_list = sorted(result_list, key=lambda r: (r.get("date", ""), r.get("text", "")))

        if not result_list and omit_empty and field_name not in REQUIRED_LIST_FIELDS:
            return None

        return result_list

    # Handle tuples -> list
    if isinstance(obj, tuple):
        return clean_nested(list(obj), omit_empty, field_handlers, current_path)

    # Handle sets -> sorted list
    if isinstance(obj, set):
        return clean_nested(sorted(obj), omit_empty, field_handlers, current_path)

    # Primitives pass through
    return obj


# =============================================================================
# Convenience Functions
# =============================================================================

def cards_to_json(
    df: DataFrame,
    model: type | None = None,
    sort_by: tuple[str, ...] = ("number", "side"),
) -> list[dict[str, Any]]:
    """
    Convert cards DataFrame to JSON-ready list using model or DataFrame cleaning.

    This is the main entry point for serializing cards.

    Args:
        df: Cards DataFrame
        model: Optional model class (CardSet, CardToken, etc.)
        sort_by: Sort columns

    Returns:
        List of cleaned card dicts
    """
    return dataframe_to_cards_list(df, sort_by=sort_by, use_model=model)


def tokens_to_json(
    df: DataFrame,
    sort_by: tuple[str, ...] = ("number", "side"),
) -> list[dict[str, Any]]:
    """Convert tokens DataFrame to JSON-ready list."""
    from .cards import CardToken
    return dataframe_to_cards_list(df, sort_by=sort_by, use_model=CardToken)


def atomic_to_json(
    df: DataFrame,
    sort_by: tuple[str, ...] = ("name",),
) -> list[dict[str, Any]]:
    """Convert atomic cards DataFrame to JSON-ready list."""
    from .cards import CardAtomic
    return dataframe_to_cards_list(df, sort_by=sort_by, use_model=CardAtomic)
