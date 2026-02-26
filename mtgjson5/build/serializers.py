"""
MTGJSON serialization utilities.

Provides DataFrame-level cleaning and conversion to JSON-compatible formats.
Consolidates serialization logic from across the codebase.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import orjson
import polars as pl

# =============================================================================
# SQL Serialization
# =============================================================================


def normalize_optional_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Nullify optional fields for SQL/CSV/Parquet output.

    Applies the same MTGJSON conventions as prepare_cards_for_json:
    - Optional bool fields that are False become NULL
    - Empty lists in omit-empty-list fields become NULL
    """
    from mtgjson5.consts import OMIT_EMPTY_LIST_FIELDS, OPTIONAL_BOOL_FIELDS

    expressions: list[pl.Expr] = []
    schema = df.schema

    for field in OPTIONAL_BOOL_FIELDS:
        if field in schema and schema[field] == pl.Boolean:
            expressions.append(
                pl.when(pl.col(field) == True).then(True).otherwise(None).alias(field)  # pylint: disable=singleton-comparison
            )

    for field in OMIT_EMPTY_LIST_FIELDS:
        if field in schema and isinstance(schema[field], pl.List):
            expressions.append(
                pl.when(pl.col(field).list.len() == 0).then(None).otherwise(pl.col(field)).alias(field)
            )

    if expressions:
        df = df.with_columns(expressions)

    return df


def serialize_complex_types(df: pl.DataFrame) -> pl.DataFrame:
    """Convert List and Struct columns to SQL-compatible strings.

    Lists are converted to comma-separated strings (e.g., "Defender, Flying").
    Structs are converted to JSON strings with null values removed.
    """
    schema = df.schema
    struct_cols = [c for c in df.columns if isinstance(schema[c], pl.Struct)]
    list_cols = [c for c in df.columns if isinstance(schema[c], pl.List)]

    if not struct_cols and not list_cols:
        return df

    result = df

    if struct_cols:
        for col_name in struct_cols:
            result = result.with_columns(
                pl.col(col_name).map_batches(_struct_to_json_batch, return_dtype=pl.String).alias(col_name)
            )

    if list_cols:
        for col_name in list_cols:
            result = result.with_columns(
                pl.col(col_name).map_batches(_list_to_csv_batch, return_dtype=pl.String).alias(col_name)
            )

    return result


def _list_to_csv_batch(series: pl.Series) -> pl.Series:
    """Batch convert list Series to comma-separated strings."""
    return pl.Series(
        [", ".join(str(item) for item in x) if x is not None else None for x in series.to_list()],
        dtype=pl.String,
    )


def _struct_to_json_batch(series: pl.Series) -> pl.Series:
    """Batch convert struct Series to JSON strings, dropping null values."""
    return pl.Series(
        [_struct_to_json(x) if x is not None else None for x in series.to_list()],
        dtype=pl.String,
    )


def _struct_to_json(obj: dict[str, Any]) -> str:
    """Convert a struct/dict to JSON string, dropping null values recursively."""
    cleaned = _drop_nulls(obj)
    return orjson.dumps(cleaned).decode("utf-8")


def _drop_nulls(obj: Any) -> Any:
    """Recursively drop null values from dicts and empty lists."""
    if isinstance(obj, dict):
        return {k: _drop_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_drop_nulls(item) for item in obj]
    return obj


def escape_postgres(value: Any) -> str:
    """Escape value for PostgreSQL COPY format."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, int | float):
        return str(value)
    s = json.dumps(value) if isinstance(value, list | dict) else str(value)
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def escape_sqlite(value: Any) -> str:
    """Escape value for SQLite INSERT statement."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    s = json.dumps(value) if isinstance(value, list | dict) else str(value)
    return "'" + s.replace("'", "''") + "'"


def escape_mysql(value: Any) -> str:
    """Escape value for MySQL INSERT statement."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return str(value)
    s = json.dumps(value) if isinstance(value, list | dict) else str(value)
    # MySQL uses backslash escaping
    s = (
        s.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\0", "\\0")
    )
    return "'" + s + "'"


def batched(iterable: Any, n: int) -> Iterator[list[Any]]:
    """Yield batches of n items."""
    batch: list[Any] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch
    return []  # type: ignore[return-value]
