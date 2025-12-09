"""Shared utilities for output formats."""

import json
from typing import Any, Iterator

import orjson
import polars as pl


def serialize_complex_types(df: pl.DataFrame) -> pl.DataFrame:
    """Convert List and Struct columns to JSON strings for SQL export."""
    schema = df.schema
    struct_cols = [c for c in df.columns if isinstance(schema[c], pl.Struct)]
    list_cols = [c for c in df.columns if isinstance(schema[c], pl.List)]

    if not struct_cols and not list_cols:
        return df

    result = df

    if struct_cols:
        struct_exprs = [
            pl.col(col_name).struct.json_encode().alias(col_name)
            for col_name in struct_cols
        ]
        result = result.with_columns(struct_exprs)

    if list_cols:
        for col_name in list_cols:
            result = result.with_columns(
                pl.col(col_name)
                .map_batches(_to_json_batch, return_dtype=pl.String)
                .alias(col_name)
            )

    return result


def _to_json_batch(series: pl.Series) -> pl.Series:
    """Batch convert Series to JSON strings using orjson."""
    return pl.Series(
        [
            orjson.dumps(x).decode("utf-8") if x is not None else None
            for x in series.to_list()
        ],
        dtype=pl.String,
    )


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


def escape_postgres(value: Any) -> str:
    """Escape value for PostgreSQL COPY format."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (int, float)):
        return str(value)
    s = json.dumps(value) if isinstance(value, (list, dict)) else str(value)
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
