"""
Schema utilities for converting Pydantic models to Polars schemas.

Provides functions to:
- Generate Polars schemas from Pydantic models
- Apply schemas to DataFrames (select, reorder, cast)
- Validate DataFrames against expected schemas
"""

from typing import Any, Type, Union, get_args, get_origin

import polars as pl
from pydantic import BaseModel


def pydantic_type_to_polars(py_type: Any) -> pl.DataType:
    """
    Convert a Python/Pydantic type annotation to a Polars DataType.

    Args:
        py_type: Python type annotation (str, int, list[str], etc.)

    Returns:
        Corresponding Polars DataType
    """
    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Optional (Union with None)
    if origin is Union:
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return pydantic_type_to_polars(non_none_args[0])
        return pl.String()  # Fallback for complex unions

    # Handle List types
    if origin is list:
        if args:
            inner_type = pydantic_type_to_polars(args[0])
            # If inner type is a Pydantic model, use Struct
            if isinstance(args[0], type) and issubclass(args[0], BaseModel):
                return pl.List(pydantic_model_to_struct(args[0]))
            return pl.List(inner_type)
        return pl.List(pl.String())

    # Handle dict types (serialize as JSON string or Struct)
    if origin is dict:
        return pl.String()  # JSON serialized

    # Handle Pydantic BaseModel (nested struct)
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return pydantic_model_to_struct(py_type)

    # Primitive type mapping
    type_map = {
        str: pl.String(),
        int: pl.Int64(),
        float: pl.Float64(),
        bool: pl.Boolean(),
        bytes: pl.Binary(),
    }

    return type_map.get(py_type, pl.String())


def pydantic_model_to_struct(model: Type[BaseModel]) -> pl.Struct:
    """
    Convert a Pydantic model to a Polars Struct type.

    Args:
        model: Pydantic BaseModel class

    Returns:
        Polars Struct type matching the model fields
    """
    fields = []
    for field_name, field_info in model.model_fields.items():
        polars_type = pydantic_type_to_polars(field_info.annotation)
        fields.append(pl.Field(field_name, polars_type))
    return pl.Struct(fields)


def pydantic_model_to_schema(model: Type[BaseModel]) -> dict[str, pl.DataType]:
    """
    Convert a Pydantic model to a Polars schema dict.

    Args:
        model: Pydantic BaseModel class

    Returns:
        Dict mapping field names to Polars DataTypes
    """
    schema = {}
    for field_name, field_info in model.model_fields.items():
        schema[field_name] = pydantic_type_to_polars(field_info.annotation)
    return schema


def get_model_columns(model: Type[BaseModel]) -> list[str]:
    """
    Get ordered list of column names from a Pydantic model.

    Args:
        model: Pydantic BaseModel class

    Returns:
        List of field names in definition order
    """
    return list(model.model_fields.keys())


def get_required_columns(model: Type[BaseModel]) -> list[str]:
    """
    Get list of required (non-optional) columns from a Pydantic model.

    Args:
        model: Pydantic BaseModel class

    Returns:
        List of required field names
    """
    required = []
    for field_name, field_info in model.model_fields.items():
        # Use Pydantic's is_required() method for accurate detection
        if field_info.is_required():
            required.append(field_name)

    return required


def apply_schema(
    df: pl.DataFrame | pl.LazyFrame,
    model: Type[BaseModel],
    strict: bool = False,
    fill_missing: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Apply a Pydantic model schema to a Polars DataFrame.

    This will:
    1. Select only columns defined in the model
    2. Reorder columns to match model field order
    3. Cast columns to expected types
    4. Optionally fill missing columns with nulls

    Args:
        df: Input DataFrame or LazyFrame
        model: Pydantic BaseModel defining expected schema
        strict: If True, raise error for missing required columns
        fill_missing: If True, add missing optional columns as null

    Returns:
        DataFrame/LazyFrame with columns matching model schema
    """
    schema = pydantic_model_to_schema(model)
    model_columns = get_model_columns(model)
    required_columns = get_required_columns(model)

    # Get existing columns
    existing_cols = set(
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )

    # Check for missing required columns
    if strict:
        missing_required = set(required_columns) - existing_cols
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")

    # Build column expressions
    exprs = []
    for col_name in model_columns:
        expected_type = schema[col_name]

        if col_name in existing_cols:
            # Cast existing column to expected type
            exprs.append(pl.col(col_name).cast(expected_type).alias(col_name))
        elif fill_missing:
            # Add null column with expected type
            exprs.append(pl.lit(None).cast(expected_type).alias(col_name))

    return df.select(exprs)


def validate_schema(
    df: pl.DataFrame | pl.LazyFrame,
    model: Type[BaseModel],
) -> tuple[bool, list[str]]:
    """
    Validate that a DataFrame matches a Pydantic model schema.

    Args:
        df: DataFrame to validate
        model: Expected Pydantic model schema

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    schema = pydantic_model_to_schema(model)
    required_columns = get_required_columns(model)

    df_schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema

    # Check for missing required columns
    for col in required_columns:
        if col not in df_schema:
            errors.append(f"Missing required column: {col}")

    # Check column types
    for col_name, expected_type in schema.items():
        if col_name in df_schema:
            actual_type = df_schema[col_name]
            # Allow compatible types (e.g., Int32 for Int64)
            if not _types_compatible(actual_type, expected_type):
                errors.append(
                    f"Column '{col_name}' type mismatch: "
                    f"expected {expected_type}, got {actual_type}"
                )

    return len(errors) == 0, errors


def _types_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    """Check if actual type is compatible with expected type."""
    from typing import cast

    # Same type
    if actual == expected:
        return True

    # Numeric compatibility
    numeric_types = (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    )
    if isinstance(actual, type(expected)) and type(actual) in [
        type(t) for t in numeric_types
    ]:
        return True

    # List compatibility (check inner type)
    if isinstance(actual, pl.List) and isinstance(expected, pl.List):
        actual_inner = cast(Any, actual).inner
        expected_inner = cast(Any, expected).inner
        return _types_compatible(actual_inner, expected_inner)

    # Struct compatibility (check field types)
    if isinstance(actual, pl.Struct) and isinstance(expected, pl.Struct):
        actual_fields = {f.name: cast(Any, f).dtype for f in cast(Any, actual).fields}
        expected_fields = {
            f.name: cast(Any, f).dtype for f in cast(Any, expected).fields
        }
        for name, exp_type in expected_fields.items():
            if name in actual_fields:
                if not _types_compatible(actual_fields[name], exp_type):
                    return False
        return True

    # Null is compatible with anything
    if actual == pl.Null:
        return True

    return False


def struct_from_model(model: Type[BaseModel]) -> pl.Expr:
    """
    Create a Polars struct expression from a Pydantic model.

    Useful for reshaping list columns to match expected nested structure.

    Args:
        model: Pydantic BaseModel class

    Returns:
        Polars struct expression
    """
    fields = []
    for field_name in model.model_fields:
        fields.append(pl.col(field_name))
    return pl.struct(fields)


def reshape_list_column(
    col_name: str,
    model: Type[BaseModel],
    alias: str | None = None,
) -> pl.Expr:
    """
    Reshape a list column to match a Pydantic model struct.

    Extracts only the fields defined in the model from each struct in the list.

    Args:
        col_name: Name of the list column
        model: Pydantic model defining expected struct fields
        alias: Optional alias for the resulting column

    Returns:
        Polars expression that reshapes the list column
    """
    field_names = list(model.model_fields.keys())

    expr = pl.col(col_name).list.eval(
        pl.struct([pl.element().struct.field(f) for f in field_names])
    )

    if alias:
        expr = expr.alias(alias)

    return expr


def reshape_deck_cards(col_name: str, alias: str | None = None) -> pl.Expr:
    """
    Reshape a deck card list column to only include count and uuid fields.

    Removes fields like is_foil, is_etched from deck card structs.

    Args:
        col_name: Name of the list column containing card structs
        alias: Optional alias for the resulting column (defaults to col_name)

    Returns:
        Polars expression that reshapes the card list
    """
    expr = pl.col(col_name).list.eval(
        pl.struct(
            [
                pl.element().struct.field("count"),
                pl.element().struct.field("uuid"),
            ]
        )
    )

    if alias:
        expr = expr.alias(alias)
    else:
        expr = expr.alias(col_name)

    return expr
