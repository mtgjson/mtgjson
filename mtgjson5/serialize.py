"""
Fast JSON serialization for MTGJSON data.

This module provides functions to clean and serialize MTGJSON data
structures into JSON-compatible formats, handling optional fields,
empty values, and nested structures efficiently.
"""

from typing import Any, Callable, FrozenSet

import polars as pl

from mtgjson5.models.schema.mtgjson import REQUIRED_DECK_LIST_FIELDS

# Fields where empty list should be present in output (card-level)
_REQUIRED_CARD_LIST_FIELDS: FrozenSet[str] = frozenset(
    {
        "availability",
        "colorIdentity",
        "colors",
        "finishes",
        "foreignData",
        "printings",
        "subtypes",
        "supertypes",
        "types",
    }
)

# Combined: card fields + deck structure fields
REQUIRED_LIST_FIELDS: FrozenSet[str] = (
    _REQUIRED_CARD_LIST_FIELDS | REQUIRED_DECK_LIST_FIELDS
)

# Fields where empty list should be OMITTED
OMIT_EMPTY_LIST_FIELDS: FrozenSet[str] = frozenset(
    {
        "artistIds",
        "attractionLights",
        "boosterTypes",
        "cardParts",
        "frameEffects",
        "keywords",
        "originalPrintings",
        "otherFaceIds",
        "promoTypes",
        "rebalancedPrintings",
        "reverseRelated",
        "rulings",
        "subsets",
        "variations",
        "sealedProductUuids",
    }
)

# Optional boolean fields - omit unless True
OPTIONAL_BOOL_FIELDS: FrozenSet[str] = frozenset(
    {
        "hasAlternativeDeckLimit",
        "hasContentWarning",
        "isAlternative",
        "isFullArt",
        "isFunny",
        "isGameChanger",
        "isOnlineOnly",
        "isOversized",
        "isPromo",
        "isRebalanced",
        "isReprint",
        "isReserved",
        "isStarter",
        "isStorySpotlight",
        "isTextless",
        "isTimeshifted",
    }
)

# Other fields to clean up - omit if empty/null
OTHER_OPTIONAL_FIELDS: FrozenSet[str] = frozenset(
    {
        "asciiName",
        "colorIndicator",
        "defense",
        "duelDeck",
        "edhrecSaltiness",  # Omit when 0 or null
        "faceConvertedManaCost",
        "faceManaValue",
        "faceName",
        "flavorName",
        "hand",
        "leadershipSkills",
        "life",
        "loyalty",
        "manaCost",  # Omit empty string
        "power",
        "printedName",
        "side",  # Only present for multi-face cards
        "printedType",
        "relatedCards",
        "securityStamp",
        "sourceProducts",
        "toughness",
        "watermark",
    }
)

# Combined set of all fields to omit/clean
OMIT_FIELDS: FrozenSet[str] = frozenset(
    OPTIONAL_BOOL_FIELDS.union(OMIT_EMPTY_LIST_FIELDS.union(OTHER_OPTIONAL_FIELDS))
)


def prepare_cards_for_json(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans frame, drops nulls/empty/false fields"""
    expressions = []

    # replace nulls with empty list - makes sense
    for field in REQUIRED_LIST_FIELDS:
        if field in df.columns:
            expressions.append(pl.col(field).fill_null([]).alias(field))

    # replace empty lists with null - because reasons i guess
    for field in OMIT_EMPTY_LIST_FIELDS:
        if field in df.columns:
            expressions.append(
                pl.when(pl.col(field).list.len() == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )

    # drop these if false
    for field in OPTIONAL_BOOL_FIELDS:
        if field in df.columns:
            # pylint: disable=singleton-comparison
            expressions.append(
                pl.when(pl.col(field) == True).then(True).otherwise(None).alias(field)
            )

    # Nullify empty strings/lists/structs for optional fields
    for field in OTHER_OPTIONAL_FIELDS:
        if field not in df.columns:
            continue

        col_type = df.schema[field]

        if col_type == pl.Utf8:
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
            # For struct fields, clean each sub-field and rebuild
            # But preserve null if entire struct is null
            struct_field_names = [f.name for f in col_type.fields]

            # Build cleaned struct: extract each field, nullify empty values, rebuild
            cleaned_fields = []
            for sf_name in struct_field_names:
                sf_expr = pl.col(field).struct.field(sf_name)
                # Get the subfield type
                sf_type = next(
                    (f.dtype for f in col_type.fields if f.name == sf_name), None
                )

                if sf_type == pl.Utf8:
                    # Empty string -> null
                    cleaned_fields.append(
                        pl.when(sf_expr == "")
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf_name)
                    )
                elif isinstance(sf_type, pl.List):
                    # Empty list -> null
                    cleaned_fields.append(
                        pl.when(sf_expr.list.len() == 0)
                        .then(None)
                        .otherwise(sf_expr)
                        .alias(sf_name)
                    )
                else:
                    cleaned_fields.append(sf_expr.alias(sf_name))

            # Rebuild struct with cleaned fields, but preserve null if original was null
            expressions.append(
                pl.when(pl.col(field).is_null())
                .then(None)
                .otherwise(pl.struct(cleaned_fields))
                .alias(field)
            )
        elif col_type in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            # Numeric fields: nullify 0 values
            expressions.append(
                pl.when(pl.col(field) == 0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
            )
        else:
            # Other types pass through as-is
            continue

    # Apply all transformations at once
    if expressions:
        df = df.with_columns(expressions)

    # Drop internal columns
    df = df.select([c for c in df.columns if not c.startswith("_")])

    return df


def dataframe_to_cards_list(
    df: pl.DataFrame,
    sort_by: tuple[str, ...] = ("number", "side"),
) -> list[dict[str, Any]]:
    """
    Convert cards DataFrame to cleaned list of dicts.

    All cleaning done in Polars using OMIT_FIELDS logic, then recursively
    removes any remaining null values from nested structures.
    """
    # Sort for consistent output
    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        if "number" in sort_cols:
            df = df.with_columns(pl.col("number").str.zfill(10).alias("_sort_num"))
            sort_cols = ["_sort_num" if c == "number" else c for c in sort_cols]
        df = df.sort(sort_cols, nulls_last=True)
        if "_sort_num" in df.columns:
            df = df.drop("_sort_num")

    # Clean using OMIT_FIELDS in Polars
    df = prepare_cards_for_json(df)

    # Convert to dicts and recursively strip all null values
    raw_dicts = df.to_dicts()
    return [clean_nested(card, omit_empty=True) for card in raw_dicts]


def clean_nested(
    obj: Any,
    omit_empty: bool = True,
    field_handlers: dict[str, Callable[[Any], Any]] | None = None,
    current_path: str = "",
) -> Any:
    """
    Recursively clean any nested structure with custom field handling.

    Args:
        obj: Any Python object to clean
        omit_empty: If True, omit empty lists/dicts/None values
        field_handlers: Dict of field_name -> handler_function
        current_path: Internal tracking of nested path (e.g., "foreignData.identifiers")

    Returns:
        Cleaned version of the input object
    """
    # Handle None
    if obj is None:
        return None if not omit_empty else None

    # Handle dictionaries
    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for key, value in obj.items():
            # Build path for nested fields
            field_path = f"{current_path}.{key}" if current_path else key

            # Check for custom handler
            if field_handlers and field_path in field_handlers:
                cleaned_value = field_handlers[field_path](value)
            else:
                cleaned_value = clean_nested(
                    value,
                    omit_empty=omit_empty,
                    field_handlers=field_handlers,
                    current_path=field_path,
                )

            # Omit None values, but keep required list fields as []
            if cleaned_value is None and omit_empty:
                # Required list fields should be [] not omitted
                if key in REQUIRED_LIST_FIELDS:
                    result[key] = []
                continue

            # Omit False for optional boolean fields (only include when True)
            if omit_empty and key in OPTIONAL_BOOL_FIELDS and cleaned_value is False:
                continue

            # Omit empty collections if requested, but keep required list fields
            if (
                omit_empty
                and isinstance(cleaned_value, (dict, list))
                and not cleaned_value
            ):
                # Keep empty lists for required fields
                if isinstance(cleaned_value, list) and key in REQUIRED_LIST_FIELDS:
                    pass  # Keep it
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

            # Skip None items if omitting empty
            if cleaned_item is None and omit_empty:
                continue

            result_list.append(cleaned_item)

        # Return empty list for required fields, None for others
        if not result_list:
            field_name = current_path.split(".")[-1] if current_path else ""
            if omit_empty and field_name not in REQUIRED_LIST_FIELDS:
                return None
        return result_list

    # Handle tuples (convert to list)
    if isinstance(obj, tuple):
        return clean_nested(list(obj), omit_empty, field_handlers, current_path)

    # Handle sets (convert to sorted list)
    if isinstance(obj, set):
        return clean_nested(sorted(obj), omit_empty, field_handlers, current_path)

    # Handle primitives (str, int, float, bool, etc.)
    return obj
