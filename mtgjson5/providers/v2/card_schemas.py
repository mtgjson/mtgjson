"""
Polars schema generation from MTGJSON Pydantic object models.

This module bridges the objectmodels.py Pydantic definitions with Polars DataFrames,
enabling:
- Schema generation for different card types (set cards, tokens, atomic, deck)
- Set-level DataFrame schemas for entire MTG sets
- Nested struct flattening for SQL-like exports
- Type-safe DataFrame transformations
- camelCase/snake_case conversion for JSON/parquet compatibility

Usage:
    from mtgjson5.providers.v2.card_schemas import (
        CardSchemas,
        SetSchemas,
        get_card_schema,
        get_set_schema,
        to_camel_case,
        to_snake_case,
    )

    # Get schema for set cards
    schema = CardSchemas.SET_CARD
    df = pl.read_parquet("cards.parquet").select(schema.columns())

    # Get full set schema
    set_schema = SetSchemas.FULL_SET
"""

import re
from enum import Enum
from typing import Any, ClassVar

import polars as pl
from pydantic import BaseModel, TypeAdapter

from mtgjson5.mtgjson_models import (
    pydantic_model_to_schema,
    pydantic_model_to_struct,
    pydantic_type_to_polars,
)
from mtgjson5.providers.v2.objectmodels import (
    MtgjsonBoosterConfigObject,
    MtgjsonCardAtomicObject,
    MtgjsonCardDeckObject,
    MtgjsonCardSetDeckObject,
    MtgjsonCardSetObject,
    MtgjsonCardTokenObject,
    MtgjsonDeckSetObject,
    MtgjsonForeignDataObject,
    MtgjsonIdentifiersObject,
    MtgjsonLeadershipSkillsObject,
    MtgjsonLegalitiesObject,
    MtgjsonPurchaseUrlsObject,
    MtgjsonRelatedCardsObject,
    MtgjsonRulingsObject,
    MtgjsonSealedProductObject,
    MtgjsonSetObject,
    MtgjsonSourceProductsObject,
    MtgjsonTranslationsObject,
)


_CAMEL_TO_SNAKE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_TO_SNAKE_2 = re.compile(r"([a-z0-9])([A-Z])")


def to_snake_case(camel_str: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = _CAMEL_TO_SNAKE_1.sub(r"\1_\2", camel_str)
    return _CAMEL_TO_SNAKE_2.sub(r"\1_\2", s1).lower()


def to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def rename_columns_to_snake(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """Rename all columns from camelCase to snake_case."""
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    rename_map = {col: to_snake_case(col) for col in schema.names()}
    return df.rename(rename_map)


def rename_columns_to_camel(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """Rename all columns from snake_case to camelCase."""
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    rename_map = {col: to_camel_case(col) for col in schema.names()}
    return df.rename(rename_map)


class NestedStructs:
    """Pre-built Polars Struct types for nested MTGJSON objects."""

    # Full model-derived structs
    IDENTIFIERS: pl.Struct = pydantic_model_to_struct(MtgjsonIdentifiersObject)
    LEGALITIES: pl.Struct = pydantic_model_to_struct(MtgjsonLegalitiesObject)
    PURCHASE_URLS: pl.Struct = pydantic_model_to_struct(MtgjsonPurchaseUrlsObject)
    LEADERSHIP_SKILLS: pl.Struct = pydantic_model_to_struct(
        MtgjsonLeadershipSkillsObject
    )
    RELATED_CARDS: pl.Struct = pydantic_model_to_struct(MtgjsonRelatedCardsObject)
    SOURCE_PRODUCTS: pl.Struct = pydantic_model_to_struct(MtgjsonSourceProductsObject)
    RULINGS: pl.Struct = pydantic_model_to_struct(MtgjsonRulingsObject)
    TRANSLATIONS: pl.Struct = pydantic_model_to_struct(MtgjsonTranslationsObject)
    FOREIGN_DATA: pl.Struct = pydantic_model_to_struct(MtgjsonForeignDataObject)
    FOREIGN_DATA_OUTPUT: pl.Struct = pl.Struct(
        {
            "faceName": pl.String(),
            "identifiers": pl.Struct(
                {
                    "scryfallId": pl.String(),
                    "multiverseId": pl.String(),
                }
            ),
            "language": pl.String(),
            "multiverseId": pl.Int64(),  # Top-level integer field
            "name": pl.String(),
            "text": pl.String(),
            "type": pl.String(),
            "uuid": pl.String(),
        }
    )


class CardType(str, Enum):
    """MTGJSON card type categories."""

    SET_CARD = "set_card"  # Full card in a set
    TOKEN = "token"  # Token card
    ATOMIC = "atomic"  # Oracle-level atomic card
    DECK_CARD = "deck_card"  # Card in deck output
    SET_DECK_CARD = "set_deck_card"  # Card reference in set deck


class CardSchemaRegistry:
    """
    Registry of Polars schemas for different MTGJSON card types.

    Each schema includes column names, types, and whether fields are required.
    """

    _MODELS: ClassVar[dict[CardType, type[BaseModel]]] = {
        CardType.SET_CARD: MtgjsonCardSetObject,
        CardType.TOKEN: MtgjsonCardTokenObject,
        CardType.ATOMIC: MtgjsonCardAtomicObject,
        CardType.DECK_CARD: MtgjsonCardDeckObject,
        CardType.SET_DECK_CARD: MtgjsonCardSetDeckObject,
    }

    _cache: ClassVar[dict[CardType, dict[str, pl.DataType]]] = {}

    @classmethod
    def get_schema(cls, card_type: CardType) -> dict[str, pl.DataType]:
        """Get Polars schema for a card type."""
        if card_type not in cls._cache:
            model = cls._MODELS[card_type]
            cls._cache[card_type] = pydantic_model_to_schema(model)
        return cls._cache[card_type]

    @classmethod
    def get_model(cls, card_type: CardType) -> type[BaseModel]:
        """Get Pydantic model for a card type."""
        return cls._MODELS[card_type]

    @classmethod
    def get_columns(cls, card_type: CardType) -> list[str]:
        """Get ordered column names for a card type."""
        model = cls._MODELS[card_type]
        return list(model.model_fields.keys())

    @classmethod
    def get_struct(cls, card_type: CardType) -> pl.Struct:
        """Get Polars Struct type for a card type (for list columns)."""
        model = cls._MODELS[card_type]
        return pydantic_model_to_struct(model)


class SetSchemaRegistry:
    """
    Registry for set-level Polars schemas.

    A "set schema" represents the structure of an entire MTG set file,
    including cards, tokens, decks, sealed products, and boosters.

    Generated from MtgjsonSetObject Pydantic model for type safety.
    """

    # Fields that contain nested card/token arrays (handled specially)
    _NESTED_ARRAY_FIELDS: ClassVar[set[str]] = {"cards", "tokens", "decks", "sealed_product"}

    # Fields with highly variable schemas (stored as JSON strings in parquet)
    _JSON_STRING_FIELDS: ClassVar[set[str]] = {"booster"}

    # Schema caches
    _metadata_cache: ClassVar[dict[str, pl.DataType] | None] = None
    _full_cache: ClassVar[dict[str, pl.DataType] | None] = None

    @classmethod
    def get_schema(cls) -> dict[str, pl.DataType]:
        """Get full Polars schema for MtgjsonSetObject."""
        return pydantic_model_to_schema(MtgjsonSetObject)

    @classmethod
    def get_model(cls) -> type[BaseModel]:
        """Get the Pydantic model for sets."""
        return MtgjsonSetObject

    @classmethod
    def get_columns(cls) -> list[str]:
        """Get ordered column names from MtgjsonSetObject model."""
        return list(MtgjsonSetObject._model_fields.keys())

    @classmethod
    def get_struct(cls) -> pl.Struct:
        """Get Polars Struct type for a complete set."""
        return pydantic_model_to_struct(MtgjsonSetObject)

    @classmethod
    def get_set_metadata_schema(cls) -> dict[str, pl.DataType]:
        """
        Schema for set metadata fields (excluding card/token arrays).

        Generated from MtgjsonSetObject, filtering out nested array fields.
        """
        if cls._metadata_cache is not None:
            return cls._metadata_cache

        # Generate from model
        full_schema = pydantic_model_to_schema(MtgjsonSetObject)

        # Filter to metadata only (exclude nested arrays and JSON string fields)
        exclude = cls._NESTED_ARRAY_FIELDS | cls._JSON_STRING_FIELDS
        cls._metadata_cache = {k: v for k, v in full_schema.items() if k not in exclude}
        return cls._metadata_cache

    @classmethod
    def get_full_set_schema(cls) -> dict[str, pl.DataType]:
        """
        Schema for a complete set including all nested arrays.

        Cards/tokens/decks use their respective model structs.
        Booster is stored as JSON string due to highly variable schema per set.
        """
        if cls._full_cache is not None:
            return cls._full_cache

        metadata = cls.get_set_metadata_schema()
        cls._full_cache = {
            **metadata,
            "cards": pl.List(CardSchemaRegistry.get_struct(CardType.SET_CARD)),
            "tokens": pl.List(CardSchemaRegistry.get_struct(CardType.TOKEN)),
            "decks": pl.List(pydantic_model_to_struct(MtgjsonDeckSetObject)),
            "sealed_product": pl.List(
                pydantic_model_to_struct(MtgjsonSealedProductObject)
            ),
            "booster": pl.String(),  # JSON string - MtgjsonBoosterConfigObject varies per set
        }
        return cls._full_cache

    @staticmethod
    def get_deck_schema() -> dict[str, pl.DataType]:
        """Schema for deck objects within a set."""
        return pydantic_model_to_schema(MtgjsonDeckSetObject)

    @staticmethod
    def get_sealed_product_schema() -> dict[str, pl.DataType]:
        """Schema for sealed product objects."""
        return pydantic_model_to_schema(MtgjsonSealedProductObject)

    @staticmethod
    def get_booster_config_schema() -> dict[str, pl.DataType]:
        """Schema for booster configuration objects."""
        return pydantic_model_to_schema(MtgjsonBoosterConfigObject)


def get_field_polars_type(model: type[BaseModel], field_name: str) -> pl.DataType:
    """
    Get the Polars type for a specific field from a Pydantic model.

    This is useful for getting individual field types without generating
    the full schema.

    Args:
        model: Pydantic BaseModel class
        field_name: Name of the field

    Returns:
        Polars DataType for the field

    Raises:
        KeyError: If field_name is not in the model
    """
    if field_name not in model.model_fields:
        raise KeyError(f"Field '{field_name}' not found in {model.__name__}")
    field_info = model.model_fields[field_name]
    return pydantic_type_to_polars(field_info.annotation)


def select_card_columns(
    df: pl.DataFrame | pl.LazyFrame,
    card_type: CardType,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Select columns from a card DataFrame based on card type schema.

    Args:
        df: Input DataFrame
        card_type: Type of card (determines available columns)
        include: Only include these columns (if specified)
        exclude: Exclude these columns

    Returns:
        DataFrame with selected columns
    """
    all_columns = CardSchemaRegistry.get_columns(card_type)
    existing = set(
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )

    # Start with all columns that exist in the DataFrame
    columns = [c for c in all_columns if c in existing]

    # Apply include filter
    if include:
        columns = [c for c in columns if c in include]

    # Apply exclude filter
    if exclude:
        columns = [c for c in columns if c not in exclude]

    return df.select(columns)


def select_token_columns(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """Select only columns valid for token cards."""
    token_cols = CardSchemaRegistry.get_columns(CardType.TOKEN)
    existing = set(
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )
    return df.select([c for c in token_cols if c in existing])


def select_atomic_columns(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """Select only columns valid for atomic card data."""
    atomic_cols = CardSchemaRegistry.get_columns(CardType.ATOMIC)
    existing = set(
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )
    return df.select([c for c in atomic_cols if c in existing])


def extract_identifiers(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Extract identifiers struct into flat columns.

    Converts: {"identifiers": {"scryfallId": "abc", "tcgplayerId": "123"}}
    To: {"scryfall_id": "abc", "tcgplayer_id": "123", ...}

    Only extracts fields that exist in the struct.
    """
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    if "identifiers" not in schema:
        return df

    # Get actual struct fields from the DataFrame schema
    id_dtype = schema["identifiers"]
    if not isinstance(id_dtype, pl.Struct):
        return df

    actual_fields = [f.name for f in id_dtype.fields]

    return df.with_columns(
        [pl.col("identifiers").struct.field(f).alias(f) for f in actual_fields]
    ).drop("identifiers")


def extract_legalities(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Extract legalities struct into flat columns.

    Converts: {"legalities": {"modern": "Legal", "standard": "Banned"}}
    To: {"legality_modern": "Legal", "legality_standard": "Banned", ...}

    Only extracts fields that exist in the struct.
    """
    schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
    if "legalities" not in schema:
        return df

    # Get actual struct fields from the DataFrame schema
    leg_dtype = schema["legalities"]
    if not isinstance(leg_dtype, pl.Struct):
        return df

    actual_fields = [f.name for f in leg_dtype.fields]

    return df.with_columns(
        [
            pl.col("legalities").struct.field(f).alias(f"legality_{f}")
            for f in actual_fields
        ]
    ).drop("legalities")


def flatten_card_for_sql(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Flatten a card DataFrame for SQL-like storage.

    - Extracts identifiers to flat columns
    - Extracts legalities to flat columns
    - Converts list columns to JSON strings
    - Removes complex nested structs
    """
    result = df

    # Extract common structs
    result = extract_identifiers(result)
    result = extract_legalities(result)

    # Get current schema
    schema = (
        result.collect_schema() if isinstance(result, pl.LazyFrame) else result.schema
    )

    # Convert remaining list/struct columns to JSON strings
    json_cols = []
    for col_name, dtype in schema.items():
        if isinstance(dtype, (pl.List, pl.Struct)):
            json_cols.append(
                pl.col(col_name)
                .map_elements(
                    lambda x: None if x is None else str(x), return_dtype=pl.String
                )
                .alias(col_name)
            )

    if json_cols:
        # Keep non-complex columns as-is, replace complex ones
        simple_cols: list[str] = [
            c for c in schema.names() if not isinstance(schema[c], (pl.List, pl.Struct))
        ]
        simple_col_exprs: list[pl.Expr] = [pl.col(c) for c in simple_cols]
        result = result.select(simple_col_exprs + json_cols)

    return result


def validate_card_dataframe(
    df: pl.DataFrame | pl.LazyFrame,
    card_type: CardType,
) -> tuple[bool, list[str]]:
    """
    Validate that a DataFrame matches expected card schema.

    Args:
        df: DataFrame to validate
        card_type: Expected card type

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    expected_schema = CardSchemaRegistry.get_schema(card_type)
    actual_schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema

    # Check for missing required columns
    model = CardSchemaRegistry.get_model(card_type)
    for field_name, field_info in model.model_fields.items():
        if field_info.is_required() and field_name not in actual_schema:
            errors.append(f"Missing required column: {field_name}")

    # Check type compatibility for existing columns
    for col_name in actual_schema.names():
        if col_name in expected_schema:
            actual_type = actual_schema[col_name]
            expected_type = expected_schema[col_name]

            # Allow Null to match any type
            if actual_type not in (pl.Null, expected_type):
                # Check if it's a compatible numeric type
                if not _types_broadly_compatible(actual_type, expected_type):
                    errors.append(
                        f"Column '{col_name}': expected {expected_type}, got {actual_type}"
                    )

    return len(errors) == 0, errors


def _types_broadly_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    """Check if types are broadly compatible (allows numeric coercion)."""
    # Same type
    if actual == expected:
        return True

    # Null is compatible with anything
    if actual == pl.Null:
        return True

    # Numeric types are broadly compatible
    numeric = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    }
    if type(actual) in numeric and type(expected) in numeric:
        return True

    # List types - check inner compatibility
    # Handle both DataTypeClass (pl.List) and instances of List type
    actual_is_list = (
        (isinstance(actual, type)
        and issubclass(actual, pl.List))
        or isinstance(actual, pl.List)
    )
    expected_is_list = (
        (isinstance(expected, type)
        and issubclass(expected, pl.List))
        or isinstance(expected, pl.List)
    )

    if actual_is_list and expected_is_list:
        # Only check inner types if both are instances (not classes)
        if not isinstance(actual, type) and not isinstance(expected, type):
            from typing import cast

            actual_inner = cast(Any, actual).inner
            expected_inner = cast(Any, expected).inner
            return _types_broadly_compatible(actual_inner, expected_inner)
        return True

    return False


def get_model_field_names(
    card_type: CardType, use_alias: bool = True
) -> frozenset[str]:
    """
    Get all field names from a card type's Pydantic model.

    Args:
        card_type: The card type enum
        use_alias: If True, return camelCase names (JSON output format).
                   If False, return snake_case names (Python/Polars format).

    Returns:
        Frozenset of field names
    """
    model = CardSchemaRegistry.get_model(card_type)
    if use_alias:
        names = set()
        for field_name, field_info in model.model_fields.items():
            if field_info.alias:
                names.add(field_info.alias)
            else:
                names.add(to_camel_case(field_name))
        return frozenset(names)
    return frozenset(model.model_fields.keys())


def get_excluded_fields(
    base_type: CardType,
    target_type: CardType,
    use_alias: bool = True,
) -> frozenset[str]:
    """
    Compute which fields from base_type are excluded in target_type.

    This derives exclusion sets from model definitions rather than hardcoding.

    Args:
        base_type: The base card type (usually SET_CARD as the most complete)
        target_type: The target card type to compare against
        use_alias: Return camelCase (True) or snake_case (False) names

    Returns:
        Frozenset of field names present in base_type but not in target_type
    """
    base_fields = get_model_field_names(base_type, use_alias)
    target_fields = get_model_field_names(target_type, use_alias)
    return base_fields - target_fields


_type_adapter_cache: dict[CardType, TypeAdapter] = {}


def _get_type_adapter(card_type: CardType) -> TypeAdapter:
    """Get or create a TypeAdapter for a card type's model."""
    if card_type not in _type_adapter_cache:
        model = CardSchemaRegistry.get_model(card_type)
        _type_adapter_cache[card_type] = TypeAdapter(list[model])  # type: ignore[valid-type]
    return _type_adapter_cache[card_type]


def to_clean_dicts(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to list of dicts, stripping None values."""
    return [{k: v for k, v in row.items() if v is not None} for row in df.to_dicts()]


def to_model_dicts(
    df: pl.DataFrame,
    card_type: CardType,
    exclude_none: bool = True,
) -> list[dict[str, Any]]:
    """
    Convert DataFrame to list of dicts via Pydantic model.

    Validates against the model and strips None values on dump.
    """

    adapter = _get_type_adapter(card_type)
    # Strip None values before validation so defaults apply
    clean_dicts = to_clean_dicts(df)
    validated = adapter.validate_python(clean_dicts)
    dumped = adapter.dump_python(validated, exclude_none=exclude_none)
    # TypeAdapter.dump_python returns the same type as was validated
    assert isinstance(dumped, list)
    return dumped


# Quick access to schemas
CardSchemas = CardSchemaRegistry
SetSchemas = SetSchemaRegistry

# Functions
get_card_schema = CardSchemaRegistry.get_schema
get_card_columns = CardSchemaRegistry.get_columns
get_set_schema = SetSchemaRegistry.get_full_set_schema
get_set_metadata_schema = SetSchemaRegistry.get_set_metadata_schema
get_deck_schema = SetSchemaRegistry.get_deck_schema
get_sealed_product_schema = SetSchemaRegistry.get_sealed_product_schema
