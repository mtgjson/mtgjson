"""
MTGJSON schema definitions with class-based inheritance and Pydantic integration.

This module provides:
- Field sets and Polars schema definitions for MTGJSON card types
  (CardSet, CardToken, CardAtomic, CardDeck)
- Class-based schema definitions with exclusion patterns
- Pydantic model to Polars schema conversion utilities
- DataFrame validation and transformation helpers
"""

from abc import ABC
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    FrozenSet,
    Literal,
    Type,
    Union,
    get_args,
    get_origin,
    overload,
)

import polars as pl
from pydantic import BaseModel

from mtgjson5.utils import to_camel_case

if TYPE_CHECKING:
    import pyarrow as pa


ALL_CARD_FIELDS: FrozenSet[str] = frozenset(
    {
        # Identity
        "uuid",
        "name",
        "asciiName",
        "faceName",
        "flavorName",
        "setCode",
        "number",
        "side",
        # Mana & Colors
        "manaCost",
        "manaValue",
        "convertedManaCost",
        "faceManaValue",
        "faceConvertedManaCost",
        "colors",
        "colorIdentity",
        "colorIndicator",
        # Types
        "type",
        "supertypes",
        "types",
        "subtypes",
        # Text
        "text",
        "flavorText",
        "originalText",
        "originalType",
        # Stats
        "power",
        "toughness",
        "loyalty",
        "defense",
        "hand",
        "life",
        # Appearance
        "artist",
        "artistIds",
        "borderColor",
        "frameVersion",
        "frameEffects",
        "securityStamp",
        "watermark",
        "signature",
        "orientation",
        # Finishes & Availability
        "finishes",
        "hasFoil",
        "hasNonFoil",
        "availability",
        "boosterTypes",
        # Boolean Flags
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
        # Gameplay
        "keywords",
        "layout",
        "rarity",
        "edhrecRank",
        "edhrecSaltiness",
        "attractionLights",
        "duelDeck",
        # Printing Info
        "language",
        "printedName",
        "printedText",
        "printedType",
        "originalReleaseDate",
        "promoTypes",
        # Relations
        "otherFaceIds",
        "variations",
        "cardParts",
        "printings",
        "firstPrinting",
        "originalPrintings",
        "rebalancedPrintings",
        "reverseRelated",
        "subsets",
        # Nested Objects
        "identifiers",
        "legalities",
        "leadershipSkills",
        "purchaseUrls",
        "relatedCards",
        "rulings",
        "foreignData",
        "sourceProducts",
        # Deck-specific
        "count",
        "isFoil",
    }
)

IDENTIFIER_FIELDS: FrozenSet[str] = frozenset(
    {
        "abuId",
        "cardKingdomEtchedId",
        "cardKingdomFoilId",
        "cardKingdomId",
        "cardsphereId",
        "cardsphereFoilId",
        "cardtraderId",
        "csiId",
        "mcmId",
        "mcmMetaId",
        "miniaturemarketId",
        "mtgArenaId",
        "mtgjsonFoilVersionId",
        "mtgjsonNonFoilVersionId",
        "mtgjsonV4Id",
        "mtgoFoilId",
        "mtgoId",
        "multiverseId",
        "scgId",
        "scryfallId",
        "scryfallCardBackId",
        "scryfallOracleId",
        "scryfallIllustrationId",
        "tcgplayerProductId",
        "tcgplayerEtchedProductId",
        "tntId",
    }
)

LEGALITY_FORMATS: FrozenSet[str] = frozenset(
    {
        "alchemy",
        "brawl",
        "commander",
        "duel",
        "explorer",
        "future",
        "gladiator",
        "historic",
        "historicbrawl",
        "legacy",
        "modern",
        "oathbreaker",
        "oldschool",
        "pauper",
        "paupercommander",
        "penny",
        "pioneer",
        "predh",
        "premodern",
        "standard",
        "standardbrawl",
        "timeless",
        "vintage",
    }
)

PURCHASE_URL_FIELDS: FrozenSet[str] = frozenset(
    {
        "cardKingdom",
        "cardKingdomEtched",
        "cardKingdomFoil",
        "cardmarket",
        "tcgplayer",
        "tcgplayerEtched",
    }
)

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

REQUIRED_BOOL_FIELDS: FrozenSet[str] = frozenset(
    {
        "hasFoil",
        "hasNonFoil",
    }
)

REQUIRED_DECK_LIST_FIELDS: FrozenSet[str] = frozenset(
    {
        "commander",
        "displayCommander",
        "mainBoard",
        "planes",
        "schemes",
        "sideBoard",
        "sourceSetCodes",
        "tokens",
    }
)

FIELD_TYPES: dict[str, pl.DataType] = {
    # Identity
    "uuid": pl.String(),
    "name": pl.String(),
    "asciiName": pl.String(),
    "faceName": pl.String(),
    "faceFlavorName": pl.String(),
    "flavorName": pl.String(),
    "setCode": pl.String(),
    "number": pl.String(),
    "side": pl.String(),
    # Mana & Colors
    "manaCost": pl.String(),
    "manaValue": pl.Float64(),
    "convertedManaCost": pl.Float64(),
    "faceManaValue": pl.Float64(),
    "faceConvertedManaCost": pl.Float64(),
    "colors": pl.List(pl.String()),
    "colorIdentity": pl.List(pl.String()),
    "colorIndicator": pl.List(pl.String()),
    # Types
    "type": pl.String(),
    "supertypes": pl.List(pl.String()),
    "types": pl.List(pl.String()),
    "subtypes": pl.List(pl.String()),
    # Text
    "text": pl.String(),
    "flavorText": pl.String(),
    "originalText": pl.String(),
    "originalType": pl.String(),
    # Stats (String - can be "*", "+1", etc)
    "power": pl.String(),
    "toughness": pl.String(),
    "loyalty": pl.String(),
    "defense": pl.String(),
    "hand": pl.String(),
    "life": pl.String(),
    # Appearance
    "artist": pl.String(),
    "artistIds": pl.List(pl.String()),
    "borderColor": pl.String(),
    "frameVersion": pl.String(),
    "frameEffects": pl.List(pl.String()),
    "securityStamp": pl.String(),
    "watermark": pl.String(),
    "signature": pl.String(),
    "orientation": pl.String(),
    # Finishes & Availability
    "finishes": pl.List(pl.String()),
    "hasFoil": pl.Boolean(),
    "hasNonFoil": pl.Boolean(),
    "availability": pl.List(pl.String()),
    "boosterTypes": pl.List(pl.String()),
    # Boolean Flags
    "hasAlternativeDeckLimit": pl.Boolean(),
    "hasContentWarning": pl.Boolean(),
    "isAlternative": pl.Boolean(),
    "isFullArt": pl.Boolean(),
    "isFunny": pl.Boolean(),
    "isGameChanger": pl.Boolean(),
    "isOnlineOnly": pl.Boolean(),
    "isOversized": pl.Boolean(),
    "isPromo": pl.Boolean(),
    "isRebalanced": pl.Boolean(),
    "isReprint": pl.Boolean(),
    "isReserved": pl.Boolean(),
    "isStarter": pl.Boolean(),
    "isStorySpotlight": pl.Boolean(),
    "isTextless": pl.Boolean(),
    "isTimeshifted": pl.Boolean(),
    # Gameplay
    "keywords": pl.List(pl.String()),
    "layout": pl.String(),
    "rarity": pl.String(),
    "edhrecRank": pl.Int64(),
    "edhrecSaltiness": pl.Float64(),
    "attractionLights": pl.List(pl.String()),
    "duelDeck": pl.String(),
    # Printing Info
    "language": pl.String(),
    "printedName": pl.String(),
    "printedText": pl.String(),
    "printedType": pl.String(),
    "originalReleaseDate": pl.String(),
    "promoTypes": pl.List(pl.String()),
    # Relations
    "otherFaceIds": pl.List(pl.String()),
    "variations": pl.List(pl.String()),
    "cardParts": pl.List(pl.String()),
    "printings": pl.List(pl.String()),
    "firstPrinting": pl.String(),
    "originalPrintings": pl.List(pl.String()),
    "rebalancedPrintings": pl.List(pl.String()),
    "reverseRelated": pl.List(pl.String()),
    "subsets": pl.List(pl.String()),
    # Nested Objects
    "identifiers": pl.Struct({field: pl.String() for field in IDENTIFIER_FIELDS}),
    "legalities": pl.Struct({fmt: pl.String() for fmt in LEGALITY_FORMATS}),
    "leadershipSkills": pl.Struct(
        {
            "brawl": pl.Boolean(),
            "commander": pl.Boolean(),
            "oathbreaker": pl.Boolean(),
        }
    ),
    "purchaseUrls": pl.Struct({field: pl.String() for field in PURCHASE_URL_FIELDS}),
    "relatedCards": pl.Struct(
        {
            "reverseRelated": pl.List(pl.String()),
            "spellbook": pl.List(pl.String()),
        }
    ),
    "rulings": pl.List(pl.Struct({"date": pl.String(), "text": pl.String()})),
    "foreignData": pl.List(
        pl.Struct(
            {
                "faceName": pl.String(),
                "flavorText": pl.String(),
                "language": pl.String(),
                "multiverseId": pl.Int64(),
                "name": pl.String(),
                "text": pl.String(),
                "type": pl.String(),
            }
        )
    ),
    "sourceProducts": pl.List(
        pl.Struct({"name": pl.String(), "productId": pl.String()})
    ),
    # Deck-specific
    "count": pl.Int64(),
    "isFoil": pl.Boolean(),
}

CardTypes = Literal["card_set", "card_token", "card_atomic", "card_deck"]


class CardSchema(ABC):
    """Base class for MTGJSON card schema definitions."""

    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset()
    EXTRA_FIELDS: ClassVar[FrozenSet[str]] = frozenset()
    CARD_TYPE_NAME: ClassVar[str] = ""

    SPECIAL_RENAMES: ClassVar[dict[str, str]] = {
        "set": "setCode",
        "cmc": "convertedManaCost",
    }

    MULTIFACE_LAYOUTS: ClassVar[list[str]] = [
        "transform",
        "modal_dfc",
        "meld",
        "adventure",
        "flip",
        "split",
        "reversible_card",
    ]

    @classmethod
    @lru_cache(maxsize=1)
    def get_fields(cls) -> FrozenSet[str]:
        """Get the set of allowed fields for this card type."""
        return (ALL_CARD_FIELDS - cls.EXCLUDE_FIELDS) | cls.EXTRA_FIELDS

    @classmethod
    @lru_cache(maxsize=1)
    def get_field_list(cls) -> list[str]:
        """Cached sorted list of fields for this card type."""
        return sorted(cls.get_fields())

    @classmethod
    @lru_cache(maxsize=1)
    def get_schema(cls) -> dict[str, pl.DataType]:
        """Get the Polars schema for this card type."""
        allowed_fields = cls.get_fields()
        return {
            field: dtype
            for field, dtype in FIELD_TYPES.items()
            if field in allowed_fields
        }

    @classmethod
    @lru_cache(maxsize=1)
    def get_select_exprs(cls) -> tuple[pl.Expr, ...]:
        """
        Pre-built select expressions with type casting.
        Computed once per process, reused for all cards.
        """
        schema = cls.get_schema()
        return tuple(
            pl.col(field).cast(dtype) for field, dtype in sorted(schema.items())
        )

    @classmethod
    def build_rename_map(cls, lf: pl.LazyFrame) -> dict[str, str]:
        """Build rename map for snake_case to camelCase conversion."""
        lf_schema = lf.collect_schema()
        rename_map = {}

        for col in lf_schema.names():
            if col in cls.SPECIAL_RENAMES:
                rename_map[col] = cls.SPECIAL_RENAMES[col]
            elif "_" in col:
                rename_map[col] = to_camel_case(col)

        return rename_map

    @classmethod
    def apply_face_field_logic(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Null face-specific fields for non-multiface cards."""
        schema = lf.collect_schema()
        has_face_value = "faceManaValue" in schema
        has_face_cmc = "faceConvertedManaCost" in schema

        if not (has_face_value or has_face_cmc):
            return lf

        exprs = []
        if has_face_value:
            exprs.append(
                pl.when(pl.col("layout").is_in(cls.MULTIFACE_LAYOUTS))
                .then(pl.col("faceManaValue"))
                .otherwise(pl.lit(None).cast(pl.Float64))
                .alias("faceManaValue")
            )
        if has_face_cmc:
            exprs.append(
                pl.when(pl.col("layout").is_in(cls.MULTIFACE_LAYOUTS))
                .then(pl.col("faceConvertedManaCost"))
                .otherwise(pl.lit(None).cast(pl.Float64))
                .alias("faceConvertedManaCost")
            )

        return lf.with_columns(exprs)

    @classmethod
    def transform(
        cls,
        lf: pl.LazyFrame,
        *,
        rename: bool = True,
        face_field_logic: bool = True,
    ) -> pl.LazyFrame:
        """Apply transformations to LazyFrame for this card type."""
        if rename:
            rename_map = cls.build_rename_map(lf)
            if rename_map:
                lf = lf.rename(rename_map, strict=False)

        if face_field_logic:
            lf = cls.apply_face_field_logic(lf)

        return cls.lazy_select_and_cast(lf)

    @classmethod
    def lazy_select_and_cast(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Lazily select and cast in single operation."""
        lf_schema = lf.collect_schema()
        exprs = [
            expr
            for expr in cls.get_select_exprs()
            if expr.meta.output_name() in lf_schema
        ]
        return lf.select(exprs)

    @classmethod
    def fast_filter(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Fast filter to only relevant columns (assumes correct types)."""
        lf_cols = lf.collect_schema().names()
        valid_cols = [col for col in cls.get_field_list() if col in lf_cols]
        return lf.select(valid_cols)

    @classmethod
    def validate_dataframe(
        cls,
        df: pl.DataFrame,
        *,
        strict: bool = False,
    ) -> pl.DataFrame:
        """Validate and coerce DataFrame to match this card schema."""
        schema = cls.get_schema()
        return df.match_to_schema(
            schema,
            missing_columns="insert",
            extra_columns="raise" if strict else "ignore",
            integer_cast="upcast",
            float_cast="upcast",
            missing_struct_fields="insert",
            extra_struct_fields="ignore",
        )

    @classmethod
    def select_columns(
        cls,
        df: pl.DataFrame,
        *,
        strip_bools: bool = True,
        validate: bool = False,
    ) -> pl.DataFrame:
        """Select and order columns for this card type."""
        if validate:
            df = cls.validate_dataframe(df)

        if strip_bools:
            for col in OPTIONAL_BOOL_FIELDS:
                if col in df.columns:
                    if not df.select(pl.col(col).any()).item():
                        df = df.drop(col)

        allowed_fields = cls.get_fields()
        existing_fields = [col for col in df.columns if col in allowed_fields]
        return df.select(sorted(existing_fields))


class CardSetSchema(CardSchema):
    """Schema for CardSet (standard set card)."""

    CARD_TYPE_NAME: ClassVar[str] = "card_set"
    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "orientation",
            "reverseRelated",
            "firstPrinting",
            "count",
            "isFoil",
        }
    )


class CardTokenSchema(CardSchema):
    """Schema for CardToken (tokens, emblems, art cards)."""

    CARD_TYPE_NAME: ClassVar[str] = "card_token"
    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "manaValue",
            "convertedManaCost",
            "faceManaValue",
            "faceConvertedManaCost",
            "rarity",
            "edhrecRank",
            "legalities",
            "leadershipSkills",
            "purchaseUrls",
            "rulings",
            "foreignData",
            "printings",
            "firstPrinting",
            "isRebalanced",
            "originalPrintings",
            "rebalancedPrintings",
            "isStarter",
            "isReserved",
            "isTimeshifted",
            "isAlternative",
            "isGameChanger",
            "hasAlternativeDeckLimit",
            "hasContentWarning",
            "duelDeck",
            "variations",
            "hand",
            "life",
            "printedName",
            "printedText",
            "printedType",
            "originalReleaseDate",
            "sourceProducts",
            "count",
            "isFoil",
        }
    )


class CardAtomicSchema(CardSchema):
    """Schema for CardAtomic (oracle-level, no printing info)."""

    CARD_TYPE_NAME: ClassVar[str] = "card_atomic"
    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "setCode",
            "number",
            "artist",
            "artistIds",
            "borderColor",
            "frameVersion",
            "frameEffects",
            "securityStamp",
            "watermark",
            "signature",
            "orientation",
            "finishes",
            "hasFoil",
            "hasNonFoil",
            "availability",
            "boosterTypes",
            "flavorText",
            "flavorName",
            "faceFlavorName",
            "originalReleaseDate",
            "promoTypes",
            "rarity",
            "language",
            "printedName",
            "printedText",
            "printedType",
            "duelDeck",
            "isAlternative",
            "isFullArt",
            "isOnlineOnly",
            "isOversized",
            "isPromo",
            "isRebalanced",
            "isReprint",
            "isStarter",
            "isStorySpotlight",
            "isTextless",
            "isTimeshifted",
            "hasContentWarning",
            "otherFaceIds",
            "variations",
            "originalPrintings",
            "rebalancedPrintings",
            "reverseRelated",
            "sourceProducts",
            "foreignData",
            "edhrecRank",
            "count",
            "isFoil",
        }
    )


class CardDeckSchema(CardSchema):
    """Schema for CardDeck (card in a deck list)."""

    CARD_TYPE_NAME: ClassVar[str] = "card_deck"
    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "orientation",
            "firstPrinting",
            "originalReleaseDate",
        }
    )
    EXTRA_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "count",
            "isFoil",
        }
    )


SCHEMA_REGISTRY: dict[str, type[CardSchema]] = {
    "card_set": CardSetSchema,
    "card_token": CardTokenSchema,
    "card_atomic": CardAtomicSchema,
    "card_deck": CardDeckSchema,
}


def _get_fields_for_card_type(card_type: str) -> FrozenSet[str]:
    """Internal function to get fields for a single card type."""
    if card_type not in SCHEMA_REGISTRY:
        raise ValueError(f"Invalid card type: {card_type}")
    return SCHEMA_REGISTRY[card_type].get_fields()


@overload
def get_card_fields(card_type: CardTypes) -> FrozenSet[str]: ...


@overload
def get_card_fields(card_type: list[CardTypes]) -> dict[str, FrozenSet[str]]: ...


def get_card_fields(
    card_type: CardTypes | list[CardTypes],
) -> FrozenSet[str] | dict[str, FrozenSet[str]]:
    """Get allowed fields for specific card type(s)."""
    if isinstance(card_type, list):
        return {ct: _get_fields_for_card_type(ct) for ct in card_type}
    return _get_fields_for_card_type(card_type)


@overload
def get_card_schema(card_type: str) -> dict[str, pl.DataType]: ...


@overload
def get_card_schema(card_type: list[str]) -> dict[str, dict[str, pl.DataType]]: ...


def get_card_schema(
    card_type: str | list[str],
) -> dict[str, pl.DataType] | dict[str, dict[str, pl.DataType]]:
    """Get Polars schema dict for passed card_type(s)."""
    if isinstance(card_type, list):
        return {ct: SCHEMA_REGISTRY[ct].get_schema() for ct in card_type}
    return SCHEMA_REGISTRY[card_type].get_schema()


def validate_card_schema(
    df: pl.DataFrame,
    card_type: str = "card_set",
    *,
    strict: bool = False,
) -> pl.DataFrame:
    """Validate and coerce DataFrame to match MTGJSON card schema."""
    return SCHEMA_REGISTRY[card_type].validate_dataframe(df, strict=strict)


def select_columns_for_type(
    df: pl.DataFrame,
    card_type: str = "card_set",
    strip_bools: bool = True,
    validate: bool = False,
) -> pl.DataFrame:
    """Select and order columns for a specific card type."""
    return SCHEMA_REGISTRY[card_type].select_columns(
        df, strip_bools=strip_bools, validate=validate
    )


def pydantic_type_to_polars(py_type: Any) -> pl.DataType:
    """Convert a Python/Pydantic type annotation to a Polars DataType."""
    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Optional (Union with None)
    if origin is Union:
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return pydantic_type_to_polars(non_none_args[0])
        return pl.String()

    # Handle List types
    if origin is list:
        if args:
            inner_type = pydantic_type_to_polars(args[0])
            if isinstance(args[0], type) and issubclass(args[0], BaseModel):
                return pl.List(pydantic_model_to_struct(args[0]))
            return pl.List(inner_type)
        return pl.List(pl.String())

    # Handle dict types
    if origin is dict:
        return pl.String()

    # Handle Pydantic BaseModel
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return pydantic_model_to_struct(py_type)

    type_map = {
        str: pl.String(),
        int: pl.Int64(),
        float: pl.Float64(),
        bool: pl.Boolean(),
        bytes: pl.Binary(),
    }
    return type_map.get(py_type, pl.String())


def pydantic_model_to_struct(model: Type[BaseModel]) -> pl.Struct:
    """Convert a Pydantic model to a Polars Struct type."""
    fields = []
    for field_name, field_info in model.model_fields.items():
        polars_type = pydantic_type_to_polars(field_info.annotation)
        fields.append(pl.Field(field_name, polars_type))
    return pl.Struct(fields)


def pydantic_model_to_schema(model: Type[BaseModel]) -> dict[str, pl.DataType]:
    """Convert a Pydantic model to a Polars schema dict."""
    schema = {}
    for field_name, field_info in model.model_fields.items():
        schema[field_name] = pydantic_type_to_polars(field_info.annotation)
    return schema


def get_model_columns(model: Type[BaseModel]) -> list[str]:
    """Get ordered list of column names from a Pydantic model."""
    return list(model.model_fields.keys())


def get_required_columns(model: Type[BaseModel]) -> list[str]:
    """Get list of required (non-optional) columns from a Pydantic model."""
    return [name for name, info in model.model_fields.items() if info.is_required()]


def apply_pydantic_schema(
    df: pl.DataFrame | pl.LazyFrame,
    model: Type[BaseModel],
    strict: bool = False,
    fill_missing: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Apply a Pydantic model schema to a Polars DataFrame.

    Selects only columns defined in the model, reorders to match field order,
    casts to expected types, and optionally fills missing columns with nulls.
    """
    schema = pydantic_model_to_schema(model)
    model_columns = get_model_columns(model)
    required_columns = get_required_columns(model)

    existing_cols = set(
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )

    if strict:
        missing_required = set(required_columns) - existing_cols
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")

    exprs = []
    for col_name in model_columns:
        expected_type = schema[col_name]
        if col_name in existing_cols:
            exprs.append(pl.col(col_name).cast(expected_type).alias(col_name))
        elif fill_missing:
            exprs.append(pl.lit(None).cast(expected_type).alias(col_name))

    return df.select(exprs)


def validate_pydantic_schema(
    df: pl.DataFrame | pl.LazyFrame,
    model: Type[BaseModel],
) -> tuple[bool, list[str]]:
    """Validate that a DataFrame matches a Pydantic model schema."""
    errors = []
    schema = pydantic_model_to_schema(model)
    required_columns = get_required_columns(model)

    df_schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema

    for col in required_columns:
        if col not in df_schema:
            errors.append(f"Missing required column: {col}")

    for col_name, expected_type in schema.items():
        if col_name in df_schema:
            actual_type = df_schema[col_name]
            if not _types_compatible(actual_type, expected_type):
                errors.append(
                    f"Column '{col_name}' type mismatch: "
                    f"expected {expected_type}, got {actual_type}"
                )

    return len(errors) == 0, errors


def _types_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    """Check if actual type is compatible with expected type."""
    if actual == expected:
        return True

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

    # Handle both DataTypeClass and instances for List
    actual_is_list = (
        isinstance(actual, type)
        and issubclass(actual, pl.List)
        or isinstance(actual, pl.List)
    )
    expected_is_list = (
        isinstance(expected, type)
        and issubclass(expected, pl.List)
        or isinstance(expected, pl.List)
    )

    if actual_is_list and expected_is_list:
        # Only check inner types if both are instances (not classes)
        if not isinstance(actual, type) and not isinstance(expected, type):
            from typing import cast

            actual_inner = cast(Any, actual).inner
            expected_inner = cast(Any, expected).inner
            return _types_compatible(actual_inner, expected_inner)
        return True

    # Handle both DataTypeClass and instances for Struct
    actual_is_struct = (
        isinstance(actual, type)
        and issubclass(actual, pl.Struct)
        or isinstance(actual, pl.Struct)
    )
    expected_is_struct = (
        isinstance(expected, type)
        and issubclass(expected, pl.Struct)
        or isinstance(expected, pl.Struct)
    )

    if actual_is_struct and expected_is_struct:
        # Only check fields if both are instances (not classes)
        if not isinstance(actual, type) and not isinstance(expected, type):
            from typing import cast

            actual_fields = {
                f.name: cast(Any, f).dtype for f in cast(Any, actual).fields
            }
            expected_fields = {
                f.name: cast(Any, f).dtype for f in cast(Any, expected).fields
            }
            for name, exp_type in expected_fields.items():
                if name in actual_fields:
                    if not _types_compatible(actual_fields[name], exp_type):
                        return False
        return True

    if actual == pl.Null:
        return True

    return False


def struct_from_model(model: Type[BaseModel]) -> pl.Expr:
    """Create a Polars struct expression from a Pydantic model."""
    fields = [pl.col(name) for name in model.model_fields]
    return pl.struct(fields)


def reshape_list_column(
    col_name: str,
    model: Type[BaseModel],
    alias: str | None = None,
) -> pl.Expr:
    """Reshape a list column to match a Pydantic model struct."""
    field_names = list(model.model_fields.keys())
    expr = pl.col(col_name).list.eval(
        pl.struct([pl.element().struct.field(f) for f in field_names])
    )
    if alias:
        expr = expr.alias(alias)
    return expr


def pydantic_to_arrow(
    df: pl.DataFrame | pl.LazyFrame,
    model: Type[BaseModel],
    *,
    strict: bool = False,
    fill_missing: bool = True,
) -> "pa.Table":
    """
    Convert DataFrame to Arrow table using Pydantic model schema.

    :param df: Input DataFrame or LazyFrame
    :param model: Pydantic BaseModel defining expected schema
    :param strict: If True, raise error for missing required columns
    :param fill_missing: If True, add missing optional columns as null
    :return: PyArrow Table with schema derived from model
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    result = apply_pydantic_schema(df, model, strict=strict, fill_missing=fill_missing)
    # Since df is now a DataFrame (collected above), result should also be a DataFrame
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    return result.to_arrow()


def pydantic_to_arrow_schema(model: Type[BaseModel]) -> "pa.Schema":
    """
    Get PyArrow schema from a Pydantic model.

    :param model: Pydantic BaseModel class
    :return: PyArrow Schema matching the model's fields
    """
    schema = pydantic_model_to_schema(model)
    df = pl.DataFrame(schema=schema)
    return df.to_arrow().schema
