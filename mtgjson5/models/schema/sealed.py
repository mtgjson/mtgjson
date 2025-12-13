"""
MTGJSON schema definitions for sealed products, sets, and decks.

This module provides:
- Field sets and Polars schema definitions for sealed product types
- Deck schema with card list structures
- Class-based schema definitions with transformation helpers
"""

from abc import ABC
from functools import lru_cache
from typing import Any, ClassVar, FrozenSet, Literal, get_args, get_origin

import polars as pl
from pydantic import BaseModel, Field


def python_type_to_polars(py_type: Any) -> pl.DataType:
    """Convert a Python type annotation to a Polars DataType."""
    origin = get_origin(py_type)
    args = get_args(py_type)

    if py_type is type(None):
        return pl.Null()

    if origin is type(None) or (hasattr(origin, "__class__") and origin is None):
        return pl.Null()

    if origin is not None and hasattr(origin, "__mro__") is False:
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return python_type_to_polars(non_none_args[0])
        if len(non_none_args) > 1:
            return python_type_to_polars(non_none_args[0])

    if origin is list:
        if args:
            inner_type = python_type_to_polars(args[0])
            return pl.List(inner_type)
        return pl.List(pl.Unknown())

    if origin is dict:
        return pl.Object()

    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        fields = []
        for field_name, field_info in py_type.model_fields.items():
            field_dtype = python_type_to_polars(field_info.annotation)
            fields.append(pl.Field(field_name, field_dtype))
        return pl.Struct(fields)

    type_map = {
        str: pl.String(),
        int: pl.Int64(),
        float: pl.Float64(),
        bool: pl.Boolean(),
        bytes: pl.Binary(),
    }

    if py_type in type_map:
        return type_map[py_type]

    return pl.Unknown()


class PolarsSchemaModel(BaseModel):
    """Base model with Polars schema generation."""

    @classmethod
    def polars_schema(cls) -> dict[str, pl.DataType]:
        """Generate a Polars schema dict from this Pydantic model."""
        schema = {}
        for field_name, field_info in cls.model_fields.items():
            schema[field_name] = python_type_to_polars(field_info.annotation)
        return schema

    @classmethod
    def polars_struct(cls) -> pl.Struct:
        """Generate a Polars Struct type from this model."""
        fields = []
        for field_name, field_info in cls.model_fields.items():
            field_dtype = python_type_to_polars(field_info.annotation)
            fields.append(pl.Field(field_name, field_dtype))
        return pl.Struct(fields)

    @classmethod
    def polars_schema_doc(cls) -> str:
        """Generate formatted Polars schema documentation."""
        schema = cls.polars_schema()
        lines = [f"{cls.__name__} Polars schema:"]
        for name, dtype in schema.items():
            lines.append(f"    {name}: {dtype}")
        return "\n".join(lines)

    @classmethod
    def field_names(cls) -> list[str]:
        """Get ordered list of field names."""
        return list(cls.model_fields.keys())


class DeckRef(PolarsSchemaModel):
    """Reference to a deck in sealed product contents."""

    set: str
    name: str


class PackRef(PolarsSchemaModel):
    """Reference to a booster pack in sealed product contents."""

    set: str
    code: str


class SealedRef(PolarsSchemaModel):
    """Reference to a nested sealed product in contents."""

    set: str
    count: int
    name: str
    uuid: str


class CardRef(PolarsSchemaModel):
    """Reference to a card in sealed product contents."""

    name: str
    set: str
    number: str
    uuid: str
    foil: bool


class VariableConfig(PolarsSchemaModel):
    """Weight/chance configuration for variable contents."""

    chance: int
    weight: int


class ContentConfig(PolarsSchemaModel):
    """Configuration for variable content options."""

    deck: list[DeckRef] | None = None
    variable_config: list[VariableConfig] | None = None
    pack: list[PackRef] | None = None
    sealed: list[SealedRef] | None = None
    card: list[CardRef] | None = None


class CardEntryModel(PolarsSchemaModel):
    """A card entry in a deck list with count and foil info."""

    uuid: str = Field(..., description="The UUID of the card.")
    count: int = Field(..., description="The count of the card.")
    isFoil: bool = Field(default=False, description="Indicates if the card is foil.")
    isEtched: bool = Field(
        default=False, description="Indicates if the card is etched foil."
    )


class CardEntryOutputModel(PolarsSchemaModel):
    """Card entry in MTGJSON output format (count + uuid only)."""

    count: int = Field(..., description="The count of the card.")
    uuid: str = Field(..., description="The UUID of the card.")


class SealedProductIdentifiersModel(PolarsSchemaModel):
    """External identifiers for a sealed product."""

    abuId: str | None = None
    cardKingdomId: str | None = None
    cardtraderId: str | None = None
    csiId: str | None = None
    mcmId: str | None = None
    miniaturemarketId: str | None = None
    mvpId: str | None = None
    scgId: str | None = None
    tcgplayerProductId: str | None = None
    tntId: str | None = None


class SealedProductModel(PolarsSchemaModel):
    """A sealed product input model."""

    setCode: str = Field(..., description="The set code of the sealed product.")
    productName: str = Field(..., description="The name of the sealed product.")
    category: str = Field(..., description="The category of the sealed product.")
    identifiers: dict[str, str | None] = Field(default_factory=dict)
    subtype: str | None = None
    release_date: str | None = None
    language: str | None = None


class SealedContentModel(PolarsSchemaModel):
    """A single content entry for a sealed product."""

    setCode: str = Field(..., description="The set code of the sealed content.")
    productName: str = Field(..., description="The name of the sealed product.")
    productSize: int | None = None
    cardCount: int | None = None
    contentType: str = Field(..., description="The type of content.")
    set: str | None = None
    count: int | None = None
    name: str | None = None
    uuid: str | None = None
    code: str | None = None
    configs: list[ContentConfig] | None = None
    number: str | None = None
    foil: bool | None = None


class SealedPurchaseUrlsModel(PolarsSchemaModel):
    """Purchase URLs for a sealed product."""

    cardKingdom: str | None = None
    cardmarket: str | None = None
    tcgplayer: str | None = None


class SealedContentsOutputModel(PolarsSchemaModel):
    """Contents structure in MTGJSON sealed product output."""

    card: list[CardRef] | None = None
    sealed: list[SealedRef] | None = None
    other: list[dict] | None = None


class SealedProductOutputModel(PolarsSchemaModel):
    """Sealed product in MTGJSON output format."""

    setCode: str
    name: str
    category: str | None = None
    subtype: str | None = None
    releaseDate: str | None = None
    identifiers: SealedProductIdentifiersModel | None = None
    contents: SealedContentsOutputModel | None = None
    purchaseUrls: SealedPurchaseUrlsModel | None = None
    uuid: str


class DeckOutputModel(PolarsSchemaModel):
    """Deck in MTGJSON output format."""

    setCode: str
    code: str
    name: str
    type: str | None = None
    releaseDate: str | None = None
    sealedProductUuids: list[str] | None = Field(default_factory=list)
    mainBoard: list[CardEntryOutputModel] = Field(default_factory=list)
    sideBoard: list[CardEntryOutputModel] = Field(default_factory=list)
    commander: list[CardEntryOutputModel] = Field(default_factory=list)
    displayCommander: list[str] | None = Field(default_factory=list)
    planes: list[str] | None = Field(default_factory=list)
    schemes: list[str] | None = Field(default_factory=list)
    sourceSetCodes: list[str] | None = Field(default_factory=list)
    tokens: list[str] | None = Field(default_factory=list)


# =============================================================================
# Booster Model
# =============================================================================


class BoosterModel(PolarsSchemaModel):
    """Booster configuration model."""

    setCode: str
    config: str


# =============================================================================
# Set Metadata Model
# =============================================================================


class CardToProductsModel(PolarsSchemaModel):
    """Maps a card UUID to sealed products containing it."""

    uuid: str
    foil: list[str] = Field(default_factory=list)
    nonfoil: list[str] = Field(default_factory=list)
    etched: list[str] = Field(default_factory=list)


# =============================================================================
# Field Definitions
# =============================================================================

# Sealed Product Identifier Fields
SEALED_IDENTIFIER_FIELDS: FrozenSet[str] = frozenset(
    {
        "abuId",
        "cardKingdomId",
        "cardtraderId",
        "csiId",
        "mcmId",
        "miniaturemarketId",
        "mvpId",
        "scgId",
        "tcgplayerProductId",
        "tntId",
    }
)

# Sealed Product Purchase URL Fields
SEALED_PURCHASE_URL_FIELDS: FrozenSet[str] = frozenset(
    {
        "cardKingdom",
        "cardmarket",
        "tcgplayer",
    }
)

# All Sealed Product Fields
ALL_SEALED_PRODUCT_FIELDS: FrozenSet[str] = frozenset(
    {
        "setCode",
        "name",
        "category",
        "subtype",
        "releaseDate",
        "language",
        "identifiers",
        "contents",
        "purchaseUrls",
        "uuid",
    }
)

# Set Metadata Fields
ALL_SET_FIELDS: FrozenSet[str] = frozenset(
    {
        # Core
        "code",
        "name",
        "releaseDate",
        "type",
        "block",
        # Sizes
        "baseSetSize",
        "totalSetSize",
        # Identifiers
        "mcmId",
        "mcmIdExtras",
        "mcmName",
        "tcgplayerGroupId",
        "mtgoCode",
        "keyruneCode",
        "tokenSetCode",
        # External
        "cardsphereSetId",
        # Flags
        "isFoilOnly",
        "isNonFoilOnly",
        "isOnlineOnly",
        "isForeignOnly",
        "isPartialPreview",
        # Relations
        "parentCode",
        # Nested
        "booster",
        "translations",
        "sealedProduct",
        "decks",
        # Languages (derived from foreign data)
        "languages",
    }
)

# Set Translation Languages
TRANSLATION_LANGUAGES: FrozenSet[str] = frozenset(
    {
        "Chinese Simplified",
        "Chinese Traditional",
        "French",
        "German",
        "Italian",
        "Japanese",
        "Korean",
        "Portuguese (Brazil)",
        "Russian",
        "Spanish",
    }
)

# Deck Fields
ALL_DECK_FIELDS: FrozenSet[str] = frozenset(
    {
        "setCode",
        "code",
        "name",
        "type",
        "releaseDate",
        "sealedProductUuids",
        "mainBoard",
        "sideBoard",
        "commander",
        "displayCommander",
        "planes",
        "schemes",
        "sourceSetCodes",
        "tokens",
    }
)

# Deck Card List Fields (for mainBoard, sideBoard, commander)
DECK_CARD_FIELDS: FrozenSet[str] = frozenset(
    {
        "count",
        "uuid",
    }
)


# =============================================================================
# Polars Type Definitions
# =============================================================================

# Sealed Content Card Struct
SEALED_CONTENT_CARD_STRUCT = pl.Struct(
    {
        "name": pl.String(),
        "number": pl.String(),
        "set": pl.String(),
        "uuid": pl.String(),
        "foil": pl.Boolean(),
    }
)

# Sealed Content Sealed Struct
SEALED_CONTENT_SEALED_STRUCT = pl.Struct(
    {
        "count": pl.Int64(),
        "name": pl.String(),
        "set": pl.String(),
        "uuid": pl.String(),
    }
)

# Sealed Content Other Struct
SEALED_CONTENT_OTHER_STRUCT = pl.Struct(
    {
        "name": pl.String(),
    }
)

# Sealed Product Contents Struct
SEALED_CONTENTS_STRUCT = pl.Struct(
    {
        "card": pl.List(SEALED_CONTENT_CARD_STRUCT),
        "sealed": pl.List(SEALED_CONTENT_SEALED_STRUCT),
        "other": pl.List(SEALED_CONTENT_OTHER_STRUCT),
    }
)

# Sealed Product Identifiers Struct
SEALED_IDENTIFIERS_STRUCT = pl.Struct(
    {field: pl.String() for field in SEALED_IDENTIFIER_FIELDS}
)

# Sealed Purchase URLs Struct
SEALED_PURCHASE_URLS_STRUCT = pl.Struct(
    {field: pl.String() for field in SEALED_PURCHASE_URL_FIELDS}
)

# Deck Card Entry Struct
DECK_CARD_STRUCT = pl.Struct(
    {
        "count": pl.Int64(),
        "uuid": pl.String(),
    }
)

# Set Translations Struct
TRANSLATIONS_STRUCT = pl.Struct({lang: pl.String() for lang in TRANSLATION_LANGUAGES})

# Field Type Mappings
SEALED_PRODUCT_FIELD_TYPES: dict[str, pl.DataType] = {
    "setCode": pl.String(),
    "name": pl.String(),
    "category": pl.String(),
    "subtype": pl.String(),
    "releaseDate": pl.String(),
    "language": pl.String(),
    "identifiers": SEALED_IDENTIFIERS_STRUCT,
    "contents": SEALED_CONTENTS_STRUCT,
    "purchaseUrls": SEALED_PURCHASE_URLS_STRUCT,
    "uuid": pl.String(),
}

SET_FIELD_TYPES: dict[str, pl.DataType] = {
    # Core
    "code": pl.String(),
    "name": pl.String(),
    "releaseDate": pl.String(),
    "type": pl.String(),
    "block": pl.String(),
    # Sizes
    "baseSetSize": pl.Int64(),
    "totalSetSize": pl.Int64(),
    # Identifiers
    "mcmId": pl.Int64(),
    "mcmIdExtras": pl.Int64(),
    "mcmName": pl.String(),
    "tcgplayerGroupId": pl.Int64(),
    "mtgoCode": pl.String(),
    "keyruneCode": pl.String(),
    "tokenSetCode": pl.String(),
    # External
    "cardsphereSetId": pl.Int64(),
    # Flags
    "isFoilOnly": pl.Boolean(),
    "isNonFoilOnly": pl.Boolean(),
    "isOnlineOnly": pl.Boolean(),
    "isForeignOnly": pl.Boolean(),
    "isPartialPreview": pl.Boolean(),
    # Relations
    "parentCode": pl.String(),
    # Nested - booster is kept as JSON string to avoid schema conflicts
    "booster": pl.String(),
    "translations": TRANSLATIONS_STRUCT,
    "sealedProduct": pl.List(pl.String()),  # UUIDs of sealed products
    "decks": pl.List(pl.String()),  # Names of decks in set
    # Languages
    "languages": pl.List(pl.String()),
}

DECK_FIELD_TYPES: dict[str, pl.DataType] = {
    "setCode": pl.String(),
    "code": pl.String(),
    "name": pl.String(),
    "type": pl.String(),
    "releaseDate": pl.String(),
    "sealedProductUuids": pl.List(pl.String()),
    "mainBoard": pl.List(DECK_CARD_STRUCT),
    "sideBoard": pl.List(DECK_CARD_STRUCT),
    "commander": pl.List(DECK_CARD_STRUCT),
    "displayCommander": pl.List(pl.String()),
    "planes": pl.List(pl.String()),
    "schemes": pl.List(pl.String()),
    "sourceSetCodes": pl.List(pl.String()),
    "tokens": pl.List(pl.String()),
}


# =============================================================================
# Schema Type Literals
# =============================================================================

SealedTypes = Literal["sealed_product"]
SetTypes = Literal["set"]
DeckTypes = Literal["deck"]


# =============================================================================
# Base Schema Class
# =============================================================================


class BaseSchema(ABC):
    """Base class for MTGJSON schema definitions."""

    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset()
    EXTRA_FIELDS: ClassVar[FrozenSet[str]] = frozenset()
    ALL_FIELDS: ClassVar[FrozenSet[str]] = frozenset()
    FIELD_TYPES: ClassVar[dict[str, pl.DataType]] = {}
    TYPE_NAME: ClassVar[str] = ""

    @classmethod
    @lru_cache(maxsize=1)
    def get_fields(cls) -> FrozenSet[str]:
        """Get the set of allowed fields for this type."""
        return (cls.ALL_FIELDS - cls.EXCLUDE_FIELDS) | cls.EXTRA_FIELDS

    @classmethod
    @lru_cache(maxsize=1)
    def get_field_list(cls) -> list[str]:
        """Cached sorted list of fields for this type."""
        return sorted(cls.get_fields())

    @classmethod
    @lru_cache(maxsize=1)
    def get_schema(cls) -> dict[str, pl.DataType]:
        """Get the Polars schema for this type."""
        allowed_fields = cls.get_fields()
        return {
            field: dtype
            for field, dtype in cls.FIELD_TYPES.items()
            if field in allowed_fields
        }

    @classmethod
    @lru_cache(maxsize=1)
    def get_select_exprs(cls) -> tuple[pl.Expr, ...]:
        """Pre-built select expressions with type casting."""
        schema = cls.get_schema()
        return tuple(
            pl.col(field).cast(dtype) for field, dtype in sorted(schema.items())
        )

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
        """Validate and coerce DataFrame to match this schema."""
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
        validate: bool = False,
    ) -> pl.DataFrame:
        """Select and order columns for this type."""
        if validate:
            df = cls.validate_dataframe(df)

        allowed_fields = cls.get_fields()
        existing_fields = [col for col in df.columns if col in allowed_fields]
        return df.select(sorted(existing_fields))


# =============================================================================
# Sealed Product Schema
# =============================================================================


class SealedProductSchema(BaseSchema):
    """Schema for MTGJSON sealed product output."""

    TYPE_NAME: ClassVar[str] = "sealed_product"
    ALL_FIELDS: ClassVar[FrozenSet[str]] = ALL_SEALED_PRODUCT_FIELDS
    FIELD_TYPES: ClassVar[dict[str, pl.DataType]] = SEALED_PRODUCT_FIELD_TYPES
    EXCLUDE_FIELDS: ClassVar[FrozenSet[str]] = frozenset({"language"})

    @classmethod
    def reshape_contents(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Reshape raw content lists into the contents struct.

        Expects columns: _card_list, _sealed_list, _other_list
        Returns: contents struct column
        """
        return lf.with_columns(
            pl.struct(
                card=pl.col("_card_list"),
                sealed=pl.col("_sealed_list"),
                other=pl.col("_other_list"),
            ).alias("contents")
        ).drop(["_card_list", "_sealed_list", "_other_list"], strict=False)

    @classmethod
    def build_purchase_urls(
        cls,
        lf: pl.LazyFrame,
        base_url: str = "https://mtgjson.com/links/",
    ) -> pl.LazyFrame:
        """
        Build purchaseUrls struct from identifiers.

        Args:
            lf: LazyFrame with uuid and identifiers columns
            base_url: Base URL for MTGJSON redirect links

        Returns:
            LazyFrame with purchaseUrls struct column added
        """
        schema = lf.collect_schema()
        if "identifiers" not in schema:
            return lf.with_columns(pl.struct([]).alias("purchaseUrls"))

        id_schema = schema.get("identifiers")
        if not isinstance(id_schema, pl.Struct):
            return lf.with_columns(pl.struct([]).alias("purchaseUrls"))

        id_fields = {f.name for f in id_schema.fields}
        purchase_exprs = []

        # Card Kingdom
        if "cardKingdomId" in id_fields:
            purchase_exprs.append(
                pl.when(
                    pl.col("identifiers").struct.field("cardKingdomId").is_not_null()
                )
                .then(pl.lit(base_url) + pl.col("uuid") + pl.lit("cardKingdom"))
                .otherwise(None)
                .alias("cardKingdom")
            )

        # TCGPlayer
        if "tcgplayerProductId" in id_fields:
            purchase_exprs.append(
                pl.when(
                    pl.col("identifiers")
                    .struct.field("tcgplayerProductId")
                    .is_not_null()
                )
                .then(pl.lit(base_url) + pl.col("uuid") + pl.lit("tcgplayer"))
                .otherwise(None)
                .alias("tcgplayer")
            )

        # Cardmarket (MCM)
        if "mcmId" in id_fields:
            purchase_exprs.append(
                pl.when(pl.col("identifiers").struct.field("mcmId").is_not_null())
                .then(pl.lit(base_url) + pl.col("uuid") + pl.lit("cardmarket"))
                .otherwise(None)
                .alias("cardmarket")
            )

        if purchase_exprs:
            return lf.with_columns(pl.struct(purchase_exprs).alias("purchaseUrls"))
        return lf.with_columns(pl.struct([]).alias("purchaseUrls"))


# =============================================================================
# Set Metadata Schema
# =============================================================================


class SetSchema(BaseSchema):
    """Schema for MTGJSON set metadata output."""

    TYPE_NAME: ClassVar[str] = "set"
    ALL_FIELDS: ClassVar[FrozenSet[str]] = ALL_SET_FIELDS
    FIELD_TYPES: ClassVar[dict[str, pl.DataType]] = SET_FIELD_TYPES

    # Scryfall-only fields to exclude from MTGJSON output
    SCRYFALL_ONLY_FIELDS: ClassVar[FrozenSet[str]] = frozenset(
        {
            "object",
            "uri",
            "scryfallUri",
            "searchUri",
            "iconSvgUri",
            "id",
            "scryfall_id",
            "arena_code",
            "scryfall_set_uri",
        }
    )

    @classmethod
    def build_base_exprs(cls, available_cols: list[str]) -> list[pl.Expr]:
        """
        Build base transformation expressions for set metadata.

        Args:
            available_cols: List of available columns in source DataFrame

        Returns:
            List of Polars expressions for column transformations
        """
        exprs = [
            pl.col("code").str.to_uppercase().alias("code"),
            pl.col("name"),
            pl.col("releasedAt").alias("releaseDate"),
            pl.col("setType").alias("type"),
            pl.col("digital").alias("isOnlineOnly"),
            pl.col("foilOnly").alias("isFoilOnly"),
        ]

        # Optional columns with renames
        if "mtgoCode" in available_cols:
            exprs.append(pl.col("mtgoCode").str.to_uppercase().alias("mtgoCode"))
        if "tcgplayerId" in available_cols:
            exprs.append(pl.col("tcgplayerId").alias("tcgplayerGroupId"))
        if "nonfoilOnly" in available_cols:
            exprs.append(pl.col("nonfoilOnly").alias("isNonFoilOnly"))
        if "parentSetCode" in available_cols:
            exprs.append(pl.col("parentSetCode").str.to_uppercase().alias("parentCode"))
        if "block" in available_cols:
            exprs.append(pl.col("block"))

        # Set sizes
        if "cardCount" in available_cols:
            exprs.append(pl.col("cardCount").alias("totalSetSize"))
        if "printedSize" in available_cols:
            exprs.append(pl.col("printedSize").alias("baseSetSize"))
        elif "cardCount" in available_cols:
            exprs.append(pl.col("cardCount").alias("baseSetSize"))

        # Keyrune code extraction from icon URL
        if "iconSvgUri" in available_cols:
            exprs.append(
                pl.col("iconSvgUri")
                .str.extract(r"/([^/]+)\.svg", 1)
                .str.to_uppercase()
                .alias("keyruneCode")
            )

        # Token set code
        if "tokenSetCode" in available_cols:
            exprs.append(
                pl.coalesce(
                    pl.col("tokenSetCode"),
                    pl.when(pl.col("code").str.starts_with("T"))
                    .then(pl.col("code").str.to_uppercase())
                    .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase()),
                ).alias("tokenSetCode")
            )
        else:
            exprs.append(
                pl.when(pl.col("code").str.starts_with("T"))
                .then(pl.col("code").str.to_uppercase())
                .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase())
                .alias("tokenSetCode")
            )

        return exprs

    @classmethod
    def drop_scryfall_fields(cls, df: pl.DataFrame) -> pl.DataFrame:
        """Remove Scryfall-only fields from DataFrame."""
        cols_to_drop = [
            c
            for c in df.columns
            if c in cls.SCRYFALL_ONLY_FIELDS or c.lower() in cls.SCRYFALL_ONLY_FIELDS
        ]
        if cols_to_drop:
            return df.drop(cols_to_drop, strict=False)
        return df

    @classmethod
    def default_translations(cls) -> dict[str, str | None]:
        """Return default translations struct with all languages as None."""
        return {lang: None for lang in TRANSLATION_LANGUAGES}


class DeckSchema(BaseSchema):
    """Schema for MTGJSON deck output."""

    TYPE_NAME: ClassVar[str] = "deck"
    ALL_FIELDS: ClassVar[FrozenSet[str]] = ALL_DECK_FIELDS
    FIELD_TYPES: ClassVar[dict[str, pl.DataType]] = DECK_FIELD_TYPES

    @staticmethod
    def reshape_card_list(col_name: str) -> pl.Expr:
        """
        Reshape a deck card list column to only include count and uuid fields.

        Removes fields like is_foil, is_etched, isFoil from deck card structs.

        Args:
            col_name: Name of the list column containing card structs

        Returns:
            Polars expression that reshapes the card list
        """
        return (
            pl.col(col_name)
            .list.eval(
                pl.struct(
                    [
                        pl.element().struct.field("count"),
                        pl.element().struct.field("uuid"),
                    ]
                )
            )
            .alias(col_name)
        )

    @classmethod
    def build_select_exprs(cls, available_cols: list[str]) -> list[pl.Expr]:
        """
        Build select expressions for deck output.

        Args:
            available_cols: List of available columns in source DataFrame

        Returns:
            List of Polars expressions for all 14 deck fields
        """
        exprs = [
            (
                pl.col("setCode")
                if "setCode" in available_cols
                else pl.lit(None).cast(pl.String).alias("setCode")
            ),
            (
                pl.col("setCode").alias("code")
                if "setCode" in available_cols
                else pl.lit(None).cast(pl.String).alias("code")
            ),
            (
                pl.col("name")
                if "name" in available_cols
                else pl.lit(None).cast(pl.String).alias("name")
            ),
            (
                pl.col("type")
                if "type" in available_cols
                else pl.lit(None).cast(pl.String).alias("type")
            ),
        ]

        # releaseDate
        if "releaseDate" in available_cols:
            exprs.append(pl.col("releaseDate"))
        else:
            exprs.append(pl.lit(None).cast(pl.String).alias("releaseDate"))

        # sealedProductUuids
        if "sealedProductUuids" in available_cols:
            exprs.append(pl.col("sealedProductUuids"))
        else:
            exprs.append(
                pl.lit([]).cast(pl.List(pl.String)).alias("sealedProductUuids")
            )

        # Card list columns with reshaping
        for col in ["mainBoard", "sideBoard", "commander"]:
            if col in available_cols:
                exprs.append(cls.reshape_card_list(col))
            else:
                exprs.append(pl.lit([]).cast(pl.List(DECK_CARD_STRUCT)).alias(col))

        # String list columns
        for col in [
            "displayCommander",
            "planes",
            "schemes",
            "sourceSetCodes",
            "tokens",
        ]:
            if col in available_cols:
                exprs.append(pl.col(col))
            else:
                exprs.append(pl.lit([]).cast(pl.List(pl.String)).alias(col))

        return exprs


SEALED_SCHEMA_REGISTRY: dict[str, type[BaseSchema]] = {
    "sealed_product": SealedProductSchema,
    "set": SetSchema,
    "deck": DeckSchema,
}


def get_sealed_product_schema() -> dict[str, pl.DataType]:
    """Get Polars schema dict for sealed products."""
    return SealedProductSchema.get_schema()


def get_set_schema() -> dict[str, pl.DataType]:
    """Get Polars schema dict for set metadata."""
    return SetSchema.get_schema()


def get_deck_schema() -> dict[str, pl.DataType]:
    """Get Polars schema dict for decks."""
    return DeckSchema.get_schema()
