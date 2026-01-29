"""Sealed product data models."""

from typing import Any, get_args, get_origin

import polars as pl
from pydantic import BaseModel, Field


def _python_type_to_polars(py_type: Any) -> pl.DataType:
    """Convert a Python type annotation to a Polars DataType."""
    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle None type
    if py_type is type(None):
        return pl.Null()

    # Handle Union types (e.g., str | None)
    if origin is type(None) or (hasattr(origin, "__class__") and origin is None):
        return pl.Null()

    # Handle Optional/Union (str | None becomes args=(str, NoneType))
    if origin is not None and hasattr(origin, "__mro__") is False:
        # Check for Union-like types
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return _python_type_to_polars(non_none_args[0])
        if len(non_none_args) > 1:
            # Multiple non-None types - just use first one
            return _python_type_to_polars(non_none_args[0])

    # Handle list types
    if origin is list:
        if args:
            inner_type = _python_type_to_polars(args[0])
            return pl.List(inner_type)
        return pl.List(pl.Unknown)

    # Handle dict types
    if origin is dict:
        return pl.Object()

    # Handle Pydantic BaseModel (convert to Struct)
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        fields = []
        for field_name, field_info in py_type.model_fields.items():
            field_dtype = _python_type_to_polars(field_info.annotation)
            fields.append(pl.Field(field_name, field_dtype))
        return pl.Struct(fields)

    # Primitive types
    type_map = {
        str: pl.String(),
        int: pl.Int64(),
        float: pl.Float64(),
        bool: pl.Boolean(),
        bytes: pl.Binary(),
    }

    if py_type in type_map:
        return type_map[py_type]

    # Fallback
    return pl.Unknown()


class PolarsSchemaModel(BaseModel):
    """Base model with Polars schema generation."""

    @classmethod
    def polars_schema(cls) -> dict[str, pl.DataType]:
        """Generate a Polars schema dict from this Pydantic model."""
        schema = {}
        for field_name, field_info in cls.model_fields.items():
            schema[field_name] = _python_type_to_polars(field_info.annotation)
        return schema

    @classmethod
    def polars_schema_doc(cls) -> str:
        """Generate formatted Polars schema documentation."""
        schema = cls.polars_schema()
        lines = [f"{cls.__name__} Polars schema:"]
        for name, dtype in schema.items():
            lines.append(f"    {name}: {dtype}")
        return "\n".join(lines)


class DeckRef(PolarsSchemaModel):
    """Reference to a deck in sealed product content."""

    set: str
    name: str


class PackRef(PolarsSchemaModel):
    """Reference to a booster pack in sealed product content."""

    set: str
    code: str


class SealedRef(PolarsSchemaModel):
    """Reference to a sealed product within sealed product content."""

    set: str
    count: int
    name: str
    uuid: str


class CardRef(PolarsSchemaModel):
    """Reference to a card in sealed product content."""

    name: str
    set: str
    number: str
    uuid: str
    foil: bool


class VariableConfig(PolarsSchemaModel):
    """Configuration for variable content with chance and weight."""

    chance: int
    weight: int


class ContentConfig(PolarsSchemaModel):
    """Configuration for sealed product content types."""

    deck: list[DeckRef] | None = None
    variable_config: list[VariableConfig] | None = None
    pack: list[PackRef] | None = None
    sealed: list[SealedRef] | None = None
    card: list[CardRef] | None = None


class CardEntryModel(PolarsSchemaModel):
    """Card entry in a deck or sealed product."""

    uuid: str = Field(..., description="The UUID of the card.")
    count: int = Field(..., description="The count of the card.")
    isFoil: bool = Field(default=False, description="Indicates if the card is foil.")
    isEtched: bool = Field(
        default=False, description="Indicates if the card is etched foil."
    )


class CardToProductsModel(PolarsSchemaModel):
    """Mapping of card UUIDs to sealed product UUIDs by finish type."""

    uuid: str = Field(..., description="The UUID of the card.")
    foil: list[str] = Field(
        default_factory=list,
        description="List of product UUIDs where this card is available in foil.",
    )
    nonfoil: list[str] = Field(
        default_factory=list,
        description="List of product UUIDs where this card is available in non-foil.",
    )
    etched: list[str] = Field(
        default_factory=list,
        description="List of product UUIDs where this card is available in etched foil.",
    )


class SealedProductModel(PolarsSchemaModel):
    """Sealed product metadata model."""

    setCode: str = Field(..., description="The set code of the sealed product.")
    productName: str = Field(..., description="The name of the sealed product.")
    category: str = Field(..., description="The category of the sealed product.")
    identifiers: dict[str, str | None] = Field(
        default_factory=dict,
        description="A dictionary of various identifiers for the product.",
    )
    subtype: str | None = Field(
        default=None, description="The subtype of the sealed product."
    )
    release_date: str | None = Field(
        default=None, description="The release date of the sealed product."
    )
    language: str | None = Field(
        default=None, description="The language of the sealed product."
    )


class SealedContentModel(PolarsSchemaModel):
    """Sealed product content details model."""

    setCode: str = Field(..., description="The set code of the sealed content.")
    productName: str = Field(..., description="The name of the sealed product.")
    productSize: int | None = Field(
        default=None, description="The size of the product, if applicable."
    )
    cardCount: int | None = Field(
        default=None, description="The number of cards in the sealed content."
    )
    contentType: str = Field(
        ..., description="The type of content (e.g., booster, deck)."
    )
    set: str | None = Field(
        default=None, description="The set associated with the content."
    )
    count: int | None = Field(
        default=None, description="The count of this particular content item."
    )
    name: str | None = Field(
        default=None, description="The name of the card in the sealed content."
    )
    uuid: str | None = Field(
        default=None, description="The UUID of the card in the sealed content."
    )
    code: str | None = Field(
        default=None, description="The code of the card in the sealed content."
    )
    configs: list[ContentConfig] | None = Field(
        default=None, description="Configuration details for the sealed content."
    )
    number: str | None = Field(
        default=None, description="The collector number of the card."
    )
    foil: bool | None = Field(
        default=None, description="Indicates if the card is foil."
    )


class PreconModel(PolarsSchemaModel):
    """Preconstructed deck model."""

    name: str = Field(..., description="The name of the deck.")
    setCode: str = Field(..., description="The set code associated with the deck.")
    type: str = Field(
        ..., description="The type of the deck (e.g., Commander, Standard)."
    )
    releaseDate: str = Field(..., description="The release date of the deck.")
    sourceSetCodes: list[str] = Field(
        default_factory=list,
        description="List of source set codes for the deck.",
    )
    sealedProductUuids: list[str | None] = Field(
        default_factory=list,
        description="List of sealed product UUIDs included in the deck.",
    )
    mainBoard: list[CardEntryModel] = Field(
        default_factory=list,
        description="List of uuids of cards found in the main board of the deck.",
    )
    sideBoard: list[CardEntryModel] = Field(
        default_factory=list,
        description="List of uuids of cards found in the side board of the deck.",
    )
    commander: list[CardEntryModel] = Field(
        default_factory=list,
        description="List of uuids of the decks commander, if any.",
    )


class BoosterModel(PolarsSchemaModel):
    """Booster configuration model."""

    setCode: str = Field(..., description="The set code of the booster configuration.")
    config: str = Field(..., description="The configuration name of the booster.")
