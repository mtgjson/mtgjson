"""TCGPlayer API data models."""

from collections.abc import Callable

import polars as pl
from pydantic import BaseModel, Field


class Sku(BaseModel):
    """Single SKU from TCGPlayer API."""

    skuId: int
    languageId: int
    printingId: int
    conditionId: int


class Product(BaseModel):
    """Single product from TCGPlayer API."""

    productId: int
    name: str = ""
    cleanName: str = ""
    groupId: int | None = None
    url: str = ""
    skus: list[Sku] = Field(default_factory=list)


class ProductsResponse(BaseModel):
    """TCGPlayer catalog/products API response."""

    totalItems: int = 0
    results: list[Product] = Field(default_factory=list)


class FetchResult(BaseModel):
    """Result of fetching a products page."""

    products: list[Product] = Field(default_factory=list)
    total_items: int = 0
    offset: int = 0
    success: bool = True
    error_message: str | None = None

    @property
    def error(self) -> bool:
        """Return True if fetch failed."""
        return not self.success


SKU_STRUCT = pl.Struct(
    {
        "skuId": pl.Int64,
        "languageId": pl.Int64,
        "printingId": pl.Int64,
        "conditionId": pl.Int64,
    }
)

PRODUCT_SCHEMA = {
    "productId": pl.Int64,
    "name": pl.String,
    "cleanName": pl.String,
    "groupId": pl.Int64,
    "url": pl.String,
    "skus": pl.List(SKU_STRUCT),
}

LANGUAGE_MAP = {
    1: "ENGLISH",
    2: "CHINESE SIMPLIFIED",
    3: "CHINESE TRADITIONAL",
    4: "FRENCH",
    5: "GERMAN",
    6: "ITALIAN",
    7: "JAPANESE",
    8: "KOREAN",
    9: "PORTUGUESE BRAZIL",
    10: "RUSSIAN",
    11: "SPANISH",
}
PRINTING_MAP = {1: "NON_FOIL", 2: "FOIL"}
CONDITION_MAP = {
    1: "NEAR MINT",
    2: "LIGHTLY PLAYED",
    3: "MODERATELY PLAYED",
    4: "HEAVILY PLAYED",
    5: "DAMAGED",
    6: "UNOPENED",
}

ENGLISH = 1
NEAR_MINT = 1
NON_FOIL = 1
FOIL = 2

ProgressCallback = Callable[[int, int, str], None]
