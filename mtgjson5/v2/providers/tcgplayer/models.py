"""TCGPlayer API data models."""

from collections.abc import Callable
from typing import Optional

import polars as pl
from pydantic import BaseModel, Field

from mtgjson5.mtgjson_config import MtgjsonConfig


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


class TcgPlayerConfig(BaseModel):
    """TCGPlayer API credentials and settings."""

    public_key: str
    private_key: str
    base_url: str = "https://api.tcgplayer.com"
    api_version: str = "v1.39.0"

    model_config = {"frozen": True}

    @property
    def token_url(self) -> str:
        """Return TCGPlayer token endpoint URL."""
        return f"{self.base_url}/token"

    def endpoint_url(self, endpoint: str, versioned: bool = True) -> str:
        """Return full URL for API endpoint."""
        if versioned:
            return f"{self.base_url}/{self.api_version}/{endpoint}"
        return f"{self.base_url}/{endpoint}"

    @classmethod
    def from_mtgjson_config(cls, suffix: str = "") -> Optional["TcgPlayerConfig"]:
        """Load from mtgjson.properties."""
        config = MtgjsonConfig()
        if not config.has_section("TCGPlayer"):
            return None

        key_suffix = f"_{suffix}" if suffix else ""
        try:
            public_key = config.get("TCGPlayer", f"client_id{key_suffix}")
            private_key = config.get("TCGPlayer", f"client_secret{key_suffix}")

            # Skip if keys are empty or missing
            if not public_key or not private_key:
                return None

            return cls(
                public_key=public_key,
                private_key=private_key,
                api_version=config.get("TCGPlayer", "api_version", fallback="v1.39.0"),
            )
        except Exception:
            return None

    @classmethod
    def load_all(cls) -> list["TcgPlayerConfig"]:
        """Load all available API key configs."""
        configs = []
        primary = cls.from_mtgjson_config("")
        if primary:
            configs.append(primary)
        for i in range(2, 10):
            alt = cls.from_mtgjson_config(str(i))
            if alt:
                configs.append(alt)
            else:
                break
        return configs


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
