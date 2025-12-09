"""Card Kingdom API data models."""

from typing import Optional

from pydantic import BaseModel, Field


class ConditionValues(BaseModel):
    """Condition-specific pricing from Card Kingdom."""

    nm_price: Optional[str] = None
    nm_qty: int = 0
    ex_price: Optional[str] = None
    ex_qty: int = 0
    vg_price: Optional[str] = None
    vg_qty: int = 0
    g_price: Optional[str] = None
    g_qty: int = 0


class V1Record(BaseModel):
    """Card record from Card Kingdom V1 API (has etched identifiers)."""

    id: int
    sku: str
    scryfall_id: Optional[str] = None
    url: str
    name: str
    variation: Optional[str] = None
    edition: str
    is_foil: str
    price_retail: Optional[str] = None
    qty_retail: int = 0
    price_buy: Optional[str] = None
    qty_buying: int = 0
    condition_values: ConditionValues = Field(default_factory=ConditionValues)


class V2Record(BaseModel):
    """Card record from Card Kingdom V2 API (has condition pricing)."""

    id: int
    sku: str
    scryfall_id: Optional[str] = None
    url: str
    name: str
    variation: Optional[str] = None
    edition: str
    is_foil: str
    price_retail: Optional[str] = None
    qty_retail: int = 0
    price_buy: Optional[str] = None
    qty_buying: int = 0
    condition_values: ConditionValues = Field(default_factory=ConditionValues)


class SealedRecord(BaseModel):
    """Sealed product record from Card Kingdom API."""

    id: int
    url: str
    name: str
    edition: str
    price_retail: Optional[str] = None
    qty_retail: int = 0
    price_buy: Optional[str] = None
    qty_buying: int = 0
    ships_internationally: bool = False
    # Defaults for fields not in sealed response
    is_foil: str = "false"
    sku: str = ""
    scryfall_id: Optional[str] = None
    variation: Optional[str] = None
    condition_values: ConditionValues = Field(default_factory=ConditionValues)


CKRecord = V1Record | V2Record | SealedRecord


class ApiMeta(BaseModel):
    """Metadata from Card Kingdom API response."""

    created_at: str
    base_url: str


class ApiResponse(BaseModel):
    """Full Card Kingdom API response."""

    meta: ApiMeta
    data: list[CKRecord]
