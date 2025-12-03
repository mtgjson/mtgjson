"""
Card Kingdom price data fetcher and processor.

Fetches pricing data from the Card Kingdom API and provides it in formats
optimized for joining to MTGJSON card data.
"""

import hashlib
import logging
from pathlib import Path
from typing import Optional

import aiohttp
import polars as pl
from pydantic import BaseModel, Field

LOGGER = logging.getLogger(__name__)


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


class CardRecord(BaseModel):
    """Single card record from Card Kingdom API."""

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


class ApiMeta(BaseModel):
    """Metadata from Card Kingdom API response."""

    created_at: str
    base_url: str


class ApiResponse(BaseModel):
    """Full Card Kingdom API response."""

    meta: ApiMeta
    data: list[CardRecord]


CK_API_URL = "https://api.cardkingdom.com/api/v2/pricelist"
CK_URL_PREFIX = "https://www.cardkingdom.com"


async def fetch_card_kingdom_data(timeout: float = 120.0) -> ApiResponse:
    """
    Fetch pricing data from Card Kingdom API.
    Args:
        timeout: Request timeout in seconds
    """
    LOGGER.info("Fetching data from Card Kingdom API...")
    headers = {"User-Agent": "mtgelmo-dev/mtgjson", "Accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as client:
        async with client.get(
            CK_API_URL, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            response.raise_for_status()
            data = await response.json()

    api_response = ApiResponse.model_validate(data)
    LOGGER.info(f"Successfully fetched {len(api_response.data):,} CK records")
    return api_response


def _parse_price(price_str: Optional[str]) -> Optional[float]:
    """Parse price string to float, returning None for empty/invalid."""
    if not price_str:
        return None
    try:
        return float(price_str)
    except ValueError:
        return None


def _to_lazyframe(response: ApiResponse) -> pl.LazyFrame:
    """
    Convert API response to a flat Polars DataFrame.

    This is the raw format with one row per SKU (foil and non-foil separate).
    """
    records = []

    for card in response.data:
        records.append(
            {
                "id": card.id,
                "sku": card.sku,
                "name": card.name,
                "edition": card.edition,
                "variation": card.variation,
                "is_foil": card.is_foil,
                "scryfall_id": card.scryfall_id,
                "url": card.url,
                "price_retail": _parse_price(card.price_retail),
                "qty_retail": card.qty_retail,
                "price_buy": _parse_price(card.price_buy),
                "qty_buying": card.qty_buying,
                "condition_nm_price": _parse_price(card.condition_values.nm_price),
                "condition_nm_qty": card.condition_values.nm_qty,
                "condition_ex_price": _parse_price(card.condition_values.ex_price),
                "condition_ex_qty": card.condition_values.ex_qty,
                "condition_vg_price": _parse_price(card.condition_values.vg_price),
                "condition_vg_qty": card.condition_values.vg_qty,
                "condition_g_price": _parse_price(card.condition_values.g_price),
                "condition_g_qty": card.condition_values.g_qty,
            }
        )

    return pl.DataFrame(records).lazy()


def to_pivoted_dataframe(response: ApiResponse) -> pl.LazyFrame:
    """
    Convert API response to a pivoted DataFrame ready for MTGJSON joins.

    Output has one row per scryfall_id with columns:
    - scryfall_id (renamed to 'id' for joining to Scryfall data)
    - card_kingdom_id (non-foil CK product ID as string)
    - card_kingdom_foil_id (foil CK product ID as string)
    - card_kingdom_url (non-foil URL path)
    - card_kingdom_foil_url (foil URL path)

    Cards without scryfall_id are excluded.
    """
    lf = _to_lazyframe(response)

    # Filter to cards with scryfall_id and pivot
    return (
        lf.filter(pl.col("scryfall_id").is_not_null())
        .with_columns(
            # Determine foil from SKU prefix (F prefix = foil)
            pl.col("sku")
            .str.starts_with("F")
            .alias("_is_foil")
        )
        .group_by("scryfall_id")
        .agg(
            [
                # Non-foil: id and url
                pl.col("id")
                .filter(~pl.col("_is_foil"))
                .first()  # Get first non-foil ID
                .cast(pl.String)  # cast and alias
                .alias("card_kingdom_id"),
                pl.col("url")  # aggregate URL
                .filter(~pl.col("_is_foil"))
                .first()
                .alias("card_kingdom_url"),
                # Foil: id and url (same as above)
                pl.col("id")
                .filter(pl.col("_is_foil"))
                .first()
                .cast(pl.String)
                .alias("card_kingdom_foil_id"),
                pl.col("url")
                .filter(pl.col("_is_foil"))
                .first()
                .alias("card_kingdom_foil_url"),
            ]
        )
        # Rename to match Scryfall bulk data join key
        .rename({"scryfall_id": "id"})
    )


def url_keygen(seed: str) -> str:
    """Generate MTGJSON redirect URL from seed string."""
    hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
    return f"https://mtgjson.com/links/{hash_val}"


def generate_purchase_url(url_path: str | None, uuid: str) -> str | None:
    """
    Generate MTGJSON-style purchase URL for Card Kingdom.

    Args:
        url_path: CK URL path (e.g., "/mtg/card/name")
        uuid: MTGJSON card UUID

    Returns:
        Hashed redirect URL or None if url_path is None
    """
    if not url_path:
        return None
    return url_keygen(f"{CK_URL_PREFIX}{url_path}{uuid}")


def write_parquet(
    df: pl.DataFrame,
    path: str | Path,
    compression: str = "zstd",
    compression_level: int = 9,
) -> Path:
    """
    Write DataFrame to Parquet with optimized settings.
    Polars provides excellent Parquet support out of the box.
    this is mostly here for dev/local purposes, but is worth having.
    Args:
        df: DataFrame to write
        path: Output file path
        compression: Compression algorithm (default: zstd)
        compression_level: Compression level (default: 9)
    Returns:
        Path to written file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(
        path,
        compression=compression,
        compression_level=compression_level,
        statistics=True,
        row_group_size=100_000,
    )

    size_mb = path.stat().st_size / 1024 / 1024
    LOGGER.info(f"Wrote {len(df):,} records to {path} ({size_mb:.2f} MB)")

    return path


def load_parquet(path: str | Path) -> pl.DataFrame:
    """Load Card Kingdom data from Parquet file."""
    return pl.read_parquet(path)


class CardKingdomProviderV2:
    """
    Card Kingdom V2 data provider for MTGJSON pipeline.

    Fetches pricing data and provides it in formats ready for joining
    to MTGJSON card data.
    (V2 provides a more detailed API than V1, but also lacks etched-foil data
    so we call both :shrug:)
    Usage:
        # Fetch and get pivoted DataFrame directly
        provider = CardKingdomProvider()
        provider.fetch()
        df = provider.get_join_data()

        # Or load from cached parquet
        provider = CardKingdomProvider()
        provider.load("./cache/ck_prices.parquet")
        df = provider.get_join_data()
    """

    def __init__(self):
        self._raw_df: pl.DataFrame | None = None
        self._pivoted_df: pl.DataFrame | None = None
        self._api_meta: ApiMeta | None = None

    async def download(self, timeout: float = 120.0) -> "CardKingdomProviderV2":
        """Fetch fresh data from Card Kingdom API."""
        response = await fetch_card_kingdom_data(timeout=timeout)
        self._api_meta = response.meta
        self._raw_df = _to_lazyframe(response).collect()
        self._pivoted_df = None
        return self

    def load(self, path: str | Path) -> "CardKingdomProviderV2":
        """Load data from Parquet file."""
        self._raw_df = load_parquet(path)
        self._pivoted_df = None
        return self

    def save(
        self,
        path: str | Path,
        compression: str = "zstd",
        compression_level: int = 9,
    ) -> Path:
        """Save raw data to Parquet file."""
        if self._raw_df is None:
            raise ValueError("No data loaded. Call fetch() or load() first.")
        return write_parquet(self._raw_df, path, compression, compression_level)

    @property
    def raw_df(self) -> pl.DataFrame:
        """Raw DataFrame with one row per SKU."""
        if self._raw_df is None:
            raise ValueError("No data loaded. Call fetch() or load() first.")
        return self._raw_df

    def get_join_data(self) -> pl.DataFrame:
        """
        Get pivoted DataFrame ready for joining to MTGJSON cards.

        Join on 'id' column (scryfall_id).

        Columns:
        - id: Scryfall ID (join key)
        - card_kingdom_id: Non-foil CK product ID
        - card_kingdom_foil_id: Foil CK product ID
        - card_kingdom_url: Non-foil URL path
        - card_kingdom_foil_url: Foil URL path
        """
        if self._pivoted_df is None:
            if self._raw_df is None:
                raise ValueError("No data loaded. Call fetch() or download() first.")
            self._pivoted_df = self._pivot_raw_df()
        return self._pivoted_df

    def _pivot_raw_df(self) -> pl.DataFrame:
        """Pivot raw DataFrame to one row per scryfall_id."""
        if self._raw_df is None:
            raise ValueError("No data loaded.")

        return (
            self._raw_df.filter(pl.col("scryfall_id").is_not_null())
            .with_columns(pl.col("sku").str.starts_with("F").alias("_is_foil"))
            .group_by("scryfall_id")
            .agg(
                [
                    pl.col("id")
                    .filter(~pl.col("_is_foil"))
                    .first()
                    .cast(pl.String)
                    .alias("card_kingdom_id"),
                    pl.col("url")
                    .filter(~pl.col("_is_foil"))
                    .first()
                    .alias("card_kingdom_url"),
                    pl.col("id")
                    .filter(pl.col("_is_foil"))
                    .first()
                    .cast(pl.String)
                    .alias("card_kingdom_foil_id"),
                    pl.col("url")
                    .filter(pl.col("_is_foil"))
                    .first()
                    .alias("card_kingdom_foil_url"),
                ]
            )
            .rename({"scryfall_id": "id"})
        )

    def __len__(self) -> int:
        """Return number of records in raw data."""
        if self._raw_df is None:
            return 0
        return len(self._raw_df)

    def __repr__(self) -> str:
        status = "loaded" if self._raw_df is not None else "empty"
        count = len(self) if self._raw_df is not None else 0
        return f"CardKingdomProvider({status}, {count:,} records)"
