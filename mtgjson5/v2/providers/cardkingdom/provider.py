"""Card Kingdom provider - unified facade."""

import logging
from pathlib import Path
from typing import Any

import polars as pl

from .cache import CardKingdomStorage
from .client import CardKingdomClient, FetchResult
from .models import CKRecord
from .prices import CardKingdomPriceProcessor, generate_purchase_url
from .transformer import CardKingdomTransformer

LOGGER = logging.getLogger(__name__)


class CKProvider:
    """
    Card Kingdom data provider for MTGJSON pipeline.

    Facade that composes:
    - CardKingdomClient: API fetching
    - CardKingdomTransformer: Data normalization
    - CardKingdomPriceProcessor: Price formatting
    - CardKingdomStorage: Parquet persistence

    Usage:
        # Fetch and use
        provider = CKProvider()
        await provider.fetch()
        join_df = provider.get_join_data()

        # Or load from cache
        provider = CKProvider()
        provider.load(cache_path)

        # Price generation
        prices = provider.get_price_processor().generate_today_prices(id_map)
    """

    def __init__(
        self,
        client: CardKingdomClient | None = None,
        cache_path: Path | str | None = None,
    ):
        self._client = client or CardKingdomClient()
        self._cache_path = Path(cache_path) if cache_path else None

        self._raw_df: pl.DataFrame | None = None
        self._pivoted_df: pl.DataFrame | None = None
        self._fetch_results: list[FetchResult] | None = None

    async def fetch(
        self,
        include_v1: bool = True,
        include_v2: bool = True,
        include_sealed: bool = True,
    ) -> "CKProvider":
        """Fetch from CK API endpoints."""
        self._fetch_results = await self._client.fetch_all(
            include_v1=include_v1,
            include_v2=include_v2,
            include_sealed=include_sealed,
        )

        # Combine all successful records
        all_records: list[CKRecord] = []
        for result in self._fetch_results:
            if result.success:
                all_records.extend(result.records)

        if not all_records:
            LOGGER.warning("No records fetched from any CK endpoint")
            self._raw_df = pl.DataFrame()
        else:
            self._raw_df = CardKingdomTransformer.records_to_dataframe(all_records)

        self._pivoted_df = None  # Invalidate cache
        return self

    def fetch_sync(self, **kwargs: bool) -> "CKProvider":
        """Sync wrapper for fetch."""
        import asyncio

        return asyncio.run(self.fetch(**kwargs))

    def load(self, path: Path | str) -> "CKProvider":
        """Load from Parquet cache."""
        self._raw_df = CardKingdomStorage.read(path)
        self._pivoted_df = None
        return self

    def save(self, path: Path | str | None = None) -> Path:
        """Save to Parquet cache."""
        path = Path(path) if path else self._cache_path
        if path is None:
            raise ValueError("No path specified and no default cache_path set")
        if self._raw_df is None:
            raise ValueError("No data to save. Call fetch() first.")
        return CardKingdomStorage.write(self._raw_df, path)

    def load_or_fetch(self, cache_path: Path | str | None = None) -> "CKProvider":
        """Load from cache if exists, otherwise fetch and cache."""
        path = Path(cache_path) if cache_path else self._cache_path

        if path and CardKingdomStorage.exists(path):
            LOGGER.info(f"Loading CK data from cache: {path}")
            return self.load(path)

        LOGGER.info("Fetching fresh CK data...")
        self.fetch_sync()

        if path:
            self.save(path)

        return self

    @property
    def raw_df(self) -> pl.DataFrame:
        """Raw DataFrame with one row per SKU."""
        if self._raw_df is None:
            raise ValueError("No data loaded. Call fetch() or load() first.")
        return self._raw_df

    def get_join_data(self) -> pl.DataFrame:
        """Get pivoted DataFrame for joining to MTGJSON cards by scryfall_id."""
        if self._pivoted_df is None:
            self._pivoted_df = CardKingdomTransformer.pivot_by_scryfall_id(self.raw_df)
        return self._pivoted_df

    def get_pricing_df(self) -> pl.DataFrame:
        """Get pricing DataFrame for price processing."""
        return CardKingdomTransformer.to_pricing_df(self.raw_df)

    def get_price_processor(self) -> CardKingdomPriceProcessor:
        """Get price processor for MTGJSON format generation."""
        return CardKingdomPriceProcessor(self.get_pricing_df())

    def generate_purchase_url(self, url_path: str | None, uuid: str) -> str | None:
        """Generate MTGJSON purchase URL."""
        return generate_purchase_url(url_path, uuid)

    def get_prices_for_uuids(
        self,
        scryfall_to_uuid: pl.DataFrame,
    ) -> dict[str, Any]:
        """
        One-shot price generation from scryfall->uuid mapping.

        Convenience method that handles the full pipeline:
        1. Build CK ID -> UUID mapping
        2. Generate prices in MTGJSON format
        """
        processor = self.get_price_processor()
        id_map = processor.build_ck_id_to_uuid_map(scryfall_to_uuid)
        return processor.generate_today_prices(id_map)

    @property
    def fetch_results(self) -> list[FetchResult] | None:
        """Results from last fetch, including any errors."""
        return self._fetch_results

    def __len__(self) -> int:
        return len(self._raw_df) if self._raw_df is not None else 0

    def __repr__(self) -> str:
        status = "loaded" if self._raw_df is not None else "empty"
        return f"CKProvider({status}, {len(self):,} records)"
