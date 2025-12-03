import json
import pathlib
import logging
import time
import polars as pl
from typing import Optional
from mtgjson5 import constants

from mtgjson5.providers import (
    BulkDataProvider, ScryfallProvider, TCGPlayerProvider,
    CardKingdomProvider, CardKingdomProviderV2,
)

LOGGER = logging.getLogger(__name__)


def load_resource_json(filename: str) -> dict | list:
    """Load a JSON resource file and return raw data."""
    file_path = constants.RESOURCE_PATH / filename
    if not file_path.exists():
        LOGGER.warning(f"Resource file not found: {file_path}")
        return {}
    with file_path.open("rb") as f:
        return json.loads(f.read())


def cache_fresh(path: pathlib.Path, max_age_hours: int = 24) -> bool:
    """Check if a cache file exists and is fresh enough."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours <= max_age_hours
    
    
class GlobalCache:
    """Global shared access cache for provider data."""

    _instance: Optional["GlobalCache"] = None

    def __new__(cls) -> "GlobalCache":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        
        self.CACHE_DIR: pathlib.Path = constants.CACHE_PATH
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Bulk data
        self.cards_df: pl.LazyFrame | None = None
        self.rulings_df: pl.LazyFrame | None = None
        
        # Provider instances
        self._bulk_provider: BulkDataProvider | None = None
        self._tcgplayer: TCGPlayerProvider | None = None
        self._scryfall: ScryfallProvider | None = None
        self._cardkingdom: CardKingdomProvider | None = None
        self._cardkingdom2: CardKingdomProviderV2 | None = None
        
        # Provider dataframes
        self.ck_df: pl.DataFrame | None = None
        self.ck_v2_df: pl.DataFrame | None = None
        self.tcg_df: pl.DataFrame | None = None
        
        
        self._initialized = True
        
    # provider property accessors
    @property
    def bulk_provider(self) -> BulkDataProvider:
        """Get or create the BulkDataProvider instance."""
        if self._bulk_provider is None:
            self._bulk_provider = BulkDataProvider()
        return self._bulk_provider
        
    @property
    def tcgplayer(self) -> TCGPlayerProvider:
        """Get or create the TCGPlayerProvider instance."""
        if self._tcgplayer is None:
            self._tcgplayer = TCGPlayerProvider()
        return self._tcgplayer
    
    @property
    def scryfall(self) -> ScryfallProvider:
        """Get or create the ScryfallProvider instance."""
        if self._scryfall is None:
            self._scryfall = ScryfallProvider()
        return self._scryfall
    
    @property
    def cardkingdom(self) -> CardKingdomProvider:
        """Get or create OG CardKingdomProvider instance."""
        if self._cardkingdom is None:
            self._cardkingdom = CardKingdomProvider()
        return self._cardkingdom
    
    @property
    def cardkingdom2(self) -> CardKingdomProviderV2:
        """Get or create V2 CardKingdomProvider instance."""
        if self._cardkingdom2 is None:
            self._cardkingdom2 = CardKingdomProviderV2()
        return self._cardkingdom2
        
        
# ------------------ Provider Data Downloads ------------------ #
    
    async def _download_bulk_data(self, force_refresh: bool = False) -> None:
        """Download Scryfall bulk data if missing or stale."""
        cards_path = self.CACHE_DIR / "all_cards.ndjson"
       
        needs_download = force_refresh
        
        if not needs_download and cards_path.exists():
            age_hours = (time.time() - cards_path.stat().st_mtime) / 3600
            if age_hours > 24:
                LOGGER.info(f"Bulk data is {age_hours:.1f}h old, refreshing...")
                needs_download = True

        if needs_download:
            LOGGER.info("[1/5] Downloading bulk data...")
            await self._bulk_provider.download_bulk_files(self.CACHE_DIR, ["all_cards", "rulings"], force_refresh)
        else:
            LOGGER.info("[1/5] Using cached bulk data")
    
    
    async def _download_cardkingdomv2_data(self, cache: bool = True) -> None:
        """Download CardKingdom V2 data, with optional caching"""
        cache_path = self.CACHE_DIR / "ck_prices.parquet"
        
        if cache and cache_fresh(cache_path):
            # polars 'scan' options product lazyframes:
            #   they dont pull data into memory until collected
            #   and allow the polars engine to optimize its query plan
            LOGGER.info("[CK V2] Using cached data")
            self.ck_v2_df = pl.scan_parquet(cache_path).collect()
            return
            
        LOGGER.info("[CK V2] Downloading fresh data...")
        await self.cardkingdom2.download()
        self.cardkingdom2.save(cache_path)
        self.ck_v2_df = self.cardkingdom2.get_join_data()
    
    @classmethod
    def get_instance(cls) -> "GlobalCache":
        if cls._instance is None:
            cls()
        return cls._instance


GLOBAL_CACHE = GlobalCache.get_instance()