"""
Polars-based Price Builder for MTGJSON v2.
"""

import asyncio
import datetime
import gc
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import requests

from mtgjson5 import constants
from mtgjson5.build.price_archive import (
    PRICE_SCHEMA,
    load_archive,
    load_partitioned_archive,
    migrate_legacy_archive,
    prune_partitions,
    save_prices_partitioned,
)
from mtgjson5.build.price_s3 import (
    get_price_archive_from_s3,
    sync_local_partitions_to_s3,
    sync_missing_partitions_from_s3,
    sync_partition_to_s3,
    upload_archive_to_s3,
)
from mtgjson5.build.price_writers import (
    stream_write_all_prices_json,
    stream_write_today_prices_json,
    write_prices_csv,
    write_prices_psql,
    write_prices_sql,
    write_prices_sqlite,
)
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.providers import (
    CardHoarderPriceProvider,
    CardMarketProvider,
    CKProvider,
    ManapoolPriceProvider,
    TCGPlayerPriceProvider,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from mtgjson5.data import GlobalCache

    ProgressCallback = Callable[[int, int, str], None]


LOGGER = logging.getLogger(__name__)


@dataclass
class PriceBuilderContext:
    """
    Context for price building with derived ID mappings.

    Owns the ID -> UUID mappings needed by price providers:
    - tcg_to_uuid: TCGPlayer product ID -> MTGJSON UUIDs
    - tcg_etched_to_uuid: TCGPlayer etched product ID -> MTGJSON UUIDs
    - mtgo_to_uuid: MTGO ID -> MTGJSON UUIDs
    - scryfall_to_uuid: Scryfall ID -> single MTGJSON UUID

    Mappings are built lazily on first access from GlobalCache LazyFrames.
    """

    _cache: "GlobalCache | None" = field(default=None, repr=False)

    # Derived mappings (built lazily)
    _tcg_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _tcg_etched_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _mtgo_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _scryfall_to_uuid: dict[str, str] | None = field(default=None, repr=False)

    @classmethod
    def from_cache(cls) -> "PriceBuilderContext":
        """Create context from global cache.

        Ensures ID mappings are loaded from parquet files if they exist
        (for standalone price builds that run after a card build).
        """
        from mtgjson5.data import GLOBAL_CACHE

        # Load ID mappings from cache if they haven't been set
        # (happens when running price build separately from card build)
        if GLOBAL_CACHE.tcg_to_uuid_lf is None and GLOBAL_CACHE.mtgo_to_uuid_lf is None:
            GLOBAL_CACHE.load_id_mappings()

        return cls(_cache=GLOBAL_CACHE)

    @property
    def tcg_to_uuid(self) -> dict[str, set[str]]:
        """TCGPlayer product ID -> MTGJSON UUID(s) mapping."""
        if self._tcg_to_uuid is None:
            self._tcg_to_uuid = self._build_tcg_to_uuid()
        return self._tcg_to_uuid

    @property
    def tcg_etched_to_uuid(self) -> dict[str, set[str]]:
        """TCGPlayer etched product ID -> MTGJSON UUID(s) mapping."""
        if self._tcg_etched_to_uuid is None:
            self._tcg_etched_to_uuid = self._build_tcg_etched_to_uuid()
        return self._tcg_etched_to_uuid

    @property
    def mtgo_to_uuid(self) -> dict[str, set[str]]:
        """MTGO ID -> MTGJSON UUID(s) mapping."""
        if self._mtgo_to_uuid is None:
            self._mtgo_to_uuid = self._build_mtgo_to_uuid()
        return self._mtgo_to_uuid

    @property
    def scryfall_to_uuid(self) -> dict[str, str]:
        """Scryfall ID -> single MTGJSON UUID mapping."""
        if self._scryfall_to_uuid is None:
            self._scryfall_to_uuid = self._build_scryfall_to_uuid()
        return self._scryfall_to_uuid

    def _build_tcg_to_uuid(self) -> dict[str, set[str]]:
        """Build TCGPlayer product ID -> UUID mapping from cache."""
        if self._cache is None or self._cache.tcg_to_uuid_lf is None:
            return {}
        df = self._cache.tcg_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            tcg_id = str(row.get("tcgplayerProductId", ""))
            uuid = row.get("uuid")
            if tcg_id and uuid:
                if tcg_id not in result:
                    result[tcg_id] = set()
                result[tcg_id].add(uuid)
        return result

    def _build_tcg_etched_to_uuid(self) -> dict[str, set[str]]:
        """Build TCGPlayer etched product ID -> UUID mapping from cache."""
        if self._cache is None or self._cache.tcg_etched_to_uuid_lf is None:
            return {}
        df = self._cache.tcg_etched_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            tcg_id = str(row.get("tcgplayerEtchedProductId", ""))
            uuid = row.get("uuid")
            if tcg_id and uuid:
                if tcg_id not in result:
                    result[tcg_id] = set()
                result[tcg_id].add(uuid)
        return result

    def _build_mtgo_to_uuid(self) -> dict[str, set[str]]:
        """Build MTGO ID -> UUID mapping from cache."""
        if self._cache is None or self._cache.mtgo_to_uuid_lf is None:
            return {}
        df = self._cache.mtgo_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            mtgo_id = str(row.get("mtgoId", ""))
            uuid = row.get("uuid")
            if mtgo_id and uuid:
                if mtgo_id not in result:
                    result[mtgo_id] = set()
                result[mtgo_id].add(uuid)
        return result

    def release(self) -> None:
        """Free ID-mapping dicts (no longer needed after fetch)."""
        self._tcg_to_uuid = None
        self._tcg_etched_to_uuid = None
        self._mtgo_to_uuid = None
        self._scryfall_to_uuid = None
        self._cache = None

    def _build_scryfall_to_uuid(self) -> dict[str, str]:
        """Build Scryfall ID -> single UUID mapping from pipeline-derived cache.

        Picks one UUID per scryfallId to avoid duplicating prices across
        card faces (e.g. both sides of a double-faced card).
        """
        if self._cache is None:
            return {}

        if self._cache.scryfall_to_uuid_lf is not None:
            df = self._cache.scryfall_to_uuid_lf.unique(subset=["scryfallId"], keep="first").collect()
            if not df.is_empty():
                return dict(
                    zip(
                        df.get_column("scryfallId").to_list(),
                        df.get_column("uuid").to_list(),
                        strict=False,
                    )
                )

        if self._cache.uuid_cache_lf is not None:
            df = (
                self._cache.uuid_cache_lf.filter(pl.col("side") == "a")
                .unique(subset=["scryfallId"], keep="first")
                .collect()
            )
            if not df.is_empty():
                return dict(
                    zip(
                        df.get_column("scryfallId").to_list(),
                        df.get_column("cachedUuid").to_list(),
                        strict=False,
                    )
                )

        return {}


RAW_CACHE_FILES = {
    "tcgplayer": "tcg_raw_prices.parquet",
    "cardhoarder": "ch_raw_prices.parquet",
    "manapool": "manapool_raw_prices.parquet",
    "cardmarket": "mcm_raw_prices.parquet",
    "cardkingdom": "ck_raw.parquet",
}

ID_MAPPING_FILES = {
    "tcg_to_uuid": "tcg_to_uuid.parquet",
    "tcg_etched_to_uuid": "tcg_etched_to_uuid.parquet",
    "mtgo_to_uuid": "mtgo_to_uuid.parquet",
    "scryfall_to_uuid": "scryfall_to_uuid.parquet",
    "cardmarket_to_uuid": "cardmarket_to_uuid.parquet",
}


class PolarsPriceBuilder:
    """
    Build daily prices using Polars DataFrames with v2 async providers.

    Price data is stored in a flat tabular format optimized for:
    - Fast date-based filtering (pruning old entries)
    - Efficient merging of new price data
    - Compact parquet storage

    Providers:
    - TCGPlayer: retail only (no buylist - deprecated API)
    - CardHoarder: MTGO prices
    - Manapool: paper prices
    - CardMarket: paper prices (EUR)
    - CardKingdom: paper prices
    """

    all_printings_path: Path
    today_date: str
    on_progress: "ProgressCallback | None"

    def __init__(
        self,
        all_printings_path: Path | None = None,
        on_progress: "ProgressCallback | None" = None,
    ) -> None:
        self.on_progress = on_progress
        self.all_printings_path = (
            all_printings_path if all_printings_path else MtgjsonConfig().output_path.joinpath("AllPrintings.json")
        )
        self.today_date = datetime.date.today().strftime("%Y-%m-%d")

        # V2 providers (lazy init)
        self._tcg_provider: TCGPlayerPriceProvider | None = None
        self._ch_provider: CardHoarderPriceProvider | None = None
        self._manapool_provider: ManapoolPriceProvider | None = None
        self._mcm_provider: CardMarketProvider | None = None
        self._ck_provider: CKProvider | None = None

    async def build_today_prices_async(self, ctx: PriceBuilderContext | None = None) -> pl.DataFrame:
        """
        Fetch today's prices from v2 async providers.

        Args:
            ctx: PriceBuilderContext with ID mappings (created from cache if None)

        Returns:
            DataFrame with flat price records
        """
        # Get ID mappings from context
        if ctx is None:
            ctx = PriceBuilderContext.from_cache()

        tcg_to_uuid = ctx.tcg_to_uuid
        tcg_etched_to_uuid = ctx.tcg_etched_to_uuid
        mtgo_to_uuid = ctx.mtgo_to_uuid
        scryfall_to_uuid = ctx.scryfall_to_uuid

        # Initialize v2 providers
        self._tcg_provider = TCGPlayerPriceProvider(on_progress=self.on_progress)
        self._ch_provider = CardHoarderPriceProvider(on_progress=self.on_progress)
        self._manapool_provider = ManapoolPriceProvider(on_progress=self.on_progress)
        self._mcm_provider = CardMarketProvider()
        self._ck_provider = CKProvider()

        frames: list[pl.DataFrame] = []

        # Fetch TCGPlayer prices (largest, async with streaming)
        LOGGER.info("Fetching TCGPlayer prices")
        if tcg_to_uuid or tcg_etched_to_uuid:
            tcg_df = await self._tcg_provider.fetch_all_prices(tcg_to_uuid or {}, tcg_etched_to_uuid or {})
            if len(tcg_df) > 0:
                frames.append(tcg_df)
                LOGGER.info(f"  TCGPlayerPriceProvider: {len(tcg_df):,} price points")
            del tcg_df

        # Fetch CardHoarder (MTGO) - simple bulk
        LOGGER.info("Fetching CardHoarder prices")
        if mtgo_to_uuid:
            ch_df = await self._ch_provider.fetch_prices(mtgo_to_uuid)
            if len(ch_df) > 0:
                frames.append(ch_df)
                LOGGER.info(f"  CardHoarderPriceProvider: {len(ch_df):,} price points")
            del ch_df

        # Fetch Manapool - simple bulk
        LOGGER.info("Fetching Manapool prices")
        if scryfall_to_uuid:
            manapool_df = await self._manapool_provider.fetch_prices(scryfall_to_uuid)
            if len(manapool_df) > 0:
                frames.append(manapool_df)
                LOGGER.info(f"  ManapoolPriceProvider: {len(manapool_df):,} price points")
            del manapool_df

        # Fetch CardMarket - bulk API
        LOGGER.info("Fetching CardMarket prices")
        mcm_dict = await self._mcm_provider.generate_today_price_dict(self.all_printings_path)
        if mcm_dict:
            mcm_df = self._prices_dict_to_dataframe(mcm_dict)
            del mcm_dict
            if len(mcm_df) > 0:
                frames.append(mcm_df)
                LOGGER.info(f"  CardMarketProvider: {len(mcm_df):,} price points")
            del mcm_df
        await self._mcm_provider.close()

        # Fetch CardKingdom - async fetch, convert to DataFrame
        LOGGER.info("Fetching CardKingdom prices")
        try:
            await self._ck_provider.load_or_fetch_async(constants.CACHE_PATH / "ck_raw.parquet")
            ck_pricing_df = self._ck_provider.get_pricing_df()
            if len(ck_pricing_df) > 0:
                ck_df = self._convert_ck_pricing(ck_pricing_df, scryfall_to_uuid or {})
                del ck_pricing_df
                if len(ck_df) > 0:
                    frames.append(ck_df)
                    LOGGER.info(f"  CKProvider: {len(ck_df):,} price points")
                del ck_df
        except Exception as e:
            LOGGER.warning(f"Failed to fetch CardKingdom prices: {e}")

        if not frames:
            LOGGER.warning("No price data collected from providers")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        result = pl.concat(frames)
        del frames
        self._tcg_provider = None
        self._ch_provider = None
        self._manapool_provider = None
        self._mcm_provider = None
        self._ck_provider = None
        gc.collect()
        return result

    def build_today_prices(self, ctx: PriceBuilderContext | None = None) -> pl.DataFrame:
        """Sync wrapper for build_today_prices_async."""
        return asyncio.run(self.build_today_prices_async(ctx))

    def map_raw_to_today_df(self, cache_dir: Path | None = None) -> pl.DataFrame:
        """Build today_df from pre-fetched raw parquets + ID mapping parquets.

        This is the Phase 2 mapping step: reads cached raw price data and
        applies Polars joins with UUID mappings. No network I/O.
        """
        if cache_dir is None:
            cache_dir = constants.CACHE_PATH

        frames: list[pl.DataFrame] = []

        # TCGPlayer
        tcg_raw = cache_dir / RAW_CACHE_FILES["tcgplayer"]
        tcg_uuid = cache_dir / ID_MAPPING_FILES["tcg_to_uuid"]
        tcg_etched = cache_dir / ID_MAPPING_FILES["tcg_etched_to_uuid"]
        if tcg_raw.exists() and (tcg_uuid.exists() or tcg_etched.exists()):
            df = self._map_tcg_raw(tcg_raw, tcg_uuid, tcg_etched)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  TCGPlayer mapped: {len(df):,} price points")

        # CardHoarder
        ch_raw = cache_dir / RAW_CACHE_FILES["cardhoarder"]
        mtgo_uuid = cache_dir / ID_MAPPING_FILES["mtgo_to_uuid"]
        if ch_raw.exists() and mtgo_uuid.exists():
            df = self._map_cardhoarder_raw(ch_raw, mtgo_uuid)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  CardHoarder mapped: {len(df):,} price points")

        # Manapool
        mp_raw = cache_dir / RAW_CACHE_FILES["manapool"]
        sf_uuid = cache_dir / ID_MAPPING_FILES["scryfall_to_uuid"]
        if mp_raw.exists() and sf_uuid.exists():
            df = self._map_manapool_raw(mp_raw, sf_uuid)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  Manapool mapped: {len(df):,} price points")

        # CardMarket
        mcm_raw = cache_dir / RAW_CACHE_FILES["cardmarket"]
        mcm_uuid = cache_dir / ID_MAPPING_FILES["cardmarket_to_uuid"]
        if mcm_raw.exists() and mcm_uuid.exists():
            df = self._map_cardmarket_raw(mcm_raw, mcm_uuid, cache_dir)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  CardMarket mapped: {len(df):,} price points")

        # CardKingdom
        ck_raw = cache_dir / RAW_CACHE_FILES["cardkingdom"]
        if ck_raw.exists() and sf_uuid.exists():
            df = self._map_ck_raw(ck_raw, sf_uuid)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  CardKingdom mapped: {len(df):,} price points")

        if not frames:
            LOGGER.warning("No raw price data to map")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        result = pl.concat(frames)
        del frames
        gc.collect()
        LOGGER.info(f"Mapped {len(result):,} total price points from raw cache")
        return result

    def _map_tcg_raw(self, raw_path: Path, uuid_path: Path, etched_uuid_path: Path) -> pl.DataFrame:
        """Join TCGPlayer raw data with UUID mappings."""
        raw = pl.scan_parquet(raw_path)
        frames: list[pl.LazyFrame] = []

        # Normal/foil via tcg_to_uuid
        if uuid_path.exists():
            tcg_uuid = pl.scan_parquet(uuid_path)
            normal_foil = (
                raw.join(tcg_uuid, left_on="productId", right_on="tcgplayerProductId", how="inner")
                .with_columns(
                    pl.when(pl.col("subTypeName") == "Normal")
                    .then(pl.lit("normal"))
                    .otherwise(pl.lit("foil"))
                    .alias("finish")
                )
                .select(
                    pl.col("uuid"),
                    pl.lit(self.today_date).alias("date"),
                    pl.lit("paper").alias("source"),
                    pl.lit("tcgplayer").alias("provider"),
                    pl.lit("retail").alias("price_type"),
                    pl.col("finish"),
                    pl.col("marketPrice").alias("price"),
                    pl.lit("USD").alias("currency"),
                )
            )
            frames.append(normal_foil)

        # Etched via tcg_etched_to_uuid
        if etched_uuid_path.exists():
            tcg_etched = pl.scan_parquet(etched_uuid_path)
            etched = (
                raw.join(tcg_etched, left_on="productId", right_on="tcgplayerEtchedProductId", how="inner")
                .with_columns(
                    pl.when(pl.col("subTypeName") == "Normal")
                    .then(pl.lit("normal"))
                    .otherwise(pl.lit("etched"))
                    .alias("finish")
                )
                .select(
                    pl.col("uuid"),
                    pl.lit(self.today_date).alias("date"),
                    pl.lit("paper").alias("source"),
                    pl.lit("tcgplayer").alias("provider"),
                    pl.lit("retail").alias("price_type"),
                    pl.col("finish"),
                    pl.col("marketPrice").alias("price"),
                    pl.lit("USD").alias("currency"),
                )
            )
            frames.append(etched)

        if not frames:
            return pl.DataFrame(schema=PRICE_SCHEMA)

        return pl.concat(frames).collect()

    def _map_cardhoarder_raw(self, raw_path: Path, uuid_path: Path) -> pl.DataFrame:
        """Join CardHoarder raw data with UUID mappings."""
        raw = pl.scan_parquet(raw_path)
        mtgo_uuid = pl.scan_parquet(uuid_path)

        return (
            raw.join(mtgo_uuid, left_on="mtgoId", right_on="mtgoId", how="inner")
            .with_columns(pl.when(pl.col("is_foil")).then(pl.lit("foil")).otherwise(pl.lit("normal")).alias("finish"))
            .select(
                pl.col("uuid"),
                pl.lit(self.today_date).alias("date"),
                pl.lit("mtgo").alias("source"),
                pl.lit("cardhoarder").alias("provider"),
                pl.lit("retail").alias("price_type"),
                pl.col("finish"),
                pl.col("price"),
                pl.lit("USD").alias("currency"),
            )
            .collect()
        )

    def _map_manapool_raw(self, raw_path: Path, uuid_path: Path) -> pl.DataFrame:
        """Join Manapool raw data with UUID mappings, unpivot finishes."""
        raw = pl.scan_parquet(raw_path)
        sf_uuid = pl.scan_parquet(uuid_path).unique(subset=["scryfallId"], keep="first")

        joined = raw.join(sf_uuid, on="scryfallId", how="inner").collect()

        frames: list[pl.DataFrame] = []

        for col, finish in [
            ("price_cents", "normal"),
            ("price_cents_foil", "foil"),
            ("price_cents_etched", "etched"),
        ]:
            sub = joined.filter(pl.col(col) > 0).select(
                pl.col("uuid"),
                pl.lit(self.today_date).alias("date"),
                pl.lit("paper").alias("source"),
                pl.lit("manapool").alias("provider"),
                pl.lit("retail").alias("price_type"),
                pl.lit(finish).alias("finish"),
                (pl.col(col).cast(pl.Float64) / 100.0).alias("price"),
                pl.lit("USD").alias("currency"),
            )
            if len(sub) > 0:
                frames.append(sub)

        if not frames:
            return pl.DataFrame(schema=PRICE_SCHEMA)
        return pl.concat(frames)

    def _map_cardmarket_raw(self, raw_path: Path, uuid_path: Path, cache_dir: Path) -> pl.DataFrame:
        """Join CardMarket raw data with UUID + finishes mappings."""
        raw = pl.read_parquet(raw_path)
        mcm_uuid = pl.read_parquet(uuid_path)

        # Join raw prices with UUID mapping
        joined = raw.join(mcm_uuid, left_on="productId", right_on="mcmId", how="inner")

        frames: list[pl.DataFrame] = []

        # Normal (trend) prices
        normal = joined.filter(pl.col("trend").is_not_null()).select(
            pl.col("uuid"),
            pl.lit(self.today_date).alias("date"),
            pl.lit("paper").alias("source"),
            pl.lit("cardmarket").alias("provider"),
            pl.lit("retail").alias("price_type"),
            pl.lit("normal").alias("finish"),
            pl.col("trend").alias("price"),
            pl.lit("EUR").alias("currency"),
        )
        if len(normal) > 0:
            frames.append(normal)

        # Foil/etched (trend_foil) prices — use finishes to distinguish
        foil_data = joined.filter(pl.col("trend_foil").is_not_null())
        if len(foil_data) > 0:
            finishes_path = cache_dir / "mcm_finishes.parquet"
            if finishes_path.exists():
                finishes_df = pl.read_parquet(finishes_path)
                foil_joined = foil_data.join(finishes_df, left_on="productId", right_on="mcmId", how="left")

                # Etched if finishes list contains "etched"
                if "finishes" in foil_joined.columns:
                    etched = foil_joined.filter(pl.col("finishes").list.contains("etched")).select(
                        pl.col("uuid"),
                        pl.lit(self.today_date).alias("date"),
                        pl.lit("paper").alias("source"),
                        pl.lit("cardmarket").alias("provider"),
                        pl.lit("retail").alias("price_type"),
                        pl.lit("etched").alias("finish"),
                        pl.col("trend_foil").alias("price"),
                        pl.lit("EUR").alias("currency"),
                    )
                    non_etched = foil_joined.filter(~pl.col("finishes").list.contains("etched")).select(
                        pl.col("uuid"),
                        pl.lit(self.today_date).alias("date"),
                        pl.lit("paper").alias("source"),
                        pl.lit("cardmarket").alias("provider"),
                        pl.lit("retail").alias("price_type"),
                        pl.lit("foil").alias("finish"),
                        pl.col("trend_foil").alias("price"),
                        pl.lit("EUR").alias("currency"),
                    )
                    if len(etched) > 0:
                        frames.append(etched)
                    if len(non_etched) > 0:
                        frames.append(non_etched)
                else:
                    # No finishes column — default to foil
                    foil = foil_data.select(
                        pl.col("uuid"),
                        pl.lit(self.today_date).alias("date"),
                        pl.lit("paper").alias("source"),
                        pl.lit("cardmarket").alias("provider"),
                        pl.lit("retail").alias("price_type"),
                        pl.lit("foil").alias("finish"),
                        pl.col("trend_foil").alias("price"),
                        pl.lit("EUR").alias("currency"),
                    )
                    if len(foil) > 0:
                        frames.append(foil)
            else:
                # No finishes data — default all to foil
                foil = foil_data.select(
                    pl.col("uuid"),
                    pl.lit(self.today_date).alias("date"),
                    pl.lit("paper").alias("source"),
                    pl.lit("cardmarket").alias("provider"),
                    pl.lit("retail").alias("price_type"),
                    pl.lit("foil").alias("finish"),
                    pl.col("trend_foil").alias("price"),
                    pl.lit("EUR").alias("currency"),
                )
                if len(foil) > 0:
                    frames.append(foil)

        if not frames:
            return pl.DataFrame(schema=PRICE_SCHEMA)
        return pl.concat(frames)

    def _map_ck_raw(self, raw_path: Path, uuid_path: Path) -> pl.DataFrame:
        """Join CardKingdom raw data with UUID mappings."""
        from mtgjson5.providers.cardkingdom.provider import CardKingdomTransformer

        raw_df = pl.read_parquet(raw_path)
        ck_pricing = CardKingdomTransformer.to_pricing_df(raw_df)

        sf_uuid = pl.scan_parquet(uuid_path).unique(subset=["scryfallId"], keep="first").collect()
        scryfall_to_uuid = dict(zip(sf_uuid["scryfallId"].to_list(), sf_uuid["uuid"].to_list(), strict=False))

        return self._convert_ck_pricing(ck_pricing, scryfall_to_uuid)

    def _convert_ck_pricing(
        self,
        ck_df: pl.DataFrame,
        scryfall_to_uuid: dict[str, str],
    ) -> pl.DataFrame:
        """
        Convert CardKingdom pricing DataFrame to flat price records.

        Uses vectorized Polars join/filter/select instead of row iteration.

        Args:
            ck_df: CK pricing DataFrame with columns:
                   ck_id, scryfall_id, is_foil, is_etched,
                   price_retail, price_buy, qty_retail, qty_buying
            scryfall_to_uuid: Scryfall ID -> single MTGJSON UUID mapping

        Returns:
            DataFrame matching PRICE_SCHEMA
        """
        if not scryfall_to_uuid:
            return pl.DataFrame(schema=PRICE_SCHEMA)

        uuid_df = pl.DataFrame(
            {
                "scryfall_id": list(scryfall_to_uuid.keys()),
                "uuid": list(scryfall_to_uuid.values()),
            }
        )

        base = ck_df.join(uuid_df, on="scryfall_id", how="inner").with_columns(
            pl.when(pl.col("is_etched"))
            .then(pl.lit("etched"))
            .when(pl.col("is_foil"))
            .then(pl.lit("foil"))
            .otherwise(pl.lit("normal"))
            .alias("finish")
        )

        retail = base.filter(pl.col("price_retail").is_not_null() & (pl.col("qty_retail") > 0)).select(
            pl.col("uuid"),
            pl.lit(self.today_date).alias("date"),
            pl.lit("paper").alias("source"),
            pl.lit("cardkingdom").alias("provider"),
            pl.lit("retail").alias("price_type"),
            pl.col("finish"),
            pl.col("price_retail").cast(pl.Float64).alias("price"),
            pl.lit("USD").alias("currency"),
        )

        buylist = base.filter(pl.col("price_buy").is_not_null() & (pl.col("qty_buying") > 0)).select(
            pl.col("uuid"),
            pl.lit(self.today_date).alias("date"),
            pl.lit("paper").alias("source"),
            pl.lit("cardkingdom").alias("provider"),
            pl.lit("buylist").alias("price_type"),
            pl.col("finish"),
            pl.col("price_buy").cast(pl.Float64).alias("price"),
            pl.lit("USD").alias("currency"),
        )

        return pl.concat([retail, buylist])

    def _prices_dict_to_dataframe(self, prices: dict[str, Any]) -> pl.DataFrame:
        """
        Convert provider's MtgjsonPriceEntry dict to flat DataFrame.

        Args:
            prices: Dict mapping uuid -> MtgjsonPriceEntry

        Returns:
            Flat DataFrame with one row per price point
        """
        records: list[dict[str, Any]] = []

        for uuid, price_obj in prices.items():
            if hasattr(price_obj, "to_json"):
                nested = price_obj.to_json()
            elif isinstance(price_obj, dict):
                nested = price_obj
            else:
                continue

            for source, providers in nested.items():
                if not isinstance(providers, dict):
                    continue
                for provider_name, price_data in providers.items():
                    if not isinstance(price_data, dict):
                        continue

                    currency = price_data.get("currency", "USD")

                    for price_type in ("buylist", "retail"):
                        type_data = price_data.get(price_type, {})
                        if not isinstance(type_data, dict):
                            continue
                        for finish in ("normal", "foil", "etched"):
                            finish_data = type_data.get(finish, {})
                            if not isinstance(finish_data, dict):
                                continue
                            for date, price in finish_data.items():
                                if price is not None:
                                    records.append(
                                        {
                                            "uuid": uuid,
                                            "date": date,
                                            "source": source,
                                            "provider": provider_name,
                                            "price_type": price_type,
                                            "finish": finish,
                                            "price": float(price),
                                            "currency": currency,
                                        }
                                    )

        if not records:
            return pl.DataFrame(schema=PRICE_SCHEMA)

        return pl.DataFrame(records, schema=PRICE_SCHEMA)

    def _json_to_dataframe(self, data: dict[str, Any]) -> pl.DataFrame:
        """Convert nested JSON price dict to flat DataFrame."""
        if "data" in data:
            data = data["data"]

        records = [
            {
                "uuid": uuid,
                "date": date,
                "source": source,
                "provider": provider,
                "price_type": price_type,
                "finish": finish,
                "price": float(price),
                "currency": price_data.get("currency", "USD"),
            }
            for uuid, sources in data.items()
            if isinstance(sources, dict)
            for source, providers in sources.items()
            if isinstance(providers, dict)
            for provider, price_data in providers.items()
            if isinstance(price_data, dict)
            for price_type in ("buylist", "retail")
            for finish, finish_data in price_data.get(price_type, {}).items()
            if isinstance(finish_data, dict)
            for date, price in finish_data.items()
            if price is not None
        ]

        if not records:
            return pl.DataFrame(schema=PRICE_SCHEMA)
        return pl.DataFrame(records, schema=PRICE_SCHEMA)

    def load_archive(self, path: Path | None = None) -> pl.LazyFrame:
        """Load price archive from parquet or JSON."""
        return load_archive(path, json_to_dataframe=self._json_to_dataframe)

    def save_prices_partitioned(self, df: pl.LazyFrame | pl.DataFrame) -> Path:
        """Save today's prices to date-partitioned directory."""
        return save_prices_partitioned(df, self.today_date)

    def load_partitioned_archive(self, days: int = 90) -> pl.LazyFrame:
        """Load archive from partitioned directory, lazy streaming."""
        return load_partitioned_archive(days)

    def prune_partitions(self, days: int = 90) -> int:
        """Delete partition directories older than retention period."""
        return prune_partitions(days)

    def migrate_legacy_archive(self) -> bool:
        """One-time migration from single parquet/JSON to partitioned format."""
        return migrate_legacy_archive()

    def list_local_partitions(self) -> list[str]:
        """List available date partitions locally."""
        from mtgjson5.build.price_archive import list_local_partitions as _list_local

        return _list_local()

    def sync_partition_to_s3(self, date: str) -> bool:
        """Upload a single date partition to S3."""
        return sync_partition_to_s3(date)

    def sync_partition_from_s3(self, date: str) -> bool:
        """Download a single date partition from S3."""
        from mtgjson5.build.price_s3 import sync_partition_from_s3 as _sync

        return _sync(date)

    def list_s3_partitions(self) -> list[str]:
        """List available date partitions on S3."""
        from mtgjson5.build.price_s3 import list_s3_partitions as _list_s3

        return _list_s3()

    def sync_missing_partitions_from_s3(self, days: int = 90) -> int:
        """Download partitions from S3 that we don't have locally."""
        return sync_missing_partitions_from_s3(days)

    def sync_partition_to_s3_with_retry(self, date: str, max_retries: int = 3, base_delay: float = 1.0) -> bool:
        """Upload a single date partition to S3 with retry logic."""
        from mtgjson5.build.price_s3 import sync_partition_to_s3_with_retry as _sync

        return _sync(date, max_retries, base_delay)

    def sync_local_partitions_to_s3(self, days: int = 90, max_workers: int = 16, max_retries: int = 3) -> int:
        """Upload local partitions to S3 that S3 doesn't have."""
        return sync_local_partitions_to_s3(days, max_workers, max_retries)

    def stream_write_all_prices_json(self, lf: pl.LazyFrame, path: Path, source_path: Path | None = None) -> None:
        """Stream-write AllPrices.json using Prefix Partitioning."""
        stream_write_all_prices_json(lf, path, self.today_date, source_path=source_path)

    def stream_write_today_prices_json(self, df: pl.DataFrame, path: Path) -> None:
        """Stream-write AllPricesToday.json for today's prices only."""
        stream_write_today_prices_json(df, path, self.today_date)

    def write_prices_sqlite(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data to a SQLite binary database."""
        write_prices_sqlite(df, path)

    def write_prices_sql(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data as a MySQL text dump with INSERT statements."""
        write_prices_sql(df, path)

    def write_prices_psql(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data as a PostgreSQL dump with COPY format."""
        write_prices_psql(df, path)

    def write_prices_csv(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data as CSV matching v1 cardPrices.csv format."""
        write_prices_csv(df, path)

    def get_price_archive_from_s3(self) -> pl.LazyFrame:
        """Download price archive from S3 and convert to LazyFrame."""
        return get_price_archive_from_s3(
            load_archive_fn=self.load_archive,
            json_to_dataframe_fn=self._json_to_dataframe,
        )

    def upload_archive_to_s3(self, archive_data: dict[str, Any] | pl.LazyFrame | pl.DataFrame) -> None:
        """Upload price archive to S3 in Parquet format."""
        upload_archive_to_s3(archive_data, json_to_dataframe_fn=self._json_to_dataframe)

    def download_old_all_printings(self) -> None:
        """
        Download the hosted version of AllPrintings from MTGJSON for consumption.

        Uses streaming decompression to avoid holding the full compressed +
        decompressed file in memory simultaneously.
        """
        import lzma

        LOGGER.info("Downloading AllPrintings.json from MTGJSON")
        MtgjsonConfig().output_path.mkdir(parents=True, exist_ok=True)
        response = requests.get(
            "https://mtgjson.com/api/v5/AllPrintings.json.xz",
            stream=True,
            timeout=60,
        )
        response.raise_for_status()
        decompressor = lzma.LZMADecompressor()
        with self.all_printings_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    try:
                        decompressed = decompressor.decompress(chunk)
                        if decompressed:
                            f.write(decompressed)
                    except lzma.LZMAError:
                        break

        LOGGER.info(f"Downloaded AllPrintings.json to {self.all_printings_path}")

    def build_prices(
        self,
        today_df: pl.DataFrame | None = None,
        parquet_output_dir: Path | None = None,
        write_json: bool = True,
        raw_cache_dir: Path | None = None,
    ) -> tuple[Path | None, Path | None]:
        """
        Full price build with partitioned storage and streaming output.

        Args:
            today_df: Pre-fetched today prices (skips provider fetch if given).
            parquet_output_dir: When set, write AllPrices.parquet and
                AllPricesToday.parquet to this directory.
            write_json: When True (default), produce JSON/SQL/CSV outputs.
            raw_cache_dir: When set, build today_df from pre-fetched raw
                parquets via Polars joins (no network fetch).

        Returns:
            Tuple of (all_prices_path, today_prices_path) or (None, None) on failure
        """
        LOGGER.info("Polars Price Builder - Building Prices (V2 Partitioned)")

        # Auto-download AllPrintings if needed
        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, attempting to download")
            self.download_old_all_printings()

        if not self.all_printings_path.is_file():
            LOGGER.error("Failed to get AllPrintings")
            return None, None

        # Migrate legacy archive if it exists (one-time operation)
        migrated = self.migrate_legacy_archive()
        if migrated:
            LOGGER.info("Legacy archive migration complete")
            # Push migrated partitions to S3 to make it the authoritative hive
            LOGGER.info("Uploading migrated partitions to S3")
            uploaded = self.sync_local_partitions_to_s3(days=90)
            if uploaded > 0:
                LOGGER.info(f"Uploaded {uploaded} migrated partitions to S3")

        # Sync missing partitions from S3 (bidirectional sync)
        LOGGER.info("Syncing partitions with S3")
        downloaded = self.sync_missing_partitions_from_s3(days=90)
        if downloaded > 0:
            LOGGER.info(f"Downloaded {downloaded} partitions from S3")

        # Fetch today's prices from providers (skip if pre-fetched)
        if today_df is None:
            if raw_cache_dir is not None:
                LOGGER.info("Mapping today's prices from raw cache (no network fetch)")
                today_df = self.map_raw_to_today_df(raw_cache_dir)
            else:
                LOGGER.info("Fetching today's prices from V2 providers")
                ctx = PriceBuilderContext.from_cache()
                today_df = self.build_today_prices(ctx)
                ctx.release()
                del ctx
                gc.collect()
        else:
            LOGGER.info("Using pre-fetched today prices (%s rows)", f"{len(today_df):,}")

        if len(today_df) == 0:
            LOGGER.warning("No price data generated")
            return None, None

        LOGGER.info(f"Fetched {len(today_df):,} price points for today")

        # Save today's prices to partition
        LOGGER.info("Saving today's prices to partition")
        self.save_prices_partitioned(today_df)

        # Upload today's partition to S3 (non-fatal if fails)
        try:
            LOGGER.info("Uploading today's partition to S3")
            if self.sync_partition_to_s3(self.today_date):
                LOGGER.info("S3 upload complete")
            else:
                LOGGER.warning("S3 upload failed (continuing)")
        except Exception as e:
            LOGGER.error(f"S3 upload failed (continuing): {e}")

        # Prune old LOCAL partitions only (S3 is append-only historical archive)
        LOGGER.info("Pruning old local partitions")
        local_pruned = self.prune_partitions(days=90)
        if local_pruned > 0:
            LOGGER.info(f"Pruned {local_pruned} local partitions")

        # Load archive from partitions, filtered to 90 days for output
        LOGGER.info("Loading archive from partitions (90 day window)")
        archive_lf = self.load_partitioned_archive(days=90)

        # --- Parquet output (optional, streaming) ---
        if parquet_output_dir is not None:
            parquet_output_dir.mkdir(parents=True, exist_ok=True)
            output_all = parquet_output_dir / "AllPrices.parquet"
            LOGGER.info(f"Sinking AllPrices.parquet to {output_all}")
            try:
                archive_lf.sink_parquet(output_all, compression="zstd", compression_level=3)
                LOGGER.info(f"  {output_all.name}: written via streaming sink")
            except Exception as exc:
                LOGGER.warning(f"Streaming sink failed ({exc}), falling back to collect")
                df = archive_lf.collect()
                df.write_parquet(output_all, compression="zstd", compression_level=3)
                LOGGER.info(f"  {output_all.name}: {len(df):,} rows")
                del df

            if len(today_df) > 0:
                today_parquet = parquet_output_dir / "AllPricesToday.parquet"
                today_df.write_parquet(today_parquet, compression="zstd", compression_level=3)
                LOGGER.info(f"  {today_parquet.name}: {len(today_df):,} rows")

            gc.collect()
            # Re-scan from consolidated parquet (1 file vs 86 partitions)
            archive_lf = pl.scan_parquet(output_all)

        # --- JSON / SQL / CSV output (optional) ---
        all_prices_path: Path | None = None
        today_prices_path: Path | None = None
        _temp_parquet: Path | None = None

        if write_json:
            output_path = MtgjsonConfig().output_path
            output_path.mkdir(parents=True, exist_ok=True)

            # Consolidate partitions to a single file for efficient per-prefix reads
            if parquet_output_dir is not None:
                source_parquet = parquet_output_dir / "AllPrices.parquet"
            else:
                # No parquet output requested — write temp consolidated file
                source_parquet = constants.CACHE_PATH / "_all_prices_temp.parquet"
                source_parquet.parent.mkdir(parents=True, exist_ok=True)
                LOGGER.info("Writing temp consolidated parquet for JSON streaming")
                archive_lf.sink_parquet(source_parquet, compression="zstd", compression_level=1)
                archive_lf = pl.scan_parquet(source_parquet)
                _temp_parquet = source_parquet

            all_prices_path = output_path / "AllPrices.json"
            LOGGER.info(f"Streaming AllPrices.json to {all_prices_path}")
            self.stream_write_all_prices_json(archive_lf, all_prices_path, source_path=source_parquet)
            del archive_lf
            gc.collect()

            # Clean up temp file if created
            if _temp_parquet is not None and _temp_parquet.exists():
                _temp_parquet.unlink()
                LOGGER.debug("Removed temp consolidated parquet")

            today_prices_path = output_path / "AllPricesToday.json"
            LOGGER.info(f"Streaming AllPricesToday.json to {today_prices_path}")
            self.stream_write_today_prices_json(today_df, today_prices_path)
            gc.collect()

            # Write SQL formats for AllPricesToday
            LOGGER.info("Writing AllPricesToday SQL formats")
            self.write_prices_sqlite(today_df, output_path / "AllPricesToday.sqlite")
            self.write_prices_sql(today_df, output_path / "AllPricesToday.sql")
            self.write_prices_psql(today_df, output_path / "AllPricesToday.psql")

            # Write CSV format for cardPrices (goes in csv/ directory with other CSVs)
            csv_dir = output_path / "csv"
            csv_dir.mkdir(parents=True, exist_ok=True)
            LOGGER.info("Writing cardPrices.csv")
            self.write_prices_csv(today_df, csv_dir / "cardPrices.csv")
        else:
            del archive_lf

        LOGGER.info("Price build complete")
        return all_prices_path, today_prices_path
