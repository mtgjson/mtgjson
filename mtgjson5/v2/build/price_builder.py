"""
Polars-based Price Builder for MTGJSON v2.
"""

import asyncio
import contextlib
import datetime
import json
import logging
import lzma
import shutil
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

import dateutil.relativedelta
import orjson
import polars as pl
import requests

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler
from mtgjson5.v2.providers import (
    CardHoarderPriceProvider,
    CardMarketProvider,
    CKProvider,
    ManapoolPriceProvider,
    TCGPlayerPriceProvider,
)

if TYPE_CHECKING:
    from mtgjson5.v2.data import GlobalCache

LOGGER = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, str], None]

# Partitioned price archive directory
PRICES_PARTITION_DIR = constants.CACHE_PATH / "prices"

# Schema for price data
PRICE_SCHEMA = {
    "uuid": pl.String,
    "date": pl.String,
    "source": pl.String,
    "provider": pl.String,
    "price_type": pl.String,  # "buylist" or "retail"
    "finish": pl.String,  # "normal", "foil", "etched"
    "price": pl.Float64,
    "currency": pl.String,
}


@dataclass
class PriceBuilderContext:
    """
    Context for price building with derived ID mappings.

    Owns the ID -> UUID mappings needed by price providers:
    - tcg_to_uuid: TCGPlayer product ID -> MTGJSON UUIDs
    - tcg_etched_to_uuid: TCGPlayer etched product ID -> MTGJSON UUIDs
    - mtgo_to_uuid: MTGO ID -> MTGJSON UUIDs
    - scryfall_to_uuid: Scryfall ID -> MTGJSON UUIDs

    Mappings are built lazily on first access from GlobalCache LazyFrames.
    """

    _cache: "GlobalCache | None" = field(default=None, repr=False)

    # Derived mappings (built lazily)
    _tcg_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _tcg_etched_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _mtgo_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)
    _scryfall_to_uuid: dict[str, set[str]] | None = field(default=None, repr=False)

    @classmethod
    def from_cache(cls) -> "PriceBuilderContext":
        """Create context from global cache.

        Ensures ID mappings are loaded from parquet files if they exist
        (for standalone price builds that run after a card build).
        """
        from mtgjson5.v2.data import GLOBAL_CACHE

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
    def scryfall_to_uuid(self) -> dict[str, set[str]]:
        """Scryfall ID -> MTGJSON UUID(s) mapping."""
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

    def _build_scryfall_to_uuid(self) -> dict[str, set[str]]:
        """Build Scryfall ID -> UUID mapping from cache."""
        if self._cache is None or self._cache.uuid_cache_lf is None:
            return {}
        df = self._cache.uuid_cache_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            scryfall_id = row.get("scryfallId")
            uuid = row.get("cachedUuid")
            if scryfall_id and uuid:
                if scryfall_id not in result:
                    result[scryfall_id] = set()
                result[scryfall_id].add(uuid)
        return result


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
    on_progress: ProgressCallback | None

    def __init__(
        self,
        all_printings_path: Path | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        self.on_progress = on_progress
        self.all_printings_path = (
            all_printings_path
            if all_printings_path
            else MtgjsonConfig().output_path.joinpath("AllPrintings.json")
        )
        self.today_date = datetime.date.today().strftime("%Y-%m-%d")

        # V2 providers (lazy init)
        self._tcg_provider: TCGPlayerPriceProvider | None = None
        self._ch_provider: CardHoarderPriceProvider | None = None
        self._manapool_provider: ManapoolPriceProvider | None = None
        self._mcm_provider: CardMarketProvider | None = None
        self._ck_provider: CKProvider | None = None

    async def build_today_prices_async(
        self, ctx: PriceBuilderContext | None = None
    ) -> pl.DataFrame:
        """
        Fetch today's prices from v2 async providers.

        Providers:
        - TCGPlayer: retail only (no buylist)
        - CardHoarder: MTGO prices
        - Manapool: paper prices
        - CardMarket: paper prices (EUR)
        - CardKingdom: paper prices

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
            tcg_df = await self._tcg_provider.fetch_all_prices(
                tcg_to_uuid or {}, tcg_etched_to_uuid or {}
            )
            if len(tcg_df) > 0:
                frames.append(tcg_df)
                LOGGER.info(f"  TCGPlayerPriceProvider: {len(tcg_df):,} price points")

        # Fetch CardHoarder (MTGO) - simple bulk
        LOGGER.info("Fetching CardHoarder prices")
        if mtgo_to_uuid:
            ch_df = await self._ch_provider.fetch_prices(mtgo_to_uuid)
            if len(ch_df) > 0:
                frames.append(ch_df)
                LOGGER.info(f"  CardHoarderPriceProvider: {len(ch_df):,} price points")

        # Fetch Manapool - simple bulk
        LOGGER.info("Fetching Manapool prices")
        if scryfall_to_uuid:
            manapool_df = await self._manapool_provider.fetch_prices(scryfall_to_uuid)
            if len(manapool_df) > 0:
                frames.append(manapool_df)
                LOGGER.info(
                    f"  ManapoolPriceProvider: {len(manapool_df):,} price points"
                )

        # Fetch CardMarket - bulk API
        LOGGER.info("Fetching CardMarket prices")
        mcm_dict = await self._mcm_provider.generate_today_price_dict(
            self.all_printings_path
        )
        if mcm_dict:
            mcm_df = self._prices_dict_to_dataframe(mcm_dict)
            if len(mcm_df) > 0:
                frames.append(mcm_df)
                LOGGER.info(f"  CardMarketProvider: {len(mcm_df):,} price points")
        await self._mcm_provider.close()

        # Fetch CardKingdom - async fetch, convert to DataFrame
        LOGGER.info("Fetching CardKingdom prices")
        try:
            await self._ck_provider.load_or_fetch_async(
                constants.CACHE_PATH / "ck_raw.parquet"
            )
            ck_pricing_df = self._ck_provider.get_pricing_df()
            if len(ck_pricing_df) > 0:
                # Convert CK pricing df to flat price schema
                ck_records = self._convert_ck_pricing(
                    ck_pricing_df, scryfall_to_uuid or {}
                )
                if ck_records:
                    ck_df = pl.DataFrame(ck_records, schema=PRICE_SCHEMA)
                    frames.append(ck_df)
                    LOGGER.info(f"  CKProvider: {len(ck_df):,} price points")
        except Exception as e:
            LOGGER.warning(f"Failed to fetch CardKingdom prices: {e}")

        if not frames:
            LOGGER.warning("No price data collected from providers")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        return pl.concat(frames)

    def build_today_prices(
        self, ctx: PriceBuilderContext | None = None
    ) -> pl.DataFrame:
        """Sync wrapper for build_today_prices_async."""
        return asyncio.run(self.build_today_prices_async(ctx))

    def _convert_ck_pricing(
        self,
        ck_df: pl.DataFrame,
        scryfall_to_uuid: dict[str, set[str]],
    ) -> list[dict[str, Any]]:
        """
        Convert CardKingdom pricing DataFrame to flat price records.

        Args:
            ck_df: CK pricing DataFrame with columns:
                   ck_id, scryfall_id, is_foil, is_etched, price_retail, price_buy
            scryfall_to_uuid: Scryfall ID -> MTGJSON UUIDs mapping

        Returns:
            List of price record dicts
        """
        records: list[dict[str, Any]] = []

        for row in ck_df.iter_rows(named=True):
            scryfall_id = row.get("scryfall_id")
            if not scryfall_id:
                continue

            uuids = scryfall_to_uuid.get(scryfall_id)
            if not uuids:
                continue

            is_foil = row.get("is_foil", False)
            is_etched = row.get("is_etched", False)
            price_retail = row.get("price_retail")
            price_buy = row.get("price_buy")

            if is_etched:
                finish = "etched"
            elif is_foil:
                finish = "foil"
            else:
                finish = "normal"

            for uuid in uuids:
                # Retail price
                if price_retail is not None:
                    records.append(
                        {
                            "uuid": uuid,
                            "date": self.today_date,
                            "source": "paper",
                            "provider": "cardkingdom",
                            "price_type": "retail",
                            "finish": finish,
                            "price": float(price_retail),
                            "currency": "USD",
                        }
                    )

                # Buylist price
                if price_buy is not None:
                    records.append(
                        {
                            "uuid": uuid,
                            "date": self.today_date,
                            "source": "paper",
                            "provider": "cardkingdom",
                            "price_type": "buylist",
                            "finish": finish,
                            "price": float(price_buy),
                            "currency": "USD",
                        }
                    )

        return records

    def _prices_dict_to_dataframe(self, prices: dict[str, Any]) -> pl.DataFrame:
        """
        Convert provider's MtgjsonPricesObject dict to flat DataFrame.

        Args:
            prices: Dict mapping uuid -> MtgjsonPricesObject

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

    @staticmethod
    def prune_prices(df: pl.LazyFrame, months: int = 3) -> pl.LazyFrame:
        """
        Filter out price entries older than `months` months.

        Args:
            df: LazyFrame with price data
            months: Number of months to keep (default 3)

        Returns:
            Filtered LazyFrame
        """
        cutoff = (
            datetime.date.today() + dateutil.relativedelta.relativedelta(months=-months)
        ).strftime("%Y-%m-%d")

        return df.filter(pl.col("date") >= cutoff)

    @staticmethod
    def merge_prices(archive: pl.LazyFrame, today: pl.LazyFrame) -> pl.LazyFrame:
        """
        Merge today's prices into archive, keeping latest price per unique key.

        Uses concat + group_by to handle duplicates, preferring today's values.

        Args:
            archive: Existing price archive
            today: Today's price data

        Returns:
            Merged LazyFrame with deduplication
        """
        key_cols = ["uuid", "date", "source", "provider", "price_type", "finish"]

        combined = pl.concat([archive, today])

        return combined.group_by(key_cols).agg(
            [
                pl.col("price").last(),
                pl.col("currency").last(),
            ]
        )

    def load_archive(self, path: Path | None = None) -> pl.LazyFrame:
        """
        Load price archive from parquet or JSON.

        Args:
            path: Path to archive file (default: cache dir)

        Returns:
            LazyFrame with archive data
        """
        cache_dir = constants.CACHE_PATH
        parquet_path = cache_dir / "prices_archive.parquet"
        json_path = cache_dir / "prices.json"

        if path is not None:
            if path.suffix == ".parquet" and path.exists():
                LOGGER.info(f"Loading price archive from {path}")
                return pl.scan_parquet(path)
            elif path.exists():
                return self._load_json_archive(path)

        if parquet_path.exists():
            LOGGER.info(f"Loading parquet archive from {parquet_path}")
            return pl.scan_parquet(parquet_path)

        if json_path.exists():
            LOGGER.info(f"Loading JSON archive from {json_path}")
            return self._load_json_archive(json_path)

        LOGGER.info("No existing price archive found, starting fresh")
        return pl.DataFrame(schema=PRICE_SCHEMA).lazy()

    def _load_json_archive(self, path: Path) -> pl.LazyFrame:
        """Load archive from JSON file and convert to LazyFrame."""
        try:
            LOGGER.info(f"Converting JSON archive (orjson): {path}")
            with open(path, "rb") as f:
                data = orjson.loads(f.read())
        except ImportError:
            LOGGER.info(f"Converting JSON archive (json): {path}")
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

        df = self._json_to_dataframe(data)
        LOGGER.info(f"Loaded {len(df):,} price records from JSON")
        return df.lazy()

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

    def save_archive(self, df: pl.LazyFrame, path: Path | None = None) -> Path:
        """
        Save price archive to parquet with zstd compression.

        Args:
            df: LazyFrame with price data
            path: Output path (default: cache dir)

        Returns:
            Path to saved file
        """
        if path is None:
            cache_dir = constants.CACHE_PATH
            path = cache_dir / "prices_archive.parquet"

        path.parent.mkdir(parents=True, exist_ok=True)

        collected = df.collect()
        collected.write_parquet(path, compression="zstd", compression_level=9)

        LOGGER.info(
            f"Saved price archive: {len(collected):,} rows, {path.stat().st_size:,} bytes"
        )
        return path

    def save_prices_partitioned(self, df: pl.LazyFrame | pl.DataFrame) -> Path:
        """
        Save today's prices to date-partitioned directory.

        Creates a partition structure like:
            .mtgjson5_cache/prices/date=2024-01-30/data.parquet

        Args:
            df: DataFrame or LazyFrame with today's price data

        Returns:
            Path to the created partition directory
        """
        partition_path = PRICES_PARTITION_DIR / f"date={self.today_date}"
        partition_path.mkdir(parents=True, exist_ok=True)

        output_file = partition_path / "data.parquet"

        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(output_file, compression="zstd", compression_level=9)
        else:
            df.write_parquet(output_file, compression="zstd", compression_level=9)

        LOGGER.info(f"Saved today's prices to partition: {partition_path}")
        return partition_path

    def load_partitioned_archive(self, days: int = 90) -> pl.LazyFrame:
        """
        Load archive from partitioned directory, lazy streaming.

        Scans all date partitions and returns a LazyFrame.

        Args:
            days: Maximum age of partitions to include (90 default)

        Returns:
            LazyFrame with all price data from partitions
        """
        if not PRICES_PARTITION_DIR.exists():
            LOGGER.info("No partitioned archive found, returning empty LazyFrame")
            return pl.LazyFrame(schema=PRICE_SCHEMA)

        partitions = list(PRICES_PARTITION_DIR.glob("date=*/data.parquet"))

        if not partitions:
            LOGGER.info("No partition files found, returning empty LazyFrame")
            return pl.LazyFrame(schema=PRICE_SCHEMA)

        cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

        valid_partitions = []
        for p in partitions:
            date_part = p.parent.name
            if date_part.startswith("date="):
                date_str = date_part[5:]
                if date_str >= cutoff:
                    valid_partitions.append(p)

        if not valid_partitions:
            LOGGER.info("No valid partitions within retention period")
            return pl.LazyFrame(schema=PRICE_SCHEMA)

        LOGGER.info(f"Loading {len(valid_partitions)} partitions from archive")

        return pl.scan_parquet(valid_partitions)

    def prune_partitions(self, days: int = 90) -> int:
        """
        Delete partition directories older than retention period.

        Args:
            days: Number of days to keep (90 default)

        Returns:
            Number of partitions deleted
        """
        if not PRICES_PARTITION_DIR.exists():
            return 0

        cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

        deleted_count = 0
        for partition in PRICES_PARTITION_DIR.glob("date=*"):
            if not partition.is_dir():
                continue

            date_str = partition.name.split("=")[1] if "=" in partition.name else ""
            if date_str and date_str < cutoff:
                try:
                    shutil.rmtree(partition)
                    LOGGER.info(f"Pruned old partition: {partition.name}")
                    deleted_count += 1
                except Exception as e:
                    LOGGER.warning(f"Failed to prune {partition}: {e}")

        if deleted_count > 0:
            LOGGER.info(f"Pruned {deleted_count} old partitions")

        return deleted_count

    def migrate_legacy_archive(self) -> bool:
        """
        One-time migration from single parquet/JSON to partitioned format.

        Reads legacy archive files and splits them into date-partitioned
        directories. After successful migration, removes the legacy files.

        Returns:
            True if migration occurred, False if no legacy files found
        """
        legacy_parquet = constants.CACHE_PATH / "prices_archive.parquet"
        legacy_s3 = constants.CACHE_PATH / "prices_archive_s3.parquet"

        migrated = False

        for legacy_path in [legacy_parquet, legacy_s3]:
            if not legacy_path.exists():
                continue

            LOGGER.info(f"Migrating legacy archive: {legacy_path}")

            try:
                lf = pl.scan_parquet(legacy_path)
                df = lf.collect()

                if len(df) == 0:
                    LOGGER.info(f"Legacy archive is empty, removing: {legacy_path}")
                    legacy_path.unlink()
                    continue

                dates = df.select("date").unique()["date"].to_list()
                LOGGER.info(f"Migrating {len(df):,} rows across {len(dates)} dates")

                for date_val in dates:
                    if not date_val:
                        continue

                    date_group = df.filter(pl.col("date") == date_val)
                    partition_path = PRICES_PARTITION_DIR / f"date={date_val}"
                    partition_path.mkdir(parents=True, exist_ok=True)

                    output_file = partition_path / "data.parquet"

                    if output_file.exists():
                        existing = pl.read_parquet(output_file)
                        date_group = pl.concat([existing, date_group]).unique(
                            subset=[
                                "uuid",
                                "date",
                                "source",
                                "provider",
                                "price_type",
                                "finish",
                            ]
                        )

                    date_group.write_parquet(
                        output_file, compression="zstd", compression_level=9
                    )

                legacy_path.unlink()
                LOGGER.info(f"Migration complete, removed {legacy_path}")
                migrated = True

            except Exception as e:
                LOGGER.error(f"Failed to migrate {legacy_path}: {e}")

        return migrated

    def _get_s3_config(self) -> tuple[str, str] | None:
        """
        Get S3 bucket configuration for prices.

        Returns:
            Tuple of (bucket_name, base_path) or None if not configured
        """
        if not MtgjsonConfig().has_section("Prices"):
            return None

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        # bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        return bucket_name, "price_archive"

    def sync_partition_to_s3(self, date: str) -> bool:
        """
        Upload a single date partition to S3.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            True if upload succeeded
        """
        config = self._get_s3_config()
        if config is None:
            LOGGER.debug("No S3 config, skipping partition upload")
            return False

        bucket_name, base_path = config
        local_path = PRICES_PARTITION_DIR / f"date={date}" / "data.parquet"

        if not local_path.exists():
            LOGGER.warning(f"Local partition not found: {local_path}")
            return False

        s3_path = f"{base_path}/date={date}/data.parquet"

        return MtgjsonS3Handler().upload_file(str(local_path), bucket_name, s3_path)

    def sync_partition_from_s3(self, date: str) -> bool:
        """
        Download a single date partition from S3.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            True if download succeeded
        """
        config = self._get_s3_config()
        if config is None:
            return False

        bucket_name, base_path = config
        local_path = PRICES_PARTITION_DIR / f"date={date}" / "data.parquet"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        s3_path = f"{base_path}/date={date}/data.parquet"

        return MtgjsonS3Handler().download_file(bucket_name, s3_path, str(local_path))

    def list_s3_partitions(self) -> list[str]:
        """
        List available date partitions on S3.

        Returns:
            List of date strings (YYYY-MM-DD) for available partitions
        """
        config = self._get_s3_config()
        if config is None:
            return []

        bucket_name, base_path = config

        try:
            import boto3

            s3 = boto3.client("s3")
            prefix = f"{base_path}/date="

            dates = []
            paginator = s3.get_paginator("list_objects_v2")

            for page in paginator.paginate(
                Bucket=bucket_name, Prefix=prefix, Delimiter="/"
            ):
                # CommonPrefixes contains the "folders" (date=YYYY-MM-DD/)
                for prefix_obj in page.get("CommonPrefixes", []):
                    folder = prefix_obj.get("Prefix", "")
                    # Extract date from path like "price_archive/date=2024-01-30/"
                    if "date=" in folder:
                        date_part = folder.split("date=")[1].rstrip("/")
                        if date_part:
                            dates.append(date_part)

            return sorted(dates)

        except Exception as e:
            LOGGER.warning(f"Failed to list S3 partitions: {e}")
            return []

    def list_local_partitions(self) -> list[str]:
        """
        List available date partitions locally.

        Returns:
            List of date strings (YYYY-MM-DD) for available partitions
        """
        if not PRICES_PARTITION_DIR.exists():
            return []

        dates = []
        for partition in PRICES_PARTITION_DIR.glob("date=*"):
            if partition.is_dir() and (partition / "data.parquet").exists():
                date_str = partition.name.split("=")[1] if "=" in partition.name else ""
                if date_str:
                    dates.append(date_str)

        return sorted(dates)

    def sync_missing_partitions_from_s3(self, days: int = 90) -> int:
        """
        Download partitions from S3 that we don't have locally.

        Only downloads partitions within the retention period.

        Args:
            days: Maximum age of partitions to sync (90 default)

        Returns:
            Number of partitions downloaded
        """
        config = self._get_s3_config()
        if config is None:
            LOGGER.info("No S3 config, skipping partition sync")
            return 0

        cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

        s3_partitions = set(self.list_s3_partitions())
        local_partitions = set(self.list_local_partitions())

        # Filter to only include partitions within retention period
        s3_partitions = {d for d in s3_partitions if d >= cutoff}

        missing = s3_partitions - local_partitions

        if not missing:
            LOGGER.info("All S3 partitions are synced locally")
            return 0

        LOGGER.info(f"Downloading {len(missing)} missing partitions from S3")

        downloaded = 0
        for date in sorted(missing):
            if self.sync_partition_from_s3(date):
                downloaded += 1
                LOGGER.info(f"Downloaded partition: date={date}")

        return downloaded

    def sync_local_partitions_to_s3(self, days: int = 90) -> int:
        """
        Upload local partitions to S3 that S3 doesn't have.

        Used after migration to push the converted partitions to S3,
        making S3 the authoritative hive.

        Args:
            days: Maximum age of partitions to sync (default 90)

        Returns:
            Number of partitions uploaded
        """
        config = self._get_s3_config()
        if config is None:
            LOGGER.info("No S3 config, skipping partition upload")
            return 0

        cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()

        s3_partitions = set(self.list_s3_partitions())
        local_partitions = set(self.list_local_partitions())

        # Filter to only include partitions within retention period
        local_partitions = {d for d in local_partitions if d >= cutoff}

        missing_on_s3 = local_partitions - s3_partitions

        if not missing_on_s3:
            LOGGER.info("All local partitions are synced to S3")
            return 0

        LOGGER.info(f"Uploading {len(missing_on_s3)} local partitions to S3")

        uploaded = 0
        for date in sorted(missing_on_s3):
            if self.sync_partition_to_s3(date):
                uploaded += 1
                if uploaded % 10 == 0:
                    LOGGER.info(
                        f"  Uploaded {uploaded}/{len(missing_on_s3)} partitions..."
                    )

        LOGGER.info(f"Uploaded {uploaded} partitions to S3")
        return uploaded

    def stream_write_all_prices_json(self, lf: pl.LazyFrame, path: Path) -> None:
        """
        Stream-write AllPrices.json using Prefix Partitioning.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        prefixes = "0123456789abcdef"

        LOGGER.info(f"Streaming AllPrices.json to {path}")

        opt_lf = lf.with_columns(
            [
                pl.col("source").cast(pl.Categorical),
                pl.col("provider").cast(pl.Categorical),
                pl.col("price_type").cast(pl.Categorical),
                pl.col("finish").cast(pl.Categorical),
                pl.col("currency").cast(pl.Categorical),
            ]
        )

        with open(path, "wb") as f:
            f.write(b'{"meta":')
            meta = {
                "date": self.today_date,
                "version": MtgjsonConfig().mtgjson_version,
            }
            f.write(orjson.dumps(meta))
            f.write(b',"data":{')

            total_processed = 0
            first_chunk_written = False

            for prefix in prefixes:
                chunk_lf = opt_lf.filter(pl.col("uuid").str.starts_with(prefix))

                try:
                    df_chunk = chunk_lf.collect()
                except Exception as e:
                    LOGGER.error(f"Failed to collect chunk {prefix}: {e}")
                    continue

                if df_chunk.height == 0:
                    del df_chunk
                    continue

                # We sort here because we need grouped UUIDs for the iterator
                df_chunk = df_chunk.sort(
                    ["uuid", "source", "provider", "price_type", "finish", "date"]
                )

                # Handle comma between chunks
                if first_chunk_written:
                    f.write(b",")

                items_written = self._process_chunk_to_json(f, df_chunk)

                if items_written > 0:
                    first_chunk_written = True
                    total_processed += items_written

                # Explicitly release memory before next iteration
                del df_chunk

                LOGGER.info(
                    f"  Processed prefix '{prefix}' (Total: {total_processed:,})"
                )

            f.write(b"}}")

        LOGGER.info(
            f"Finished streaming AllPrices.json. Total UUIDs: {total_processed:,}"
        )

    def _process_chunk_to_json(self, f: BinaryIO, df: pl.DataFrame) -> int:
        """
        Aggregates a materialized DataFrame and writes to the open file handle.
        Returns number of UUIDs written.
        """
        aggregated = (
            df.group_by(
                ["uuid", "source", "provider", "currency", "price_type", "finish"]
            )
            .agg([pl.col("date"), pl.col("price")])
            .sort("uuid")
        )

        current_uuid = None
        uuid_data: dict[str, dict[str, dict[str, Any]]] = {}
        written_count = 0
        first_in_chunk = True

        rows = aggregated.iter_rows(named=True)

        for row in rows:
            uuid = row["uuid"]

            # Switch to new UUID
            if uuid != current_uuid:
                if current_uuid is not None:
                    # Flush previous UUID data
                    if not first_in_chunk:
                        f.write(b",")

                    f.write(f'"{current_uuid}":'.encode())
                    f.write(orjson.dumps(uuid_data))
                    first_in_chunk = False
                    written_count += 1

                current_uuid = uuid
                uuid_data = {}

            source = row["source"]
            provider = row["provider"]
            currency = row["currency"]
            p_type = row["price_type"]
            finish = row["finish"]

            date_prices = dict(zip(row["date"], row["price"]))

            if source not in uuid_data:
                uuid_data[source] = {}
            if provider not in uuid_data[source]:
                uuid_data[source][provider] = {
                    "buylist": {},
                    "retail": {},
                    "currency": currency,
                }

            if p_type in uuid_data[source][provider]:
                uuid_data[source][provider][p_type][finish] = date_prices

        # Flush the final UUID of the chunk
        if current_uuid is not None:
            if not first_in_chunk:
                f.write(b",")
            f.write(f'"{current_uuid}":'.encode())
            f.write(orjson.dumps(uuid_data))
            written_count += 1

        return written_count

    def _flush_uuid_to_json(
        self, file_handle: BinaryIO, uuid: str, rows: list[dict], is_first: bool
    ) -> None:
        """
        Helper to structure and write a single UUID's data to the open JSON file handle.
        """
        uuid_data: dict[str, Any] = {}

        for row in rows:
            source = row["source"]
            provider = row["provider"]
            p_type = row["price_type"]
            finish = row["finish"]
            date = row["date"]
            price = row["price"]
            currency = row["currency"]

            if source not in uuid_data:
                uuid_data[source] = {}

            if provider not in uuid_data[source]:
                uuid_data[source][provider] = {
                    "buylist": {},
                    "retail": {},
                    "currency": currency,
                }

            if not p_type or not finish:
                continue

            target_dict = uuid_data[source][provider].get(p_type)
            if target_dict is None:
                continue

            if finish not in target_dict:
                target_dict[finish] = {}

            target_dict[finish][date] = price

        if not uuid_data:
            return

        if not is_first:
            file_handle.write(b",")
        file_handle.write(f'"{uuid}":'.encode())
        file_handle.write(orjson.dumps(uuid_data))

    def stream_write_today_prices_json(self, df: pl.DataFrame, path: Path) -> None:
        """
        Stream-write AllPricesToday.json for today's prices only.

        Args:
            df: DataFrame with today's price data
            path: Output path for AllPricesToday.json
        """
        self.stream_write_all_prices_json(df.lazy(), path)

    # -----------------------------------------------------------------
    # SQL format writers
    # -----------------------------------------------------------------

    _PRICE_SQL_COLUMNS = (
        '"uuid" TEXT',
        '"date" TEXT',
        '"source" TEXT',
        '"provider" TEXT',
        '"priceType" TEXT',
        '"finish" TEXT',
        '"price" REAL',
        '"currency" TEXT',
    )

    _PRICE_INDEXES = (
        ("uuid", "uuid"),
        ("date", "date"),
        ("provider", "provider"),
    )

    def _prepare_price_df_for_sql(self, df: pl.DataFrame) -> pl.DataFrame:
        """Rename snake_case columns to camelCase for SQL output."""
        renames = {c: c for c in df.columns}
        if "price_type" in df.columns:
            renames["price_type"] = "priceType"
        return df.rename({k: v for k, v in renames.items() if k != v})

    def write_prices_sqlite(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data to a SQLite binary database.

        Creates a ``prices`` table and a ``meta`` table with indexes
        on uuid, date, and provider.
        """
        from mtgjson5.classes import MtgjsonMetaObject

        prepared = self._prepare_price_df_for_sql(df)

        if path.exists():
            path.unlink()

        conn = sqlite3.connect(str(path))
        cursor = conn.cursor()

        cols = ", ".join(self._PRICE_SQL_COLUMNS)
        cursor.execute(f'CREATE TABLE "prices" ({cols})')

        placeholders = ", ".join(["?" for _ in prepared.columns])
        col_names = ", ".join([f'"{c}"' for c in prepared.columns])

        batch_size = 10_000
        rows = prepared.rows()
        for i in range(0, len(rows), batch_size):
            cursor.executemany(
                f'INSERT INTO "prices" ({col_names}) VALUES ({placeholders})',
                rows[i : i + batch_size],
            )

        for idx_name, col in self._PRICE_INDEXES:
            with contextlib.suppress(Exception):
                cursor.execute(
                    f'CREATE INDEX "idx_prices_{idx_name}" ON "prices" ("{col}")'
                )

        meta = MtgjsonMetaObject()
        cursor.execute('CREATE TABLE "meta" ("date" TEXT, "version" TEXT)')
        cursor.execute(
            'INSERT INTO "meta" VALUES (?, ?)', (meta.date, meta.version)
        )

        conn.commit()
        conn.close()
        LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")

    def write_prices_sql(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data as a SQL text dump with INSERT statements."""
        from mtgjson5.classes import MtgjsonMetaObject

        from .serializers import escape_sqlite

        prepared = self._prepare_price_df_for_sql(df)
        meta = MtgjsonMetaObject()

        with open(path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON Price SQL Dump\n"
                f"-- Generated: {datetime.date.today().isoformat()}\n"
            )
            f.write("BEGIN TRANSACTION;\n\n")

            cols = ",\n    ".join(self._PRICE_SQL_COLUMNS)
            f.write(f'CREATE TABLE IF NOT EXISTS "prices" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in prepared.columns])
            for row in prepared.rows():
                values = ", ".join(escape_sqlite(v) for v in row)
                f.write(f'INSERT INTO "prices" ({col_names}) VALUES ({values});\n')

            for idx_name, col in self._PRICE_INDEXES:
                f.write(
                    f'CREATE INDEX IF NOT EXISTS "idx_prices_{idx_name}" '
                    f'ON "prices" ("{col}");\n'
                )

            f.write("\n")
            f.write('CREATE TABLE IF NOT EXISTS "meta" ("date" TEXT, "version" TEXT);\n')
            f.write(
                f"INSERT INTO \"meta\" VALUES ({escape_sqlite(meta.date)}, "
                f"{escape_sqlite(meta.version)});\n"
            )

            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")

    def write_prices_psql(self, df: pl.DataFrame, path: Path) -> None:
        """Write price data as a PostgreSQL dump with COPY format."""
        from .serializers import escape_postgres

        prepared = self._prepare_price_df_for_sql(df)

        with open(path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON Price PostgreSQL Dump\n"
                f"-- Generated: {datetime.date.today().isoformat()}\n"
            )
            f.write("BEGIN;\n\n")

            cols = ",\n    ".join(self._PRICE_SQL_COLUMNS)
            f.write(f'CREATE TABLE IF NOT EXISTS "prices" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in prepared.columns])
            f.write(f'COPY "prices" ({col_names}) FROM stdin;\n')

            for row in prepared.rows():
                escaped = [escape_postgres(v) for v in row]
                f.write("\t".join(escaped) + "\n")

            f.write("\\.\n\n")

            for idx_name, col in self._PRICE_INDEXES:
                f.write(
                    f'CREATE INDEX IF NOT EXISTS "idx_prices_{idx_name}" '
                    f'ON "prices" ("{col}");\n'
                )

            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")

    def get_price_archive_from_s3(self) -> pl.LazyFrame:
        """
        Download price archive from S3 and convert to LazyFrame.

        Tries Parquet format first, falls back to legacy JSON format.
        Falls back to local archive if S3 config is missing or download fails.

        Returns:
            LazyFrame with archive data
        """
        if not MtgjsonConfig().has_section("Prices"):
            LOGGER.info("No S3 config, using local archive only")
            return self.load_archive()

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)

        parquet_path = bucket_object_path.replace(".json.xz", ".parquet")
        local_parquet = constants.CACHE_PATH / "prices_archive_s3.parquet"

        LOGGER.info(f"Trying Parquet archive from S3: {parquet_path}")
        if MtgjsonS3Handler().download_file(
            bucket_name, parquet_path, str(local_parquet)
        ):
            try:
                lf = pl.scan_parquet(local_parquet)
                row_count = lf.select(pl.len()).collect().item()
                LOGGER.info(
                    f"Loaded {row_count:,} price records from S3 Parquet archive"
                )
                return lf
            except Exception as e:
                LOGGER.warning(f"Failed to read Parquet archive: {e}")
                if local_parquet.exists():
                    local_parquet.unlink()

        LOGGER.info(f"Trying legacy JSON archive from S3: {bucket_object_path}")
        temp_file = constants.CACHE_PATH / "temp_prices.json.xz"

        if not MtgjsonS3Handler().download_file(
            bucket_name, bucket_object_path, str(temp_file)
        ):
            LOGGER.warning("S3 download failed, using local archive")
            return self.load_archive()

        try:
            LOGGER.info("Decompressing JSON archive...")
            with lzma.open(temp_file) as f:
                data = json.load(f)
            temp_file.unlink()

            LOGGER.info("Converting JSON to DataFrame...")
            df = self._json_to_dataframe(data.get("data", data))
            LOGGER.info(f"Loaded {len(df):,} price records from S3 JSON archive")
            return df.lazy()
        except Exception as e:
            LOGGER.error(f"Failed to process S3 archive: {e}")
            if temp_file.exists():
                temp_file.unlink()
            return self.load_archive()

    def upload_archive_to_s3(
        self, archive_data: dict[str, Any] | pl.LazyFrame | pl.DataFrame
    ) -> None:
        """
        Upload price archive to S3 in Parquet format.
        Args:
            archive_data: Price data as dict, DataFrame, or LazyFrame
        """
        if not MtgjsonConfig().has_section("Prices"):
            LOGGER.info("No S3 config, skipping upload")
            return

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        parquet_path = bucket_object_path.replace(".json.xz", ".parquet")
        local_parquet = constants.CACHE_PATH / "prices_archive_upload.parquet"
        local_parquet.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(archive_data, dict):
            LOGGER.info("Converting dict to DataFrame for upload...")
            df = self._json_to_dataframe(archive_data)
        elif isinstance(archive_data, pl.LazyFrame):
            df = archive_data.collect()
        else:
            df = archive_data

        LOGGER.info(f"Writing {len(df):,} rows to Parquet...")
        df.write_parquet(local_parquet, compression="zstd", compression_level=9)
        LOGGER.info(
            f"Parquet archive: {local_parquet.stat().st_size / 1024 / 1024:.1f} MB"
        )

        LOGGER.info(f"Uploading to S3: {parquet_path}")
        MtgjsonS3Handler().upload_file(str(local_parquet), bucket_name, parquet_path)
        LOGGER.info("S3 upload complete")

        final_path = constants.CACHE_PATH / "prices_archive.parquet"
        if local_parquet != final_path:
            local_parquet.rename(final_path)

    def download_old_all_printings(self) -> None:
        """
        Download the hosted version of AllPrintings from MTGJSON for consumption.

        Downloads the compressed file, decompresses it, and writes to the
        configured output path.
        """
        LOGGER.info("Downloading AllPrintings.json from MTGJSON")
        file_bytes = b""
        file_data = requests.get(
            "https://mtgjson.com/api/v5/AllPrintings.json.xz",
            stream=True,
            timeout=60,
        )
        for chunk in file_data.iter_content(chunk_size=1024 * 36):
            if chunk:
                file_bytes += chunk

        MtgjsonConfig().output_path.mkdir(parents=True, exist_ok=True)
        with self.all_printings_path.open("w", encoding="utf8") as f:
            f.write(lzma.decompress(file_bytes).decode())

        LOGGER.info(f"Downloaded AllPrintings.json to {self.all_printings_path}")

    def to_nested_dict(self, df: pl.LazyFrame) -> dict[str, Any]:
        """
        Convert flat DataFrame back to nested MTGJSON price format.

        Output structure:
            {uuid: {source: {provider: {buylist: {finish: {date: price}}, retail: {...}, currency: str}}}}

        Args:
            df: LazyFrame with flat price data

        Returns:
            Nested dict matching MTGJSON AllPrices.json format
        """

        aggregated = (
            df.group_by(
                ["uuid", "source", "provider", "currency", "price_type", "finish"]
            )
            .agg([pl.struct(["date", "price"]).alias("prices")])
            .collect()
        )

        if len(aggregated) == 0:
            return {}

        result: dict[str, Any] = {}

        for row in aggregated.iter_rows(named=True):
            uuid = row["uuid"]
            source = row["source"]
            provider = row["provider"]
            currency = row["currency"]
            price_type = row["price_type"]
            finish = row["finish"]
            prices = row["prices"]  # List of {date, price} structs

            if uuid not in result:
                result[uuid] = {}
            if source not in result[uuid]:
                result[uuid][source] = {}
            if provider not in result[uuid][source]:
                result[uuid][source][provider] = {
                    "buylist": {},
                    "retail": {},
                    "currency": currency,
                }

            date_prices = {p["date"]: p["price"] for p in prices}
            result[uuid][source][provider][price_type][finish] = date_prices

        return result

    def build_prices(self) -> tuple[Path | None, Path | None]:
        """
        Full price build with partitioned storage and streaming output.

        This memory-efficient implementation:
        1. Migrates legacy archives to partitioned format (one-time)
        2. Syncs missing partitions from S3 (download recent partitions)
        3. Saves today's prices to a date partition
        4. Uploads today's partition to S3 (append-only historical archive)
        5. Prunes old LOCAL partitions only (S3 keeps full history)
        6. Stream-writes JSON outputs filtered to 90 days

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

        # Fetch today's prices from providers
        LOGGER.info("Fetching today's prices from V2 providers")
        today_df = self.build_today_prices()

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

        # Stream-write JSON outputs
        output_path = MtgjsonConfig().output_path
        output_path.mkdir(parents=True, exist_ok=True)

        all_prices_path = output_path / "AllPrices.json"
        LOGGER.info(f"Streaming AllPrices.json to {all_prices_path}")
        self.stream_write_all_prices_json(archive_lf, all_prices_path)

        today_prices_path = output_path / "AllPricesToday.json"
        LOGGER.info(f"Streaming AllPricesToday.json to {today_prices_path}")
        self.stream_write_today_prices_json(today_df, today_prices_path)

        # Write SQL formats for AllPricesToday
        LOGGER.info("Writing AllPricesToday SQL formats")
        self.write_prices_sqlite(today_df, output_path / "AllPricesToday.sqlite")
        self.write_prices_sql(today_df, output_path / "AllPricesToday.sql")
        self.write_prices_psql(today_df, output_path / "AllPricesToday.psql")

        # Write SQL formats for AllPrices
        LOGGER.info("Writing AllPrices SQL formats")
        archive_df = archive_lf.collect()
        self.write_prices_sqlite(archive_df, output_path / "AllPrices.sqlite")
        self.write_prices_sql(archive_df, output_path / "AllPrices.sql")
        self.write_prices_psql(archive_df, output_path / "AllPrices.psql")

        LOGGER.info("Price build complete")
        return all_prices_path, today_prices_path

    def build_prices_legacy(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Legacy price build operation returning dicts.

        Downloads AllPrintings.json if missing, fetches today's prices from
        v2 providers, merges with archive (from S3 if configured), prunes
        old entries, and uploads the updated archive back to S3.

        Note: This method is kept for backwards compatibility but uses more
        memory than build_prices(). Prefer build_prices() for new code.

        Returns:
            Tuple of (all_prices_dict, today_prices_dict)
        """
        LOGGER.info("Polars Price Builder - Building Prices (V2 Legacy)")

        # Auto-download AllPrintings if needed
        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, attempting to download")
            self.download_old_all_printings()

        if not self.all_printings_path.is_file():
            LOGGER.error("Failed to get AllPrintings")
            return {}, {}

        # Fetch today's prices
        LOGGER.info("Fetching today's prices from V2 providers")
        today_df = self.build_today_prices()

        if len(today_df) == 0:
            LOGGER.warning("No price data generated")
            return {}, {}

        today_prices = self.to_nested_dict(today_df.lazy())

        # Load archive (try S3 first, then local)
        LOGGER.info("Loading price archive")
        archive_lf = self.get_price_archive_from_s3()

        # Merge and prune
        LOGGER.info("Merging archive with today's prices")
        merged_lf = self.merge_prices(archive_lf, today_df.lazy())

        LOGGER.info("Pruning old price data")
        pruned_lf = self.prune_prices(merged_lf)

        # Save locally as parquet
        LOGGER.info("Saving updated archive")
        self.save_archive(pruned_lf)

        self.upload_archive_to_s3(pruned_lf)

        LOGGER.info("Converting to nested dict for JSON output...")
        all_prices = self.to_nested_dict(pruned_lf)

        return all_prices, today_prices

    def build_prices_parquet(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Build prices returning DataFrames instead of dicts.

        More efficient when downstream consumers can work with DataFrames directly.

        Returns:
            Tuple of (all_prices_df, today_prices_df)
        """
        LOGGER.info("Polars Price Builder - Building Prices (Parquet mode, V2)")

        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, cannot build prices")
            empty = pl.DataFrame(schema=PRICE_SCHEMA)
            return empty, empty

        LOGGER.info("Fetching today's prices from V2 providers")
        today_df = self.build_today_prices()

        if len(today_df) == 0:
            LOGGER.warning("No price data generated")
            empty = pl.DataFrame(schema=PRICE_SCHEMA)
            return empty, empty

        LOGGER.info("Loading price archive")
        archive_lf = self.load_archive()

        LOGGER.info("Merging archive with today's prices")
        merged_lf = self.merge_prices(archive_lf, today_df.lazy())

        LOGGER.info("Pruning old price data")
        pruned_lf = self.prune_prices(merged_lf)

        LOGGER.info("Saving updated archive")
        self.save_archive(pruned_lf)

        return pruned_lf.collect(), today_df

    def load_archive_only(self) -> pl.LazyFrame:
        """
        Load existing archive without building new prices.

        Useful for analysis/inspection.

        Returns:
            LazyFrame with archive data
        """
        return self.load_archive()


def load_json_archive_to_parquet(json_path: Path, output_path: Path) -> pl.DataFrame:
    """
    Convert existing JSON price archive to parquet format.

    Utility for migrating from old format to new Polars-based storage.

    Args:
        json_path: Path to AllPrices.json or similar
        output_path: Path for output parquet file

    Returns:
        DataFrame with converted data
    """
    LOGGER.info(f"Converting {json_path} to parquet")

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    if "data" in data:
        data = data["data"]

    records: list[dict[str, Any]] = []

    for uuid, sources in data.items():
        if not isinstance(sources, dict):
            continue
        for source, providers in sources.items():
            if not isinstance(providers, dict):
                continue
            for provider, price_data in providers.items():
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
                                        "provider": provider,
                                        "price_type": price_type,
                                        "finish": finish,
                                        "price": float(price),
                                        "currency": currency,
                                    }
                                )

    df = pl.DataFrame(records, schema=PRICE_SCHEMA)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path, compression="zstd", compression_level=9)

    LOGGER.info(f"Converted {len(df):,} price records to {output_path}")
    return df
