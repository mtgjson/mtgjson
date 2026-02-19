"""
Polars-based Price Builder for MTGJSON v2.
"""

import asyncio
import datetime
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
    merge_prices,
    migrate_legacy_archive,
    prune_partitions,
    prune_prices,
    save_archive,
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
        """Build Scryfall ID -> UUID mapping from pipeline-derived cache."""
        if self._cache is None:
            return {}

        if self._cache.scryfall_to_uuid_lf is not None:
            df = self._cache.scryfall_to_uuid_lf.collect()
            if not df.is_empty():
                result: dict[str, set[str]] = {}
                for row in df.iter_rows(named=True):
                    scryfall_id = row.get("scryfallId")
                    uuid = row.get("uuid")
                    if scryfall_id and uuid:
                        if scryfall_id not in result:
                            result[scryfall_id] = set()
                        result[scryfall_id].add(uuid)
                return result

        if self._cache.uuid_cache_lf is not None:
            df = self._cache.uuid_cache_lf.collect()
            if not df.is_empty():
                result = {}
                for row in df.iter_rows(named=True):
                    scryfall_id = row.get("scryfallId")
                    uuid = row.get("cachedUuid")
                    if scryfall_id and uuid:
                        if scryfall_id not in result:
                            result[scryfall_id] = set()
                        result[scryfall_id].add(uuid)
                return result

        return {}


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
                LOGGER.info(f"  ManapoolPriceProvider: {len(manapool_df):,} price points")

        # Fetch CardMarket - bulk API
        LOGGER.info("Fetching CardMarket prices")
        mcm_dict = await self._mcm_provider.generate_today_price_dict(self.all_printings_path)
        if mcm_dict:
            mcm_df = self._prices_dict_to_dataframe(mcm_dict)
            if len(mcm_df) > 0:
                frames.append(mcm_df)
                LOGGER.info(f"  CardMarketProvider: {len(mcm_df):,} price points")
        await self._mcm_provider.close()

        # Fetch CardKingdom - async fetch, convert to DataFrame
        LOGGER.info("Fetching CardKingdom prices")
        try:
            await self._ck_provider.load_or_fetch_async(constants.CACHE_PATH / "ck_raw.parquet")
            ck_pricing_df = self._ck_provider.get_pricing_df()
            if len(ck_pricing_df) > 0:
                # Convert CK pricing df to flat price schema
                ck_records = self._convert_ck_pricing(ck_pricing_df, scryfall_to_uuid or {})
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

    def build_today_prices(self, ctx: PriceBuilderContext | None = None) -> pl.DataFrame:
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

    @staticmethod
    def prune_prices(df: pl.LazyFrame, months: int = 3) -> pl.LazyFrame:
        """Filter out price entries older than `months` months."""
        return prune_prices(df, months)

    @staticmethod
    def merge_prices(archive: pl.LazyFrame, today: pl.LazyFrame) -> pl.LazyFrame:
        """Merge today's prices into archive, keeping latest price per unique key."""
        return merge_prices(archive, today)

    def load_archive(self, path: Path | None = None) -> pl.LazyFrame:
        """Load price archive from parquet or JSON."""
        return load_archive(path, json_to_dataframe=self._json_to_dataframe)

    def save_archive(self, df: pl.LazyFrame, path: Path | None = None) -> Path:
        """Save price archive to parquet with zstd compression."""
        return save_archive(df, path)

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

    def stream_write_all_prices_json(self, lf: pl.LazyFrame, path: Path) -> None:
        """Stream-write AllPrices.json using Prefix Partitioning."""
        stream_write_all_prices_json(lf, path, self.today_date)

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
            df.group_by(["uuid", "source", "provider", "currency", "price_type", "finish"])
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

        # Write CSV format for cardPrices (goes in csv/ directory with other CSVs)
        csv_dir = output_path / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Writing cardPrices.csv")
        self.write_prices_csv(today_df, csv_dir / "cardPrices.csv")

        LOGGER.info("Price build complete")
        return all_prices_path, today_prices_path

    def build_prices_parquet(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Build prices returning DataFrames instead of dicts.

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
        """Load existing archive without building new prices."""
        return self.load_archive()
