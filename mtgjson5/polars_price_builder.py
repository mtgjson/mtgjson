"""
Polars-based Price Builder for MTGJSON.

Uses a flat tabular schema for efficient storage, filtering, and merging:
    uuid | date | source | provider | price_type | finish | price | currency

Benefits over nested dict approach:
    - Fast date-based pruning via Polars filtering
    - Efficient merging with concat + group_by
    - Parquet storage with zstd compression (~10x smaller than JSON)
    - Memory efficient lazy evaluation

V2 Providers (async, streaming):
    - TCGPlayerPriceProvider (retail only, no buylist)
    - CardHoarderPriceProvider (MTGO)
    - ManapoolPriceProvider (paper)
    - CardMarketProvider (paper, EUR)
    - CKProvider (paper)
"""

import asyncio
import datetime
import json
import logging
import lzma
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dateutil.relativedelta
import polars as pl
import requests

from . import constants
from .mtgjson_config import MtgjsonConfig
from .mtgjson_s3_handler import MtgjsonS3Handler
from .providers.v2 import (
    CardHoarderPriceProvider,
    CardMarketProvider,
    CKProvider,
    ManapoolPriceProvider,
    TCGPlayerPriceProvider,
)

if TYPE_CHECKING:
    from .cache import GlobalCache

LOGGER = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, str], None]


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
        """Create context from global cache."""
        from .cache import GLOBAL_CACHE

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
                LOGGER.info(f"  ManapoolPriceProvider: {len(manapool_df):,} price points")

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

        # Fetch CardKingdom - sync fetch, convert to DataFrame
        LOGGER.info("Fetching CardKingdom prices")
        try:
            self._ck_provider.load_or_fetch(constants.CACHE_PATH / "ck_prices.parquet")
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
            import orjson

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

    def get_price_archive_from_s3(self) -> pl.LazyFrame:
        """
        Download price archive from S3 and convert to LazyFrame.

        Falls back to local archive if S3 config is missing or download fails.

        Returns:
            LazyFrame with archive data
        """
        if not MtgjsonConfig().has_section("Prices"):
            LOGGER.info("No S3 config, using local archive only")
            return self.load_archive()

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        # Download compressed archive
        constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)
        temp_file = constants.CACHE_PATH / "temp_prices.tar.xz"

        LOGGER.info("Downloading price archive from S3")
        if not MtgjsonS3Handler().download_file(
            bucket_name, bucket_object_path, str(temp_file)
        ):
            LOGGER.warning("S3 download failed, using local archive")
            return self.load_archive()

        # Decompress and convert to DataFrame
        try:
            with lzma.open(temp_file) as f:
                data = json.load(f)
            temp_file.unlink()

            df = self._json_to_dataframe(data.get("data", data))
            LOGGER.info(f"Loaded {len(df):,} price records from S3 archive")
            return df.lazy()
        except Exception as e:
            LOGGER.error(f"Failed to process S3 archive: {e}")
            if temp_file.exists():
                temp_file.unlink()
            return self.load_archive()

    def upload_archive_to_s3(self, archive_dict: dict[str, Any]) -> None:
        """
        Upload price archive to S3.

        Compresses the archive with xz before uploading.

        Args:
            archive_dict: Price data dict to upload
        """
        if not MtgjsonConfig().has_section("Prices"):
            LOGGER.info("No S3 config, skipping upload")
            return

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        # Write to temp JSON, compress with xz
        local_path = constants.CACHE_PATH / bucket_object_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.parent / local_path.stem

        LOGGER.info(f"Writing price data to {tmp_path}")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(archive_dict, f)

        LOGGER.info(f"Compressing {tmp_path} for upload")
        subprocess.check_call(["xz", str(tmp_path)])

        # Upload
        LOGGER.info("Uploading price archive to S3")
        MtgjsonS3Handler().upload_file(str(local_path), bucket_name, bucket_object_path)
        local_path.unlink()

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
        result: dict[str, Any] = {}

        for row in df.collect().iter_rows(named=True):
            uuid = row["uuid"]
            source = row["source"]
            provider = row["provider"]
            price_type = row["price_type"]
            finish = row["finish"]
            date = row["date"]
            price = row["price"]
            currency = row["currency"]

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

            provider_data = result[uuid][source][provider]
            if finish not in provider_data[price_type]:
                provider_data[price_type][finish] = {}

            provider_data[price_type][finish][date] = price

        return result

    def build_prices(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Full price build operation.

        Downloads AllPrintings.json if missing, fetches today's prices from
        v2 providers, merges with archive (from S3 if configured), prunes
        old entries, and uploads the updated archive back to S3.

        Returns:
            Tuple of (all_prices_dict, today_prices_dict)
        """
        LOGGER.info("Polars Price Builder - Building Prices (V2)")

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

        # Convert to nested dict for output
        all_prices = self.to_nested_dict(pruned_lf)

        # Upload to S3
        self.upload_archive_to_s3(all_prices)

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
