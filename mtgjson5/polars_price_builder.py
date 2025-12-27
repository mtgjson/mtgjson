"""
Polars-based Price Builder for MTGJSON.

Uses a flat tabular schema for efficient storage, filtering, and merging:
    uuid | date | source | provider | price_type | finish | price | currency

Benefits over nested dict approach:
    - Fast date-based pruning via Polars filtering
    - Efficient merging with concat + group_by
    - Parquet storage with zstd compression (~10x smaller than JSON)
    - Memory efficient lazy evaluation
"""

import datetime
import logging
from pathlib import Path
from typing import Any

import dateutil.relativedelta
import polars as pl

from .mtgjson_config import MtgjsonConfig
from .providers import (
    CardHoarderProvider,
    CardKingdomProvider,
    CardMarketProvider,
    ManapoolPricesProvider,
    MultiverseBridgeProvider,
    TCGPlayerProvider,
)
from .providers.abstract import AbstractProvider


LOGGER = logging.getLogger(__name__)


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


class PolarsPriceBuilder:
    """
    Build daily prices using Polars DataFrames.

    Price data is stored in a flat tabular format optimized for:
    - Fast date-based filtering (pruning old entries)
    - Efficient merging of new price data
    - Compact parquet storage
    """

    providers: list[AbstractProvider]
    all_printings_path: Path
    today_date: str

    def __init__(
        self,
        *providers: AbstractProvider,
        all_printings_path: Path | None = None,
    ) -> None:
        if providers:
            self.providers = list(providers)
        else:
            self.providers = [
                CardHoarderProvider(),
                TCGPlayerProvider(),
                CardMarketProvider(),
                CardKingdomProvider(),
                MultiverseBridgeProvider(),
                ManapoolPricesProvider(),
            ]

        self.all_printings_path = (
            all_printings_path
            if all_printings_path
            else MtgjsonConfig().output_path.joinpath("AllPrintings.json")
        )
        self.today_date = datetime.date.today().strftime("%Y-%m-%d")

    def build_today_prices(self) -> pl.DataFrame:
        """
        Fetch today's prices from all providers and return as DataFrame.

        Returns:
            DataFrame with columns: uuid, date, source, provider, price_type, finish, price, currency
        """
        if not self.all_printings_path.is_file():
            LOGGER.error(
                f"Unable to build prices. AllPrintings not found in {MtgjsonConfig().output_path}"
            )
            return pl.DataFrame(schema=PRICE_SCHEMA)

        frames: list[pl.DataFrame] = []
        for provider in self.providers:
            df = self._fetch_provider_prices(provider)
            if len(df) > 0:
                frames.append(df)
                LOGGER.info(f"  {provider.__class__.__name__}: {len(df):,} price points")

        if not frames:
            LOGGER.warning("No price data collected from any provider")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        return pl.concat(frames)

    def _fetch_provider_prices(self, provider: AbstractProvider) -> pl.DataFrame:
        """
        Fetch prices from a single provider and convert to DataFrame.

        Args:
            provider: Price provider instance

        Returns:
            DataFrame with flat price records
        """
        try:
            price_dict = provider.generate_today_price_dict(self.all_printings_path)
            return self._prices_dict_to_dataframe(price_dict)
        except Exception as e:
            LOGGER.error(f"Failed to fetch prices from {provider.__class__.__name__}: {e}")
            return pl.DataFrame(schema=PRICE_SCHEMA)

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
                                    records.append({
                                        "uuid": uuid,
                                        "date": date,
                                        "source": source,
                                        "provider": provider_name,
                                        "price_type": price_type,
                                        "finish": finish,
                                        "price": float(price),
                                        "currency": currency,
                                    })

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

        return combined.group_by(key_cols).agg([
            pl.col("price").last(),
            pl.col("currency").last(),
        ])

    def load_archive(self, path: Path | None = None) -> pl.LazyFrame:
        """
        Load price archive from parquet or JSON.

        Args:
            path: Path to archive file (default: cache dir)

        Returns:
            LazyFrame with archive data
        """
        cache_dir = Path(r"C:\Users\rprat\projects\mtgjson-projects\mtgjson-v5.worktrees\master\.mtgjson5_cache")
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
            import json
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
                                    records.append({
                                        "uuid": uuid,
                                        "date": date,
                                        "source": source,
                                        "provider": provider,
                                        "price_type": price_type,
                                        "finish": finish,
                                        "price": float(price),
                                        "currency": currency,
                                    })

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
            cache_dir = Path(r"C:\Users\rprat\projects\mtgjson-projects\mtgjson-v5.worktrees\master\.mtgjson5_cache")
            path = cache_dir / "prices_archive.parquet"

        path.parent.mkdir(parents=True, exist_ok=True)

        collected = df.collect()
        collected.write_parquet(path, compression="zstd", compression_level=9)

        LOGGER.info(f"Saved price archive: {len(collected):,} rows, {path.stat().st_size:,} bytes")
        return path

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
        Full price build operation matching original PriceBuilder interface.

        Returns:
            Tuple of (all_prices_dict, today_prices_dict) for backwards compatibility
        """
        LOGGER.info("Polars Price Builder - Building Prices")

        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, cannot build prices")
            return {}, {}

        LOGGER.info("Fetching today's prices from providers")
        today_df = self.build_today_prices()
        if len(today_df) == 0:
            LOGGER.warning("No price data generated")
            return {}, {}

        today_prices = self.to_nested_dict(today_df.lazy())

        LOGGER.info("Loading price archive")
        archive_lf = self.load_archive()

        LOGGER.info("Merging archive with today's prices")
        merged_lf = self.merge_prices(archive_lf, today_df.lazy())

        LOGGER.info("Pruning old price data")
        pruned_lf = self.prune_prices(merged_lf)

        LOGGER.info("Saving updated archive")
        self.save_archive(pruned_lf)

        all_prices = self.to_nested_dict(pruned_lf)

        return all_prices, today_prices

    def build_prices_parquet(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Build prices returning DataFrames instead of dicts.

        More efficient when downstream consumers can work with DataFrames directly.

        Returns:
            Tuple of (all_prices_df, today_prices_df)
        """
        LOGGER.info("Polars Price Builder - Building Prices (Parquet mode)")

        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, cannot build prices")
            empty = pl.DataFrame(schema=PRICE_SCHEMA)
            return empty, empty

        LOGGER.info("Fetching today's prices from providers")
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
    import json

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
                                records.append({
                                    "uuid": uuid,
                                    "date": date,
                                    "source": source,
                                    "provider": provider,
                                    "price_type": price_type,
                                    "finish": finish,
                                    "price": float(price),
                                    "currency": currency,
                                })

    df = pl.DataFrame(records, schema=PRICE_SCHEMA)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path, compression="zstd", compression_level=9)

    LOGGER.info(f"Converted {len(df):,} price records to {output_path}")
    return df
