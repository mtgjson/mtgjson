"""
CardHoarder v2 async price provider.

Fetches MTGO prices from CardHoarder affiliate pricefile endpoints.
Simple bulk fetch - 2 URLs (normal + foil TSV files).
"""

import asyncio
import datetime
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonPricesObject
from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

# Price schema for flat storage
PRICE_SCHEMA = {
    "uuid": pl.String,
    "date": pl.String,
    "source": pl.String,
    "provider": pl.String,
    "price_type": pl.String,
    "finish": pl.String,
    "price": pl.Float64,
    "currency": pl.String,
}

ProgressCallback = Callable[[int, int, str], None]


@dataclass
class CardHoarderConfig:
    """CardHoarder API configuration."""

    token: str
    base_url: str = "https://www.cardhoarder.com/affiliates/pricefile"

    @classmethod
    def from_mtgjson_config(cls) -> "CardHoarderConfig | None":
        """Load config from mtgjson.properties."""
        config = MtgjsonConfig()
        if not config.has_section("CardHoarder"):
            return None

        token = config.get("CardHoarder", "token", fallback="")
        if not token:
            return None

        return cls(token=token)

    @property
    def normal_url(self) -> str:
        """URL for normal cards pricefile."""
        return f"{self.base_url}/{self.token}"

    @property
    def foil_url(self) -> str:
        """URL for foil cards pricefile."""
        return f"{self.base_url}/{self.token}/foil"


@dataclass
class CardHoarderPriceProvider:
    """
    Async CardHoarder pricing provider.

    Fetches MTGO prices from CardHoarder affiliate pricefile endpoints.
    Simple bulk fetch with 2 parallel requests (normal + foil).

    Usage:
        provider = CardHoarderPriceProvider()
        prices_df = await provider.fetch_prices(mtgo_to_uuid_map)
    """

    output_path: Path | None = None
    on_progress: ProgressCallback | None = None
    today_date: str = field(
        default_factory=lambda: datetime.date.today().strftime("%Y-%m-%d")
    )

    # Internal state
    _config: CardHoarderConfig | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.output_path is None:
            self.output_path = constants.CACHE_PATH / "cardhoarder_prices.parquet"
        self._config = CardHoarderConfig.from_mtgjson_config()

    async def fetch_prices(
        self,
        mtgo_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """
        Fetch MTGO prices from CardHoarder (normal + foil in parallel).

        Args:
            mtgo_to_uuid_map: MTGO ID -> MTGJSON UUIDs mapping

        Returns:
            DataFrame with flat price records
        """
        if not self._config:
            LOGGER.warning("No CardHoarder config available, skipping pricing")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        LOGGER.info("CardHoarder: Fetching MTGO prices")

        async with aiohttp.ClientSession() as session:
            # Fetch normal and foil in parallel
            normal_task = self._fetch_pricefile(
                session, self._config.normal_url, mtgo_to_uuid_map, is_foil=False
            )
            foil_task = self._fetch_pricefile(
                session, self._config.foil_url, mtgo_to_uuid_map, is_foil=True
            )

            normal_records, foil_records = await asyncio.gather(normal_task, foil_task)

        # Combine results
        all_records = normal_records + foil_records

        if not all_records:
            LOGGER.warning("No CardHoarder price data retrieved")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        df = pl.DataFrame(all_records, schema=PRICE_SCHEMA)

        # Save to output path
        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.output_path, compression="zstd")
            LOGGER.info(
                f"Saved {len(df):,} CardHoarder price records to {self.output_path}"
            )

        if self.on_progress:
            self.on_progress(2, 2, "CardHoarder complete")

        return df

    def fetch_prices_sync(
        self,
        mtgo_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """Sync wrapper for fetch_prices."""
        return asyncio.run(self.fetch_prices(mtgo_to_uuid_map))

    async def _fetch_pricefile(
        self,
        session: aiohttp.ClientSession,
        url: str,
        mtgo_to_uuid_map: dict[str, set[str]],
        is_foil: bool,
    ) -> list[dict[str, Any]]:
        """
        Fetch and parse a CardHoarder pricefile.

        TSV format (header on line 1-2):
            MTGO_ID | NAME | SET | ... | PRICE | ...

        Args:
            session: aiohttp session
            url: Pricefile URL
            mtgo_to_uuid_map: MTGO ID -> UUIDs mapping
            is_foil: Whether this is the foil pricefile

        Returns:
            List of price record dicts
        """
        records: list[dict[str, Any]] = []
        finish = "foil" if is_foil else "normal"

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                resp.raise_for_status()
                content = await resp.text()

            # Parse TSV - skip first 2 header lines
            lines = content.splitlines()
            if len(lines) <= 2:
                LOGGER.warning(f"CardHoarder pricefile empty or invalid: {url}")
                return []

            data_lines = lines[2:]  # Skip header lines
            invalid_count = 0

            for line in data_lines:
                columns = line.split("\t")
                if len(columns) < 6:
                    invalid_count += 1
                    continue

                # Column 0 = MTGO ID, Column 5 = Price
                mtgo_id = columns[0].strip('"')
                try:
                    price = float(columns[5].strip('"'))
                except (ValueError, IndexError):
                    invalid_count += 1
                    continue

                uuids = mtgo_to_uuid_map.get(mtgo_id)
                if not uuids:
                    continue

                # Create record for each UUID
                for uuid in uuids:
                    records.append(
                        {
                            "uuid": uuid,
                            "date": self.today_date,
                            "source": "mtgo",
                            "provider": "cardhoarder",
                            "price_type": "retail",
                            "finish": finish,
                            "price": price,
                            "currency": "USD",
                        }
                    )

            if invalid_count > 0:
                LOGGER.debug(
                    f"CardHoarder: {invalid_count} unmapped/invalid entries ({finish})"
                )

            LOGGER.info(
                f"CardHoarder: Parsed {len(records):,} {finish} price records"
            )

        except aiohttp.ClientError as e:
            LOGGER.error(f"Failed to fetch CardHoarder pricefile ({finish}): {e}")
        except Exception as e:
            LOGGER.error(f"Error parsing CardHoarder pricefile ({finish}): {e}")

        return records

    async def generate_today_price_dict(
        self,
        mtgo_to_uuid_map: dict[str, set[str]],
    ) -> dict[str, MtgjsonPricesObject]:
        """
        Generate MTGJSON-format price dict for compatibility with legacy code.

        Returns dict mapping UUID -> MtgjsonPricesObject.
        """
        df = await self.fetch_prices(mtgo_to_uuid_map)
        return self._dataframe_to_price_dict(df)

    def _dataframe_to_price_dict(
        self, df: pl.DataFrame
    ) -> dict[str, MtgjsonPricesObject]:
        """Convert DataFrame to MTGJSON price dict format."""
        result: dict[str, MtgjsonPricesObject] = {}

        for row in df.iter_rows(named=True):
            uuid = row["uuid"]
            finish = row["finish"]
            price = row["price"]

            if uuid not in result:
                result[uuid] = MtgjsonPricesObject(
                    "mtgo", "cardhoarder", self.today_date, "USD"
                )

            prices_obj = result[uuid]
            if finish == "normal":
                prices_obj.sell_normal = price
            elif finish == "foil":
                prices_obj.sell_foil = price

        return result


# Convenience functions


async def get_cardhoarder_prices(
    mtgo_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """
    Fetch CardHoarder MTGO prices as DataFrame.

    Args:
        mtgo_to_uuid_map: MTGO ID -> UUID mapping
        on_progress: Optional callback for progress updates

    Returns:
        DataFrame with flat price records
    """
    provider = CardHoarderPriceProvider(on_progress=on_progress)
    return await provider.fetch_prices(mtgo_to_uuid_map)


def get_cardhoarder_prices_sync(
    mtgo_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """Sync wrapper for get_cardhoarder_prices."""
    return asyncio.run(get_cardhoarder_prices(mtgo_to_uuid_map, on_progress))
