"""
Manapool v2 async price provider.

Fetches paper prices from Manapool API.
Simple bulk fetch - single endpoint for all prices.
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
from mtgjson5.models.containers import MtgjsonPriceEntry

LOGGER = logging.getLogger(__name__)

# Manapool API endpoint
MANAPOOL_API_URL = "https://manapool.com/api/v1/prices/singles"

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
class ManapoolPriceProvider:
    """
    Async Manapool pricing provider.

    Fetches paper prices from Manapool API (single bulk endpoint).
    Maps scryfall_id -> MTGJSON UUID.

    API Response format:
        {
            "data": [
                {
                    "scryfall_id": "...",
                    "price_cents": 100,
                    "price_cents_foil": 200,
                    "price_cents_etched": 150
                },
                ...
            ]
        }

    Usage:
        provider = ManapoolPriceProvider()
        prices_df = await provider.fetch_prices(scryfall_to_uuid_map)
    """

    output_path: Path | None = None
    on_progress: ProgressCallback | None = None
    today_date: str = field(default_factory=lambda: datetime.date.today().strftime("%Y-%m-%d"))

    def __post_init__(self) -> None:
        if self.output_path is None:
            self.output_path = constants.CACHE_PATH / "manapool_prices.parquet"

    async def fetch_prices(
        self,
        scryfall_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """
        Fetch paper prices from Manapool API.

        Args:
            scryfall_to_uuid_map: Scryfall ID -> MTGJSON UUIDs mapping

        Returns:
            DataFrame with flat price records
        """
        LOGGER.info("Manapool: Fetching paper prices")

        if self.on_progress:
            self.on_progress(0, 1, "Fetching Manapool prices")

        records: list[dict[str, Any]] = []

        try:
            async with aiohttp.ClientSession() as session:  # noqa: SIM117
                async with session.get(MANAPOOL_API_URL, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            # Parse response
            price_data = data.get("data", [])
            if not price_data:
                LOGGER.warning("Manapool API returned empty data")
                return pl.DataFrame(schema=PRICE_SCHEMA)

            unmapped_count = 0
            for card in price_data:
                scryfall_id = card.get("scryfall_id")
                if not scryfall_id:
                    continue

                uuids = scryfall_to_uuid_map.get(scryfall_id)
                if not uuids:
                    unmapped_count += 1
                    continue

                # Extract prices (in cents, convert to dollars)
                normal_cents = card.get("price_cents")
                foil_cents = card.get("price_cents_foil")
                etched_cents = card.get("price_cents_etched")

                # Create records for each UUID and each finish with a price
                for uuid in uuids:
                    if normal_cents:
                        records.append(
                            {
                                "uuid": uuid,
                                "date": self.today_date,
                                "source": "paper",
                                "provider": "manapool",
                                "price_type": "retail",
                                "finish": "normal",
                                "price": normal_cents / 100.0,
                                "currency": "USD",
                            }
                        )

                    if foil_cents:
                        records.append(
                            {
                                "uuid": uuid,
                                "date": self.today_date,
                                "source": "paper",
                                "provider": "manapool",
                                "price_type": "retail",
                                "finish": "foil",
                                "price": foil_cents / 100.0,
                                "currency": "USD",
                            }
                        )

                    if etched_cents:
                        records.append(
                            {
                                "uuid": uuid,
                                "date": self.today_date,
                                "source": "paper",
                                "provider": "manapool",
                                "price_type": "retail",
                                "finish": "etched",
                                "price": etched_cents / 100.0,
                                "currency": "USD",
                            }
                        )

            if unmapped_count > 0:
                LOGGER.debug(f"Manapool: {unmapped_count} scryfall IDs not mapped")

        except aiohttp.ClientError as e:
            LOGGER.error(f"Failed to fetch Manapool prices: {e}")
            return pl.DataFrame(schema=PRICE_SCHEMA)
        except Exception as e:
            LOGGER.error(f"Error processing Manapool data: {e}")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        if not records:
            LOGGER.warning("No Manapool price data retrieved")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        df = pl.DataFrame(records, schema=PRICE_SCHEMA)

        # Save to output path
        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.output_path, compression="zstd")
            LOGGER.info(f"Saved {len(df):,} Manapool price records to {self.output_path}")

        if self.on_progress:
            self.on_progress(1, 1, "Manapool complete")

        LOGGER.info(f"Manapool: Fetched {len(df):,} price records")
        return df

    def fetch_prices_sync(
        self,
        scryfall_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """Sync wrapper for fetch_prices."""
        return asyncio.run(self.fetch_prices(scryfall_to_uuid_map))

    async def generate_today_price_dict(
        self,
        scryfall_to_uuid_map: dict[str, set[str]],
    ) -> dict[str, MtgjsonPriceEntry]:
        """
        Generate MTGJSON-format price dict for compatibility with legacy code.

        Returns dict mapping UUID -> MtgjsonPriceEntry.
        """
        df = await self.fetch_prices(scryfall_to_uuid_map)
        return self._dataframe_to_price_dict(df)

    def _dataframe_to_price_dict(self, df: pl.DataFrame) -> dict[str, MtgjsonPriceEntry]:
        """Convert DataFrame to MTGJSON price dict format."""
        result: dict[str, MtgjsonPriceEntry] = {}

        for row in df.iter_rows(named=True):
            uuid = row["uuid"]
            finish = row["finish"]
            price = row["price"]

            if uuid not in result:
                result[uuid] = MtgjsonPriceEntry("paper", "manapool", self.today_date, "USD")

            prices_obj = result[uuid]
            if finish == "normal":
                prices_obj.sell_normal = price
            elif finish == "foil":
                prices_obj.sell_foil = price
            elif finish == "etched":
                prices_obj.sell_etched = price

        return result


# Convenience functions


async def get_manapool_prices(
    scryfall_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """
    Fetch Manapool paper prices as DataFrame.

    Args:
        scryfall_to_uuid_map: Scryfall ID -> UUID mapping
        on_progress: Optional callback for progress updates

    Returns:
        DataFrame with flat price records
    """
    provider = ManapoolPriceProvider(on_progress=on_progress)
    return await provider.fetch_prices(scryfall_to_uuid_map)


def get_manapool_prices_sync(
    scryfall_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """Sync wrapper for get_manapool_prices."""
    return asyncio.run(get_manapool_prices(scryfall_to_uuid_map, on_progress))
