"""
TCGPlayer v2 async pricing provider.

Key differences from legacy:
- NO buylist API calls (deprecated endpoint)
- Async with streaming and checkpointing
- Progress callbacks for monitoring
- Uses existing TcgPlayerClient for auth/connection pooling
"""

import asyncio
import contextlib
import datetime
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import polars as pl

from mtgjson5 import constants
from mtgjson5.v2.models.containers import MtgjsonPriceEntry

from .client import TcgPlayerClient
from .models import ProgressCallback
from .provider import TcgPlayerConfig

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


@dataclass
class TCGPlayerPriceProvider:
    """
    Async TCGPlayer pricing with streaming and checkpointing.

    Features:
    - Fetches RETAIL prices only (no buylist - deprecated API)
    - Streams results with periodic checkpointing
    - Progress callbacks for monitoring long fetches
    - Resumes from checkpoint on restart

    Usage:
        provider = TCGPlayerPriceProvider()
        prices_df = await provider.fetch_all_prices(tcg_to_uuid_map, tcg_etched_map)
    """

    output_path: Path | None = None
    checkpoint_interval: int = 50
    on_progress: ProgressCallback | None = None
    today_date: str = field(default_factory=lambda: datetime.date.today().strftime("%Y-%m-%d"))

    # Internal state
    _config: TcgPlayerConfig | None = field(default=None, repr=False)
    _buffer: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _checkpoint_path: Path | None = field(default=None, repr=False)
    _completed_groups: set[int] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        if self.output_path is None:
            self.output_path = constants.CACHE_PATH / "tcg_prices.parquet"
        self._checkpoint_path = self.output_path.parent / ".tcg_price_checkpoint.json"
        self._config = TcgPlayerConfig.from_mtgjson_config()

    async def fetch_all_prices(
        self,
        tcg_to_uuid_map: dict[str, set[str]],
        tcg_etched_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """
        Fetch retail prices for all TCGPlayer Magic sets.

        Args:
            tcg_to_uuid_map: TCGPlayer productId -> MTGJSON UUIDs mapping
            tcg_etched_to_uuid_map: TCGPlayer etched productId -> MTGJSON UUIDs

        Returns:
            DataFrame with flat price records
        """
        if not self._config:
            LOGGER.warning("No TCGPlayer config available, skipping pricing")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        # Load checkpoint
        self._load_checkpoint()

        async with TcgPlayerClient(self._config) as client:
            # Get all Magic set IDs
            group_ids = await self._get_magic_set_ids(client)
            total = len(group_ids)

            if not group_ids:
                LOGGER.warning("No TCGPlayer Magic sets found")
                return pl.DataFrame(schema=PRICE_SCHEMA)

            LOGGER.info(f"TCGPlayer: Fetching prices for {total} sets")

            for idx, (group_id, group_name) in enumerate(group_ids, 1):
                if group_id in self._completed_groups:
                    if self.on_progress:
                        self.on_progress(idx, total, f"{group_name} (cached)")
                    continue

                # Fetch retail prices for this group
                prices = await self._fetch_group_prices(client, group_id, tcg_to_uuid_map, tcg_etched_to_uuid_map)
                self._buffer.extend(prices)
                self._completed_groups.add(group_id)

                # Progress callback
                if self.on_progress:
                    self.on_progress(idx, total, group_name)

                # Periodic checkpoint
                if idx % self.checkpoint_interval == 0:
                    self._save_checkpoint()
                    LOGGER.info(f"TCGPlayer: {idx}/{total} sets ({idx * 100 // total}%)")

        # Final save and cleanup
        result = self._finalize()
        self._cleanup_checkpoint()
        return result

    def fetch_all_prices_sync(
        self,
        tcg_to_uuid_map: dict[str, set[str]],
        tcg_etched_to_uuid_map: dict[str, set[str]],
    ) -> pl.DataFrame:
        """Sync wrapper for fetch_all_prices."""
        return asyncio.run(self.fetch_all_prices(tcg_to_uuid_map, tcg_etched_to_uuid_map))

    async def _get_magic_set_ids(self, client: TcgPlayerClient) -> list[tuple[int, str]]:
        """
        Get all TCGPlayer Magic set IDs and names.

        Returns list of (group_id, group_name) tuples.
        """
        group_ids: list[tuple[int, str]] = []
        offset = 0

        while True:
            endpoint = f"catalog/categories/1/groups?offset={offset}&limit=100"
            try:
                resp = await client.get(endpoint, versioned=False)
                results = cast("list[dict[str, Any]]", resp.get("results", []))
                if not results:
                    break

                for group in results:
                    gid = group.get("groupId")
                    name = group.get("name", "Unknown")
                    if gid is not None:
                        group_ids.append((int(gid), str(name)))

                offset += len(results)
                if len(results) < 100:
                    break

            except Exception as e:
                LOGGER.error(f"Failed to fetch TCGPlayer groups at offset {offset}: {e}")
                break

        LOGGER.info(f"Found {len(group_ids)} TCGPlayer Magic sets")
        return group_ids

    async def _fetch_group_prices(
        self,
        client: TcgPlayerClient,
        group_id: int,
        tcg_to_uuid_map: dict[str, set[str]],
        tcg_etched_to_uuid_map: dict[str, set[str]],
    ) -> list[dict[str, Any]]:
        """
        Fetch RETAIL prices for a single set.

        Note: Buylist endpoint (/pricing/buy/group/) is deprecated and not called.
        """
        records: list[dict[str, Any]] = []

        try:
            endpoint = f"pricing/group/{group_id}"
            resp = await client.get(endpoint, versioned=True)
            results = cast("list[dict[str, Any]]", resp.get("results", []))

            for price_obj in results:
                if not isinstance(price_obj, dict):
                    continue

                product_id = str(price_obj.get("productId", ""))
                sub_type = price_obj.get("subTypeName", "")
                market_price = price_obj.get("marketPrice")

                if market_price is None:
                    continue

                # Determine which mapping to use
                is_etched = False
                uuids = tcg_to_uuid_map.get(product_id)
                if not uuids:
                    uuids = tcg_etched_to_uuid_map.get(product_id)
                    is_etched = bool(uuids)
                if not uuids:
                    continue

                # Determine finish type
                is_normal = sub_type == "Normal"
                if is_normal:
                    finish = "normal"
                elif is_etched:
                    finish = "etched"
                else:
                    finish = "foil"

                # Create price record for each UUID
                for uuid in uuids:
                    records.append(
                        {
                            "uuid": uuid,
                            "date": self.today_date,
                            "source": "paper",
                            "provider": "tcgplayer",
                            "price_type": "retail",
                            "finish": finish,
                            "price": float(market_price),
                            "currency": "USD",
                        }
                    )

        except Exception as e:
            LOGGER.debug(f"Failed to fetch prices for group {group_id}: {e}")

        return records

    def _load_checkpoint(self) -> None:
        """Load checkpoint data if exists."""
        if self._checkpoint_path and self._checkpoint_path.exists():
            try:
                with self._checkpoint_path.open() as f:
                    data = json.load(f)
                self._completed_groups = set(data.get("completed_groups", []))
                LOGGER.info(f"Resumed from checkpoint: {len(self._completed_groups)} groups done")
            except Exception as e:
                LOGGER.warning(f"Could not load checkpoint: {e}")
                self._completed_groups = set()

    def _save_checkpoint(self) -> None:
        """Save checkpoint data."""
        if not self._checkpoint_path:
            return

        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            data = {"completed_groups": list(self._completed_groups)}
            with self._checkpoint_path.open("w") as f:
                json.dump(data, f)
        except Exception as e:
            LOGGER.warning(f"Could not save checkpoint: {e}")

    def _cleanup_checkpoint(self) -> None:
        """Remove checkpoint file after successful completion."""
        if self._checkpoint_path and self._checkpoint_path.exists():
            with contextlib.suppress(Exception):
                self._checkpoint_path.unlink()

    def _finalize(self) -> pl.DataFrame:
        """Convert buffer to DataFrame and optionally save."""
        if not self._buffer:
            return pl.DataFrame(schema=PRICE_SCHEMA)

        df = pl.DataFrame(self._buffer, schema=PRICE_SCHEMA)

        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.output_path, compression="zstd")
            LOGGER.info(f"Saved {len(df):,} TCGPlayer price records to {self.output_path}")

        return df

    async def generate_today_price_dict(
        self,
        tcg_to_uuid_map: dict[str, set[str]],
        tcg_etched_to_uuid_map: dict[str, set[str]],
    ) -> dict[str, MtgjsonPriceEntry]:
        """
        Generate MTGJSON-format price dict for compatibility with legacy code.

        Returns dict mapping UUID -> MtgjsonPriceEntry.
        """
        df = await self.fetch_all_prices(tcg_to_uuid_map, tcg_etched_to_uuid_map)
        return self._dataframe_to_price_dict(df)

    def _dataframe_to_price_dict(self, df: pl.DataFrame) -> dict[str, MtgjsonPriceEntry]:
        """Convert DataFrame to MTGJSON price dict format."""
        result: dict[str, MtgjsonPriceEntry] = {}

        for row in df.iter_rows(named=True):
            uuid = row["uuid"]
            finish = row["finish"]
            price = row["price"]

            if uuid not in result:
                result[uuid] = MtgjsonPriceEntry("paper", "tcgplayer", self.today_date, "USD")

            prices_obj = result[uuid]
            # Only retail prices (no buylist in v2)
            if finish == "normal":
                prices_obj.sell_normal = price
            elif finish == "foil":
                prices_obj.sell_foil = price
            elif finish == "etched":
                prices_obj.sell_etched = price

        return result


# Convenience functions


async def get_tcgplayer_prices(
    tcg_to_uuid_map: dict[str, set[str]],
    tcg_etched_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """
    Fetch TCGPlayer prices as DataFrame.

    Args:
        tcg_to_uuid_map: TCGPlayer productId -> UUID mapping
        tcg_etched_to_uuid_map: TCGPlayer etched productId -> UUID mapping
        on_progress: Optional callback for progress updates

    Returns:
        DataFrame with flat price records
    """
    provider = TCGPlayerPriceProvider(on_progress=on_progress)
    return await provider.fetch_all_prices(tcg_to_uuid_map, tcg_etched_to_uuid_map)


def get_tcgplayer_prices_sync(
    tcg_to_uuid_map: dict[str, set[str]],
    tcg_etched_to_uuid_map: dict[str, set[str]],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """Sync wrapper for get_tcgplayer_prices."""
    return asyncio.run(get_tcgplayer_prices(tcg_to_uuid_map, tcg_etched_to_uuid_map, on_progress))
