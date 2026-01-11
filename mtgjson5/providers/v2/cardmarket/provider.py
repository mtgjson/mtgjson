"""
CardMarket (MKM) Provider - Async implementation.
"""

import asyncio
import datetime
import json
import logging
import os
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import mkmsdk.exceptions
import polars as pl
from mkmsdk.api_map import _API_MAP
from mkmsdk.mkm import Mkm

from mtgjson5.classes import MtgjsonPricesObject
from mtgjson5.constants import CACHE_PATH, RESOURCE_PATH
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import generate_entity_mapping


LOGGER = logging.getLogger(__name__)


@dataclass
class CardMarketConfig:
    """CardMarket API credentials."""

    app_token: str
    app_secret: str
    access_token: str = ""
    access_token_secret: str = ""
    prices_api_url: str = ""

    @classmethod
    def from_mtgjson_config(cls) -> "CardMarketConfig | None":
        config = MtgjsonConfig()
        if not config.has_section("CardMarket"):
            return None

        app_token = config.get("CardMarket", "app_token", fallback="")
        app_secret = config.get("CardMarket", "app_secret", fallback="")

        if not app_token or not app_secret:
            return None

        return cls(
            app_token=app_token,
            app_secret=app_secret,
            access_token=config.get("CardMarket", "mkm_access_token", fallback=""),
            access_token_secret=config.get("CardMarket", "mkm_access_token_secret", fallback=""),
            prices_api_url=config.get("CardMarket", "prices_api_url", fallback=""),
        )


@dataclass
class CardMarketProvider:
    """Async CardMarket provider for card data and prices."""

    config: CardMarketConfig | None = None
    concurrency: int = 2  # MKM rate limits aggressively
    request_delay: float = 0.5  # seconds between requests
    today_date: str = field(default_factory=lambda: datetime.datetime.today().strftime("%Y-%m-%d"))

    # Internal state
    _connection: Mkm | None = field(default=None, repr=False)
    _set_map: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    _http: aiohttp.ClientSession | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = CardMarketConfig.from_mtgjson_config()

    # Connection management

    def _ensure_mkm_connection(self) -> Mkm:
        """Lazily init mkmsdk connection."""
        if self._connection is None:
            if not self.config:
                raise RuntimeError("No CardMarket config")
            os.environ["MKM_APP_TOKEN"] = self.config.app_token
            os.environ["MKM_APP_SECRET"] = self.config.app_secret
            os.environ["MKM_ACCESS_TOKEN"] = self.config.access_token
            os.environ["MKM_ACCESS_TOKEN_SECRET"] = self.config.access_token_secret
            self._connection = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])
        return self._connection

    async def _ensure_http(self) -> aiohttp.ClientSession:
        """Lazily init async http client."""
        if self._http is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self._http = aiohttp.ClientSession(timeout=timeout)
        return self._http

    async def close(self) -> None:
        """Cleanup http client."""
        if self._http:
            await self._http.close()
            self._http = None

    # Set map management

    async def load_set_map(self) -> dict[str, dict[str, Any]]:
        """Load expansion set map from MKM."""
        if self._set_map:
            return self._set_map

        conn = self._ensure_mkm_connection()

        try:
            resp = await asyncio.to_thread(conn.market_place.expansions, game=1)
        except mkmsdk.exceptions.ConnectionError as e:
            LOGGER.error(f"Failed to fetch MKM expansions: {e}")
            return {}

        if resp.status_code != 200:
            LOGGER.error(f"MKM expansions request failed: {resp.status_code}")
            return {}

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            LOGGER.error(f"Failed to parse MKM expansions: {e}")
            return {}

        for exp in data.get("expansion", []):
            self._set_map[exp["enName"].lower()] = {
                "mcmId": exp["idExpansion"],
                "mcmName": exp["enName"],
            }

        # Apply manual fixes
        fixes_path = RESOURCE_PATH / "mkm_set_name_fixes.json"
        if fixes_path.exists():
            with fixes_path.open(encoding="utf-8") as f:
                fixes = json.load(f)
            for old_name, new_name in fixes.items():
                old_key = old_name.lower()
                if old_key in self._set_map:
                    self._set_map[new_name.lower()] = self._set_map.pop(old_key)
                else:
                    LOGGER.warning(f"MKM fix {old_name} -> {new_name} not found")

        LOGGER.info(f"Loaded {len(self._set_map)} MKM expansions")
        return self._set_map

    def get_set_id(self, set_name: str) -> int | None:
        """Get MKM set ID by name."""
        entry = self._set_map.get(set_name.lower())
        return int(entry["mcmId"]) if entry else None

    def get_extras_set_id(self, set_name: str) -> int | None:
        """Get MKM 'Extras' set ID (e.g. 'Throne of Eldraine: Extras')."""
        entry = self._set_map.get(f"{set_name.lower()}: extras")
        return int(entry["mcmId"]) if entry else None

    def get_set_name(self, set_name: str) -> str | None:
        """Get canonical MKM set name."""
        entry = self._set_map.get(set_name.lower())
        return str(entry["mcmName"]) if entry else None

    # Card fetching

    def _fetch_expansion_cards_sync(self, mcm_id: int, retries: int = 5) -> list[dict]:
        """Sync fetch of expansion cards - runs in thread pool."""
        import time
        conn = self._ensure_mkm_connection()
        resp = None

        for attempt in range(retries):
            try:
                resp = conn.market_place.expansion_singles(1, expansion=mcm_id)
                
                # Handle rate limiting
                if resp.status_code == 429:
                    wait = min(30, 2 ** attempt * 5)  # 5, 10, 20, 30, 30
                    LOGGER.warning(f"Rate limited on {mcm_id}, waiting {wait}s...")
                    time.sleep(wait)
                    resp = None
                    continue
                    
                break
            except mkmsdk.exceptions.ConnectionError as e:
                if "429" in str(e):
                    wait = min(30, 2 ** attempt * 5)
                    LOGGER.warning(f"Rate limited on {mcm_id}, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    LOGGER.warning(f"MKM connection error for {mcm_id} (attempt {attempt + 1}): {e}")
                    time.sleep(10)

        if resp is None or resp.status_code != 200:
            if resp:
                LOGGER.warning(f"MKM request failed for {mcm_id}: {resp.status_code}")
            else:
                LOGGER.error(f"Failed to fetch cards for expansion {mcm_id} after {retries} attempts")
            return []

        try:
            return resp.json().get("single", [])
        except json.JSONDecodeError as e:
            LOGGER.warning(f"Failed to parse MKM response for {mcm_id}: {e}")
            return []

    async def get_mkm_cards(self, mcm_id: int | None) -> dict[str, list[dict[str, Any]]]:
        """
        Get cards for a set, keyed by normalized name.

        Returns: {card_name: [card_data, ...], ...}
        """
        if mcm_id is None:
            return {}

        raw_cards = await asyncio.to_thread(self._fetch_expansion_cards_sync, mcm_id)
        if not raw_cards:
            return {}

        result: dict[str, list[dict]] = defaultdict(list)

        for card in raw_cards:
            card["number"] = (card.get("number") or "").lstrip("0")

            # Split cards get entries for each half
            for name in card["enName"].split("//"):
                normalized = name.strip().lower()
                if "token" in normalized:
                    normalized = normalized.split(" (", 1)[0]
                result[normalized].append(card)

        # Sort each name's cards by collector number
        for cards in result.values():
            cards.sort(key=lambda x: x.get("number", ""))

        return result

    async def get_all_cards(
        self,
        on_progress: Callable[[int, int, str], None] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch cards from all expansions concurrently.

        Returns DataFrame with: mcmId, mcmMetaId, name, number, expansionId, expansionName
        """
        await self.load_set_map()
        if not self._set_map:
            return pl.DataFrame()

        expansions = [
            (name, data["mcmId"], data["mcmName"])
            for name, data in self._set_map.items()
        ]
        total = len(expansions)
        completed = 0
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(self.concurrency)

        async def fetch_one(mcm_id: int, mcm_name: str) -> list[dict]:
            nonlocal completed
            async with semaphore:
                # Rate limit delay
                await asyncio.sleep(self.request_delay)
                
                raw = await asyncio.to_thread(self._fetch_expansion_cards_sync, mcm_id)
                cards = [
                    {
                        "mcmId": c.get("idProduct"),
                        "mcmMetaId": c.get("idMetaproduct"),
                        "name": c.get("enName", ""),
                        "number": (c.get("number") or "").lstrip("0"),
                        "expansionId": mcm_id,
                        "expansionName": mcm_name,
                    }
                    for c in raw
                ]
                async with lock:
                    completed += 1
                    if on_progress:
                        on_progress(completed, total, mcm_name)
                    LOGGER.info(f"[{completed}/{total}] {mcm_name}: {len(cards)} cards")
                return cards

        results = await asyncio.gather(*[fetch_one(mcm_id, mcm_name) for _, mcm_id, mcm_name in expansions])
        all_cards = [c for batch in results for c in batch]

        return pl.DataFrame(all_cards) if all_cards else pl.DataFrame()

    # Price fetching

    async def get_price_data(self) -> dict[str, dict[str, float | None]]:
        """
        Fetch price guide data from MKM price API.

        Returns: {product_id: {"trend": float, "trend-foil": float}, ...}
        """
        if not self.config or not self.config.prices_api_url:
            LOGGER.warning("No CardMarket price URL configured")
            return {}

        http = await self._ensure_http()

        try:
            async with http.get(self.config.prices_api_url) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except (aiohttp.ClientError, json.JSONDecodeError) as e:
            LOGGER.error(f"Failed to fetch MKM price data: {e}")
            return {}

        price_guides = data.get("priceGuides", [])
        if not price_guides:
            LOGGER.warning("No price guides in MKM response")
            return {}

        return {
            str(entry["idProduct"]): {
                "trend": float(entry["trend"]) if entry.get("trend") else None,
                "trend-foil": float(entry["trend-foil"]) if entry.get("trend-foil") else None,
            }
            for entry in price_guides
        }

    async def generate_today_price_dict(
        self,
        all_printings_path: Path,
    ) -> dict[str, MtgjsonPricesObject]:
        """
        Generate MTGJSON price structure from CardMarket data.

        :param all_printings_path: Path to AllPrintings.json for ID mapping
        :return: {uuid: MtgjsonPricesObject, ...}
        """
        # Try cache first, fall back to parsing AllPrintings
        from mtgjson5.cache import GLOBAL_CACHE

        mtgjson_id_map: dict[str, set[Any]] = GLOBAL_CACHE.get_cardmarket_to_uuid_map()
        if not mtgjson_id_map:
            mtgjson_id_map = generate_entity_mapping(
                all_printings_path, ("identifiers", "mcmId"), ("uuid",)
            )

        mtgjson_finish_map = generate_entity_mapping(
            all_printings_path, ("identifiers", "mcmId"), ("finishes",)
        )

        LOGGER.info("Building CardMarket retail data")

        price_data = await self.get_price_data()
        if not price_data:
            return {}

        today_dict: dict[str, MtgjsonPricesObject] = {}

        for product_id, prices in price_data.items():
            avg_sell = prices.get("trend")
            avg_foil = prices.get("trend-foil")

            if product_id not in mtgjson_id_map:
                continue

            if not avg_sell and not avg_foil:
                continue

            for uuid in mtgjson_id_map[product_id]:
                if uuid not in today_dict:
                    today_dict[uuid] = MtgjsonPricesObject(
                        "paper", "cardmarket", self.today_date, "EUR"
                    )

                if avg_sell:
                    today_dict[uuid].sell_normal = avg_sell

                if avg_foil:
                    finishes = mtgjson_finish_map.get(product_id, [])
                    if "etched" in finishes:
                        today_dict[uuid].sell_etched = avg_foil
                    else:
                        today_dict[uuid].sell_foil = avg_foil

        LOGGER.info(f"Generated prices for {len(today_dict)} cards")
        return today_dict


# Convenience functions

async def get_cardmarket_prices(all_printings_path: Path) -> dict[str, MtgjsonPricesObject]:
    """Fetch today's CardMarket prices."""
    provider = CardMarketProvider()
    try:
        return await provider.generate_today_price_dict(all_printings_path)
    finally:
        await provider.close()


async def get_cardmarket_set_cards(set_name: str) -> dict[str, list[dict[str, Any]]]:
    """Fetch cards for a single set by name."""
    provider = CardMarketProvider()
    try:
        await provider.load_set_map()
        mcm_id = provider.get_set_id(set_name)
        return await provider.get_mkm_cards(mcm_id)
    finally:
        await provider.close()


async def get_all_cardmarket_cards(
    output_path: Path | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
    concurrency: int = 2,
    request_delay: float = 0.5,
) -> pl.DataFrame:
    """Fetch all cards from all expansions."""
    provider = CardMarketProvider(concurrency=concurrency, request_delay=request_delay)
    try:
        df = await provider.get_all_cards(on_progress=on_progress)
        if output_path and not df.is_empty():
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(output_path)
        return df
    finally:
        await provider.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    async def main():
        def progress(done: int, total: int, name: str):
            print(f"[{done}/{total}] {name}")
        
        df = await get_all_cardmarket_cards(
            output_path=Path("mkm_cards.parquet"),
            on_progress=progress,
            concurrency=2,
            request_delay=0.5,
        )
        print(f"\nDone! {len(df)} cards")
        print(df.head(10))
    
    asyncio.run(main())
