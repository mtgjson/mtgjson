"""
GitHub data provider for MTGJSON supplemental data.
"""

import asyncio
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import aiohttp
import polars as pl

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.providers.v2.github.models import (
    BoosterModel,
    CardToProductsModel,
    PreconModel,
    SealedContentModel,
    SealedProductModel,
)


LOGGER = logging.getLogger(__name__)

SCHEMAS = {
    "card_to_products": CardToProductsModel.polars_schema(),
    "sealed_products": SealedProductModel.polars_schema(),
    "sealed_contents": SealedContentModel.polars_schema(),
    "precon": PreconModel.polars_schema(),
    "boosters": BoosterModel.polars_schema(),
}


def _to_lazyframe(
    records: list[dict],
    schema_key: str,
    log_label: str,
) -> pl.LazyFrame:
    """
    Convert records to LazyFrame with logging.

    Returns empty LazyFrame with proper schema if records is empty.
    """
    if not records:
        return pl.LazyFrame(schema=SCHEMAS[schema_key])

    LOGGER.info(f"  {log_label}: {len(records):,}")
    return pl.LazyFrame(records, infer_schema_length=None)


def _build_card_to_products_records(data: dict) -> list[dict]:
    """Build card-to-products mapping records."""
    if not data:
        return []
    return [
        {
            "uuid": k,
            "foil": v.get("foil"),
            "nonfoil": v.get("nonfoil"),
            "etched": v.get("etched"),
        }
        for k, v in data.items()
    ]


def _build_sealed_products_records(data: dict) -> list[dict]:
    """Build sealed products records."""
    if not data:
        return []
    return [
        {"setCode": set_code.upper(), "productName": name, **info}
        for set_code, products in data.items()
        if isinstance(products, dict)
        for name, info in products.items()
        if isinstance(info, dict)
    ]


def _build_sealed_contents_records(data: dict) -> list[dict]:
    """Build sealed contents records."""
    if not data:
        return []

    records = []
    for set_code, products in data.items():
        if not isinstance(products, dict):
            continue
        for product_name, contents in products.items():
            if not isinstance(contents, dict):
                continue

            base = {
                "setCode": set_code.upper(),
                "productName": product_name,
                "productSize": contents.get("size"),
                "cardCount": contents.get("card_count"),
            }

            for content_type, items in contents.items():
                if content_type in ("size", "card_count") or not isinstance(
                    items, list
                ):
                    continue
                for item in items:
                    record = {**base, "contentType": content_type}
                    if isinstance(item, dict):
                        record.update(item)
                    else:
                        record["item"] = item
                    records.append(record)

    return records


def _extract_card_list(cards: list[dict]) -> list[dict]:
    """Extract card entries from raw deck card list."""
    return [
        {
            "uuid": c.get("mtgjson_uuid"),
            "count": c.get("count", 1),
            "isFoil": c.get("foil", False),
            "isEtched": c.get("etched", False),
        }
        for c in cards
        if c.get("mtgjson_uuid")
    ]


def _build_decks_records(decks_raw: list, deck_map: dict) -> list[dict]:
    """Build deck records."""
    if not decks_raw:
        return []

    return [
        {
            "name": deck.get("name", ""),
            "setCode": deck.get("set_code", "").upper(),
            "type": deck.get("type"),
            "releaseDate": deck.get("release_date"),
            "sourceSetCodes": [s.upper() for s in deck.get("sourceSetCodes", [])],
            "sealedProductUuids": (deck_map or {})
            .get(deck.get("set_code", "").lower(), {})
            .get(deck.get("name", "")),
            "mainBoard": _extract_card_list(deck.get("cards", [])),
            "sideBoard": _extract_card_list(deck.get("sideboard", [])),
            "commander": _extract_card_list(deck.get("commander", [])),
            "displayCommander": _extract_card_list(deck.get("displayCommander", [])),
            "tokens": _extract_card_list(deck.get("tokens", [])),
            "planes": _extract_card_list(deck.get("planarDeck", [])),
            "schemes": _extract_card_list(deck.get("schemeDeck", [])),
        }
        for deck in decks_raw
    ]


def _build_boosters_records(data: dict) -> list[dict]:
    """Build booster config records."""
    if not data:
        return []
    return [{"setCode": k, "config": json.dumps(v)} for k, v in data.items()]


class SealedDataProvider:
    """Provider for MTGJSON GitHub data."""

    URLS = {
        "card_map": "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/card_map.json?raw=True",
        "products": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/products.json?raw=true",
        "contents": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/contents.json?raw=true",
        "deck_map": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/deck_map.json?raw=True",
        "decks": "https://github.com/taw/magic-preconstructed-decks-data/blob/master/decks_v2.json?raw=true",
        "boosters": "https://github.com/taw/magic-sealed-data/blob/master/experimental_export_for_mtgjson.json?raw=true",
    }

    def __init__(self, timeout: int = 120):
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self.boosters_df: pl.LazyFrame | None = None
        self.card_to_products_df: pl.LazyFrame | None = None
        self.sealed_products_df: pl.LazyFrame | None = None
        self.sealed_contents_df: pl.LazyFrame | None = None
        self.sealed_dicts: dict[str, pl.LazyFrame] | None = None
        self.decks_df: pl.LazyFrame | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._load_future: Any = None
        self._on_complete_callback: Callable[[Any], None] | None = None

    def load_async_background(
        self, on_complete: Callable[[Any], None] | None = None
    ) -> None:
        """
        Start loading data in a background thread (non-blocking).

        Args:
            on_complete: Optional callback function(provider) called when load completes.
                        Typically used to transfer data to GlobalCache.
        """
        LOGGER.info("Callback recieved")
        if self._load_future is not None:
            LOGGER.warning("GitHub background load already in progress")
            return

        self._on_complete_callback = on_complete
        self._executor = ThreadPoolExecutor(
            max_workers=6, thread_name_prefix="github_loader"
        )
        self._load_future = self._executor.submit(self._run_async_in_thread)
        LOGGER.info("GitHub data loading started in background thread")

    def _run_async_in_thread(self) -> None:
        """Run async fetch in a new event loop (for thread execution)."""
        LOGGER.info("Started GitHub data loading in background thread")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._fetch_and_build())
            # Call callback after successful load
            if self._on_complete_callback:
                self._on_complete_callback(self)
        except Exception as e:
            import traceback

            LOGGER.error(f"GitHub data loading failed: {e}\n{traceback.format_exc()}")
        finally:
            loop.close()
            if self._executor:
                self._executor.shutdown(wait=False)

    def wait_for_load(self, timeout: float | None = None) -> bool:
        """
        Wait for background load to complete.

        Returns True if loaded successfully, False on timeout.
        """
        if self._load_future is None:
            LOGGER.warning("No GitHub background load in progress")
            return False

        try:
            self._load_future.result(timeout=timeout)
            return True
        except TimeoutError:
            LOGGER.warning(f"GitHub background load timed out after {timeout}s")
            return False

    def is_loaded(self) -> bool:
        """Check if data has been loaded."""
        return self.decks_df is not None

    def load_sync(self) -> "SealedDataProvider":
        """Load data synchronously (blocking). Returns self for chaining."""
        asyncio.run(self._fetch_and_build())
        return self

    async def _fetch_and_build(self) -> None:
        """Fetch all data from GitHub and build DataFrames."""
        LOGGER.info("Fetching GitHub data...")

        headers = self._build_headers()

        async with aiohttp.ClientSession(
            timeout=self._timeout, headers=headers
        ) as session:
            tasks = [self._fetch(session, k, url) for k, url in self.URLS.items()]
            results = await asyncio.gather(*tasks)

        raw = dict(results)
        self._build_all_dataframes(raw)
        LOGGER.info("GitHub data loaded")

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers for GitHub requests."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        try:
            token = MtgjsonConfig().get("GitHub", "api_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        except Exception:
            pass
        return headers

    async def _fetch(
        self, session: aiohttp.ClientSession, key: str, url: str
    ) -> tuple[str, Any]:
        """Fetch JSON from URL."""
        try:
            async with session.get(url) as r:
                r.raise_for_status()
                content = await r.read()
                return key, json.loads(content)
        except (aiohttp.ClientError, json.JSONDecodeError) as e:
            LOGGER.error(f"Failed to fetch {key}: {e}")
            return key, {}

    def _build_all_dataframes(self, raw: dict[str, Any]) -> None:
        """Build all DataFrames from raw fetched data."""

        builders: list[tuple[str, str, Callable, tuple]] = [
            (
                "card_to_products",
                "card_to_products",
                _build_card_to_products_records,
                ("card_map",),
            ),
            (
                "sealed_products",
                "sealed_products",
                _build_sealed_products_records,
                ("products",),
            ),
            (
                "sealed_contents",
                "sealed_contents",
                _build_sealed_contents_records,
                ("contents",),
            ),
            ("boosters", "boosters", _build_boosters_records, ("boosters",)),
        ]

        for schema_key, log_label, builder_func, raw_keys in builders:
            args = [raw.get(k, {}) for k in raw_keys]
            records = builder_func(*args) if len(args) == 1 else builder_func(*args)
            lf = _to_lazyframe(records, schema_key, log_label)
            setattr(self, f"{schema_key}_df", lf)

        # Decks needs special handling (two raw inputs, partitioned by type)
        deck_records = _build_decks_records(
            raw.get("decks", []), raw.get("deck_map", {})
        )
        decks_lf = _to_lazyframe(deck_records, "decks", "decks")
        self.decks_df = decks_lf
        self.sealed_dicts = self._partition_decks_by_type(decks_lf)

    def _partition_decks_by_type(
        self, decks_lf: pl.LazyFrame
    ) -> dict[str, pl.LazyFrame]:
        """Partition decks LazyFrame by deck type."""
        type_counts = (
            decks_lf.group_by("type")
            .len()
            .sort("len", descending=True)
            .collect()
            .filter(pl.col("type").is_not_null())
        )
        # Get unique field sets per type
        _schema_sets = (
            decks_lf.group_by("type")
            .agg(
                [
                    pl.len().alias("count"),
                    *[
                        pl.col(c).drop_nulls().len().alias(f"has_{c}")
                        for c in decks_lf.collect_schema().names()
                    ],
                ]
            )
            .collect()
        )
        partitions = {
            row["type"]: decks_lf.filter(pl.col("type") == row["type"])
            for row in type_counts.iter_rows(named=True)
        }
        return partitions

    def test_build(self) -> None:
        """Test all build methods from scratch."""
        LOGGER.info("Testing GitHub data build methods...")
        self.load_sync()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    provider = SealedDataProvider()
    provider.test_build()
