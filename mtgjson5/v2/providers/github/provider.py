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
from mtgjson5.v2.providers.github.models import (
    BoosterModel,
    CardToProductsModel,
    PreconModel,
    SealedContentModel,
    SealedProductModel,
    TokenProductsModel,
)

LOGGER = logging.getLogger(__name__)

SCHEMAS = {
    "card_to_products": CardToProductsModel.polars_schema(),
    "sealed_products": SealedProductModel.polars_schema(),
    "sealed_contents": SealedContentModel.polars_schema(),
    "precon": PreconModel.polars_schema(),
    "boosters": BoosterModel.polars_schema(),
    "token_products": TokenProductsModel.polars_schema(),
    "decks": PreconModel.polars_schema(),
}

TOKEN_PRODUCTS_DIR_URL = (
    "https://api.github.com/repos/mtgjson/mtg-sealed-content/contents/outputs/token_products_mappings"
)
TOKEN_PRODUCTS_RAW_URL = (
    "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/token_products_mappings/{}.json"
)


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


_SUBTYPE_REMAP = {
    "prerelease": "prerelease_kit",
    "starter": "starter_deck",
    "tournament": "tournament_deck",
    "six": "six-card",
    "battle": "battle_pack",
    "convention": "convention_exclusive",
}

_CATEGORY_REMAP = {
    "limited": "limited_aid_tool",
    "multi_deck": "multiple_decks",
    "limited_case": "limited_aid_case",
}


def _build_sealed_products_records(data: dict) -> list[dict]:
    """Build sealed products records."""
    if not data:
        return []
    records = []
    for set_code, products in data.items():
        if not isinstance(products, dict):
            continue
        for name, info in products.items():
            if not isinstance(info, dict):
                continue
            record = {"setCode": set_code.upper(), "productName": name, **info}
            if record.get("subtype"):
                subtype_lower = record["subtype"].lower()
                if subtype_lower in _SUBTYPE_REMAP:
                    record["subtype"] = _SUBTYPE_REMAP[subtype_lower]
            if record.get("category"):
                category_lower = record["category"].lower()
                if category_lower in _CATEGORY_REMAP:
                    record["category"] = _CATEGORY_REMAP[category_lower]
            records.append(record)
    return records


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
                if content_type in ("size", "card_count") or not isinstance(items, list):
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
            "sealedProductUuids": (deck_map or {}).get(deck.get("set_code", "").lower(), {}).get(deck.get("name", "")),
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


def _build_token_products_records(per_set_data: dict[str, dict]) -> list[dict]:
    """Build token products records from combined per-set data.

    Merges all per-set token product mappings into a single list of records,
    each with a token UUID and its JSON-encoded product list.
    """
    combined: dict[str, list] = {}
    for set_data in per_set_data.values():
        if not isinstance(set_data, dict):
            continue
        for token_uuid, products in set_data.items():
            if token_uuid in combined:
                combined[token_uuid].extend(products)
            else:
                combined[token_uuid] = list(products)

    return [{"uuid": uuid, "tokenProducts": json.dumps(products)} for uuid, products in combined.items()]


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
        self.token_products_df: pl.LazyFrame | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._load_future: Any = None
        self._on_complete_callback: Callable[[Any], None] | None = None

    def load_async_background(self, on_complete: Callable[[Any], None] | None = None) -> None:
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
        self._executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="github_loader")
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

        async with aiohttp.ClientSession(timeout=self._timeout, headers=headers) as session:
            tasks = [self._fetch(session, k, url) for k, url in self.URLS.items()]
            results = await asyncio.gather(*tasks)

            # Fetch token products (per-set files)
            token_products_data = await self._fetch_token_products(session)

        raw = dict(results)
        raw["token_products"] = token_products_data
        self._build_all_dataframes(raw)
        LOGGER.info("GitHub data loaded")

    async def _fetch_token_products(self, session: aiohttp.ClientSession) -> dict[str, dict]:
        """Fetch all per-set token product mapping files from GitHub.

        Uses the GitHub Contents API to list available files, then fetches
        all of them concurrently.
        """
        LOGGER.info("Fetching token products directory listing...")

        # Get directory listing
        set_codes: list[str] = []
        try:
            async with session.get(TOKEN_PRODUCTS_DIR_URL) as r:
                if r.ok:
                    content = await r.read()
                    entries = json.loads(content)
                    set_codes = [
                        entry["name"].replace(".json", "")
                        for entry in entries
                        if isinstance(entry, dict) and entry.get("name", "").endswith(".json")
                    ]
                else:
                    LOGGER.warning(f"Failed to list token products directory: HTTP {r.status}")
        except (aiohttp.ClientError, json.JSONDecodeError) as e:
            LOGGER.warning(f"Failed to list token products directory: {e}")

        if not set_codes:
            LOGGER.warning("No token product files found")
            return {}

        LOGGER.info(f"Fetching token products for {len(set_codes)} sets...")

        # Fetch all per-set files concurrently
        sem = asyncio.Semaphore(20)

        async def _fetch_one(code: str) -> tuple[str, dict]:
            async with sem:
                url = TOKEN_PRODUCTS_RAW_URL.format(code)
                try:
                    async with session.get(url) as r:
                        if r.ok:
                            data = json.loads(await r.read())
                            return code, data
                except (aiohttp.ClientError, json.JSONDecodeError):
                    pass
                return code, {}

        results = await asyncio.gather(*[_fetch_one(c) for c in set_codes])
        combined = {code: data for code, data in results if data}
        LOGGER.info(f"Fetched token products for {len(combined)} sets")
        return combined

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

    async def _fetch(self, session: aiohttp.ClientSession, key: str, url: str) -> tuple[str, Any]:
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
            records = builder_func(*args)
            lf = _to_lazyframe(records, schema_key, log_label)
            setattr(self, f"{schema_key}_df", lf)

        # Decks needs special handling (two raw inputs, partitioned by type)
        deck_records = _build_decks_records(raw.get("decks", []), raw.get("deck_map", {}))
        decks_lf = _to_lazyframe(deck_records, "decks", "decks")
        self.decks_df = decks_lf
        self.sealed_dicts = self._partition_decks_by_type(decks_lf)

        # Token products (combined from per-set files)
        token_records = _build_token_products_records(raw.get("token_products", {}))
        self.token_products_df = _to_lazyframe(token_records, "token_products", "token_products")

    def _partition_decks_by_type(self, decks_lf: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        """Partition decks LazyFrame by deck type."""
        type_counts = (
            decks_lf.group_by("type").len().sort("len", descending=True).collect().filter(pl.col("type").is_not_null())
        )
        # Get unique field sets per type
        _schema_sets = (
            decks_lf.group_by("type")
            .agg(
                [
                    pl.len().alias("count"),
                    *[pl.col(c).drop_nulls().len().alias(f"has_{c}") for c in decks_lf.collect_schema().names()],
                ]
            )
            .collect()
        )
        partitions = {
            row["type"]: decks_lf.filter(pl.col("type") == row["type"]) for row in type_counts.iter_rows(named=True)
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
