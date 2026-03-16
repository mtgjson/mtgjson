"""
GitHub data provider for MTGJSON supplemental data.
"""

import asyncio
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.providers.github.models import (
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
    "https://raw.githubusercontent.com/mtgjson/mtg-sealed-content/main/outputs/token_products_mappings/{}.json"
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
        "decks": "https://raw.githubusercontent.com/taw/magic-preconstructed-decks-data/master/decks_v2.json",
        "boosters": "https://raw.githubusercontent.com/taw/magic-sealed-data/master/experimental_export_for_mtgjson.json",
    }

    TARBALL_URL = "https://github.com/mtgjson/mtg-sealed-content/archive/refs/heads/main.tar.gz"

    def __init__(self, timeout: int = 120, cache_path: Path | None = None):
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._cache_path = cache_path
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
        # Inline compilation data (populated by _fetch_and_build)
        self.products_dir: Path | None = None
        self.contents_dir: Path | None = None
        self.products_dict: dict | None = None
        self.boosters_raw: dict | None = None
        self.decks_raw: list | None = None

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

    async def _fetch_and_extract_yaml(self, session: aiohttp.ClientSession) -> tuple[Path, Path]:
        """Fetch YAML tarball and extract products + contents directories.

        Returns (products_dir, contents_dir). Retries 3 times on failure,
        raises RuntimeError if all attempts fail.
        """
        import io
        import tarfile as _tarfile
        import tempfile

        # Determine extraction target
        if self._cache_path:
            extract_base = self._cache_path / "sealed_yaml"
        else:
            extract_base = Path(tempfile.mkdtemp(prefix="sealed_yaml_"))

        products_dir = extract_base / "data" / "products"
        contents_dir = extract_base / "data" / "contents"

        # Skip if already extracted
        if products_dir.exists() and contents_dir.exists():
            n_p = len(list(products_dir.glob("*.yaml")))
            n_c = len(list(contents_dir.glob("*.yaml")))
            if n_p > 0 and n_c > 0:
                LOGGER.info(f"Using cached YAMLs: {n_p} products, {n_c} contents")
                return products_dir, contents_dir

        # Download tarball with retries
        tarball_bytes = None
        last_error = None
        for attempt in range(3):
            try:
                async with session.get(self.TARBALL_URL) as r:
                    r.raise_for_status()
                    tarball_bytes = await r.read()
                    break
            except (TimeoutError, aiohttp.ClientError) as e:
                last_error = e
                LOGGER.warning("Tarball download attempt %d failed: %s", attempt + 1, e)
        if tarball_bytes is None:
            raise RuntimeError(f"Failed to download sealed content YAML tarball after 3 attempts: {last_error}")

        LOGGER.info(f"Extracting YAMLs from tarball ({len(tarball_bytes):,} bytes)...")

        with _tarfile.open(fileobj=io.BytesIO(tarball_bytes), mode="r:gz") as tf:
            # Find the prefix (mtg-sealed-content-{sha}/)
            prefix = ""
            for m in tf.getmembers():
                if "/" in m.name:
                    prefix = m.name.split("/")[0] + "/"
                    break

            for member in tf.getmembers():
                if not member.name.startswith(prefix):
                    continue
                rel = member.name[len(prefix) :]
                if rel.startswith(("data/products/", "data/contents/")) and rel.endswith(".yaml"):
                    dest = extract_base / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    f = tf.extractfile(member)
                    if f is not None:
                        dest.write_bytes(f.read())

        n_p = len(list(products_dir.glob("*.yaml")))
        n_c = len(list(contents_dir.glob("*.yaml")))
        LOGGER.info(f"Extracted {n_p} product YAMLs, {n_c} content YAMLs")
        return products_dir, contents_dir

    async def _fetch_and_build(self) -> None:
        """Fetch all data from GitHub and build DataFrames."""
        LOGGER.info("Fetching GitHub data...")
        headers = self._build_headers()

        async with aiohttp.ClientSession(timeout=self._timeout, headers=headers) as session:
            # Fetch URLs + tarball + token products in parallel
            url_tasks = [self._fetch(session, k, url) for k, url in self.URLS.items()]
            tarball_task = self._fetch_and_extract_yaml(session)
            token_task = self._fetch_token_products(session)

            results = await asyncio.gather(*url_tasks)
            self.products_dir, self.contents_dir = await tarball_task
            token_products_data = await token_task

        raw = dict(results)
        raw["token_products"] = token_products_data

        # Store raw data for inline compilation (used by cache.py callback)
        self.boosters_raw = raw.get("boosters", {})
        self.decks_raw = raw.get("decks", [])

        # Compile products from YAML (no UUID dependency)
        from mtgjson5.pipeline.stages.sealed import compile_products

        self.products_dict = compile_products(self.products_dir)

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
            async with session.get(TOKEN_PRODUCTS_DIR_URL, headers={"Accept": "application/json"}) as r:
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

    def _build_headers(self, *, api: bool = False) -> dict[str, str]:
        """Build HTTP headers for GitHub requests."""
        headers: dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        if api:
            headers["Accept"] = "application/json"
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

        # Card-to-products: built in cache.py on_github_complete() via inline compilation
        self.card_to_products_df = None

        # Sealed products: from compiled YAML (not pre-built JSON)
        products_records = _build_sealed_products_records(self.products_dict or {})
        self.sealed_products_df = _to_lazyframe(products_records, "sealed_products", "sealed_products")

        # Boosters: unchanged
        booster_records = _build_boosters_records(raw.get("boosters", {}))
        self.boosters_df = _to_lazyframe(booster_records, "boosters", "boosters")

        # Token products: unchanged
        token_records = _build_token_products_records(raw.get("token_products", {}))
        self.token_products_df = _to_lazyframe(token_records, "token_products", "token_products")

        # sealed_contents_df and decks_df: NOT built here — they require
        # uuid_map + deck_map from inline compilation using pipeline data.
        # Built in cache.py on_github_complete() callback.
        self.sealed_contents_df = None
        self.decks_df = None
        self.sealed_dicts = {}

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
