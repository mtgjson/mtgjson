"""
TCGPlayer Provider V2 - Async client with connection pooling.

Fetches all Magic products with nested SKUs. Streams results to parquet.
Supports multiple API keys for increased throughput.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import aiohttp
import polars as pl

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig

from .models import PRODUCT_SCHEMA, ProductsResponse
from .published import PUBLISHED_SKUS_URL, download_published_catalog

LOGGER = logging.getLogger(__name__)

PRODUCTS_PER_PAGE = 100
CONCURRENT_REQUESTS = 75
MAX_RETRIES = 3
RETRY_DELAY = 2.0
MAX_RATE_LIMIT_WAITS = 8
# Pages that still failed after the main pass get a second, gentler sweep.
RETRY_PASS_CONCURRENCY = 4
# A live catalog shifts under offset pagination, so require near-completeness
# rather than an exact match against the up-front totalItems.
MIN_COMPLETENESS = 0.98
# The catalog only ever grows in practice, so a real day-over-day shrink of more
# than this is a fetch problem rather than TCGPlayer delisting products.
MIN_CATALOG_RETENTION = 0.95
# MTGJSON publishes daily, so a catalog rebuilt from a published build goes stale
# quickly. Past this it is refetched rather than reused.
PUBLISHED_CATALOG_MAX_AGE_HOURS = 24.0
NEAR_MINT_CONDITION = 1
ENGLISH_LANGUAGE = 1
NON_FOIL_PRINTING = 1
FOIL_PRINTING = 2

SEALED_PRODUCT_TYPES = [
    "Booster Box",
    "Booster Pack",
    "Sealed Products",
    "Intro Pack",
    "Fat Pack",
    "Box Sets",
    "Precon/Event Decks",
    "Magic Deck Pack",
    "Magic Booster Box Case",
    "All 5 Intro Packs",
    "Intro Pack Display",
    "3x Magic Booster Packs",
    "Booster Battle Pack",
]
ALL_PRODUCT_TYPES = ",".join(["Cards", *SEALED_PRODUCT_TYPES])

ProgressCallback = Callable[[int, int, str], None]


def published_catalog_path(output_path: Path | None = None) -> Path:
    """Where a catalog rebuilt from the last published build is kept.

    A build that fell back never writes ``tcg_skus.parquet``, so anything that
    reads the catalog off disk - assembly subprocesses, which have no shared
    cache - has to know about this file too.
    """
    base = output_path or (constants.CACHE_PATH / "tcg_skus.parquet")
    return base.with_name(f"{base.stem}_published.parquet")


class TcgPlayerIncompleteFetchError(RuntimeError):
    """Raised when the TCGPlayer catalog could not be fetched in full.

    A partial catalog silently drops every SKU for the products that went
    missing, so callers must treat this as a failed fetch and fall back to the
    previous good data rather than publishing the truncated result.
    """


class TcgPlayerCatalogUnavailableError(RuntimeError):
    """Raised when the fetch failed and no previous catalog could be recovered.

    Publishing an empty TcgplayerSkus.json drops every SKU for every card, so a
    build that has nothing to fall back on stops instead of shipping the hole.
    """


@dataclass
class TcgPlayerConfig:
    """TCGPlayer API credentials and settings."""

    public_key: str
    private_key: str
    base_url: str = "https://api.tcgplayer.com"
    api_version: str = "v1.39.0"

    @property
    def token_url(self) -> str:
        """Return TCGPlayer token endpoint URL."""
        return f"{self.base_url}/token"

    def endpoint_url(self, endpoint: str, versioned: bool = True) -> str:
        """Return full URL for API endpoint."""
        if versioned:
            return f"{self.base_url}/{self.api_version}/{endpoint}"
        return f"{self.base_url}/{endpoint}"

    @classmethod
    def from_mtgjson_config(cls, suffix: str = "") -> TcgPlayerConfig | None:
        """Load config from mtgjson.properties [TCGPlayer] section."""
        config = MtgjsonConfig()
        if not config.has_section("TCGPlayer"):
            return None

        key_suffix = f"_{suffix}" if suffix else ""
        public_key = config.get("TCGPlayer", f"client_id{key_suffix}")
        private_key = config.get("TCGPlayer", f"client_secret{key_suffix}")

        # Skip if keys are empty or missing
        if not public_key or not private_key:
            return None

        return cls(
            public_key=public_key,
            private_key=private_key,
            api_version=config.get("TCGPlayer", "api_version", fallback="v1.39.0"),
        )

    @classmethod
    def load_all(cls) -> list[TcgPlayerConfig]:
        """Load all available API key configs (primary + secondary)."""
        configs = []
        primary = cls.from_mtgjson_config("")
        if primary:
            configs.append(primary)
        secondary = cls.from_mtgjson_config("2")
        if secondary:
            configs.append(secondary)
        return configs


class TcgPlayerClient:
    """Async TCGPlayer API client with connection pooling."""

    def __init__(self, config: TcgPlayerConfig):
        self.config = config
        self.access_token: str | None = None
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> TcgPlayerClient:
        connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        try:
            await self.authenticate()
        except Exception:
            # Close session if authentication fails to avoid unclosed connector
            await self._session.close()
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._session:
            await self._session.close()

    async def authenticate(self) -> None:
        """Obtain bearer token from TCGPlayer OAuth endpoint."""
        if self._session is None:
            raise RuntimeError("Session not initialized")

        url = f"{self.config.base_url}/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.public_key,
            "client_secret": self.config.private_key,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with self._session.post(url, data=data, headers=headers) as resp:
            resp.raise_for_status()
            result = await resp.json()
            self.access_token = result["access_token"]
            LOGGER.debug("TCGPlayer authentication successful")

    async def _get(self, endpoint: str, versioned: bool = True) -> dict[str, object]:
        """Execute authenticated GET request with retry on errors."""
        url = (
            f"{self.config.base_url}/{self.config.api_version}/{endpoint}"
            if versioned
            else f"{self.config.base_url}/{endpoint}"
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"bearer {self.access_token}",
        }

        if self._session is None:
            raise RuntimeError("Session not initialized")

        last_error: Exception | None = None
        attempt = 0
        rate_limit_waits = 0
        # Waiting out a 429 is not a failed attempt, so it gets its own budget.
        # Sharing one counter meant three consecutive rate limits abandoned the
        # page even though the API never actually errored.
        while attempt < MAX_RETRIES:
            try:
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status == 429:
                        if rate_limit_waits >= MAX_RATE_LIMIT_WAITS:
                            last_error = aiohttp.ClientError(
                                f"Still rate limited after {rate_limit_waits} waits for {endpoint}"
                            )
                            break
                        rate_limit_waits += 1
                        retry_after = float(resp.headers.get("Retry-After", RETRY_DELAY * rate_limit_waits))
                        LOGGER.warning(f"Rate limited, waiting {retry_after}s ({endpoint})")
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    result: dict[str, object] = await resp.json()
                    return result
            except (TimeoutError, aiohttp.ClientError) as e:
                last_error = e
                attempt += 1
                if attempt < MAX_RETRIES:
                    LOGGER.debug(f"Retry {attempt}/{MAX_RETRIES} for {endpoint}: {e}")
                    await asyncio.sleep(RETRY_DELAY * attempt)

        raise last_error or aiohttp.ClientError(f"Failed after {MAX_RETRIES} retries")

    async def get_products_page(
        self,
        category_id: int = 1,
        product_types: str = "Cards",
        offset: int = 0,
        limit: int = PRODUCTS_PER_PAGE,
        include_skus: bool = True,
    ) -> dict[str, object]:
        """Fetch a page of Magic card products."""
        endpoint = (
            f"catalog/products?categoryId={category_id}&productTypes={product_types}&limit={limit}&offset={offset}"
        )
        if include_skus:
            endpoint += "&includeSkus=true"
        return await self._get(endpoint, versioned=False)

    async def get_total_products(self, product_types: str = "Cards") -> int:
        """Get total count of Magic products for the given product types."""
        resp = await self.get_products_page(product_types=product_types, offset=0, limit=1, include_skus=False)
        total_items = resp.get("totalItems", 0)
        return int(total_items) if isinstance(total_items, (int, float)) else 0


class TCGProvider:
    """
    Complete TCGPlayer data provider with caching and multiple output formats.

    Handles:
    - Parallel fetching with multiple API keys
    - Streaming to parquet with incremental flushes
    - SKU map building (productId -> foil/nonfoil skuIds)
    - Enhanced SKU output (UUID -> SKU details)
    """

    def __init__(
        self,
        output_path: Path | None = None,
        configs: list[TcgPlayerConfig] | None = None,
        on_progress: ProgressCallback | None = None,
        flush_threshold: int = 50_000,
        product_types: str | None = None,
        published_catalog_url: str | None = PUBLISHED_SKUS_URL,
    ):
        self.output_path = output_path or (constants.CACHE_PATH / "tcg_skus.parquet")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.configs = configs or TcgPlayerConfig.load_all()
        self.on_progress = on_progress
        self.flush_threshold = flush_threshold
        self.product_types = product_types or ALL_PRODUCT_TYPES
        # Set to None to keep a build off the network when no catalog is cached.
        self.published_catalog_url = published_catalog_url
        self.published_path = published_catalog_path(self.output_path)
        self._published_download_failed = False

    def previous_catalog(self) -> pl.LazyFrame | None:
        """Return the last complete catalog, or None if none can be reached.

        A rerun on the same machine has the previous ``tcg_skus.parquet`` on
        disk. The nightly does not - it starts in a fresh container - so it falls
        back to MTGJSON's last published TcgplayerSkus.json, which is the only
        copy of yesterday's catalog the build machine can get to.
        """
        if self.output_path.exists():
            # A run with no usable API keys writes an empty catalog here, so an
            # existing file is only a baseline once it actually holds products.
            try:
                if pl.scan_parquet(self.output_path).select(pl.len()).collect().item():
                    return pl.scan_parquet(self.output_path)
                LOGGER.warning(f"Ignoring the empty TCG catalog at {self.output_path}")
            except Exception as e:
                LOGGER.warning(f"Could not read the TCG catalog at {self.output_path}: {e}")
        return self._published_catalog()

    def _published_catalog(self) -> pl.LazyFrame | None:
        """Rebuild the last published catalog, downloading it at most once.

        A copy left by an earlier build is only reused while it is still recent.
        Falling back to a catalog from weeks ago would quietly hand a regressed
        fetch a baseline small enough to pass the day-over-day check.
        """
        if self._published_catalog_fresh():
            return pl.scan_parquet(self.published_path)
        if not self.published_catalog_url or self._published_download_failed:
            return None

        staging_path = self.published_path.with_suffix(".parquet.staging")
        try:
            catalog = download_published_catalog(self.published_catalog_url)
            # Swap the file in whole: a half-written copy would still look fresh
            # to the next run and to the assembly subprocesses.
            catalog.write_parquet(staging_path)
            staging_path.replace(self.published_path)
        except Exception as e:
            self._published_download_failed = True
            staging_path.unlink(missing_ok=True)
            LOGGER.error(f"Could not rebuild the last published TCGPlayer catalog: {e}")
            return None

        return pl.scan_parquet(self.published_path)

    def _published_catalog_fresh(self) -> bool:
        """Return True if a rebuilt catalog from this or a recent build is on disk."""
        if not self.published_path.exists():
            return False
        age_hours = (time.time() - self.published_path.stat().st_mtime) / 3600
        return age_hours < PUBLISHED_CATALOG_MAX_AGE_HOURS

    async def fetch_all_products(self) -> pl.LazyFrame:
        """
        Fetch all TCGPlayer products with nested SKUs.

        Streams results to parquet incrementally using part files.
        Returns LazyFrame of final combined output.
        """
        empty_schema = cast("dict", PRODUCT_SCHEMA)

        if not self.configs:
            LOGGER.warning("No TCGPlayer API keys configured")
            pl.DataFrame(schema=empty_schema).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        # Create all clients upfront and keep them alive throughout
        # This avoids rate limiting on token requests
        async with contextlib.AsyncExitStack() as stack:
            clients: list[TcgPlayerClient] = []
            for config in self.configs:
                client = await stack.enter_async_context(TcgPlayerClient(config))
                clients.append(client)

            if not clients:
                LOGGER.warning("Failed to create any TCGPlayer clients")
                pl.DataFrame(schema=empty_schema).write_parquet(self.output_path)
                return pl.scan_parquet(self.output_path)

            # Get total count using first client
            total_items = await clients[0].get_total_products(product_types=self.product_types)

            if total_items == 0:
                # Credentials worked but the catalog came back empty, which is an
                # upstream failure rather than a real answer.
                raise TcgPlayerIncompleteFetchError("TCGPlayer reported 0 products for the requested product types")

            # Calculate pagination
            offsets = list(range(0, total_items, PRODUCTS_PER_PAGE))
            total_pages = len(offsets)
            LOGGER.info(f"Fetching {total_items} products in {total_pages} pages")

            # Distribute work across clients
            offsets_per_client: list[list[int]] = [[] for _ in clients]
            for i, offset in enumerate(offsets):
                offsets_per_client[i % len(clients)].append(offset)

            # Fetch with streaming to part files (pass authenticated clients)
            part_files, fetched = await self._fetch_with_streaming_clients(clients, offsets_per_client, total_pages)

            if fetched < total_items * MIN_COMPLETENESS:
                self._discard_parts(part_files)
                raise TcgPlayerIncompleteFetchError(
                    f"TCGPlayer returned {fetched:,} of {total_items:,} products "
                    f"({fetched / total_items:.1%}); refusing to publish a truncated catalog"
                )

            # Combine part files
            return await self._combine_part_files(part_files)

    @staticmethod
    def _parse_products(resp: dict[str, object]) -> list[dict]:
        """Validate a catalog/products response into part-file rows.

        Validation raises on a malformed page, which sends it back through the
        retry sweep. Coercing it to an empty list instead would drop the page's
        products from the catalog without anything noticing.
        """
        page = ProductsResponse.model_validate(resp)
        return [
            {
                "productId": product.productId,
                "name": product.name,
                "cleanName": product.cleanName,
                "groupId": product.groupId,
                "url": product.url,
                "skus": [sku.model_dump() for sku in product.skus],
            }
            for product in page.results
        ]

    def _discard_parts(self, part_files: list[Path]) -> None:
        """Delete part files from an aborted fetch."""
        for part_file in part_files:
            try:
                part_file.unlink()
            except OSError as e:
                LOGGER.warning(f"Failed to delete {part_file}: {e}")
        part_files.clear()

    async def _fetch_with_streaming_clients(
        self,
        clients: list[TcgPlayerClient],
        offsets_per_client: list[list[int]],
        total_pages: int,
    ) -> tuple[list[Path], int]:
        """Fetch products in parallel using pre-authenticated clients.

        Uses a semaphore to limit concurrent requests while still parallelizing
        within each client for better performance.

        Every page that never landed is tracked, retried at low concurrency, and
        then raised as :class:`TcgPlayerIncompleteFetchError` if it still fails.
        A dropped page silently removes ~100 products from the catalog, and every
        SKU of every card mapped to those products disappears from the build.

        Returns:
            Tuple of (part file paths, number of products fetched).
        """
        part_files: list[Path] = []
        part_counter = 0
        buffer: list[dict] = []
        lock = asyncio.Lock()
        fetched = 0
        succeeded: set[int] = set()
        failures: dict[int, str] = {}

        async def flush_buffer() -> None:
            nonlocal buffer, part_counter
            if not buffer:
                return

            to_write = buffer
            buffer = []

            part_path = self.output_path.parent / f".tcg_part_{part_counter:04d}.parquet"
            part_counter += 1
            pl.DataFrame(to_write, schema=cast("dict", PRODUCT_SCHEMA)).write_parquet(part_path)
            part_files.append(part_path)
            LOGGER.debug(f"Flushed {len(to_write)} products to {part_path}")

        async def fetch_single_page(client: TcgPlayerClient, offset: int, semaphore: asyncio.Semaphore) -> None:
            nonlocal fetched, buffer
            async with semaphore:
                try:
                    resp = await client.get_products_page(
                        offset=offset,
                        include_skus=True,
                        product_types=self.product_types,
                    )
                    page_products = self._parse_products(resp)
                except Exception as e:
                    LOGGER.warning(f"Failed offset {offset}: {e}")
                    async with lock:
                        failures[offset] = str(e)
                    return

                async with lock:
                    buffer.extend(page_products)
                    fetched += len(page_products)
                    succeeded.add(offset)
                    failures.pop(offset, None)
                    if len(buffer) >= self.flush_threshold:
                        await flush_buffer()
                    if self.on_progress:
                        self.on_progress(len(succeeded), total_pages, f"offset={offset}")

        try:
            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
            await asyncio.gather(
                *[
                    fetch_single_page(client, offset, semaphore)
                    for client, client_offsets in zip(clients, offsets_per_client, strict=False)
                    for offset in client_offsets
                ]
            )

            if failures:
                # Failures arrive in bursts when the API rate limits or wobbles,
                # so a burst takes out a whole run of neighbouring pages. Sweep
                # them again slowly instead of accepting the hole.
                retry_offsets = sorted(failures)
                LOGGER.warning(
                    f"{len(retry_offsets):,} of {total_pages:,} TCGPlayer pages failed; "
                    f"retrying at concurrency {RETRY_PASS_CONCURRENCY}"
                )
                retry_semaphore = asyncio.Semaphore(RETRY_PASS_CONCURRENCY)
                await asyncio.gather(
                    *[
                        fetch_single_page(clients[i % len(clients)], offset, retry_semaphore)
                        for i, offset in enumerate(retry_offsets)
                    ]
                )

            async with lock:
                await flush_buffer()

            if failures:
                sample = ", ".join(f"{offset} ({failures[offset]})" for offset in sorted(failures)[:3])
                raise TcgPlayerIncompleteFetchError(
                    f"{len(failures):,} of {total_pages:,} TCGPlayer catalog pages still failed "
                    f"after retries; first failures: {sample}"
                )

            LOGGER.info(f"TCGPlayer fetch complete: {fetched:,} products in {len(part_files)} part files")
            return part_files, fetched

        except Exception as e:
            if not isinstance(e, TcgPlayerIncompleteFetchError):
                LOGGER.error(f"Error during TCGPlayer fetch: {e}")
            self._discard_parts(part_files)
            raise

    def _reject_regression(self, staging_path: Path) -> None:
        """Refuse a catalog that lost ground against the last good one.

        Counting pages is not enough on its own: the API can answer every page
        and still hand back products whose ``skus`` array is empty, which drops
        the same cards from TcgplayerSkus.json without losing a single product.
        Comparing both totals against the previous catalog catches either shape.

        On the nightly the baseline is the last published TcgplayerSkus.json,
        which only covers the products that mapped to an MTGJSON UUID. That makes
        it a floor rather than an exact match: a catalog can never legitimately
        come back smaller than the slice of itself that shipped yesterday.
        """
        previous_lf = self.previous_catalog()
        if previous_lf is None:
            LOGGER.warning("No previous TCGPlayer catalog to compare against; skipping the day-over-day check")
            return

        counts = pl.col("skus").list.len().sum().alias("skus")
        try:
            previous = previous_lf.select(pl.len().alias("products"), counts).collect()
            current = pl.scan_parquet(staging_path).select(pl.len().alias("products"), counts).collect()
        except Exception as e:
            LOGGER.warning(f"Could not compare against the previous TCG catalog: {e}")
            return

        for label in ("products", "skus"):
            before = previous[label][0] or 0
            after = current[label][0] or 0
            if before and after < before * MIN_CATALOG_RETENTION:
                raise TcgPlayerIncompleteFetchError(
                    f"TCGPlayer catalog dropped from {before:,} to {after:,} {label} "
                    f"({after / before:.1%} of the previous build); refusing to publish it"
                )

    async def _combine_part_files(self, part_files: list[Path]) -> pl.LazyFrame:
        """Combine part files into single output parquet."""
        if not part_files:
            pl.DataFrame(schema=cast("dict", PRODUCT_SCHEMA)).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        # Build alongside the existing cache and swap it in only once the whole
        # catalog is on disk, so a crash mid-write leaves yesterday's good file.
        staging_path = self.output_path.with_suffix(".parquet.staging")
        try:
            # Scan and combine this run's parts. Listing them explicitly keeps
            # orphans from an earlier interrupted fetch out of the catalog.
            lf = pl.scan_parquet(source=part_files, rechunk=True)

            # Stream to final output
            lf.sink_parquet(staging_path)
            self._reject_regression(staging_path)
            staging_path.replace(self.output_path)
            LOGGER.info(f"Combined {len(part_files)} parts to {self.output_path}")

        except Exception as e:
            LOGGER.error(f"Error combining part files: {e}")
            staging_path.unlink(missing_ok=True)
            raise
        finally:
            self._discard_parts(part_files)

        return pl.scan_parquet(self.output_path)

    # Sync wrapper methods
    def fetch_all_products_sync(self) -> pl.LazyFrame:
        """Synchronous wrapper for fetch_all_products."""
        return asyncio.run(self.fetch_all_products())

    @classmethod
    def create_background_task(
        cls,
        output_path: Path | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> asyncio.Task[pl.LazyFrame]:
        """
        Start TCGPlayer fetch as background task.

        Returns immediately with Task that can be awaited later.

        Usage:
            tcg_task = TCGPlayerProvider.create_background_task()
            # Do other work...
            products_lf = await tcg_task
        """
        provider = cls(output_path=output_path, on_progress=on_progress)
        return asyncio.create_task(provider.fetch_all_products())
