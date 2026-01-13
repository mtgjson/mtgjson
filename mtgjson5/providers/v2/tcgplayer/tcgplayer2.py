"""
TCGPlayer Provider V2 - Async client with connection pooling.

Fetches all Magic products with nested SKUs. Streams results to parquet.
Supports multiple API keys for increased throughput.
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import aiohttp
import polars as pl

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

PRODUCTS_PER_PAGE = 100
CONCURRENT_REQUESTS = 75
MAX_RETRIES = 3
RETRY_DELAY = 2.0
NEAR_MINT_CONDITION = 1
ENGLISH_LANGUAGE = 1
NON_FOIL_PRINTING = 1
FOIL_PRINTING = 2

ProgressCallback = Callable[[int, int, str], None]


@dataclass
class TcgPlayerConfig:
    """TCGPlayer API credentials and settings."""

    public_key: str
    private_key: str
    base_url: str = "https://api.tcgplayer.com"
    api_version: str = "v1.39.0"

    @classmethod
    def from_mtgjson_config(cls, suffix: str = "") -> Optional["TcgPlayerConfig"]:
        """Load config from mtgjson.properties [TCGPlayer] section."""
        config = MtgjsonConfig()
        if not config.has_section("TCGPlayer"):
            return None

        key_suffix = f"_{suffix}" if suffix else ""
        return cls(
            public_key=config.get("TCGPlayer", f"client_id{key_suffix}"),
            private_key=config.get("TCGPlayer", f"client_secret{key_suffix}"),
            api_version=config.get("TCGPlayer", "api_version", fallback="v1.39.0"),
        )

    @classmethod
    def load_all(cls) -> list["TcgPlayerConfig"]:
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
        self.access_token: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "TcgPlayerClient":
        connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
        self._session = aiohttp.ClientSession(connector=connector)
        await self.authenticate()
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

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = float(
                            resp.headers.get("Retry-After", RETRY_DELAY * (attempt + 1))
                        )
                        LOGGER.warning(f"Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    result: dict[str, object] = await resp.json()
                    return result
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    LOGGER.debug(
                        f"Retry {attempt + 1}/{MAX_RETRIES} for {endpoint}: {e}"
                    )

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
        endpoint = f"catalog/products?categoryId={category_id}&productTypes={product_types}&limit={limit}&offset={offset}"
        if include_skus:
            endpoint += "&includeSkus=true"
        return await self._get(endpoint, versioned=False)

    async def get_total_products(self) -> int:
        """Get total count of Magic card products."""
        resp = await self.get_products_page(offset=0, limit=1, include_skus=False)
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
        on_progress: Optional[ProgressCallback] = None,
        flush_threshold: int = 50_000,
    ):
        self.output_path = output_path or (constants.CACHE_PATH / "tcg_skus.parquet")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.configs = configs or TcgPlayerConfig.load_all()
        self.on_progress = on_progress
        self.flush_threshold = flush_threshold

    async def fetch_all_products(self) -> pl.LazyFrame:
        """
        Fetch all TCGPlayer products with nested SKUs.

        Streams results to parquet incrementally using part files.
        Returns LazyFrame of final combined output.
        """
        if not self.configs:
            LOGGER.warning("No TCGPlayer API keys configured")
            pl.DataFrame(
                schema={
                    "productId": pl.Int64(),
                    "name": pl.String(),
                    "cleanName": pl.String(),
                    "groupId": pl.Int64(),
                    "url": pl.String(),
                    "skus": pl.List(
                        pl.Struct(
                            {
                                "skuId": pl.Int64(),
                                "languageId": pl.Int64(),
                                "printingId": pl.Int64(),
                                "conditionId": pl.Int64(),
                            }
                        )
                    ),
                }
            ).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        # Get total count
        async with TcgPlayerClient(self.configs[0]) as client:
            total_items = await client.get_total_products()

        if total_items == 0:
            LOGGER.info("No TCGPlayer products found")
            pl.DataFrame(
                schema={
                    "productId": pl.Int64(),
                    "name": pl.String(),
                    "cleanName": pl.String(),
                    "groupId": pl.Int64(),
                    "url": pl.String(),
                    "skus": pl.List(
                        pl.Struct(
                            {
                                "skuId": pl.Int64(),
                                "languageId": pl.Int64(),
                                "printingId": pl.Int64(),
                                "conditionId": pl.Int64(),
                            }
                        )
                    ),
                }
            ).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        # Calculate pagination
        offsets = list(range(0, total_items, PRODUCTS_PER_PAGE))
        total_pages = len(offsets)
        LOGGER.info(f"Fetching {total_items} products in {total_pages} pages")

        # Distribute work across API keys
        offsets_per_client: list[list[int]] = [[] for _ in self.configs]
        for i, offset in enumerate(offsets):
            offsets_per_client[i % len(self.configs)].append(offset)

        # Fetch with streaming to part files
        part_files = await self._fetch_with_streaming(offsets_per_client, total_pages)

        # Combine part files
        return await self._combine_part_files(part_files)

    async def _fetch_with_streaming(
        self, offsets_per_client: list[list[int]], total_pages: int
    ) -> list[Path]:
        """Fetch products in parallel, streaming to part files."""
        part_files: list[Path] = []
        part_counter = 0
        buffer: list[dict] = []
        lock = asyncio.Lock()
        completed = 0

        async def flush_buffer() -> None:
            nonlocal buffer, part_counter
            if not buffer:
                return

            to_write = buffer
            buffer = []

            part_path = (
                self.output_path.parent / f".tcg_part_{part_counter:04d}.parquet"
            )
            part_counter += 1
            pl.DataFrame(to_write).write_parquet(part_path)
            part_files.append(part_path)
            LOGGER.debug(f"Flushed {len(to_write)} products to {part_path}")

        async def fetch_client_pages(
            config: TcgPlayerConfig, client_offsets: list[int]
        ) -> None:
            nonlocal completed, buffer
            async with TcgPlayerClient(config) as client:
                for offset in client_offsets:
                    try:
                        resp = await client.get_products_page(
                            offset=offset, include_skus=True
                        )
                        products_raw = resp.get("results", [])
                        products = (
                            products_raw if isinstance(products_raw, list) else []
                        )
                        page_products = [
                            {
                                "productId": product["productId"],
                                "name": product.get("name", ""),
                                "cleanName": product.get("cleanName", ""),
                                "groupId": product.get("groupId"),
                                "url": product.get("url", ""),
                                "skus": [
                                    {
                                        "skuId": sku["skuId"],
                                        "languageId": sku["languageId"],
                                        "printingId": sku["printingId"],
                                        "conditionId": sku["conditionId"],
                                    }
                                    for sku in (
                                        product.get("skus", [])
                                        if isinstance(product.get("skus", []), list)
                                        else []
                                    )
                                ],
                            }
                            for product in products
                        ]

                        async with lock:
                            buffer.extend(page_products)
                            completed += 1
                            if len(buffer) >= self.flush_threshold:
                                await flush_buffer()
                            if self.on_progress:
                                self.on_progress(
                                    completed, total_pages, f"offset={offset}"
                                )
                    except Exception as e:
                        LOGGER.warning(f"Failed offset {offset}: {e}")
                        async with lock:
                            completed += 1

        try:
            # Run all clients in parallel
            await asyncio.gather(
                *[
                    fetch_client_pages(config, client_offsets)
                    for config, client_offsets in zip(self.configs, offsets_per_client)
                ]
            )

            # Final flush
            async with lock:
                await flush_buffer()

            LOGGER.info(f"TCGPlayer fetch complete: {len(part_files)} part files")
            return part_files

        except Exception as e:
            LOGGER.error(f"Error during TCGPlayer fetch: {e}")
            raise

    async def _combine_part_files(self, part_files: list[Path]) -> pl.LazyFrame:
        """Combine part files into single output parquet."""
        if not part_files:
            pl.DataFrame(
                schema={
                    "productId": pl.Int64(),
                    "name": pl.String(),
                    "cleanName": pl.String(),
                    "groupId": pl.Int64(),
                    "url": pl.String(),
                    "skus": pl.List(
                        pl.Struct(
                            {
                                "skuId": pl.Int64(),
                                "languageId": pl.Int64(),
                                "printingId": pl.Int64(),
                                "conditionId": pl.Int64(),
                            }
                        )
                    ),
                }
            ).write_parquet(self.output_path)
            return pl.scan_parquet(self.output_path)

        try:
            # Scan and combine all parts
            lf = pl.scan_parquet(
                source=str(self.output_path.parent / ".tcg_part_*.parquet"),
                glob=True,
                rechunk=True,
            )

            # Stream to final output
            lf.sink_parquet(self.output_path)
            LOGGER.info(f"Combined {len(part_files)} parts to {self.output_path}")

        except Exception as e:
            LOGGER.error(f"Error combining part files: {e}")
            raise
        finally:
            # Clean up part files
            for part_file in part_files:
                try:
                    part_file.unlink()
                except OSError as e:
                    LOGGER.warning(f"Failed to delete {part_file}: {e}")

        return pl.scan_parquet(self.output_path)

    # Sync wrapper methods
    def fetch_all_products_sync(self) -> pl.LazyFrame:
        """Synchronous wrapper for fetch_all_products."""
        return asyncio.run(self.fetch_all_products())

    @classmethod
    def create_background_task(
        cls,
        output_path: Path | None = None,
        on_progress: Optional[ProgressCallback] = None,
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
