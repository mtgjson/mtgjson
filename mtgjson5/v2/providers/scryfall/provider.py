"""
Scryfall Provider (v2)
Complete Scryfall provider - bulk downloads + API access.

This module provides async streaming download and rapid NDJSON conversion
of Scryfall bulk data files for Polars ingestion, plus API access methods.
"""

import asyncio
import gzip
import logging
import pathlib
import time
from io import BytesIO
from typing import Any

import aiohttp
import ijson
import orjson


class ScryfallProvider:
    """
    Complete Scryfall provider - bulk downloads + API access.

    Combines bulk data downloading with API access methods,
    replacing both the legacy ScryfallProvider and BulkDataProvider.
    """

    LOGGER = logging.getLogger(__name__)
    BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

    # API URLs (from legacy provider)
    ALL_SETS_URL: str = "https://api.scryfall.com/sets/"
    CARDS_URL: str = "https://api.scryfall.com/cards/"
    CARDS_WITHOUT_LIMITS_URL: str = (
        "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20"
        "o:cards%20o:named)%20or%20(o:deck%20o:have%20o:up%20o:to%20o:cards%20o:named)"
    )

    def __init__(self) -> None:
        self._cards_without_limits: set[str] | None = None

    async def get_bulk_download_url(
        self, session: aiohttp.ClientSession, bulk_type: str
    ) -> tuple[str, int]:
        """Fetch download URL and file size for a bulk data type."""
        async with session.get(self.BULK_DATA_URL) as response:
            response.raise_for_status()
            data = await response.json()

        for item in data.get("data", []):
            if item.get("type") == bulk_type:
                download_uri = item.get("download_uri")
                size = item.get("size", 0)
                if download_uri:
                    return str(download_uri), int(size)

        raise ValueError(f"Unknown bulk type: {bulk_type}")

    async def download_to_ndjson(
        self,
        session: aiohttp.ClientSession,
        url: str,
        destination: pathlib.Path,
        total_size: int = 0,
    ) -> pathlib.Path:
        """Download JSON array and stream-convert to NDJSON."""
        size_label = f" ({total_size / (1024**2):.1f} MB)" if total_size else ""
        self.LOGGER.info(f"Downloading {url}{size_label}...")

        chunks = []
        downloaded = 0
        last_log = time.monotonic()
        last_downloaded = 0

        async with session.get(url) as response:
            response.raise_for_status()
            actual_size = total_size or int(response.headers.get("Content-Length", 0))

            async for chunk in response.content.iter_chunked(1024 * 256):
                chunks.append(chunk)
                downloaded += len(chunk)
                now = time.monotonic()
                elapsed = now - last_log
                if actual_size and elapsed >= 10:
                    delta_bytes = downloaded - last_downloaded
                    speed_mbps = (delta_bytes * 8) / (elapsed * 1_000_000)
                    if speed_mbps >= 1:
                        speed_label = f"{speed_mbps:.1f} Mbps"
                    else:
                        speed_label = f"{speed_mbps * 1000:.0f} Kbps"
                    last_log = now
                    last_downloaded = downloaded
                    pct = int(downloaded / actual_size * 100)
                    self.LOGGER.info(
                        f"  Progress: {pct}% "
                        f"({downloaded / (1024**2):.1f}/{actual_size / (1024**2):.1f} MB) "
                        f"@ {speed_label}"
                    )

        content = b"".join(chunks)
        self.LOGGER.info(f"Downloaded {len(content) / (1024**2):.1f} MB, converting...")

        # asyncio.to_thread to offload blocking conversion
        await asyncio.to_thread(self._convert_to_ndjson, content, destination)
        return destination

    def _convert_to_ndjson(self, content: bytes, destination: pathlib.Path) -> None:
        """
        Convert JSON array to NDJSON and save to destination.
        Handles gzip decompression if needed.
        (We need JSONL here due to the variance in Scryfalls bulk data schema.)
        """
        if content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)

        destination.parent.mkdir(parents=True, exist_ok=True)

        count = 0
        with destination.open("wb") as f:
            for item in ijson.items(BytesIO(content), "item"):
                f.write(orjson.dumps(item, default=str))
                f.write(b"\n")
                count += 1

        self.LOGGER.info(f"Saved {count:,} items to {destination}")

    async def download_bulk_files(
        self,
        cache_dir: pathlib.Path,
        bulk_types: list[str],
        force_refresh: bool = False,
    ) -> dict[str, pathlib.Path]:
        """
        Download multiple bulk files concurrently.

        Returns dict mapping bulk_type to file path.
        """
        headers = {
            "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
            "Accept": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=1800)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            # Get all download URLs and sizes first
            bulk_info = {}
            for bulk_type in bulk_types:
                url, size = await self.get_bulk_download_url(session, bulk_type)
                bulk_info[bulk_type] = (url, size)

            # Download concurrently
            tasks = []
            for bulk_type, (url, size) in bulk_info.items():
                dest = cache_dir / f"{bulk_type}.ndjson"
                # local caching for dev convenience - wont matter in prod
                if not force_refresh and dest.exists() and dest.stat().st_size > 0:
                    self.LOGGER.info(f"Using cached {bulk_type}")
                    continue
                # send to executor to avoid blocking event loop
                tasks.append(self.download_to_ndjson(session, url, dest, size))

            if tasks:
                # we waits
                await asyncio.gather(*tasks)

        return {bt: cache_dir / f"{bt}.ndjson" for bt in bulk_types}

    def download_bulk_files_sync(
        self,
        cache_dir: pathlib.Path,
        bulk_types: list[str] | None = None,
        force_refresh: bool = False,
    ) -> dict[str, pathlib.Path]:
        """Sync wrapper for non-async contexts."""
        if bulk_types is None:
            bulk_types = ["all_cards", "rulings"]
        # convenience method for sync contexts
        return asyncio.run(
            self.download_bulk_files(cache_dir, bulk_types, force_refresh)
        )

    @staticmethod
    async def fetch_all_spellbooks() -> dict[str, list[str]]:
        """Fetch all alchemy spellbook mappings from Scryfall."""
        async with aiohttp.ClientSession(
            headers={
                "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
                "Accept": "application/json",
            }
        ) as session:

            async def get_all_pages(url: str | None) -> list[dict]:
                results = []
                while url:
                    print(f"Fetching: {url}")
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        data = await resp.json()
                    if data.get("object") == "error":
                        print(f"Error: {data}")
                        break
                    results.extend(data.get("data", []))
                    url = data.get("next_page") if data.get("has_more") else None
                return results

            async def get_cards_by_ids(ids: list[str]) -> list[dict]:
                """Fetch cards in batches of 75 using collection endpoint."""
                print(f"Fetching {len(ids)} cards in batches of 75...")
                tasks = []
                for i in range(0, len(ids), 75):
                    batch = ids[i : i + 75]
                    identifiers = [{"id": card_id} for card_id in batch]
                    tasks.append(
                        session.post(
                            "https://api.scryfall.com/cards/collection",
                            json={"identifiers": identifiers},
                            timeout=aiohttp.ClientTimeout(total=30),
                        )
                    )

                all_cards = []
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                for resp in responses:
                    if isinstance(resp, BaseException):
                        if isinstance(resp, Exception):
                            print(f"Error fetching batch: {resp}")
                        continue
                    async with resp:
                        data = await resp.json()
                        all_cards.extend(data.get("data", []))
                print(f"Fetched {len(all_cards)} cards")
                return all_cards

            parents_url = "https://api.scryfall.com/cards/search?q=is:alchemy%20and%20oracle:/conjure|draft|%27s%20spellbook/&include_extras=true"
            spellbook_url = 'https://api.scryfall.com/cards/search?q=spellbook:"{}"'

            # Get parent cards
            print("Fetching parent cards...")
            parent_cards = await get_all_pages(parents_url)
            print(f"Found {len(parent_cards)} parent cards")

            # Collect all spellbook card IDs first
            all_spellbook_ids = {}
            for parent in parent_cards:
                parent_name = parent["name"]
                print(f"Fetching spellbook for: {parent_name}")
                spellbook_pages = await get_all_pages(spellbook_url.format(parent_name))
                all_spellbook_ids[parent_name] = [
                    card["id"] for card in spellbook_pages
                ]

            # Fetch all cards by ID in batches
            all_ids = [card_id for ids in all_spellbook_ids.values() for card_id in ids]
            all_cards = await get_cards_by_ids(all_ids)

            # Map IDs to names
            id_to_name = {
                card["id"]: card["name"]
                for card in all_cards
                if "id" in card and "name" in card
            }

            # Build final result
            return {
                parent: [
                    id_to_name[card_id] for card_id in ids if card_id in id_to_name
                ]
                for parent, ids in all_spellbook_ids.items()
            }

    # -------------------------------------------------------------------------
    # API Methods (sync wrappers matching legacy ScryfallProvider interface)
    # -------------------------------------------------------------------------

    async def _fetch_url_async(
        self,
        url: str,
        retry_count: int = 3,
    ) -> dict[str, Any]:
        """Rate-limited async API fetch with retry."""
        headers = {
            "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
            "Accept": "application/json",
        }

        for attempt in range(retry_count):
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    # Rate limit: 10 requests per second
                    await asyncio.sleep(0.1)
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        data: dict[str, Any] = await response.json()
                        return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == retry_count - 1:
                    self.LOGGER.error(
                        f"Failed to fetch {url} after {retry_count} attempts: {e}"
                    )
                    return {
                        "object": "error",
                        "code": "network_error",
                        "details": str(e),
                    }
                self.LOGGER.warning(f"Retry {attempt + 1}/{retry_count} for {url}: {e}")
                await asyncio.sleep(1 * (attempt + 1))

        return {
            "object": "error",
            "code": "max_retries",
            "details": "Max retries exceeded",
        }

    async def _fetch_all_pages_async(
        self,
        starting_url: str,
    ) -> list[dict[str, Any]]:
        """Paginated async API fetch."""
        all_cards: list[dict[str, Any]] = []
        url: str | None = starting_url

        headers = {
            "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            page = 1
            while url:
                self.LOGGER.debug(f"Downloading page {page} -- {url}")
                await asyncio.sleep(0.1)  # Rate limit

                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        data = await response.json()
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    self.LOGGER.warning(f"Failed to fetch page {page}: {e}")
                    break

                if data.get("object") == "error":
                    if data.get("code") != "not_found":
                        self.LOGGER.warning(f"Unable to download {url}: {data}")
                    break

                all_cards.extend(data.get("data", []))

                if not data.get("has_more"):
                    break

                url = data.get("next_page")
                page += 1

        return all_cards

    def download(
        self,
        url: str,
        params: dict[str, str | int] | None = None,  # pylint: disable=unused-argument
        retry_ttl: int = 3,  # pylint: disable=unused-argument
    ) -> Any:
        """
        Sync wrapper for API fetch - matches legacy ScryfallProvider interface.

        :param url: URL to download from
        :param params: Options for URL download (unused, for API compatibility)
        :param retry_ttl: How many times to retry (unused, for API compatibility)
        :return: JSON response data
        """
        return asyncio.run(self._fetch_url_async(url))

    def download_all_pages_api(
        self,
        starting_url: str | None,
        params: dict[str, str | int] | None = None,  # pylint: disable=unused-argument
    ) -> list[dict[str, Any]]:
        """
        Sync wrapper for paginated API fetch - matches legacy ScryfallProvider interface.

        :param starting_url: First page URL
        :param params: Options for URL download (unused, for API compatibility)
        :return: List of all card/item objects across all pages
        """
        if starting_url is None:
            return []
        return asyncio.run(self._fetch_all_pages_async(starting_url))

    @property
    def cards_without_limits(self) -> set[str]:
        """
        Property returning cards that can have unlimited copies in a deck.
        Matches legacy ScryfallProvider interface.
        """
        if self._cards_without_limits is None:
            cards = self.download(self.CARDS_WITHOUT_LIMITS_URL)
            self._cards_without_limits = {
                card["name"] for card in cards.get("data", [])
            }
        return self._cards_without_limits


# Alias for backwards compatibility during migration
BulkDataProvider = ScryfallProvider
