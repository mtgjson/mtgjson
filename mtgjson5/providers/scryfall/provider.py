"""
Scryfall Provider (v2)
Complete Scryfall provider - bulk downloads + API access.

This module provides async streaming download and rapid NDJSON conversion
of Scryfall bulk data files for Polars ingestion, plus API access methods.
"""

import asyncio
import datetime
import gzip
import json
import logging
import os
import pathlib
import time
from typing import IO, Any, cast

import aiohttp
import ijson
import orjson

from mtgjson5 import constants


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
    TYPE_CATALOG_URL: str = "https://api.scryfall.com/catalog/{0}"

    # Scryfall rate limits (as of 2026-04):
    #   /cards/search, /cards/named, /cards/random, /cards/collection = 2/sec (500ms)
    #   All other endpoints = 10/sec (100ms)
    SLOW_ENDPOINT_PREFIXES: tuple[str, ...] = (
        "/cards/search",
        "/cards/named",
        "/cards/random",
        "/cards/collection",
    )
    SLOW_DELAY: float = 0.5  # 500ms for card endpoints
    FAST_DELAY: float = 0.1  # 100ms for everything else
    RATE_LIMIT_BACKOFF_BASE: float = 2.0
    RATE_LIMIT_MAX_RETRIES: int = 5

    # Only used when Scryfall omits `updated_at` for a bulk entry
    BULK_MAX_AGE_HOURS: float = 24.0

    def __init__(self) -> None:
        self._cards_without_limits: set[str] | None = None
        self._rate_limiter: asyncio.Semaphore = asyncio.Semaphore(2)

    def _delay_for_url(self, url: str) -> float:
        """Return the appropriate rate-limit delay based on the Scryfall endpoint."""
        path = url.split("api.scryfall.com", 1)[-1].split("?")[0] if "api.scryfall.com" in url else url
        for prefix in self.SLOW_ENDPOINT_PREFIXES:
            if path.startswith(prefix):
                return self.SLOW_DELAY
        return self.FAST_DELAY

    async def _rate_limited_get(
        self,
        session: aiohttp.ClientSession,
        url: str,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
        """Make a GET request respecting Scryfall's endpoint-specific rate limits with 429 backoff."""
        delay = self._delay_for_url(url)
        async with self._rate_limiter:
            await asyncio.sleep(delay)
            response = await session.get(url, **kwargs)

            retries = 0
            while response.status == 429 and retries < self.RATE_LIMIT_MAX_RETRIES:
                response.release()
                wait = self.RATE_LIMIT_BACKOFF_BASE ** (retries + 1)
                self.LOGGER.warning(
                    f"Rate limited by Scryfall (429). Backing off {wait:.0f}s (attempt {retries + 1}/{self.RATE_LIMIT_MAX_RETRIES})..."
                )
                await asyncio.sleep(wait)
                response = await session.get(url, **kwargs)
                retries += 1

            return response

    @staticmethod
    def _select_download_uri(item: dict[str, Any]) -> str | None:
        """Prefer the gzipped JSONL export; fall back to the legacy JSON array."""
        uri = item.get("jsonl_download_uri") or item.get("download_uri")
        return str(uri) if uri else None

    @staticmethod
    def _parse_updated_at(item: dict[str, Any]) -> datetime.datetime | None:
        """Read the timestamp Scryfall cut a bulk dump at, or None if it is unusable."""
        raw = item.get("updated_at")
        if not raw:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.UTC)
        return parsed

    def _cached_dump_is_current(
        self,
        destination: pathlib.Path,
        updated_at: datetime.datetime | None,
    ) -> bool:
        """
        Decide whether the on-disk NDJSON already holds Scryfall's newest dump.

        Downloaded files are stamped with the dump's own `updated_at` (see
        download_to_ndjson), so an mtime at or past the remote timestamp means we
        already have that dump. Files written before that stamping existed carry
        their download time, which is still later than the dump they came from.
        """
        if not destination.exists() or destination.stat().st_size == 0:
            return False
        if updated_at is None:
            age_hours = (time.time() - destination.stat().st_mtime) / 3600
            return age_hours < self.BULK_MAX_AGE_HOURS
        return destination.stat().st_mtime >= updated_at.timestamp()

    async def get_bulk_metadata(self, session: aiohttp.ClientSession) -> dict[str, dict[str, Any]]:
        """Fetch Scryfall's bulk data catalog, keyed by bulk type."""
        response = await self._rate_limited_get(session, self.BULK_DATA_URL)
        async with response:
            response.raise_for_status()
            data = await response.json()

        return {item["type"]: item for item in data.get("data", []) if item.get("type")}

    async def get_bulk_download_url(self, session: aiohttp.ClientSession, bulk_type: str) -> str:
        """Fetch the download URL for a bulk data type."""
        item = (await self.get_bulk_metadata(session)).get(bulk_type)
        download_uri = self._select_download_uri(item) if item else None
        if not download_uri:
            raise ValueError(f"Unknown bulk type: {bulk_type}")
        return download_uri

    async def download_to_ndjson(
        self,
        session: aiohttp.ClientSession,
        url: str,
        destination: pathlib.Path,
        updated_at: datetime.datetime | None = None,
    ) -> pathlib.Path:
        """Stream a bulk file to a temp file, then convert it to NDJSON on disk."""
        self.LOGGER.info(f"Downloading {url}...")

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp = destination.with_name(destination.name + ".part")

        downloaded = 0
        last_log = time.monotonic()
        last_downloaded = 0

        async with session.get(url) as response:
            response.raise_for_status()
            actual_size = int(response.headers.get("Content-Length", 0))

            with tmp.open("wb") as f:
                async for chunk in response.content.iter_chunked(1024 * 256):
                    f.write(chunk)
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

        self.LOGGER.info(f"Downloaded {downloaded / (1024**2):.1f} MB, extracting...")

        # asyncio.to_thread to offload the blocking file conversion
        await asyncio.to_thread(self._convert_file_to_ndjson, tmp, destination)
        tmp.unlink(missing_ok=True)

        # Stamp the file with the dump's own timestamp so later builds can tell
        # which Scryfall snapshot they are sitting on
        if updated_at is not None:
            stamp = updated_at.timestamp()
            os.utime(destination, (stamp, stamp))

        return destination

    @staticmethod
    def _open_maybe_gzip(path: pathlib.Path) -> IO[bytes]:
        """Open ``path`` for binary reading, transparently decompressing gzip files."""
        with open(path, "rb") as probe:
            magic = probe.read(2)
        if magic == b"\x1f\x8b":
            return cast("IO[bytes]", gzip.open(path, "rb"))
        return open(path, "rb")

    def _convert_file_to_ndjson(self, source: pathlib.Path, destination: pathlib.Path) -> int:
        """Convert a bulk file (JSONL or legacy JSON array, gzipped or plain) to NDJSON."""
        destination.parent.mkdir(parents=True, exist_ok=True)

        # Sniff the first non-whitespace byte to tell a JSON array from JSONL.
        with self._open_maybe_gzip(source) as probe:
            head = probe.read(64).lstrip()
        is_json_array = head[:1] == b"["

        count = 0
        with self._open_maybe_gzip(source) as src, destination.open("wb") as dst:
            if is_json_array:
                for item in ijson.items(src, "item"):
                    dst.write(orjson.dumps(item, default=str))
                    dst.write(b"\n")
                    count += 1
            else:
                for line in src:
                    line = line.strip()
                    if not line:
                        continue
                    dst.write(line)
                    dst.write(b"\n")
                    count += 1

        self.LOGGER.info(f"Saved {count:,} items to {destination}")
        return count

    async def download_bulk_files(
        self,
        cache_dir: pathlib.Path,
        bulk_types: list[str],
        force_refresh: bool = False,
    ) -> dict[str, pathlib.Path]:
        """
        Download multiple bulk files concurrently.

        A cached file is reused only when it holds the newest dump Scryfall has
        published; anything older is re-downloaded. Without that check a build
        pins itself to whatever snapshot happened to be on disk, so passthrough
        identifiers appear to flip back and forth between builds.

        Returns dict mapping bulk_type to file path.
        """
        headers = {
            "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
            "Accept": "application/json",
        }
        results = {bt: cache_dir / f"{bt}.ndjson" for bt in bulk_types}
        timeout = aiohttp.ClientTimeout(total=1800)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            try:
                catalog = await self.get_bulk_metadata(session)
            except (TimeoutError, aiohttp.ClientError) as exc:
                usable = all(path.exists() and path.stat().st_size > 0 for path in results.values())
                if force_refresh or not usable:
                    raise
                self.LOGGER.warning(
                    f"Could not reach the Scryfall bulk catalog ({exc}); falling back to cached bulk data"
                )
                return results

            tasks = []
            for bulk_type in bulk_types:
                item = catalog.get(bulk_type)
                if not item:
                    raise ValueError(f"Unknown bulk type: {bulk_type}")
                url = self._select_download_uri(item)
                if not url:
                    raise ValueError(f"No download URI for bulk type: {bulk_type}")

                dest = results[bulk_type]
                updated_at = self._parse_updated_at(item)
                dump = updated_at.isoformat() if updated_at else "unknown timestamp"
                if not force_refresh and self._cached_dump_is_current(dest, updated_at):
                    self.LOGGER.info(f"Using cached {bulk_type} (Scryfall dump {dump})")
                    continue

                self.LOGGER.info(f"Refreshing {bulk_type} to Scryfall dump {dump}")
                # send to executor to avoid blocking event loop
                tasks.append(self.download_to_ndjson(session, url, dest, updated_at))

            if tasks:
                # we waits
                await asyncio.gather(*tasks)

        return results

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
        return asyncio.run(self.download_bulk_files(cache_dir, bulk_types, force_refresh))

    async def fetch_all_spellbooks(self) -> dict[str, list[str]]:
        """Fetch all alchemy spellbook mappings from Scryfall."""
        headers = {
            "User-Agent": "MTGJSON/5.0 (https://mtgjson.com)",
            "Accept": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(headers=headers) as session:

            async def get_all_pages(url: str | None) -> list[dict]:
                results = []
                while url:
                    self.LOGGER.info(f"Fetching: {url}")
                    response = await self._rate_limited_get(session, url, timeout=timeout)
                    async with response:
                        data = await response.json()
                    if data.get("object") == "error":
                        if data.get("code") == "rate_limited":
                            wait = self.RATE_LIMIT_BACKOFF_BASE**2
                            self.LOGGER.warning(f"Rate limited by Scryfall (response body). Backing off {wait:.0f}s...")
                            await asyncio.sleep(wait)
                            continue
                        self.LOGGER.warning(f"Error: {data}")
                        break
                    results.extend(data.get("data", []))
                    url = data.get("next_page") if data.get("has_more") else None
                return results

            async def get_cards_by_ids(ids: list[str]) -> list[dict]:
                """Fetch cards in batches of 75 using collection endpoint (500ms rate limit)."""
                self.LOGGER.info(f"Fetching {len(ids)} cards in batches of 75...")
                all_cards = []
                collection_url = "https://api.scryfall.com/cards/collection"
                for i in range(0, len(ids), 75):
                    batch = ids[i : i + 75]
                    identifiers = [{"id": card_id} for card_id in batch]
                    async with self._rate_limiter:
                        await asyncio.sleep(self.SLOW_DELAY)
                        try:
                            retries = 0
                            async with session.post(
                                collection_url,
                                json={"identifiers": identifiers},
                                timeout=timeout,
                            ) as resp:
                                if resp.status == 429:
                                    while resp.status == 429 and retries < self.RATE_LIMIT_MAX_RETRIES:
                                        wait = self.RATE_LIMIT_BACKOFF_BASE ** (retries + 1)
                                        self.LOGGER.warning(
                                            f"Rate limited on collection endpoint (429). "
                                            f"Backing off {wait:.0f}s (attempt {retries + 1}/{self.RATE_LIMIT_MAX_RETRIES})..."
                                        )
                                        await asyncio.sleep(wait)
                                        retries += 1
                                    if retries >= self.RATE_LIMIT_MAX_RETRIES:
                                        self.LOGGER.warning(f"Max retries exceeded for batch starting at {i}")
                                        continue
                                    async with session.post(
                                        collection_url,
                                        json={"identifiers": identifiers},
                                        timeout=timeout,
                                    ) as retry_resp:
                                        data = await retry_resp.json()
                                        all_cards.extend(data.get("data", []))
                                else:
                                    data = await resp.json()
                                    all_cards.extend(data.get("data", []))
                        except (TimeoutError, aiohttp.ClientError) as e:
                            self.LOGGER.warning(f"Error fetching batch: {e}")
                self.LOGGER.info(f"Fetched {len(all_cards)} cards")
                return all_cards

            parents_url = "https://api.scryfall.com/cards/search?q=is:alchemy%20and%20oracle:/conjure|draft|%27s%20spellbook/&include_extras=true"
            spellbook_url = 'https://api.scryfall.com/cards/search?q=spellbook:"{}"'

            # Load skip-list of cards known to have no spellbook results on Scryfall
            skip_list: set[str] = set()
            skip_file = constants.RESOURCE_PATH / "spellbook_no_results.json"
            if skip_file.exists():
                with skip_file.open("rb") as f:
                    skip_list = set(json.loads(f.read()))
                self.LOGGER.info(f"Loaded spellbook skip-list: {len(skip_list)} cards")

            # Get parent cards
            self.LOGGER.info("Fetching parent cards...")
            parent_cards = await get_all_pages(parents_url)
            self.LOGGER.info(f"Found {len(parent_cards)} parent cards")

            # Collect all spellbook card IDs first
            all_spellbook_ids = {}
            skipped = 0
            for parent in parent_cards:
                parent_name = parent["name"]
                if parent_name in skip_list:
                    skipped += 1
                    continue
                self.LOGGER.info(f"Fetching spellbook for: {parent_name}")
                spellbook_pages = await get_all_pages(spellbook_url.format(parent_name))
                all_spellbook_ids[parent_name] = [card["id"] for card in spellbook_pages]
            if skipped:
                self.LOGGER.info(f"Skipped {skipped} cards with no known spellbook results")

            # Fetch all cards by ID in batches
            all_ids = [card_id for ids in all_spellbook_ids.values() for card_id in ids]
            all_cards = await get_cards_by_ids(all_ids)

            # Map IDs to names
            id_to_name = {card["id"]: card["name"] for card in all_cards if "id" in card and "name" in card}

            # Build final result
            return {
                parent: [id_to_name[card_id] for card_id in ids if card_id in id_to_name]
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
                    response = await self._rate_limited_get(session, url, timeout=aiohttp.ClientTimeout(total=30))
                    async with response:
                        data: dict[str, Any] = await response.json()
                        return data
            except (TimeoutError, aiohttp.ClientError) as e:
                if attempt == retry_count - 1:
                    self.LOGGER.error(f"Failed to fetch {url} after {retry_count} attempts: {e}")
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

                try:
                    response = await self._rate_limited_get(session, url, timeout=aiohttp.ClientTimeout(total=30))
                    async with response:
                        data = await response.json()
                except (TimeoutError, aiohttp.ClientError) as e:
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
            self._cards_without_limits = {card["name"] for card in cards.get("data", [])}
        return self._cards_without_limits

    def get_catalog_entry(self, catalog_key: str) -> list[str]:
        """Fetch Scryfall catalog data.

        Catalogs include: ability-words, keyword-abilities, keyword-actions,
        artifact-types, battle-types, creature-types, enchantment-types,
        land-types, planeswalker-types, spell-types, etc.

        Args:
            catalog_key: The catalog type to fetch (e.g., "keyword-abilities")

        Returns:
            List of catalog entries, or empty list on error
        """
        catalog_data = self.download(self.TYPE_CATALOG_URL.format(catalog_key))
        if catalog_data.get("object") == "error":
            self.LOGGER.error(f"Unable to fetch catalog {catalog_key}: not found")
            return []
        return list(catalog_data.get("data", []))


# Alias for backwards compatibility during migration
BulkDataProvider = ScryfallProvider
