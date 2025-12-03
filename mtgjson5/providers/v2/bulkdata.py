"""
Bulk Data Downloader
Async streaming download and rapid NDJSON conversion
of Scryfall bulk data files for Polars ingestion.
"""

import asyncio
import gzip
import logging
import pathlib
from io import BytesIO

import aiohttp
import ijson
import orjson


class BulkDataProvider:
    """Downloader for Scryfall bulk data with NDJSON conversion."""

    LOGGER = logging.getLogger(__name__)
    BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

    def __init__(self) -> None:
        pass

    async def get_bulk_download_url(
        self, session: aiohttp.ClientSession, bulk_type: str
    ) -> str:
        """Fetch download URL for a bulk data type."""
        async with session.get(self.BULK_DATA_URL) as response:
            response.raise_for_status()
            data = await response.json()

        for item in data.get("data", []):
            if item["type"] == bulk_type:
                return item["download_uri"]

        raise ValueError(f"Unknown bulk type: {bulk_type}")

    async def download_to_ndjson(
        self,
        session: aiohttp.ClientSession,
        url: str,
        destination: pathlib.Path,
    ) -> pathlib.Path:
        """Download JSON array and stream-convert to NDJSON."""
        self.LOGGER.info(f"Downloading {url}...")

        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.read()

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
        # Ijson (C) is a fast streaming parser for JSON
        with destination.open("wb") as f:
            for item in ijson.items(BytesIO(content), "item"):
                # Orjson's serializer (Rust) is ~10x faster than json.dumps and natively handles some additional types
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
        async with aiohttp.ClientSession() as session:
            # Get all download URLs first
            urls = {
                bulk_type: await self.get_bulk_download_url(session, bulk_type)
                for bulk_type in bulk_types
            }

            # Download concurrently
            tasks = []
            for bulk_type, url in urls.items():
                dest = cache_dir / f"{bulk_type}.ndjson"
                # local caching for dev convenience - wont matter in prod
                if not force_refresh and dest.exists() and dest.stat().st_size > 0:
                    self.LOGGER.info(f"Using cached {bulk_type}")
                    continue
                # send to executor to avoid blocking event loop
                tasks.append(self.download_to_ndjson(session, url, dest))

            if tasks:
                # we waits
                await asyncio.gather(*tasks)

        return {bt: cache_dir / f"{bt}.ndjson" for bt in bulk_types}

    def download_bulk_files_sync(
        self,
        cache_dir: pathlib.Path,
        bulk_types: list[str] = ["all_cards", "rulings"],
        force_refresh: bool = False,
    ) -> dict[str, pathlib.Path]:
        """Sync wrapper for non-async contexts."""
        # convenience method for sync contexts
        return asyncio.run(
            self.download_bulk_files(cache_dir, bulk_types, force_refresh)
        )
