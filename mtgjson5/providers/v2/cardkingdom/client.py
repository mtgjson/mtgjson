"""Card Kingdom API client."""

import asyncio
import json
import logging
from dataclasses import dataclass

import aiohttp

from .models import ApiResponse, CKRecord

LOGGER = logging.getLogger(__name__)

# API endpoints
CK_API_V1 = "https://api.cardkingdom.com/api/pricelist"
CK_API_V2 = "https://api.cardkingdom.com/api/v2/pricelist"
CK_SEALED = "https://api.cardkingdom.com/api/sealed_pricelist"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


@dataclass
class FetchResult:
    """Result of fetching from a CK endpoint."""

    endpoint: str
    records: list[CKRecord]
    error: Exception | None = None

    @property
    def success(self) -> bool:
        """Return True if fetch was successful."""
        return self.error is None


class CardKingdomClient:
    """
    Async HTTP client for Card Kingdom API.

    Handles:
    - Multiple endpoint fetching (V1, V2, Sealed)
    - HTML-wrapped JSON response parsing (CK API quirk)
    - Error handling per endpoint
    """

    def __init__(
        self,
        headers: dict[str, str] | None = None,
        timeout: float = 120.0,
    ):
        self.headers = headers or DEFAULT_HEADERS
        self.timeout = timeout

    async def fetch_endpoint(self, url: str) -> ApiResponse:
        """
        Fetch data from a single CK API endpoint.

        Handles CK's quirk of returning JSON wrapped in HTML tags
        with incorrect Content-Type header.
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as response:
                response.raise_for_status()
                text = await response.text()

                # CK API sometimes wraps JSON in HTML tags
                if text.startswith("<html>"):
                    text = text.removeprefix("<html><head></head><body>").removesuffix(
                        "</body></html>"
                    )
                data = json.loads(text)
                return ApiResponse.model_validate(data)

    async def fetch_all(
        self,
        include_v1: bool = True,
        include_v2: bool = True,
        include_sealed: bool = True,
    ) -> list[FetchResult]:
        """
        Fetch from all enabled CK API endpoints concurrently.

        Returns list of FetchResult, one per endpoint attempted.
        Failed endpoints have error set but don't prevent others.
        """
        endpoints = []
        if include_v2:
            endpoints.append(CK_API_V2)
        if include_v1:
            endpoints.append(CK_API_V1)
        if include_sealed:
            endpoints.append(CK_SEALED)

        if not endpoints:
            return []

        LOGGER.info(f"Fetching from {len(endpoints)} CK endpoints...")

        tasks = [self.fetch_endpoint(url) for url in endpoints]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for url, response in zip(endpoints, responses, strict=False):
            if isinstance(response, BaseException):
                if isinstance(response, Exception):
                    LOGGER.warning(f"CK API error for {url}: {response}")
                    results.append(
                        FetchResult(endpoint=url, records=[], error=response)
                    )
                continue
            # Check if response.data exists before accessing
            records = response.data if response.data is not None else []
            LOGGER.info(f"Fetched {len(records):,} records from {url}")
            results.append(FetchResult(endpoint=url, records=records))

        return results

    def fetch_all_sync(self, **kwargs: bool) -> list[FetchResult]:
        """Sync wrapper for fetch_all."""
        return asyncio.run(self.fetch_all(**kwargs))
