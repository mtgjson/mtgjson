"""TCGPlayer async HTTP client with connection pooling and retry logic."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from .provider import TcgPlayerConfig

LOGGER = logging.getLogger(__name__)

# Request settings
CONCURRENT_REQUESTS = 75
MAX_RETRIES = 3
RETRY_DELAY = 2.0
REQUEST_TIMEOUT = 30.0


@dataclass
class ProductsPage:
    """Result of fetching a products page."""

    products: list[dict]
    total_items: int
    offset: int
    success: bool
    error: Exception | None = None


class TcgPlayerClient:
    """
    Async TCGPlayer API client.

    Handles:
    - OAuth authentication
    - Connection pooling
    - Automatic retries with exponential backoff
    - Rate limit handling (429 responses)
    """

    def __init__(
        self,
        config: TcgPlayerConfig,
        concurrent_limit: int = CONCURRENT_REQUESTS,
        timeout: float = REQUEST_TIMEOUT,
    ):
        self.config = config
        self.concurrent_limit = concurrent_limit
        self.timeout = timeout
        self.access_token: str | None = None
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "TcgPlayerClient":
        connector = aiohttp.TCPConnector(limit=self.concurrent_limit)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )
        await self.authenticate()
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def authenticate(self) -> str:
        """
        Obtain bearer token from TCGPlayer OAuth endpoint.

        Returns the access token.
        """
        if self._session is None:
            raise RuntimeError("Client not initialized. Use async context manager.")

        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.public_key,
            "client_secret": self.config.private_key,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with self._session.post(
            self.config.token_url,
            data=data,
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            result = await resp.json()
            self.access_token = result["access_token"]
            LOGGER.debug("TCGPlayer authentication successful")
            return self.access_token

    async def _request(
        self,
        method: str,
        endpoint: str,
        versioned: bool = True,
        **kwargs: Any,
    ) -> dict[str, object]:
        """
        Execute authenticated request with retry logic.

        Handles:
        - 429 rate limits with Retry-After header
        - Transient errors with exponential backoff
        """
        if self._session is None:
            raise RuntimeError("Client not initialized")
        if self.access_token is None:
            await self.authenticate()

        url = self.config.endpoint_url(endpoint, versioned)
        headers = {
            "Accept": "application/json",
            "Authorization": f"bearer {self.access_token}",
        }

        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.request(
                    method,
                    url,
                    headers=headers,
                    **kwargs,
                ) as resp:
                    # Handle rate limiting
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", RETRY_DELAY * (attempt + 1)))
                        LOGGER.warning(f"Rate limited, waiting {retry_after:.1f}s")
                        await asyncio.sleep(retry_after)
                        continue

                    resp.raise_for_status()
                    result: dict[str, object] = await resp.json()
                    return result

            except (TimeoutError, aiohttp.ClientError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (attempt + 1)
                    LOGGER.debug(f"Retry {attempt + 1}/{MAX_RETRIES} for {endpoint}: {e}")
                    await asyncio.sleep(delay)

        raise last_error or aiohttp.ClientError(f"Failed after {MAX_RETRIES} retries")

    async def get(self, endpoint: str, versioned: bool = True) -> dict[str, object]:
        """Execute GET request."""
        return await self._request("GET", endpoint, versioned)

    async def get_products_page(
        self,
        category_id: int = 1,
        product_types: str = "Cards",
        offset: int = 0,
        limit: int = 100,
        include_skus: bool = True,
    ) -> ProductsPage:
        """
        Fetch a page of Magic card products.

        Args:
            category_id: TCGPlayer category (1 = Magic)
            product_types: Filter by product type
            offset: Pagination offset
            limit: Page size (max 100)
            include_skus: Include nested SKU data
        """
        endpoint = (
            f"catalog/products?categoryId={category_id}&productTypes={product_types}&limit={limit}&offset={offset}"
        )
        if include_skus:
            endpoint += "&includeSkus=true"

        try:
            resp = await self.get(endpoint, versioned=False)
            results = resp.get("results", [])
            total_items = resp.get("totalItems", 0)
            return ProductsPage(
                products=results if isinstance(results, list) else [],
                total_items=(int(total_items) if isinstance(total_items, (int, float)) else 0),
                offset=offset,
                success=True,
            )
        except Exception as e:
            LOGGER.warning(f"Failed to fetch offset {offset}: {e}")
            return ProductsPage(
                products=[],
                total_items=0,
                offset=offset,
                success=False,
                error=e,
            )

    async def get_total_products(
        self,
        category_id: int = 1,
        product_types: str = "Cards",
    ) -> int:
        """Get total count of products matching filters."""
        page = await self.get_products_page(
            category_id=category_id,
            product_types=product_types,
            offset=0,
            limit=1,
            include_skus=False,
        )
        return page.total_items

    async def get_group_details(self, group_id: int) -> dict[str, object]:
        """Get set/group details by ID."""
        return await self.get(f"catalog/groups/{group_id}", versioned=False)

    async def get_product_details(self, product_id: int) -> dict[str, object]:
        """Get single product details."""
        return await self.get(f"catalog/products/{product_id}", versioned=False)
