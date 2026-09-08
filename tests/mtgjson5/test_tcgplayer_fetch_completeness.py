"""Tests that a partial TCGPlayer catalog fetch never reaches the build.

A page that fails silently removes ~100 products from the catalog, and every SKU
of every card mapped to those products vanishes from TcgplayerSkus.json.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import polars as pl
import pytest

from mtgjson5.providers.tcgplayer import provider as provider_mod
from mtgjson5.providers.tcgplayer.provider import (
    PRODUCTS_PER_PAGE,
    TcgPlayerClient,
    TcgPlayerIncompleteFetchError,
    TCGProvider,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _catalog(size: int) -> list[dict]:
    return [
        {
            "productId": pid,
            "name": f"Card {pid}",
            "cleanName": f"Card {pid}",
            "groupId": 1,
            "url": f"/product/{pid}",
            "skus": [{"skuId": pid * 10, "languageId": 1, "printingId": 1, "conditionId": 1}],
        }
        for pid in range(size)
    ]


class FakeApi:
    """Serves a catalog by offset, failing chosen offsets a set number of times."""

    def __init__(self, products: list[dict], failures: dict[int, int] | None = None):
        self.products = products
        self.failures = dict(failures or {})
        self.short_pages: set[int] = set()
        self.requested: list[int] = []

    def page(self, offset: int) -> list[dict]:
        self.requested.append(offset)
        remaining = self.failures.get(offset, 0)
        if remaining:
            self.failures[offset] = remaining - 1
            raise ConnectionError(f"boom at {offset}")
        if offset in self.short_pages:
            return []
        return self.products[offset : offset + PRODUCTS_PER_PAGE]


class FakeClient:
    """Stands in for TcgPlayerClient inside TCGProvider.fetch_all_products."""

    api: FakeApi

    def __init__(self, config: object):
        self.config = config

    async def __aenter__(self) -> FakeClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def get_total_products(self, product_types: str = "Cards") -> int:
        return len(self.api.products)

    async def get_products_page(
        self,
        category_id: int = 1,
        product_types: str = "Cards",
        offset: int = 0,
        limit: int = PRODUCTS_PER_PAGE,
        include_skus: bool = True,
    ) -> dict[str, object]:
        return {"totalItems": len(self.api.products), "results": self.api.page(offset)}


@pytest.fixture
def make_provider(tmp_path, monkeypatch):
    def _make(api: FakeApi) -> TCGProvider:
        FakeClient.api = api
        monkeypatch.setattr(provider_mod, "TcgPlayerClient", FakeClient)
        return TCGProvider(
            output_path=tmp_path / "tcg_skus.parquet",
            configs=[SimpleNamespace(public_key="a", private_key="b")],
        )

    return _make


def _part_files(directory) -> list:
    return sorted(directory.glob(".tcg_part_*.parquet"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFetchCompleteness:
    def test_full_fetch_writes_every_product(self, make_provider, tmp_path):
        provider = make_provider(FakeApi(_catalog(500)))

        lf = provider.fetch_all_products_sync()

        assert lf.collect().height == 500
        assert _part_files(tmp_path) == []

    def test_transient_page_failure_is_retried(self, make_provider, tmp_path):
        api = FakeApi(_catalog(500), failures={200: 1})
        provider = make_provider(api)

        lf = provider.fetch_all_products_sync()

        # The offset that failed in the main pass came back in the retry sweep.
        assert api.requested.count(200) == 2
        df = lf.collect()
        assert df.height == 500
        assert sorted(df["productId"].to_list()) == list(range(500))

    def test_persistent_page_failure_raises(self, make_provider):
        provider = make_provider(FakeApi(_catalog(500), failures={200: 99}))

        with pytest.raises(TcgPlayerIncompleteFetchError, match="still failed after retries"):
            provider.fetch_all_products_sync()

    def test_persistent_failure_keeps_previous_catalog(self, make_provider, tmp_path):
        output_path = tmp_path / "tcg_skus.parquet"
        pl.DataFrame({"productId": [1, 2, 3]}).write_parquet(output_path)
        provider = make_provider(FakeApi(_catalog(500), failures={200: 99}))

        with pytest.raises(TcgPlayerIncompleteFetchError):
            provider.fetch_all_products_sync()

        # Yesterday's catalog survives, and no part files are left behind.
        assert pl.read_parquet(output_path)["productId"].to_list() == [1, 2, 3]
        assert _part_files(tmp_path) == []

    def test_short_page_trips_the_completeness_check(self, make_provider):
        api = FakeApi(_catalog(500))
        api.short_pages.add(300)
        provider = make_provider(api)

        # Every page answers, but one comes back empty - 400 of 500 products.
        with pytest.raises(TcgPlayerIncompleteFetchError, match="refusing to publish a truncated catalog"):
            provider.fetch_all_products_sync()

    def test_empty_catalog_raises(self, make_provider):
        provider = make_provider(FakeApi([]))

        with pytest.raises(TcgPlayerIncompleteFetchError, match="0 products"):
            provider.fetch_all_products_sync()


# ---------------------------------------------------------------------------
# Rate limit handling
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status: int, payload: dict | None = None):
        self.status = status
        self.headers = {"Retry-After": "0"}
        self._payload = payload or {}

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url: str, headers: dict | None = None) -> FakeResponse:
        self.calls += 1
        return self._responses.pop(0)


class TestRateLimitRetries:
    def test_waits_out_repeated_rate_limits(self):
        """A 429 is a 'come back later', not one of the three error retries."""
        client = TcgPlayerClient(SimpleNamespace(base_url="https://x", api_version="v1"))
        client._session = FakeSession([FakeResponse(429) for _ in range(5)] + [FakeResponse(200, {"totalItems": 7})])

        result = asyncio.run(client._get("catalog/products", versioned=False))

        assert result == {"totalItems": 7}
        assert client._session.calls == 6

    def test_gives_up_after_the_rate_limit_budget(self, monkeypatch):
        monkeypatch.setattr(provider_mod, "MAX_RATE_LIMIT_WAITS", 2)
        client = TcgPlayerClient(SimpleNamespace(base_url="https://x", api_version="v1"))
        client._session = FakeSession([FakeResponse(429) for _ in range(4)])

        with pytest.raises(Exception, match="Still rate limited"):
            asyncio.run(client._get("catalog/products", versioned=False))


class TestLastGoodCatalogFallback:
    def test_falls_back_to_the_catalog_on_disk(self, tmp_path):
        from mtgjson5.data.cache import GlobalCache

        pl.DataFrame({"productId": [11, 22]}).write_parquet(tmp_path / "tcg_skus.parquet")

        lf = GlobalCache._last_good_tcg_skus(SimpleNamespace(cache_path=tmp_path))

        assert lf.collect()["productId"].to_list() == [11, 22]

    def test_empty_frame_when_nothing_cached(self, tmp_path):
        from mtgjson5.data.cache import GlobalCache

        lf = GlobalCache._last_good_tcg_skus(SimpleNamespace(cache_path=tmp_path))

        assert lf.collect().height == 0
        assert "skus" in lf.collect_schema()
