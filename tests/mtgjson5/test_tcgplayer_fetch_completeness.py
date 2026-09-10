"""Tests that a partial TCGPlayer catalog fetch never reaches the build.

A page that fails silently removes ~100 products from the catalog, and every SKU
of every card mapped to those products vanishes from TcgplayerSkus.json.
"""

from __future__ import annotations

import asyncio
import io
import json
import lzma
import os
import time
from concurrent.futures import Future
from types import SimpleNamespace
from typing import cast

import aiohttp
import polars as pl
import pytest
import requests

from mtgjson5.providers.tcgplayer import provider as provider_mod
from mtgjson5.providers.tcgplayer import published as published_mod
from mtgjson5.providers.tcgplayer.provider import (
    PRODUCTS_PER_PAGE,
    TcgPlayerCatalogUnavailableError,
    TcgPlayerClient,
    TcgPlayerConfig,
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
        self.malformed_pages: set[int] = set()
        self.resultless_pages: set[int] = set()
        self.requested: list[int] = []

    def page(self, offset: int) -> list[dict]:
        self.requested.append(offset)
        remaining = self.failures.get(offset, 0)
        if remaining:
            self.failures[offset] = remaining - 1
            raise aiohttp.ClientError(f"boom at {offset}")
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
        if offset in self.api.malformed_pages:
            self.api.requested.append(offset)
            return {"totalItems": len(self.api.products), "results": {"unexpected": "shape"}}
        if offset in self.api.resultless_pages:
            self.api.requested.append(offset)
            return {"totalItems": len(self.api.products), "errors": ["something went wrong"]}
        return {"totalItems": len(self.api.products), "results": self.api.page(offset)}


@pytest.fixture
def make_provider(tmp_path, monkeypatch):
    def _make(api: FakeApi, published_catalog_url: str | None = None) -> TCGProvider:
        FakeClient.api = api
        monkeypatch.setattr(provider_mod, "TcgPlayerClient", FakeClient)
        return TCGProvider(
            output_path=tmp_path / "tcg_skus.parquet",
            configs=[TcgPlayerConfig(public_key="a", private_key="b")],
            published_catalog_url=published_catalog_url,
        )

    return _make


def _part_files(directory) -> list:
    return sorted(directory.glob(".tcg_part_*.parquet"))


class PublishedSkus:
    """Stands in for mtgjson.com serving the last published TcgplayerSkus.json."""

    url = "https://mtgjson.test/TcgplayerSkus.json.xz"

    def __init__(self, monkeypatch):
        self.downloads = 0
        self._body: bytes | None = None
        self._error: Exception | None = None
        monkeypatch.setattr(published_mod.requests, "get", self._get)
        monkeypatch.setattr(published_mod, "MIN_PUBLISHED_PRODUCTS", 1)

    @staticmethod
    def payload(products: dict[int, list[int]], uuids_per_product: int = 1) -> bytes:
        """Build a TcgplayerSkus.json body.

        The published file is keyed by UUID, so one product appears once per card
        it maps to.
        """
        data = {
            f"uuid-{product_id}-{copy}": [
                {
                    "condition": "NEAR MINT",
                    "language": "ENGLISH",
                    "printing": "NON FOIL",
                    "productId": product_id,
                    "skuId": sku_id,
                }
                for sku_id in sku_ids
            ]
            for product_id, sku_ids in products.items()
            for copy in range(uuids_per_product)
        }
        return json.dumps({"meta": {"date": "2026-09-09"}, "data": data}).encode()

    def serve(self, products: dict[int, list[int]], uuids_per_product: int = 1) -> None:
        self._body = lzma.compress(self.payload(products, uuids_per_product))

    def fail(self, error: Exception) -> None:
        self._error = error

    def _get(self, url: str, **kwargs: object) -> _Response:
        self.downloads += 1
        if self._error is not None:
            raise self._error
        assert self._body is not None, "nothing published"
        return _Response(self._body)


class _Raw(io.BytesIO):
    """A urllib3 raw stream stand-in, which carries a decode_content flag."""

    decode_content = False


class _Response:
    """A streamed requests response, closed by the caller's with-block."""

    def __init__(self, body: bytes):
        self.raw = _Raw(body)
        self.closed = False

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        return None


@pytest.fixture
def published(monkeypatch) -> PublishedSkus:
    return PublishedSkus(monkeypatch)


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


class TestRegressionAgainstPreviousCatalog:
    def test_a_growing_catalog_is_accepted(self, make_provider, tmp_path):
        make_provider(FakeApi(_catalog(400))).fetch_all_products_sync()
        provider = make_provider(FakeApi(_catalog(500)))

        assert provider.fetch_all_products_sync().collect().height == 500

    def test_a_shrinking_catalog_is_rejected(self, make_provider, tmp_path):
        make_provider(FakeApi(_catalog(500))).fetch_all_products_sync()
        provider = make_provider(FakeApi(_catalog(300)))

        with pytest.raises(TcgPlayerIncompleteFetchError, match="500 to 300 products"):
            provider.fetch_all_products_sync()

        assert pl.read_parquet(tmp_path / "tcg_skus.parquet").height == 500

    def test_products_that_lose_their_skus_are_rejected(self, make_provider, tmp_path):
        """Every page can answer in full and still come back stripped of SKUs."""
        make_provider(FakeApi(_catalog(500))).fetch_all_products_sync()

        stripped = _catalog(500)
        for product in stripped[:100]:
            product["skus"] = []
        provider = make_provider(FakeApi(stripped))

        with pytest.raises(TcgPlayerIncompleteFetchError, match="500 to 400 skus"):
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


def _client_with(responses: list[FakeResponse]) -> tuple[TcgPlayerClient, FakeSession]:
    client = TcgPlayerClient(TcgPlayerConfig(public_key="a", private_key="b"))
    session = FakeSession(responses)
    client._session = cast("aiohttp.ClientSession", session)
    return client, session


class TestRateLimitRetries:
    def test_waits_out_repeated_rate_limits(self):
        """A 429 is a 'come back later', not one of the three error retries."""
        client, session = _client_with([FakeResponse(429) for _ in range(5)] + [FakeResponse(200, {"totalItems": 7})])

        result = asyncio.run(client._get("catalog/products", versioned=False))

        assert result == {"totalItems": 7}
        assert session.calls == 6

    def test_gives_up_after_the_rate_limit_budget(self, monkeypatch):
        monkeypatch.setattr(provider_mod, "MAX_RATE_LIMIT_WAITS", 2)
        client, _ = _client_with([FakeResponse(429) for _ in range(4)])

        with pytest.raises(Exception, match="Still rate limited"):
            asyncio.run(client._get("catalog/products", versioned=False))


class TestLastGoodCatalogFallback:
    """The nightly starts from an empty cache directory, so every test here does too."""

    def test_falls_back_to_the_catalog_on_disk(self, tmp_path, make_provider):
        from mtgjson5.data.cache import GlobalCache

        pl.DataFrame({"productId": [11, 22]}).write_parquet(tmp_path / "tcg_skus.parquet")
        provider = make_provider(FakeApi([]))

        lf = GlobalCache._last_good_tcg_skus(SimpleNamespace(tcgplayer=provider), RuntimeError("fetch failed"))

        assert lf.collect()["productId"].to_list() == [11, 22]

    def test_falls_back_to_the_published_catalog_on_a_fresh_container(self, tmp_path, make_provider, published):
        from mtgjson5.data.cache import GlobalCache

        provider = make_provider(FakeApi([]), published_catalog_url=published.url)
        published.serve({10: [100, 101], 11: [110]})

        lf = GlobalCache._last_good_tcg_skus(SimpleNamespace(tcgplayer=provider), RuntimeError("fetch failed"))

        assert lf.collect()["productId"].to_list() == [10, 11]

    def test_no_catalog_anywhere_stops_the_build(self, tmp_path, make_provider):
        from mtgjson5.data.cache import GlobalCache

        provider = make_provider(FakeApi([]))

        # Publishing an empty TcgplayerSkus.json drops every SKU for every card,
        # so the build has to fail instead.
        with pytest.raises(TcgPlayerCatalogUnavailableError, match="refusing to publish an empty"):
            GlobalCache._last_good_tcg_skus(SimpleNamespace(tcgplayer=provider), RuntimeError("fetch failed"))

    def test_the_stop_is_latched_for_later_callers(self, tmp_path, make_provider):
        from mtgjson5.data.cache import GlobalCache

        failed: Future[pl.LazyFrame] = Future()
        failed.set_exception(TcgPlayerIncompleteFetchError("every page failed"))
        cache = SimpleNamespace(
            tcgplayer=make_provider(FakeApi([])),
            tcg_skus_lf=None,
            _tcg_skus_future=failed,
            _tcg_skus_error=None,
        )
        cache._last_good_tcg_skus = lambda error: GlobalCache._last_good_tcg_skus(cache, error)

        with pytest.raises(TcgPlayerCatalogUnavailableError):
            GlobalCache._await_tcg_skus(cache)

        # Two stages await the catalog. If the first raise is swallowed, the
        # second must not read as "there is simply nothing to write".
        with pytest.raises(TcgPlayerCatalogUnavailableError):
            GlobalCache._await_tcg_skus(cache)

    def test_an_unreachable_published_catalog_stops_the_build(self, tmp_path, make_provider, published):
        from mtgjson5.data.cache import GlobalCache

        provider = make_provider(FakeApi([]), published_catalog_url=published.url)
        published.fail(requests.ConnectionError("mtgjson.com is down"))

        with pytest.raises(TcgPlayerCatalogUnavailableError):
            GlobalCache._last_good_tcg_skus(SimpleNamespace(tcgplayer=provider), RuntimeError("fetch failed"))


class TestPublishedCatalog:
    def test_rebuilds_products_and_skus(self, published):
        catalog = published_mod.catalog_from_skus_json(io.BytesIO(published.payload({7: [70, 71]})))

        assert catalog["productId"].to_list() == [7]
        assert catalog["skus"].to_list() == [
            [
                {"skuId": 70, "languageId": 1, "printingId": 1, "conditionId": 1},
                {"skuId": 71, "languageId": 1, "printingId": 1, "conditionId": 1},
            ]
        ]
        # Names, group IDs, and URLs are not published, so they come back blank.
        assert catalog["name"].to_list() == [""]
        assert catalog["groupId"].to_list() == [None]

    def test_a_product_shared_by_several_uuids_lands_once(self, published):
        payload = published.payload({7: [70, 71]}, uuids_per_product=3)

        catalog = published_mod.catalog_from_skus_json(io.BytesIO(payload))

        assert catalog.height == 1
        assert catalog["skus"].list.len().to_list() == [2]

    def test_a_too_small_catalog_is_rejected(self, published, monkeypatch):
        monkeypatch.setattr(published_mod, "MIN_PUBLISHED_PRODUCTS", 50)

        with pytest.raises(published_mod.PublishedCatalogError, match="refusing to trust it"):
            published_mod.catalog_from_skus_json(io.BytesIO(published.payload({7: [70]})))

    def test_it_is_downloaded_once_and_cached(self, make_provider, published, tmp_path):
        provider = make_provider(FakeApi([]), published_catalog_url=published.url)
        published.serve({10: [100], 11: [110]})

        assert provider.previous_catalog().collect().height == 2
        assert provider.previous_catalog().collect().height == 2

        assert published.downloads == 1
        assert (tmp_path / "tcg_skus_published.parquet").exists()

    def test_a_stale_copy_is_downloaded_again(self, make_provider, published, tmp_path):
        provider = make_provider(FakeApi([]), published_catalog_url=published.url)
        published.serve({10: [100], 11: [110]})
        provider.previous_catalog()

        # A catalog left by a build days ago is too small a baseline to trust.
        stale = time.time() - (provider_mod.PUBLISHED_CATALOG_MAX_AGE_HOURS + 1) * 3600
        os.utime(tmp_path / "tcg_skus_published.parquet", (stale, stale))
        published.serve({10: [100], 11: [110], 12: [120]})

        assert provider.previous_catalog().collect().height == 3
        assert published.downloads == 2

    def test_a_download_failure_is_not_retried(self, make_provider, published):
        provider = make_provider(FakeApi([]), published_catalog_url=published.url)
        published.fail(requests.ConnectionError("mtgjson.com is down"))

        assert provider.previous_catalog() is None
        assert provider.previous_catalog() is None
        assert published.downloads == 1


class TestRegressionOnAFreshContainer:
    """The day-over-day check has to work with nothing on disk to compare against."""

    def test_a_shrinking_catalog_is_rejected_against_the_published_one(self, make_provider, published, tmp_path):
        published.serve({pid: [pid * 10] for pid in range(500)})
        provider = make_provider(FakeApi(_catalog(300)), published_catalog_url=published.url)

        with pytest.raises(TcgPlayerIncompleteFetchError, match="500 to 300 products"):
            provider.fetch_all_products_sync()

        assert not (tmp_path / "tcg_skus.parquet").exists()

    def test_a_growing_catalog_is_accepted(self, make_provider, published):
        published.serve({pid: [pid * 10] for pid in range(300)})
        provider = make_provider(FakeApi(_catalog(500)), published_catalog_url=published.url)

        assert provider.fetch_all_products_sync().collect().height == 500

    def test_the_check_is_skipped_when_nothing_can_be_reached(self, make_provider):
        provider = make_provider(FakeApi(_catalog(300)))

        assert provider.fetch_all_products_sync().collect().height == 300


class TestSubprocessCatalogFallback:
    def test_a_recovered_catalog_reaches_an_assembly_subprocess(self, tmp_path, monkeypatch):
        """A subprocess has no shared cache and reads the catalog off disk."""
        import mtgjson5.data as data_mod
        from mtgjson5 import constants
        from mtgjson5.build.assemble import TcgplayerSkusAssembler

        monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
        monkeypatch.setattr(
            data_mod,
            "GLOBAL_CACHE",
            SimpleNamespace(
                _await_tcg_skus=lambda: None,
                tcg_skus_lf=None,
                tcg_to_uuid_lf=None,
                tcg_etched_to_uuid_lf=None,
                tcg_alt_foil_to_uuid_lf=None,
            ),
        )
        pl.DataFrame({"productId": [3]}).write_parquet(tmp_path / "tcg_skus_published.parquet")

        assembler = TcgplayerSkusAssembler.__new__(TcgplayerSkusAssembler)
        for attr in ("_tcg_skus_lf", "_tcg_to_uuid_lf", "_tcg_etched_to_uuid_lf", "_tcg_alt_foil_to_uuid_lf"):
            setattr(assembler, attr, None)

        assembler._load_tcg_data()

        assert assembler._tcg_skus_lf.collect()["productId"].to_list() == [3]


class TestMalformedPages:
    def test_a_page_with_an_unexpected_shape_fails_the_fetch(self, make_provider):
        api = FakeApi(_catalog(500))
        api.malformed_pages.add(200)
        provider = make_provider(api)

        # Silently reading it as zero products would drop 100 products and every
        # SKU mapped to them.
        with pytest.raises(TcgPlayerIncompleteFetchError, match="still failed after retries"):
            provider.fetch_all_products_sync()

        assert api.requested.count(200) == 2

    def test_a_page_without_results_fails_the_fetch(self, make_provider):
        api = FakeApi(_catalog(500))
        api.resultless_pages.add(200)
        provider = make_provider(api)

        # The API answers some failures with a 200 and an error envelope that
        # carries no results at all.
        with pytest.raises(TcgPlayerIncompleteFetchError, match="still failed after retries"):
            provider.fetch_all_products_sync()

    def test_a_product_without_a_name_is_still_accepted(self):
        rows = TCGProvider._parse_products(
            {"results": [{"productId": 5, "name": None, "cleanName": None, "url": None, "skus": []}]}
        )

        assert rows == [{"productId": 5, "name": "", "cleanName": "", "groupId": None, "url": "", "skus": []}]
