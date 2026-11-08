"""Tests for the Cardmarket catalog crawl that feeds mkm_cards.parquet."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import polars as pl
import pytest

from mtgjson5 import constants
from mtgjson5.providers.cardmarket import provider as mcm_provider
from mtgjson5.providers.cardmarket.provider import CardMarketConfig, CardMarketProvider


def _product(id_product: int, name: str, number: str = "1") -> dict:
    return {"idProduct": id_product, "idMetaproduct": id_product * 10, "enName": name, "number": number}


def _make_provider(expansions: list[tuple[int, str]], singles: dict[int, list[dict]]) -> CardMarketProvider:
    provider = CardMarketProvider(config=CardMarketConfig(app_token="t", app_secret="s"), request_delay=0)
    # A non-empty set map makes load_set_map return early without network.
    provider._set_map = {name.lower(): {"mcmId": exp_id, "mcmName": name} for exp_id, name in expansions}
    provider._expansions = list(expansions)
    provider._fetch_expansion_cards_sync = lambda mcm_id, retries=5: singles.get(mcm_id, [])  # type: ignore[method-assign]
    return provider


def _write_catalog(path, rows: list[dict]) -> None:
    pl.DataFrame(
        rows,
        schema={
            "mcmId": pl.Int64,
            "mcmMetaId": pl.Int64,
            "name": pl.String,
            "number": pl.String,
            "expansionId": pl.Int64,
            "expansionName": pl.String,
        },
    ).write_parquet(path)


class TestGetAllCards:
    def test_refetches_previously_crawled_expansion(self, tmp_path):
        """Products added to an already-crawled expansion must be picked up."""
        out = tmp_path / "mkm_cards.parquet"
        _write_catalog(
            out,
            [{"mcmId": 1, "mcmMetaId": 10, "name": "Old", "number": "1", "expansionId": 100, "expansionName": "Set"}],
        )
        provider = _make_provider([(100, "Set")], {100: [_product(1, "Old"), _product(2, "New", "2")]})

        df = asyncio.run(provider.get_all_cards(output_path=out))

        assert sorted(df["mcmId"].to_list()) == [1, 2]
        assert sorted(pl.read_parquet(out)["mcmId"].to_list()) == [1, 2]

    def test_keeps_existing_rows_when_fetch_is_empty(self, tmp_path):
        out = tmp_path / "mkm_cards.parquet"
        _write_catalog(
            out,
            [
                {"mcmId": 1, "mcmMetaId": 10, "name": "A", "number": "1", "expansionId": 100, "expansionName": "Set A"},
                {"mcmId": 5, "mcmMetaId": 50, "name": "B", "number": "1", "expansionId": 200, "expansionName": "Set B"},
            ],
        )
        # Expansion 200 fails to fetch this run; its cached products stay.
        provider = _make_provider([(100, "Set A"), (200, "Set B")], {100: [_product(1, "A")]})

        df = asyncio.run(provider.get_all_cards(output_path=out))

        assert sorted(df["mcmId"].to_list()) == [1, 5]
        assert df.height == 2

    def test_crawls_expansions_with_colliding_names(self):
        """Two expansions sharing a name both get crawled."""
        provider = _make_provider(
            [(100, "Same Name"), (200, "Same Name")],
            {100: [_product(1, "A")], 200: [_product(2, "B")]},
        )

        df = asyncio.run(provider.get_all_cards())

        assert sorted(df["expansionId"].to_list()) == [100, 200]


class TestLoadSetMap:
    def test_expansion_list_survives_name_fix_collisions(self, monkeypatch):
        """A name fix renaming onto an existing name must not drop that expansion from the crawl list."""
        payload = {
            "expansion": [
                {"idExpansion": 10, "enName": "Fourth Edition"},
                {"idExpansion": 11, "enName": "Fourth Edition: Alternate"},
                {"idExpansion": 12, "enName": "Fourth Edition: Black Bordered"},
            ]
        }
        resp = SimpleNamespace(status_code=200, json=lambda: payload)
        conn = SimpleNamespace(market_place=SimpleNamespace(expansions=lambda game: resp))
        provider = CardMarketProvider(config=CardMarketConfig(app_token="t", app_secret="s"))
        monkeypatch.setattr(provider, "_ensure_mkm_connection", lambda: conn)
        monkeypatch.setattr(provider, "_persist_expansion_list", lambda expansions: None)

        asyncio.run(provider.load_set_map())

        assert [exp_id for exp_id, _ in provider._expansions] == [10, 11, 12]


class TestLoadCardmarketData:
    @pytest.fixture
    def cache_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
        monkeypatch.setattr(
            CardMarketConfig, "from_mtgjson_config", classmethod(lambda cls: cls(app_token="t", app_secret="s"))
        )
        return tmp_path

    def test_fresh_cache_fetches_missing_expansion_list(self, cache_dir, monkeypatch):
        catalog = cache_dir / "mkm_cards.parquet"
        _write_catalog(catalog, [])
        calls: list[bool] = []

        async def fake_fetch() -> None:
            calls.append(True)

        monkeypatch.setattr(mcm_provider, "_fetch_expansion_list", fake_fetch)

        mcm_provider.load_cardmarket_data(catalog)

        assert calls == [True]

    def test_fresh_cache_skips_fetch_when_expansion_list_present(self, cache_dir, monkeypatch):
        catalog = cache_dir / "mkm_cards.parquet"
        _write_catalog(catalog, [])
        pl.DataFrame({"expansionId": [1], "expansionName": ["X"]}).write_parquet(cache_dir / "mkm_expansions.parquet")

        async def fake_fetch() -> None:
            raise AssertionError("should not fetch")

        monkeypatch.setattr(mcm_provider, "_fetch_expansion_list", fake_fetch)

        mcm_provider.load_cardmarket_data(catalog)
