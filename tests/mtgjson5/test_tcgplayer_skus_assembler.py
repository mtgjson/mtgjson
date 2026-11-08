"""Tests for the TcgplayerSkus.json assembler."""

from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

from mtgjson5 import constants
from mtgjson5.build.assemble import TcgplayerSkusAssembler
from mtgjson5.providers.tcgplayer.models import PRODUCT_SCHEMA


def _catalog(rows: list[dict]) -> pl.LazyFrame:
    return pl.DataFrame(rows, schema=PRODUCT_SCHEMA).lazy()


def _sku(sku_id: int, printing_id: int = 1, condition_id: int = 1) -> dict:
    return {"skuId": sku_id, "languageId": 1, "printingId": printing_id, "conditionId": condition_id}


@pytest.fixture
def assembler(tmp_path, monkeypatch):
    """An assembler fed from in-memory catalog and uuid mappings.

    The assembler partitions by hex uuid prefix, so test uuids start with one.
    """
    import mtgjson5.data as data_mod

    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)

    def build(
        catalog: pl.LazyFrame,
        normal: dict[int, str] | None = None,
        etched: dict[int, str] | None = None,
    ) -> TcgplayerSkusAssembler:
        def mapping(column: str, pairs: dict[int, str] | None) -> pl.LazyFrame | None:
            if pairs is None:
                return None
            return pl.DataFrame({column: [str(pid) for pid in pairs], "uuid": list(pairs.values())}).lazy()

        monkeypatch.setattr(
            data_mod,
            "GLOBAL_CACHE",
            SimpleNamespace(
                _await_tcg_skus=lambda: None,
                tcg_skus_lf=catalog,
                tcg_to_uuid_lf=mapping("tcgplayerProductId", normal),
                tcg_etched_to_uuid_lf=mapping("tcgplayerEtchedProductId", etched),
                tcg_alt_foil_to_uuid_lf=None,
            ),
        )
        return TcgplayerSkusAssembler(SimpleNamespace(sealed_df=None, token_products={}))

    return build


class TestProductName:
    def test_every_sku_carries_its_product_name(self, assembler):
        catalog = _catalog(
            [
                {"productId": 10, "name": "Forest (151)", "skus": [_sku(100), _sku(101, printing_id=2)]},
                {"productId": 20, "name": "Opt", "skus": [_sku(200)]},
            ]
        )

        data = assembler(catalog, normal={10: "aaaa-1", 20: "bbbb-2"}).build()

        assert data["aaaa-1"] == [
            {
                "condition": "NEAR MINT",
                "language": "ENGLISH",
                "printing": "NON FOIL",
                "productId": 10,
                "productName": "Forest (151)",
                "skuId": 100,
            },
            {
                "condition": "NEAR MINT",
                "language": "ENGLISH",
                "printing": "FOIL",
                "productId": 10,
                "productName": "Forest (151)",
                "skuId": 101,
            },
        ]
        assert [sku["productName"] for sku in data["bbbb-2"]] == ["Opt"]

    def test_a_card_on_two_products_gets_each_products_name(self, assembler):
        catalog = _catalog(
            [
                {"productId": 10, "name": "Sol Ring", "skus": [_sku(100)]},
                {"productId": 11, "name": "Sol Ring (Foil Etched)", "skus": [_sku(110, printing_id=2)]},
            ]
        )

        skus = assembler(catalog, normal={10: "aaaa-1"}, etched={11: "aaaa-1"}).build()["aaaa-1"]

        assert [(sku["skuId"], sku["productName"], sku.get("finish")) for sku in skus] == [
            (100, "Sol Ring", None),
            (110, "Sol Ring (Foil Etched)", "ETCHED"),
        ]

    def test_the_flat_cache_for_parquet_keeps_the_name(self, assembler, tmp_path):
        catalog = _catalog([{"productId": 10, "name": "Opt", "skus": [_sku(100)]}])

        assembler(catalog, normal={10: "aaaa-1"}).build()

        flat = pl.read_parquet(tmp_path / "_tcgplayer_skus_flat.parquet")
        assert flat.select("uuid", "productId", "productName").to_dicts() == [
            {"uuid": "aaaa-1", "productId": 10, "productName": "Opt"}
        ]
