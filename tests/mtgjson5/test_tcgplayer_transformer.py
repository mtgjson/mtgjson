"""Tests for TCGPlayer transformer: products_to_dataframe, flatten_skus, filter_skus, build_sku_map, to_join_data."""

from __future__ import annotations

import polars as pl

from mtgjson5.providers.tcgplayer.models import ENGLISH, FOIL, NEAR_MINT, NON_FOIL
from mtgjson5.providers.tcgplayer.transformer import TcgPlayerTransformer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_product(
    product_id: int = 100,
    name: str = "Test Card",
    skus: list[dict] | None = None,
) -> dict:
    if skus is None:
        skus = [
            {"skuId": 1000, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
            {"skuId": 1001, "languageId": ENGLISH, "printingId": FOIL, "conditionId": NEAR_MINT},
        ]
    return {
        "productId": product_id,
        "name": name,
        "cleanName": name,
        "groupId": 1,
        "url": f"/test/{product_id}",
        "skus": skus,
    }


# ---------------------------------------------------------------------------
# TestProductsToDataframe
# ---------------------------------------------------------------------------


class TestProductsToDataframe:
    def test_single_product_with_skus(self):
        products = [
            _make_product(
                skus=[
                    {"skuId": 1, "languageId": 1, "printingId": 1, "conditionId": 1},
                    {"skuId": 2, "languageId": 1, "printingId": 2, "conditionId": 1},
                ]
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        assert len(df) == 1
        assert "productId" in df.columns
        assert "skus" in df.columns
        assert df["productId"][0] == 100

    def test_empty_products_list(self):
        df = TcgPlayerTransformer.products_to_dataframe([])
        assert len(df) == 0
        assert "productId" in df.columns
        assert "skus" in df.columns

    def test_product_with_no_skus(self):
        products = [_make_product(skus=[])]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        assert len(df) == 1
        skus = df["skus"][0].to_list()
        assert skus == []


# ---------------------------------------------------------------------------
# TestFlattenSkus
# ---------------------------------------------------------------------------


class TestFlattenSkus:
    def test_product_with_multiple_skus(self):
        products = [
            _make_product(
                skus=[
                    {"skuId": 1, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 2, "languageId": ENGLISH, "printingId": FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 3, "languageId": 7, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                ]
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        flat = TcgPlayerTransformer.flatten_skus(df)
        assert len(flat) == 3
        assert "language" in flat.columns
        assert "printing" in flat.columns
        assert "condition" in flat.columns

    def test_unknown_language_id(self):
        products = [
            _make_product(
                skus=[
                    {"skuId": 1, "languageId": 999, "printingId": 1, "conditionId": 1},
                ]
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        flat = TcgPlayerTransformer.flatten_skus(df)
        assert flat["language"][0] == "UNKNOWN"


# ---------------------------------------------------------------------------
# TestFilterSkus
# ---------------------------------------------------------------------------


class TestFilterSkus:
    def _flat_df(self):
        products = [
            _make_product(
                skus=[
                    {"skuId": 1, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 2, "languageId": ENGLISH, "printingId": FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 3, "languageId": 7, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 4, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": 2},
                ]
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        return TcgPlayerTransformer.flatten_skus(df)

    def test_filter_by_language_only(self):
        flat = self._flat_df()
        result = TcgPlayerTransformer.filter_skus(flat, language_id=ENGLISH, condition_id=None, printing_id=None)
        assert len(result) == 3
        assert result["languageId"].to_list() == [ENGLISH] * 3

    def test_filter_by_language_and_condition(self):
        flat = self._flat_df()
        result = TcgPlayerTransformer.filter_skus(flat, language_id=ENGLISH, condition_id=NEAR_MINT, printing_id=None)
        assert len(result) == 2

    def test_no_filter_returns_all(self):
        flat = self._flat_df()
        result = TcgPlayerTransformer.filter_skus(flat, language_id=None, condition_id=None, printing_id=None)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# TestBuildSkuMap
# ---------------------------------------------------------------------------


class TestBuildSkuMap:
    def test_product_with_both_foil_and_nonfoil(self):
        products = [
            _make_product(
                product_id=100,
                skus=[
                    {"skuId": 1000, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                    {"skuId": 1001, "languageId": ENGLISH, "printingId": FOIL, "conditionId": NEAR_MINT},
                ],
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        sku_map = TcgPlayerTransformer.build_sku_map(df)
        assert len(sku_map) == 1
        assert sku_map["tcgplayerSkuId"][0] == "1000"
        assert sku_map["tcgplayerSkuFoilId"][0] == "1001"

    def test_product_with_foil_only(self):
        products = [
            _make_product(
                product_id=200,
                skus=[
                    {"skuId": 2001, "languageId": ENGLISH, "printingId": FOIL, "conditionId": NEAR_MINT},
                ],
            )
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        sku_map = TcgPlayerTransformer.build_sku_map(df)
        assert len(sku_map) == 1
        assert sku_map["tcgplayerSkuId"][0] is None
        assert sku_map["tcgplayerSkuFoilId"][0] == "2001"

    def test_multiple_products(self):
        products = [
            _make_product(
                product_id=100,
                skus=[
                    {"skuId": 1000, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                ],
            ),
            _make_product(
                product_id=200,
                skus=[
                    {"skuId": 2000, "languageId": ENGLISH, "printingId": NON_FOIL, "conditionId": NEAR_MINT},
                ],
            ),
        ]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        sku_map = TcgPlayerTransformer.build_sku_map(df)
        assert len(sku_map) == 2
        product_ids = sorted(sku_map["productId"].to_list())
        assert product_ids == [100, 200]


# ---------------------------------------------------------------------------
# TestToJoinData
# ---------------------------------------------------------------------------


class TestToJoinData:
    def test_correct_output_columns(self):
        products = [_make_product()]
        df = TcgPlayerTransformer.products_to_dataframe(products)
        result = TcgPlayerTransformer.to_join_data(df)
        assert "tcgplayerProductId" in result.columns
        assert "tcgplayerSkuId" in result.columns
        assert "tcgplayerSkuFoilId" in result.columns
        assert result.schema["tcgplayerProductId"] == pl.String
