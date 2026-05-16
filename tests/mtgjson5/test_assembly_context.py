"""Tests for AssemblyContext.sets_df enrichment with sealed products, decks, and languages."""

from __future__ import annotations

import polars as pl

from mtgjson5.build.context import (
    _build_languages_by_set,
    _enrich_sets_with_decks,
    _enrich_sets_with_sealed,
)


class TestEnrichSetsWithSealed:
    def _make_sealed_df(self) -> pl.DataFrame:
        return pl.DataFrame(
            [
                {
                    "setCode": "M10",
                    "uuid": "sealed-001",
                    "name": "Booster Box",
                    "category": "booster_box",
                    "subtype": None,
                    "releaseDate": "2009-07-17",
                    "cardCount": 36,
                    "productSize": None,
                },
            ],
        )

    def test_sealed_products_attached_to_matching_set(self):
        records = [{"code": "M10", "name": "Magic 2010"}]
        _enrich_sets_with_sealed(records, self._make_sealed_df())
        assert "sealedProduct" in records[0]
        products = records[0]["sealedProduct"]
        assert len(products) == 1
        assert products[0]["uuid"] == "sealed-001"
        assert products[0]["name"] == "Booster Box"

    def test_set_code_and_language_stripped_from_product_dicts(self):
        records = [{"code": "M10", "name": "Magic 2010"}]
        _enrich_sets_with_sealed(records, self._make_sealed_df())
        product = records[0]["sealedProduct"][0]
        assert "setCode" not in product
        assert "language" not in product

    def test_sets_without_sealed_get_empty_list(self):
        records = [{"code": "ZZZ", "name": "No Products"}]
        _enrich_sets_with_sealed(records, self._make_sealed_df())
        assert records[0]["sealedProduct"] == []

    def test_none_sealed_df_skipped(self):
        records = [{"code": "M10", "name": "Magic 2010"}]
        _enrich_sets_with_sealed(records, None)
        assert "sealedProduct" not in records[0]


class TestEnrichSetsWithDecks:
    def _make_decks_df(self) -> pl.DataFrame:
        return pl.DataFrame(
            [
                {
                    "setCode": "M10",
                    "code": "M10",
                    "name": "Intro Pack Red",
                    "type": "Intro Pack",
                    "releaseDate": "2009-07-17",
                    "source": "https://example.com/intro-pack-red",
                    "sealedProductUuids": ["sealed-001"],
                    "sourceSetCodes": None,
                    "mainBoard": [{"uuid": "uuid-001", "count": 1}],
                    "sideBoard": [],
                    "commander": None,
                    "displayCommander": None,
                    "tokens": None,
                    "planes": None,
                    "schemes": None,
                },
            ],
        )

    def test_decks_attached_to_matching_set(self):
        records = [{"code": "M10", "name": "Magic 2010"}]
        _enrich_sets_with_decks(records, self._make_decks_df())
        assert "decks" in records[0]
        decks = records[0]["decks"]
        assert len(decks) == 1
        assert decks[0]["name"] == "Intro Pack Red"
        assert decks[0]["source"] == "https://example.com/intro-pack-red"

    def test_sets_without_decks_get_empty_list(self):
        records = [{"code": "ZZZ", "name": "No Decks"}]
        _enrich_sets_with_decks(records, self._make_decks_df())
        assert records[0]["decks"] == []

    def test_none_decks_df_skipped(self):
        records = [{"code": "M10", "name": "Magic 2010"}]
        _enrich_sets_with_decks(records, None)
        assert "decks" not in records[0]


class TestBuildLanguagesBySet:
    def test_languages_from_foreign_data(self):
        cards_df = pl.DataFrame(
            [
                {
                    "setCode": "M10",
                    "foreignData": [
                        {"language": "French", "name": "Eclair"},
                        {"language": "German", "name": "Blitz"},
                    ],
                },
                {
                    "setCode": "M10",
                    "foreignData": [{"language": "French", "name": "Croissance"}],
                },
            ],
            schema={
                "setCode": pl.String,
                "foreignData": pl.List(pl.Struct({"language": pl.String, "name": pl.String})),
            },
        )
        result = _build_languages_by_set(cards_df)
        assert "M10" in result
        assert result["M10"] == ["English", "French", "German"]

    def test_sets_with_no_foreign_data_get_english_only(self):
        cards_df = pl.DataFrame(
            [{"setCode": "M10", "foreignData": []}],
            schema={
                "setCode": pl.String,
                "foreignData": pl.List(pl.Struct({"language": pl.String, "name": pl.String})),
            },
        )
        result = _build_languages_by_set(cards_df)
        assert result.get("M10") == ["English"]

    def test_missing_set_not_in_result(self):
        cards_df = pl.DataFrame(
            [{"setCode": "M10", "foreignData": []}],
            schema={
                "setCode": pl.String,
                "foreignData": pl.List(pl.Struct({"language": pl.String, "name": pl.String})),
            },
        )
        result = _build_languages_by_set(cards_df)
        assert "ZZZ" not in result
