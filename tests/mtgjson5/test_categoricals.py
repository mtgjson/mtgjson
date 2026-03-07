"""Tests for mtgjson5.polars_utils.categoricals: DynamicCategoricals and discover_categoricals."""

from __future__ import annotations

import logging

import polars as pl

from mtgjson5.polars_utils.categoricals import (
    DynamicCategoricals,
    discover_categoricals,
)

# ---------------------------------------------------------------------------
# DynamicCategoricals
# ---------------------------------------------------------------------------


class TestDynamicCategoricals:
    def test_defaults_empty(self):
        cats = DynamicCategoricals()
        assert not cats.legalities
        assert not cats.games
        assert not cats.colors

    def test_summary_counts(self):
        cats = DynamicCategoricals(
            legalities=["standard", "modern"],
            games=["paper", "arena"],
            colors=["W", "U", "B", "R", "G"],
        )
        summary = cats.summary()
        assert summary["legalities"] == 2
        assert summary["games"] == 2
        assert summary["colors"] == 5
        assert summary["finishes"] == 0

    def test_extract_struct_fields_from_struct_column(self):
        cats = DynamicCategoricals()
        schema = pl.Schema(
            {
                "legalities": pl.Struct({"standard": pl.String, "modern": pl.String}),
            }
        )
        result = cats.extract_struct_fields(schema, "legalities")
        assert result == ["modern", "standard"]  # sorted

    def test_extract_struct_fields_missing_column(self):
        cats = DynamicCategoricals()
        schema = pl.Schema({"other": pl.String})
        result = cats.extract_struct_fields(schema, "legalities")
        assert result == []

    def test_extract_struct_fields_non_struct(self):
        cats = DynamicCategoricals()
        schema = pl.Schema({"legalities": pl.String})
        result = cats.extract_struct_fields(schema, "legalities")
        assert result == []


# ---------------------------------------------------------------------------
# discover_categoricals
# ---------------------------------------------------------------------------


class TestDiscoverCategoricals:
    def test_discovers_struct_fields(self):
        lf = pl.LazyFrame(
            {
                "legalities": [{"standard": "Legal", "modern": "Legal"}],
                "name": ["Test"],
            }
        )
        cats = discover_categoricals(lf)
        assert "standard" in cats.legalities
        assert "modern" in cats.legalities

    def test_discovers_list_column_values(self):
        lf = pl.LazyFrame(
            {
                "games": [["paper", "arena"]],
                "finishes": [["foil", "nonfoil"]],
                "name": ["Test"],
            }
        )
        cats = discover_categoricals(lf)
        assert set(cats.games) == {"paper", "arena"}
        assert set(cats.finishes) == {"foil", "nonfoil"}

    def test_discovers_scalar_column_values(self):
        lf = pl.LazyFrame(
            {
                "rarity": ["common", "rare", "common"],
                "layout": ["normal", "split", "normal"],
            }
        )
        cats = discover_categoricals(lf)
        assert set(cats.rarities) == {"common", "rare"}
        assert set(cats.layouts) == {"normal", "split"}

    def test_discovers_color_identity(self):
        lf = pl.LazyFrame(
            {
                "color_identity": [["W", "U"], ["B", "R"], ["G"]],
            }
        )
        cats = discover_categoricals(lf)
        assert set(cats.colors) == {"W", "U", "B", "R", "G"}

    def test_sets_lf_merges_set_types(self):
        cards_lf = pl.LazyFrame(
            {
                "rarity": ["common"],
            }
        )
        sets_lf = pl.LazyFrame(
            {
                "setType": ["expansion", "core"],
            }
        )
        cats = discover_categoricals(cards_lf, sets_lf=sets_lf)
        assert "expansion" in cats.set_types
        assert "core" in cats.set_types

    def test_sets_df_merges_set_types(self):
        cards_lf = pl.LazyFrame(
            {
                "rarity": ["common"],
            }
        )
        sets_df = pl.DataFrame(
            {
                "setType": ["masters"],
            }
        )
        cats = discover_categoricals(cards_lf, sets_lf=sets_df)
        assert "masters" in cats.set_types

    def test_logger_output(self, caplog):
        lf = pl.LazyFrame(
            {
                "rarity": ["common"],
                "games": [["paper"]],
            }
        )
        logger = logging.getLogger("test_categoricals")
        with caplog.at_level(logging.DEBUG, logger="test_categoricals"):
            cats = discover_categoricals(lf, logger=logger)
        assert cats.rarities == ["common"]

    def test_empty_dataframe(self):
        lf = pl.LazyFrame({"name": pl.Series([], dtype=pl.String)})
        cats = discover_categoricals(lf)
        # Should not error, just return empty categoricals
        assert not cats.legalities

    def test_handles_missing_columns_gracefully(self):
        lf = pl.LazyFrame({"unrelated_col": [1, 2, 3]})
        cats = discover_categoricals(lf)
        assert not cats.games
        assert not cats.rarities

    def test_multiple_struct_columns(self):
        lf = pl.LazyFrame(
            {
                "legalities": [{"standard": "Legal"}],
                "prices": [{"usd": "1.00", "eur": "0.90"}],
                "image_uris": [{"small": "url1", "normal": "url2"}],
            }
        )
        cats = discover_categoricals(lf)
        assert cats.legalities == ["standard"]
        assert set(cats.price_keys) == {"eur", "usd"}
        assert set(cats.image_uri_keys) == {"normal", "small"}

    def test_null_values_excluded(self):
        lf = pl.LazyFrame(
            {
                "rarity": ["common", None, "rare"],
                "games": [["paper"], None, ["arena"]],
            }
        )
        cats = discover_categoricals(lf)
        assert None not in cats.rarities
        assert set(cats.rarities) == {"common", "rare"}
