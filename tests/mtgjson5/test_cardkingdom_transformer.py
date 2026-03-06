"""Tests for Card Kingdom transformer: parse_price, add_derived_columns, pivot_by_scryfall_id, to_pricing_df."""

from __future__ import annotations

import polars as pl

from mtgjson5.providers.cardkingdom.transformer import (
    CardKingdomTransformer,
    parse_price,
)

# ---------------------------------------------------------------------------
# TestParsePrice
# ---------------------------------------------------------------------------


class TestParsePrice:
    def test_valid_price(self):
        assert parse_price("12.99") == 12.99

    def test_none_returns_none(self):
        assert parse_price(None) is None

    def test_empty_string_returns_none(self):
        assert parse_price("") is None

    def test_invalid_returns_none(self):
        assert parse_price("abc") is None


# ---------------------------------------------------------------------------
# TestAddDerivedColumns
# ---------------------------------------------------------------------------


class TestAddDerivedColumns:
    def _make_df(self, is_foil: str = "false", variation: str | None = None) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "is_foil": [is_foil],
                "variation": [variation],
            }
        )

    def test_is_foil_true(self):
        df = self._make_df(is_foil="true")
        result = CardKingdomTransformer.add_derived_columns(df)
        assert result["is_foil_bool"][0] is True

    def test_is_foil_false(self):
        df = self._make_df(is_foil="false")
        result = CardKingdomTransformer.add_derived_columns(df)
        assert result["is_foil_bool"][0] is False

    def test_variation_foil_etched(self):
        df = self._make_df(variation="Foil Etched")
        result = CardKingdomTransformer.add_derived_columns(df)
        assert result["is_etched"][0] is True

    def test_variation_none(self):
        df = self._make_df(variation=None)
        result = CardKingdomTransformer.add_derived_columns(df)
        assert result["is_etched"][0] is False


# ---------------------------------------------------------------------------
# TestPivotByScryfallId
# ---------------------------------------------------------------------------


class TestPivotByScryfallId:
    def _make_records_df(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [1, 2, 3],
                "scryfall_id": ["sf-1", "sf-1", "sf-1"],
                "is_foil": ["false", "true", "true"],
                "variation": [None, None, "Foil Etched"],
                "url": ["/normal", "/foil", "/etched"],
            }
        )

    def test_foil_and_nonfoil_and_etched(self):
        df = self._make_records_df()
        result = CardKingdomTransformer.pivot_by_scryfall_id(df)
        assert len(result) == 1
        row = result.row(0, named=True)
        assert row["id"] == "sf-1"
        assert row["cardKingdomId"] == "1"
        assert row["cardKingdomFoilId"] == "2"
        assert row["cardKingdomEtchedId"] == "3"

    def test_card_without_scryfall_id_excluded(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "scryfall_id": [None],
                "is_foil": ["false"],
                "variation": [None],
                "url": ["/test"],
            }
        )
        result = CardKingdomTransformer.pivot_by_scryfall_id(df)
        assert len(result) == 0

    def test_multiple_cards_grouped(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "scryfall_id": ["sf-1", "sf-1", "sf-2", "sf-2"],
                "is_foil": ["false", "true", "false", "true"],
                "variation": [None, None, None, None],
                "url": ["/a", "/b", "/c", "/d"],
            }
        )
        result = CardKingdomTransformer.pivot_by_scryfall_id(df)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# TestToPricingDf
# ---------------------------------------------------------------------------


class TestToPricingDf:
    def test_correct_output_columns(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "scryfall_id": ["sf-1"],
                "is_foil": ["false"],
                "variation": [None],
                "price_retail": [12.99],
                "price_buy": [10.0],
                "qty_retail": [5],
                "qty_buying": [3],
            }
        )
        result = CardKingdomTransformer.to_pricing_df(df)
        expected_cols = {
            "ck_id",
            "scryfall_id",
            "is_foil",
            "is_etched",
            "price_retail",
            "price_buy",
            "qty_retail",
            "qty_buying",
        }
        assert set(result.columns) == expected_cols

    def test_null_scryfall_id_excluded(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "scryfall_id": ["sf-1", None],
                "is_foil": ["false", "false"],
                "variation": [None, None],
                "price_retail": [12.99, 5.0],
                "price_buy": [10.0, 4.0],
                "qty_retail": [5, 2],
                "qty_buying": [3, 1],
            }
        )
        result = CardKingdomTransformer.to_pricing_df(df)
        assert len(result) == 1
        assert result["scryfall_id"][0] == "sf-1"
