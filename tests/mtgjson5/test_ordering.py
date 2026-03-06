"""Tests for ordering contracts: WUBRG, FINISH_ORDER, collector numbers, sorted lists."""

from __future__ import annotations

import polars as pl

from mtgjson5.models.cards import CardSet
from mtgjson5.pipeline.expressions import order_finishes_expr, sort_colors_wubrg_expr

# =============================================================================
# Helpers
# =============================================================================


def _make_card_set(**overrides) -> CardSet:
    defaults = {
        "name": "Test Card",
        "type": "Creature",
        "layout": "normal",
        "uuid": "test-uuid",
        "setCode": "TST",
        "number": "1",
        "borderColor": "black",
        "frameVersion": "2015",
        "availability": ["paper"],
        "finishes": ["nonfoil"],
        "hasFoil": False,
        "hasNonFoil": True,
        "rarity": "common",
        "convertedManaCost": 0.0,
        "manaValue": 0.0,
    }
    defaults.update(overrides)
    return CardSet(**defaults)


# =============================================================================
# TestFinishesFollowFinishOrder
# =============================================================================


class TestFinishesFollowFinishOrder:
    def test_finishes_ordered_by_expression(self):
        df = pl.DataFrame(
            {"finishes": [["etched", "nonfoil", "foil"]]},
            schema={"finishes": pl.List(pl.String)},
        )
        result = df.select(order_finishes_expr("finishes"))
        assert result["finishes"][0].to_list() == ["nonfoil", "foil", "etched"]

    def test_signed_sorts_last(self):
        df = pl.DataFrame(
            {"finishes": [["signed", "foil", "nonfoil"]]},
            schema={"finishes": pl.List(pl.String)},
        )
        result = df.select(order_finishes_expr("finishes"))
        assert result["finishes"][0].to_list() == ["nonfoil", "foil", "signed"]

    def test_unknown_finish_after_signed(self):
        df = pl.DataFrame(
            {"finishes": [["mystery", "nonfoil"]]},
            schema={"finishes": pl.List(pl.String)},
        )
        result = df.select(order_finishes_expr("finishes"))
        ordered = result["finishes"][0].to_list()
        assert ordered[0] == "nonfoil"
        assert ordered[1] == "mystery"


# =============================================================================
# TestColorsWubrgForSplitLayout
# =============================================================================


class TestColorsWubrgForSplitLayout:
    def test_split_all_five_colors(self):
        card = _make_card_set(layout="split", colors=["G", "R", "B", "U", "W"])
        d = card.to_polars_dict()
        assert d["colors"] == ["W", "U", "B", "R", "G"]

    def test_adventure_preserves_wubrg(self):
        card = _make_card_set(layout="adventure", colors=["R", "W"])
        d = card.to_polars_dict()
        assert d["colors"] == ["W", "R"]


# =============================================================================
# TestColorsAlphaForNormalLayout
# =============================================================================


class TestColorsAlphaForNormalLayout:
    def test_normal_layout_alpha_sorted(self):
        card = _make_card_set(layout="normal", colors=["R", "G", "W"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["colors"] == ["G", "R", "W"]

    def test_transform_layout_alpha_sorted(self):
        card = _make_card_set(layout="transform", colors=["R", "G", "W"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["colors"] == ["G", "R", "W"]


# =============================================================================
# TestWubrgExpression
# =============================================================================


class TestWubrgExpression:
    def test_wubrg_sort_all_colors(self):
        df = pl.DataFrame(
            {"colors": [["G", "R", "B", "U", "W"]]},
            schema={"colors": pl.List(pl.String)},
        )
        result = df.select(sort_colors_wubrg_expr("colors"))
        assert result["colors"][0].to_list() == ["W", "U", "B", "R", "G"]

    def test_wubrg_sort_empty_list(self):
        df = pl.DataFrame(
            {"colors": [[]]},
            schema={"colors": pl.List(pl.String)},
        )
        result = df.select(sort_colors_wubrg_expr("colors"))
        assert result["colors"][0].to_list() == []

    def test_wubrg_sort_null(self):
        df = pl.DataFrame(
            {"colors": [None]},
            schema={"colors": pl.List(pl.String)},
        )
        result = df.select(sort_colors_wubrg_expr("colors"))
        assert result["colors"][0].to_list() == []


# =============================================================================
# TestRulingsSortedDateThenText
# =============================================================================


class TestRulingsSortedDateThenText:
    def test_five_rulings_with_ties(self):
        card = _make_card_set(
            rulings=[
                {"date": "2023-01-01", "text": "Zebra"},
                {"date": "2021-06-15", "text": "Alpha"},
                {"date": "2023-01-01", "text": "Apple"},
                {"date": "2023-01-01", "text": "Banana"},
                {"date": "2021-06-15", "text": "Beta"},
            ]
        )
        d = card.to_polars_dict()
        texts = [r["text"] for r in d["rulings"]]
        assert texts == ["Alpha", "Beta", "Apple", "Banana", "Zebra"]


# =============================================================================
# TestCardCollectorNumberSort
# =============================================================================


class TestCardCollectorNumberSort:
    def test_collector_number_order(self):
        numbers = ["100", "10b", "10a", "2", "10", "1"]
        cards = [_make_card_set(number=n, name=f"Card {n}") for n in numbers]
        sorted_cards = sorted(cards)
        sorted_numbers = [c.number for c in sorted_cards]
        assert sorted_numbers == ["1", "2", "10", "10a", "10b", "100"]


# =============================================================================
# TestSortedListFieldsApplied
# =============================================================================


class TestSortedListFieldsApplied:
    def test_color_identity_sorted(self):
        card = _make_card_set(colorIdentity=["G", "W", "B"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["colorIdentity"] == ["B", "G", "W"]

    def test_keywords_sorted(self):
        card = _make_card_set(keywords=["Trample", "Flying", "Deathtouch"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["keywords"] == ["Deathtouch", "Flying", "Trample"]

    def test_availability_sorted(self):
        card = _make_card_set(availability=["paper", "mtgo", "arena"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["availability"] == ["arena", "mtgo", "paper"]
