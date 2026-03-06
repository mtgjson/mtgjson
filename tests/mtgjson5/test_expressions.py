"""Tests for v2 pipeline expression functions."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.pipeline.expressions import (
    ascii_name_expr,
    calculate_cmc_expr,
    extract_colors_from_mana_expr,
    order_finishes_expr,
    sort_colors_wubrg_expr,
)

# ---------------------------------------------------------------------------
# calculate_cmc_expr
# ---------------------------------------------------------------------------


class TestCalculateCmcExpr:
    @pytest.mark.parametrize(
        ("mana_cost", "expected"),
        [
            ("{3}", 3.0),
            ("{W}", 1.0),
            ("{2}{W}{U}", 4.0),
            ("{2/W}", 2.0),
            ("{W/U}", 1.0),
            ("{W/P}", 1.0),
            ("{HW}", 0.5),
            ("{X}{R}", 1.0),
            ("{X}{Y}{Z}", 0.0),
            (None, 0.0),
            ("", 0.0),
            ("{C}", 1.0),
        ],
        ids=[
            "plain_numeric",
            "single_color",
            "mixed",
            "hybrid_numeric",
            "hybrid_color",
            "phyrexian_hybrid",
            "half_mana",
            "variable_X",
            "all_variable",
            "null",
            "empty_string",
            "colorless",
        ],
    )
    def test_calculate_cmc(self, mana_cost: str | None, expected: float):
        df = pl.DataFrame({"manaCost": [mana_cost]}, schema={"manaCost": pl.String})
        result = df.select(calculate_cmc_expr("manaCost").alias("cmc"))
        assert result["cmc"][0] == pytest.approx(expected)


# ---------------------------------------------------------------------------
# extract_colors_from_mana_expr
# ---------------------------------------------------------------------------


class TestExtractColorsFromManaExpr:
    @pytest.mark.parametrize(
        ("mana_cost", "expected"),
        [
            ("{W}{U}{B}{R}{G}", ["W", "U", "B", "R", "G"]),
            ("{2/W}{G}", ["W", "G"]),
            ("{W}{W}{U}", ["W", "U"]),
            ("{3}", []),
            (None, []),
            ("{R}{G}", ["R", "G"]),
        ],
        ids=[
            "all_five_colors",
            "hybrid_extraction",
            "dedup",
            "no_colors",
            "null",
            "partial",
        ],
    )
    def test_extract_colors(self, mana_cost: str | None, expected: list[str]):
        df = pl.DataFrame({"manaCost": [mana_cost]}, schema={"manaCost": pl.String})
        result = df.select(extract_colors_from_mana_expr("manaCost").alias("colors"))
        assert result["colors"][0].to_list() == expected


# ---------------------------------------------------------------------------
# sort_colors_wubrg_expr
# ---------------------------------------------------------------------------


class TestSortColorsWubrgExpr:
    @pytest.mark.parametrize(
        ("colors", "expected"),
        [
            (["W", "U", "B"], ["W", "U", "B"]),
            (["G", "R", "B", "U", "W"], ["W", "U", "B", "R", "G"]),
            (["R"], ["R"]),
            (None, []),
        ],
        ids=["already_sorted", "reverse", "single", "null"],
    )
    def test_sort_colors(self, colors: list[str] | None, expected: list[str]):
        df = pl.DataFrame(
            {"colors": [colors]},
            schema={"colors": pl.List(pl.String)},
        )
        result = df.select(sort_colors_wubrg_expr("colors").alias("sorted"))
        assert result["sorted"][0].to_list() == expected


# ---------------------------------------------------------------------------
# order_finishes_expr
# ---------------------------------------------------------------------------


class TestOrderFinishesExpr:
    @pytest.mark.parametrize(
        ("finishes", "expected"),
        [
            (["foil", "nonfoil"], ["nonfoil", "foil"]),
            (["etched", "foil", "nonfoil"], ["nonfoil", "foil", "etched"]),
            (["signed", "foil"], ["foil", "signed"]),
            (["foil"], ["foil"]),
        ],
        ids=["foil_nonfoil", "three_finishes", "signed_foil", "single"],
    )
    def test_order_finishes(self, finishes: list[str], expected: list[str]):
        df = pl.DataFrame(
            {"finishes": [finishes]},
            schema={"finishes": pl.List(pl.String)},
        )
        result = df.select(order_finishes_expr("finishes").alias("ordered"))
        assert result["ordered"][0].to_list() == expected


# ---------------------------------------------------------------------------
# ascii_name_expr
# ---------------------------------------------------------------------------


class TestAsciiNameExpr:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("Ætherize", "AEtherize"),
            ("Jötun Grunt", "Jotun Grunt"),
            ("Lightning Bolt", "Lightning Bolt"),
            ("Ratonhnhake꞉ton", "Ratonhnhaketon"),
        ],
        ids=["ae_ligature", "o_umlaut", "plain_ascii", "modifier_colon"],
    )
    def test_ascii_name(self, name: str, expected: str):
        df = pl.DataFrame({"name": [name]})
        result = df.select(ascii_name_expr("name").alias("ascii"))
        assert result["ascii"][0] == expected


# ---------------------------------------------------------------------------
# uuid5_expr (requires polars_hash)
# ---------------------------------------------------------------------------


class TestUuid5Expr:
    def test_deterministic_output(self):
        pytest.importorskip("polars_hash")
        from mtgjson5.pipeline.expressions import uuid5_expr

        df = pl.DataFrame({"name": ["Lightning Bolt", "Lightning Bolt", "Grizzly Bears"]})
        result = df.select(uuid5_expr("name").alias("uuid"))
        uuids = result["uuid"].to_list()
        # Same input -> same output
        assert uuids[0] == uuids[1]
        # Different input -> different output
        assert uuids[0] != uuids[2]
        # Output is a valid UUID-like string (36 chars)
        assert len(uuids[0]) == 36
