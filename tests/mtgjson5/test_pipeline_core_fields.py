"""Tests for v2 pipeline core field functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl

from mtgjson5.pipeline.stages.basic_fields import (
    add_booster_types,
    filter_keywords_for_face,
    fix_manavalue_for_multiface,
    fix_power_toughness_for_multiface,
    fix_promo_types,
)
from mtgjson5.pipeline.stages.derived import add_is_funny
from mtgjson5.pipeline.stages.legalities import remap_availability_values

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _face_struct(
    name: str = "Test",
    mana_cost: str | None = None,
    power: str | None = None,
    toughness: str | None = None,
) -> dict:
    """Build a face_data struct dict with the fields used by core functions."""
    return {
        "name": name,
        "mana_cost": mana_cost,
        "power": power,
        "toughness": toughness,
    }


_FACE_SCHEMA = pl.Struct(
    {
        "name": pl.String,
        "mana_cost": pl.String,
        "power": pl.String,
        "toughness": pl.String,
    }
)


# ---------------------------------------------------------------------------
# TestFixPromoTypes
# ---------------------------------------------------------------------------


class TestFixPromoTypes:
    def test_preserves_existing_promo_types(self):
        lf = pl.LazyFrame(
            {
                "number": ["1"],
                "promoTypes": [["boosterfun"]],
            }
        )
        result = fix_promo_types(lf).collect()
        assert "boosterfun" in result["promoTypes"][0].to_list()

    def test_null_promo_types_stays_null(self):
        lf = pl.LazyFrame(
            {"number": ["1"], "promoTypes": [None]},
            schema={"number": pl.String, "promoTypes": pl.List(pl.String)},
        )
        result = fix_promo_types(lf).collect()
        assert result["promoTypes"][0] is None

    def test_planeswalkerstamped_added_for_p_suffix(self):
        lf = pl.LazyFrame(
            {
                "number": ["123p"],
                "promoTypes": [["boosterfun"]],
            }
        )
        result = fix_promo_types(lf).collect()
        types = result["promoTypes"][0].to_list()
        assert "planeswalkerstamped" in types
        assert "boosterfun" in types


# ---------------------------------------------------------------------------
# TestFixPowerToughnessForMultiface
# ---------------------------------------------------------------------------


class TestFixPowerToughnessForMultiface:
    def test_multiface_uses_face_data(self):
        lf = pl.LazyFrame(
            {
                "power": ["*"],
                "toughness": ["*"],
                "_face_data": [_face_struct(power="3", toughness="4")],
            },
            schema={
                "power": pl.String,
                "toughness": pl.String,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_power_toughness_for_multiface(lf).collect()
        assert result["power"][0] == "3"
        assert result["toughness"][0] == "4"

    def test_normal_card_preserves_power_toughness(self):
        lf = pl.LazyFrame(
            {
                "power": ["2"],
                "toughness": ["2"],
                "_face_data": [None],
            },
            schema={
                "power": pl.String,
                "toughness": pl.String,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_power_toughness_for_multiface(lf).collect()
        assert result["power"][0] == "2"
        assert result["toughness"][0] == "2"

    def test_multiface_null_power_from_face(self):
        """Non-creature face on multiface card should get null power/toughness from face data."""
        lf = pl.LazyFrame(
            {
                "power": ["2"],
                "toughness": ["2"],
                "_face_data": [_face_struct(power=None, toughness=None)],
            },
            schema={
                "power": pl.String,
                "toughness": pl.String,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_power_toughness_for_multiface(lf).collect()
        assert result["power"][0] is None
        assert result["toughness"][0] is None


# ---------------------------------------------------------------------------
# TestFixManavalueForMultiface
# ---------------------------------------------------------------------------


class TestFixManavalueForMultiface:
    def test_modal_dfc_uses_face_cmc(self):
        lf = pl.LazyFrame(
            {
                "layout": ["modal_dfc"],
                "manaValue": [5.0],
                "_face_data": [_face_struct(mana_cost="{2}{U}")],
            },
            schema={
                "layout": pl.String,
                "manaValue": pl.Float64,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_manavalue_for_multiface(lf).collect()
        assert result["manaValue"][0] == 3.0

    def test_normal_card_keeps_manavalue(self):
        lf = pl.LazyFrame(
            {
                "layout": ["normal"],
                "manaValue": [2.0],
                "_face_data": [None],
            },
            schema={
                "layout": pl.String,
                "manaValue": pl.Float64,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_manavalue_for_multiface(lf).collect()
        assert result["manaValue"][0] == 2.0

    def test_split_card_keeps_total_manavalue(self):
        """Split cards should use the total manaValue, not face-specific CMC."""
        lf = pl.LazyFrame(
            {
                "layout": ["split"],
                "manaValue": [5.0],
                "_face_data": [_face_struct(mana_cost="{2}{R}")],
            },
            schema={
                "layout": pl.String,
                "manaValue": pl.Float64,
                "_face_data": _FACE_SCHEMA,
            },
        )
        result = fix_manavalue_for_multiface(lf).collect()
        assert result["manaValue"][0] == 5.0


# ---------------------------------------------------------------------------
# TestAddBoosterTypes
# ---------------------------------------------------------------------------


class TestAddBoosterTypes:
    def test_in_booster_default(self):
        lf = pl.LazyFrame(
            {
                "_in_booster": [True],
                "promoTypes": [None],
            },
            schema={"_in_booster": pl.Boolean, "promoTypes": pl.List(pl.String)},
        )
        result = add_booster_types(lf).collect()
        assert result["boosterTypes"][0].to_list() == ["default"]

    def test_not_in_booster_starter_promo(self):
        lf = pl.LazyFrame(
            {
                "_in_booster": [False],
                "promoTypes": [["starterdeck"]],
            },
            schema={"_in_booster": pl.Boolean, "promoTypes": pl.List(pl.String)},
        )
        result = add_booster_types(lf).collect()
        assert result["boosterTypes"][0].to_list() == ["deck"]


# ---------------------------------------------------------------------------
# TestFilterKeywordsForFace
# ---------------------------------------------------------------------------


class TestFilterKeywordsForFace:
    def test_multiface_keywords_filtered(self):
        lf = pl.LazyFrame(
            {
                "text": ["Flying and vigilance are great"],
                "_all_keywords": [["Flying", "Vigilance", "Trample"]],
            }
        )
        result = filter_keywords_for_face(lf).collect()
        kws = sorted(result["keywords"][0].to_list())
        assert "Flying" in kws
        assert "Vigilance" in kws
        assert "Trample" not in kws

    def test_single_face_all_keywords_kept(self):
        lf = pl.LazyFrame(
            {
                "text": ["Deathtouch, lifelink, and trample combine for a deadly attack"],
                "_all_keywords": [["Deathtouch", "Lifelink", "Trample"]],
            }
        )
        result = filter_keywords_for_face(lf).collect()
        kws = result["keywords"][0].to_list()
        assert len(kws) == 3


# ---------------------------------------------------------------------------
# TestAddIsFunny
# ---------------------------------------------------------------------------


class TestAddIsFunny:
    def _make_ctx(self, set_types: list[str] | None = None):
        ctx = MagicMock()
        if set_types is not None:
            ctx.categoricals.set_types = set_types
        else:
            ctx.categoricals = None
        return ctx

    def test_funny_set_card_is_funny(self):
        ctx = self._make_ctx(set_types=["funny", "expansion"])
        lf = pl.LazyFrame(
            {
                "setType": ["funny"],
                "setCode": ["UND"],
                "securityStamp": [None],
            }
        )
        result = add_is_funny(lf, ctx).collect()
        assert result["isFunny"][0] is True

    def test_normal_set_card_not_funny(self):
        ctx = self._make_ctx(set_types=["funny", "expansion"])
        lf = pl.LazyFrame(
            {
                "setType": ["expansion"],
                "setCode": ["MH3"],
                "securityStamp": [None],
            }
        )
        result = add_is_funny(lf, ctx).collect()
        assert result["isFunny"][0] is None


# ---------------------------------------------------------------------------
# TestRemapAvailabilityValues
# ---------------------------------------------------------------------------


class TestRemapAvailabilityValues:
    def test_astral_becomes_shandalar(self):
        lf = pl.LazyFrame(
            {
                "availability": [["astral"]],
            }
        )
        result = remap_availability_values(lf).collect()
        assert "shandalar" in result["availability"][0].to_list()

    def test_paper_and_mtgo_unchanged(self):
        lf = pl.LazyFrame(
            {
                "availability": [["paper", "mtgo"]],
            }
        )
        result = remap_availability_values(lf).collect()
        avail = result["availability"][0].to_list()
        assert "mtgo" in avail
        assert "paper" in avail
