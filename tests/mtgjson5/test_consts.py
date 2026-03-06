"""Tests for v2 constants: layouts, fields, finishes."""

from __future__ import annotations

import pytest

from mtgjson5.consts import (
    ALLOW_IF_FALSEY,
    FINISH_ORDER,
    OMIT_EMPTY_LIST_FIELDS,
    REQUIRED_CARD_LIST_FIELDS,
    SORTED_LIST_FIELDS,
)
from mtgjson5.consts.layouts import (
    MULTIFACE_LAYOUTS,
    TOKEN_LAYOUTS,
    is_multiface,
    is_token_layout,
)

# ---------------------------------------------------------------------------
# TestIsMultiface
# ---------------------------------------------------------------------------


class TestIsMultiface:
    @pytest.mark.parametrize(
        ("layout", "expected"),
        [
            ("split", True),
            ("normal", False),
            ("meld", True),
            ("class", False),
            ("transform", True),
            ("modal_dfc", True),
        ],
        ids=["split", "normal", "meld", "class", "transform", "modal_dfc"],
    )
    def test_is_multiface(self, layout: str, expected: bool):
        assert is_multiface(layout) is expected


# ---------------------------------------------------------------------------
# TestIsTokenLayout
# ---------------------------------------------------------------------------


class TestIsTokenLayout:
    @pytest.mark.parametrize(
        ("layout", "expected"),
        [
            ("token", True),
            ("emblem", True),
            ("normal", False),
            ("double_faced_token", True),
            ("art_series", True),
        ],
        ids=["token", "emblem", "normal", "dft", "art_series"],
    )
    def test_is_token_layout(self, layout: str, expected: bool):
        assert is_token_layout(layout) is expected


# ---------------------------------------------------------------------------
# TestFieldSets
# ---------------------------------------------------------------------------


class TestFieldSets:
    def test_allow_if_falsey_contains_key_fields(self):
        for field in ("uuid", "setCode", "type", "layout", "name"):
            assert field in ALLOW_IF_FALSEY, f"{field} missing from ALLOW_IF_FALSEY"

    def test_finish_order_correct(self):
        assert FINISH_ORDER["nonfoil"] == 0
        assert FINISH_ORDER["foil"] == 1
        assert FINISH_ORDER["etched"] == 2
        assert FINISH_ORDER["signed"] == 3
        assert FINISH_ORDER["nonfoil"] < FINISH_ORDER["foil"] < FINISH_ORDER["etched"] < FINISH_ORDER["signed"]

    def test_sorted_list_fields_contains_expected(self):
        for field in ("colorIdentity", "keywords", "availability"):
            assert field in SORTED_LIST_FIELDS, f"{field} missing from SORTED_LIST_FIELDS"


# ---------------------------------------------------------------------------
# TestMultifaceLayouts
# ---------------------------------------------------------------------------


class TestMultifaceLayouts:
    def test_no_overlap_multiface_token(self):
        """double_faced_token is in both, but regular token is only in TOKEN_LAYOUTS."""
        assert "token" not in MULTIFACE_LAYOUTS
        assert "normal" not in TOKEN_LAYOUTS


# ---------------------------------------------------------------------------
# TestFieldSetCompleteness
# ---------------------------------------------------------------------------


class TestFieldSetCompleteness:
    def test_allow_if_falsey_includes_mana_values(self):
        assert "convertedManaCost" in ALLOW_IF_FALSEY
        assert "manaValue" in ALLOW_IF_FALSEY

    def test_sorted_list_fields_includes_finishes(self):
        """finishes are included in SORTED_LIST_FIELDS."""
        assert "finishes" in SORTED_LIST_FIELDS

    def test_required_list_fields_and_omit_empty_no_overlap(self):
        overlap = REQUIRED_CARD_LIST_FIELDS & OMIT_EMPTY_LIST_FIELDS
        assert overlap == frozenset(), f"Unexpected overlap: {overlap}"
