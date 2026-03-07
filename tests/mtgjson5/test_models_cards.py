"""Tests for v2 card models: sorting, hierarchy, serialization."""

from __future__ import annotations

import random

from mtgjson5.models.cards import (
    CardAtomic,
    CardDeck,
    CardSet,
    CardToken,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_card_set(**overrides) -> CardSet:
    """Create a minimal CardSet for testing."""
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


# ---------------------------------------------------------------------------
# TestCardPrintingBaseSorting
# ---------------------------------------------------------------------------


class TestCardPrintingBaseSorting:
    def test_simple_numeric(self):
        c1 = _make_card_set(number="1")
        c2 = _make_card_set(number="2")
        c10 = _make_card_set(number="10")
        assert c1 < c2 < c10

    def test_numeric_before_suffix(self):
        c10 = _make_card_set(number="10")
        c10a = _make_card_set(number="10a")
        assert c10 < c10a

    def test_side_tiebreaker(self):
        ca = _make_card_set(number="5", side="a", name="CardA")
        cb = _make_card_set(number="5", side="b", name="CardB")
        assert ca < cb

    def test_prefix_with_digits(self):
        # "1" sorts before "10" by integer extraction
        c1 = _make_card_set(number="1")
        c10 = _make_card_set(number="10")
        assert c1 < c10

    def test_empty_number_sorts_last(self):
        c_empty = _make_card_set(number="")
        c_normal = _make_card_set(number="1")
        assert c_normal < c_empty

    def test_randomized_sort_stability(self):
        """Shuffle 100x, sort, verify order is consistent."""
        cards = [_make_card_set(number=str(i)) for i in range(1, 20)]
        expected = sorted(cards)
        for _ in range(100):
            shuffled = list(cards)
            random.shuffle(shuffled)
            assert sorted(shuffled) == expected

    def test_non_cardprintingbase_returns_not_implemented(self):
        c = _make_card_set(number="1")
        assert c.__lt__("not a card") is NotImplemented  # pylint: disable=unnecessary-dunder-call


# ---------------------------------------------------------------------------
# TestCardModelHierarchy
# ---------------------------------------------------------------------------


class TestCardModelHierarchy:  # pylint: disable=unsupported-membership-test
    def test_card_atomic_has_first_printing(self):
        assert "first_printing" in CardAtomic.model_fields

    def test_card_atomic_no_uuid(self):
        assert "uuid" not in CardAtomic.model_fields

    def test_card_set_has_uuid(self):
        assert "uuid" in CardSet.model_fields

    def test_card_deck_has_count(self):
        assert "count" in CardDeck.model_fields
        assert "is_foil" in CardDeck.model_fields

    def test_card_token_has_related_cards(self):
        assert "related_cards" in CardToken.model_fields

    def test_card_token_no_rarity(self):
        assert "rarity" not in CardToken.model_fields

    def test_card_atomic_excludes_printing_fields(self):
        """CardAtomic should not have printing-specific fields."""
        for field in ("uuid", "set_code", "number"):
            assert field not in CardAtomic.model_fields


# ---------------------------------------------------------------------------
# TestCardBaseToPolarsDict
# ---------------------------------------------------------------------------


class TestCardBaseToPolarsDict:
    def test_split_layout_wubrg_order(self):
        card = _make_card_set(layout="split", colors=["R", "W"])
        d = card.to_polars_dict()
        assert d["colors"] == ["W", "R"]

    def test_adventure_layout_wubrg_order(self):
        card = _make_card_set(layout="adventure", colors=["G", "U"])
        d = card.to_polars_dict()
        assert d["colors"] == ["U", "G"]

    def test_normal_layout_alpha_sorted(self):
        card = _make_card_set(layout="normal", colors=["R", "G", "W"])
        d = card.to_polars_dict(sort_lists=True)
        assert d["colors"] == ["G", "R", "W"]

    def test_identifiers_always_present(self):
        card = _make_card_set()
        d = card.to_polars_dict()
        assert "identifiers" in d
        assert isinstance(d["identifiers"], dict)

    def test_identifiers_is_dict_with_string_values(self):
        card = _make_card_set(identifiers={"scryfallId": "abc", "mtgoId": "123"})
        d = card.to_polars_dict()
        assert isinstance(d["identifiers"], dict)
        for v in d["identifiers"].values():
            assert isinstance(v, str | type(None))

    def test_colors_wubrg_all_five(self):
        card = _make_card_set(layout="split", colors=["G", "R", "B", "U", "W"])
        d = card.to_polars_dict()
        assert d["colors"] == ["W", "U", "B", "R", "G"]

    def test_exclude_none_omits_optional_bools(self):
        card = _make_card_set(isFunny=None)
        d = card.to_polars_dict(exclude_none=True)
        assert "isFunny" not in d

    def test_exclude_none_keeps_falsey_required(self):
        card = _make_card_set(manaValue=0.0)
        d = card.to_polars_dict(exclude_none=True)
        assert "manaValue" in d
        assert d["manaValue"] == 0.0
