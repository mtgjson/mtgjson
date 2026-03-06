"""Tests for v2 base models: PolarsMixin, MtgjsonFileBase."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from mtgjson5.models.base import MtgjsonFileBase
from mtgjson5.models.cards import CardSet, CardSetDeck

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_card_set_deck(**overrides) -> CardSetDeck:
    defaults = {"count": 1, "uuid": "test-uuid-001"}
    defaults.update(overrides)
    return CardSetDeck(**defaults)


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


# ---------------------------------------------------------------------------
# TestPolarsMixinToPolarsDict
# ---------------------------------------------------------------------------


class TestPolarsMixinToPolarsDict:
    def test_basic_serialization_uses_alias(self):
        csd = _make_card_set_deck(is_foil=True)
        d = csd.to_polars_dict(use_alias=True)
        assert "isFoil" in d
        assert d["isFoil"] is True

    def test_exclude_none_omits_none(self):
        csd = _make_card_set_deck(is_foil=None)
        d = csd.to_polars_dict(exclude_none=True)
        assert "isFoil" not in d

    def test_allow_if_falsey_kept(self):
        """Fields in ALLOW_IF_FALSEY should be kept even when falsey with correct values."""
        card = _make_card_set()
        d = card.to_polars_dict(exclude_none=True)
        assert d["uuid"] == "test-uuid"
        assert d["setCode"] == "TST"
        assert d["type"] == "Creature"
        assert d["layout"] == "normal"
        assert d["name"] == "Test Card"
        assert d["manaValue"] == 0.0
        assert d["convertedManaCost"] == 0.0

    def test_legalities_empty_becomes_empty_dict(self):
        """Legalities default_factory=dict yields {} -> serializes as {}."""
        card = _make_card_set()
        d = card.to_polars_dict()
        assert d["legalities"] == {}

    def test_purchase_urls_none_becomes_empty_dict(self):
        card = _make_card_set()
        d = card.to_polars_dict()
        assert d["purchaseUrls"] == {}

    def test_rulings_sorted_by_date_then_text(self):
        card = _make_card_set(
            rulings=[
                {"date": "2022-01-01", "text": "B"},
                {"date": "2021-01-01", "text": "A"},
                {"date": "2022-01-01", "text": "A"},
            ]
        )
        d = card.to_polars_dict()
        dates = [r["date"] for r in d["rulings"]]
        assert dates == ["2021-01-01", "2022-01-01", "2022-01-01"]
        texts = [r["text"] for r in d["rulings"]]
        assert texts[1] == "A"
        assert texts[2] == "B"


# ---------------------------------------------------------------------------
# TestPolarsMixinRoundTrip
# ---------------------------------------------------------------------------


class TestPolarsMixinRoundTrip:
    def test_card_set_deck_roundtrip(self):
        original = _make_card_set_deck(count=3, is_foil=True)
        d = original.to_polars_dict()
        restored = CardSetDeck.from_polars_row(d)
        assert restored.count == original.count
        assert isinstance(restored.count, int)
        assert restored.uuid == original.uuid
        assert restored.is_foil is True

    def test_polars_schema_valid(self):
        schema = CardSetDeck.polars_schema()
        assert isinstance(schema, pl.Schema)
        assert len(schema) > 0

    def test_to_dataframe_empty(self):
        df = CardSetDeck.to_dataframe([])
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert len(df.columns) > 0

    def test_to_dataframe_schema(self):
        items = [_make_card_set_deck(count=1, uuid="uuid-1")]
        df = CardSetDeck.to_dataframe(items)
        assert "count" in df.columns
        assert "uuid" in df.columns
        assert df.schema["count"] == pl.Int64
        assert df.schema["uuid"] == pl.String

    def test_from_dataframe_with_known_input(self):
        df = pl.DataFrame(
            {"count": [3], "uuid": ["manual-uuid"], "isFoil": [True]},
            schema={"count": pl.Int64, "uuid": pl.String, "isFoil": pl.Boolean},
        )
        restored = CardSetDeck.from_dataframe(df)
        assert len(restored) == 1
        assert restored[0].count == 3
        assert restored[0].uuid == "manual-uuid"

    def test_to_from_dataframe_roundtrip(self):
        items = [
            _make_card_set_deck(count=1, uuid="uuid-1"),
            _make_card_set_deck(count=2, uuid="uuid-2"),
        ]
        df = CardSetDeck.to_dataframe(items)
        restored = CardSetDeck.from_dataframe(df)
        assert len(restored) == 2
        assert restored[0].uuid == "uuid-1"
        assert restored[1].uuid == "uuid-2"

    def test_from_polars_row_handles_alias_keys(self):
        d = {"count": 4, "isFoil": True, "uuid": "test-123"}
        restored = CardSetDeck.from_polars_row(d)
        assert restored.count == 4
        assert restored.uuid == "test-123"


# ---------------------------------------------------------------------------
# TestMtgjsonFileBase
# ---------------------------------------------------------------------------


class TestMtgjsonFileBase:
    def test_make_meta_has_date_and_version(self):
        meta = MtgjsonFileBase.make_meta()
        assert "date" in meta
        assert "version" in meta
        assert meta["date"] == date.today().isoformat()

    def test_model_dump_has_meta_and_data(self):
        """Subclass with data field should dump both meta and data."""

        class TestFile(MtgjsonFileBase):
            data: dict[str, str]

        f = TestFile(meta={"date": "2024-01-01", "version": "5.3.0"}, data={"key": "value"})
        dumped = f.model_dump(by_alias=True)
        assert "meta" in dumped
        assert "data" in dumped
        assert dumped["data"]["key"] == "value"

    def test_json_schema_generates_valid_dict(self):
        schema = CardSetDeck.json_schema()
        assert isinstance(schema, dict)
        assert "properties" in schema or "$defs" in schema or "title" in schema

    def test_write_read_roundtrip(self, tmp_path):
        """Write/read round-trip using orjson."""
        pytest.importorskip("orjson")

        class SimpleFile(MtgjsonFileBase):
            data: dict[str, str]

        original = SimpleFile(
            meta={"date": "2024-01-01", "version": "5.3.0"},
            data={"hello": "world"},
        )
        path = tmp_path / "test.json"
        original.write(path)
        restored = SimpleFile.read(path)
        assert restored.meta["date"] == "2024-01-01"
        assert restored.data["hello"] == "world"


# ---------------------------------------------------------------------------
# TestRoundTripTypePreservation
# ---------------------------------------------------------------------------


class TestRoundTripTypePreservation:
    def test_roundtrip_preserves_bool_false(self):
        original = _make_card_set_deck(count=1, is_foil=False)
        d = original.to_polars_dict()
        restored = CardSetDeck.from_polars_row(d)
        assert restored.is_foil is False

    def test_roundtrip_preserves_int_zero(self):
        original = _make_card_set_deck(count=0, uuid="zero-count")
        d = original.to_polars_dict()
        restored = CardSetDeck.from_polars_row(d)
        assert restored.count == 0
        assert isinstance(restored.count, int)

    def test_exclude_none_keeps_empty_required_lists(self):
        """Required list fields like colors=[] should remain as []."""
        card = _make_card_set(colors=[])
        d = card.to_polars_dict(exclude_none=True)
        assert d["colors"] == []

    def test_exclude_none_omits_empty_optional_lists(self):
        """Optional list fields like keywords=[] should be omitted."""
        card = _make_card_set(keywords=[])
        d = card.to_polars_dict(exclude_none=True)
        assert "keywords" not in d

    def test_exclude_none_keeps_falsey_mana_value(self):
        """manaValue=0.0 must be kept since it's in ALLOW_IF_FALSEY."""
        card = _make_card_set(manaValue=0.0)
        d = card.to_polars_dict(exclude_none=True)
        assert "manaValue" in d
        assert d["manaValue"] == 0.0
