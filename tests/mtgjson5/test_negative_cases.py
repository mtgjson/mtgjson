"""Tests for error handling and edge cases."""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from mtgjson5.build.serializers import (
    batched,
    escape_sqlite,
    serialize_complex_types,
)
from mtgjson5.models.cards import CardSet, CardSetDeck
from mtgjson5.models.sealed import SealedProduct

# =============================================================================
# TestCardSetMissingRequired
# =============================================================================


class TestCardSetMissingRequired:
    def test_missing_uuid_raises(self):
        with pytest.raises(ValidationError):
            CardSet(
                name="Test",
                type="Creature",
                layout="normal",
                setCode="TST",
                number="1",
                borderColor="black",
                frameVersion="2015",
                hasFoil=False,
                hasNonFoil=True,
                rarity="common",
                convertedManaCost=0.0,
                manaValue=0.0,
                # uuid intentionally omitted
            )

    def test_missing_name_raises(self):
        with pytest.raises(ValidationError):
            CardSet(
                uuid="test-uuid",
                type="Creature",
                layout="normal",
                setCode="TST",
                number="1",
                borderColor="black",
                frameVersion="2015",
                hasFoil=False,
                hasNonFoil=True,
                rarity="common",
                convertedManaCost=0.0,
                manaValue=0.0,
                # name intentionally omitted
            )


# =============================================================================
# TestSealedProductMissingRequired
# =============================================================================


class TestSealedProductMissingRequired:
    def test_missing_name_raises(self):
        with pytest.raises(ValidationError):
            SealedProduct(uuid="sp-001")

    def test_missing_uuid_raises(self):
        with pytest.raises(ValidationError):
            SealedProduct(name="Test Product")


# =============================================================================
# TestSerializeComplexTypesEdgeCases
# =============================================================================


class TestSerializeComplexTypesEdgeCases:
    def test_empty_dataframe(self):
        df = pl.DataFrame(schema={"colors": pl.List(pl.String), "ids": pl.Struct({"a": pl.String})})
        result = serialize_complex_types(df)
        assert len(result) == 0
        assert result.schema["colors"] == pl.String
        assert result.schema["ids"] == pl.String

    def test_all_null_struct(self):
        df = pl.DataFrame(
            {"ids": [None, None]},
            schema={"ids": pl.Struct({"a": pl.String})},
        )
        result = serialize_complex_types(df)
        assert result["ids"][0] is None
        assert result["ids"][1] is None

    def test_all_null_list(self):
        df = pl.DataFrame(
            {"colors": [None, None]},
            schema={"colors": pl.List(pl.String)},
        )
        result = serialize_complex_types(df)
        assert result["colors"][0] is None
        assert result["colors"][1] is None


# =============================================================================
# TestFromPolarsRowExtraKeys
# =============================================================================


class TestFromPolarsRowExtraKeys:
    def test_extra_keys_ignored(self):
        d = {"count": 4, "isFoil": True, "uuid": "test-123", "extraField": "should-ignore"}
        restored = CardSetDeck.from_polars_row(d)
        assert restored.count == 4
        assert restored.uuid == "test-123"
        assert not hasattr(restored, "extraField")


# =============================================================================
# TestEscapeSqliteNestedList
# =============================================================================


class TestEscapeSqliteNestedList:
    def test_list_of_dicts(self):
        val = [{"a": 1}, {"b": 2}]
        result = escape_sqlite(val)
        assert result.startswith("'")
        assert result.endswith("'")
        # Should be valid JSON inside the quotes
        inner = result[1:-1].replace("''", "'")
        parsed = __import__("json").loads(inner)
        assert parsed == [{"a": 1}, {"b": 2}]

    def test_nested_dict(self):
        val = {"outer": {"inner": "value"}}
        result = escape_sqlite(val)
        inner = result[1:-1].replace("''", "'")
        parsed = __import__("json").loads(inner)
        assert parsed == {"outer": {"inner": "value"}}


# =============================================================================
# TestBatchedEdgeCases
# =============================================================================


class TestBatchedEdgeCases:
    def test_single_item(self):
        assert list(batched([1], 5)) == [[1]]

    def test_batch_size_one(self):
        assert list(batched([1, 2, 3], 1)) == [[1], [2], [3]]

    def test_large_batch_size(self):
        assert list(batched([1, 2], 100)) == [[1, 2]]
