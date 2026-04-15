"""Tests for mtgjson5.providers.github.models: _python_type_to_polars and PolarsSchemaModel."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.providers.github.provider import _build_decks_records
from mtgjson5.providers.github.models import (
    CardEntryModel,
    CardRef,
    ContentConfig,
    DeckRef,
    PackRef,
    PreconModel,
    SealedProductModel,
    SealedRef,
    VariableConfig,
    _python_type_to_polars,
)

# ---------------------------------------------------------------------------
# TestPythonTypeToPolars
# ---------------------------------------------------------------------------


class TestPythonTypeToPolars:
    def test_none_type(self):
        assert _python_type_to_polars(type(None)) == pl.Null()

    def test_dict_type(self):
        result = _python_type_to_polars(dict[str, str])
        assert result == pl.Object()

    def test_list_generic(self):
        result = _python_type_to_polars(list[str])
        assert isinstance(result, pl.List)

    def test_returns_polars_dtype(self):
        """All inputs should return some pl.DataType."""
        for tp in [str, int, float, bool, type(None), list[str], dict[str, str], object]:
            result = _python_type_to_polars(tp)
            assert isinstance(result, pl.DataType), f"Failed for {tp}"


# ---------------------------------------------------------------------------
# TestPolarsSchemaModel
# ---------------------------------------------------------------------------


class TestPolarsSchemaModel:
    def test_polars_schema_returns_dict(self):
        schema = DeckRef.polars_schema()
        assert isinstance(schema, dict)
        assert "set" in schema
        assert "name" in schema

    def test_polars_schema_doc_format(self):
        doc = DeckRef.polars_schema_doc()
        assert "DeckRef Polars schema:" in doc
        assert "set:" in doc
        assert "name:" in doc

    def test_polars_schema_doc_multifield(self):
        doc = CardRef.polars_schema_doc()
        assert "CardRef Polars schema:" in doc
        for field in ["name", "set", "number", "uuid", "foil"]:
            assert f"{field}:" in doc


# ---------------------------------------------------------------------------
# TestAllModelsProduceSchemas
# ---------------------------------------------------------------------------


class TestAllModelsProduceSchemas:
    @pytest.mark.parametrize(
        "model",
        [
            DeckRef,
            PackRef,
            SealedRef,
            CardRef,
            VariableConfig,
            ContentConfig,
            CardEntryModel,
            PreconModel,
            SealedProductModel,
        ],
    )
    def test_model_schema_is_dict(self, model):
        schema = model.polars_schema()
        assert isinstance(schema, dict)
        assert len(schema) > 0

    @pytest.mark.parametrize(
        "model",
        [
            DeckRef,
            PackRef,
            SealedRef,
            CardRef,
            VariableConfig,
            ContentConfig,
            CardEntryModel,
            PreconModel,
            SealedProductModel,
        ],
    )
    def test_model_schema_doc_is_string(self, model):
        doc = model.polars_schema_doc()
        assert isinstance(doc, str)
        assert model.__name__ in doc


class TestBuildDeckRecords:
    def test_build_decks_records_preserves_source(self):
        records = _build_decks_records(
            [
                {
                    "name": "Deck A",
                    "set_code": "tst",
                    "type": "Commander",
                    "release_date": "2025-01-01",
                    "source": "https://example.com/deck-a",
                    "sourceSetCodes": ["tst"],
                    "cards": [{"mtgjson_uuid": "uuid-001", "count": 1, "foil": False}],
                    "sideboard": [],
                    "commander": [],
                    "displayCommander": [],
                    "tokens": [],
                    "planarDeck": [],
                    "schemeDeck": [],
                }
            ],
            {"tst": {"Deck A": ["sealed-001"]}},
        )

        assert len(records) == 1
        assert records[0]["source"] == "https://example.com/deck-a"
        assert records[0]["setCode"] == "TST"
        assert records[0]["sealedProductUuids"] == ["sealed-001"]
