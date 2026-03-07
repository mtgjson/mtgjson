"""Tests for mtgjson5.polars_utils.preprocess: normalization pipeline functions."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from mtgjson5.polars_utils.preprocess import (
    CARD_FACE_FIELD_RENAMES,
    SCRYFALL_TO_PIPELINE,
    add_computed_fields,
    build_face_rename_expr,
    infer_card_face_schema,
    normalize_all_parts,
    normalize_card_faces,
    normalize_column_names,
    validate_required_columns,
)


# ---------------------------------------------------------------------------
# normalize_column_names
# ---------------------------------------------------------------------------


class TestNormalizeColumnNames:
    def test_renames_known_columns(self):
        lf = pl.LazyFrame({
            "collector_number": ["1"],
            "type_line": ["Creature"],
            "oracle_text": ["Flying"],
        })
        result = normalize_column_names(lf).collect()
        assert "collectorNumber" in result.columns
        assert "typeLine" in result.columns
        assert "oracleText" in result.columns

    def test_leaves_unknown_columns(self):
        lf = pl.LazyFrame({
            "name": ["Test"],
            "custom_field": ["val"],
        })
        result = normalize_column_names(lf).collect()
        assert "name" in result.columns
        assert "custom_field" in result.columns

    def test_handles_partial_columns(self):
        lf = pl.LazyFrame({
            "collector_number": ["1"],
            "name": ["Test"],
        })
        result = normalize_column_names(lf).collect()
        assert "collectorNumber" in result.columns
        assert "name" in result.columns

    def test_empty_frame(self):
        lf = pl.LazyFrame({"id": pl.Series([], dtype=pl.String)})
        result = normalize_column_names(lf).collect()
        assert "id" in result.columns


# ---------------------------------------------------------------------------
# normalize_card_faces
# ---------------------------------------------------------------------------


class TestNormalizeCardFaces:
    def test_renames_struct_fields(self):
        face = {"type_line": "Creature", "name": "Test", "oracle_text": "Flying"}
        lf = pl.LazyFrame({
            "cardFaces": [[face]],
        })
        result = normalize_card_faces(lf).collect()
        faces = result["cardFaces"][0]
        field_names = faces[0].keys() if isinstance(faces[0], dict) else [f.name for f in faces[0]]
        assert "typeLine" in field_names
        assert "oracleText" in field_names
        assert "name" in field_names  # unchanged

    def test_no_card_faces_column(self):
        lf = pl.LazyFrame({"name": ["Test"]})
        result = normalize_card_faces(lf).collect()
        assert "name" in result.columns

    def test_non_list_card_faces(self):
        lf = pl.LazyFrame({"cardFaces": ["not_a_list"]})
        result = normalize_card_faces(lf).collect()
        assert "cardFaces" in result.columns


# ---------------------------------------------------------------------------
# normalize_all_parts
# ---------------------------------------------------------------------------


class TestNormalizeAllParts:
    def test_renames_type_line(self):
        part = {"id": "abc", "type_line": "Creature", "name": "Test", "component": "combo_piece", "uri": "u"}
        lf = pl.LazyFrame({
            "allParts": [[part]],
        })
        result = normalize_all_parts(lf).collect()
        parts = result["allParts"][0]
        field_names = parts[0].keys() if isinstance(parts[0], dict) else [f.name for f in parts[0]]
        assert "typeLine" in field_names
        assert "name" in field_names  # unchanged

    def test_no_all_parts_column(self):
        lf = pl.LazyFrame({"name": ["Test"]})
        result = normalize_all_parts(lf).collect()
        assert "name" in result.columns

    def test_non_list_all_parts(self):
        lf = pl.LazyFrame({"allParts": ["not_a_list"]})
        result = normalize_all_parts(lf).collect()
        assert "allParts" in result.columns


# ---------------------------------------------------------------------------
# add_computed_fields
# ---------------------------------------------------------------------------


class TestAddComputedFields:
    def test_uppercases_set_code(self):
        lf = pl.LazyFrame({"set": ["neo", "MKM", "dmu"]})
        result = add_computed_fields(lf).collect()
        assert result["set"].to_list() == ["NEO", "MKM", "DMU"]


# ---------------------------------------------------------------------------
# validate_required_columns
# ---------------------------------------------------------------------------


class TestValidateRequiredColumns:
    def _make_required_lf(self, **extra):
        data = {
            "id": ["1"],
            "name": ["Test"],
            "set": ["TST"],
            "lang": ["en"],
            "layout": ["normal"],
            "cmc": [2.0],
            "collectorNumber": ["1"],
            "rarity": ["common"],
            "finishes": [["nonfoil"]],
        }
        data.update(extra)
        return pl.LazyFrame(data)

    def test_all_required_present(self):
        lf = self._make_required_lf()
        result = validate_required_columns(lf).collect()
        assert "id" in result.columns

    def test_missing_required_raises(self):
        lf = pl.LazyFrame({"id": ["1"], "name": ["Test"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_required_columns(lf)

    def test_adds_optional_null_columns(self):
        lf = self._make_required_lf()
        result = validate_required_columns(lf).collect()
        # Optional columns should be added as null
        assert "promoTypes" in result.columns
        assert "frameEffects" in result.columns
        assert "keywords" in result.columns
        assert "games" in result.columns
        assert result["promoTypes"][0] is None

    def test_existing_optional_not_overwritten(self):
        lf = self._make_required_lf(promoTypes=[["boosterfun"]])
        result = validate_required_columns(lf).collect()
        assert result["promoTypes"].to_list() == [["boosterfun"]]


# ---------------------------------------------------------------------------
# infer_card_face_schema
# ---------------------------------------------------------------------------


class TestInferCardFaceSchema:
    def test_returns_schema_for_struct(self):
        df = pl.DataFrame({
            "card_faces": [[{"name": "A", "type_line": "Creature"}]],
        })
        schema = infer_card_face_schema(df)
        assert schema is not None

    def test_returns_none_without_column(self):
        df = pl.DataFrame({"name": ["Test"]})
        assert infer_card_face_schema(df) is None

    def test_returns_none_all_nulls(self):
        df = pl.DataFrame({
            "card_faces": pl.Series([None], dtype=pl.List(pl.Struct({"name": pl.String}))),
        })
        result = infer_card_face_schema(df)
        # All nulls dropped => len 0 => None
        assert result is None


# ---------------------------------------------------------------------------
# build_face_rename_expr
# ---------------------------------------------------------------------------


class TestBuildFaceRenameExpr:
    def test_none_schema_returns_col(self):
        expr = build_face_rename_expr(None)
        # Should just be pl.col("card_faces")
        lf = pl.LazyFrame({"card_faces": [None]})
        result = lf.select(expr).collect()
        assert "card_faces" in result.columns

    def test_non_struct_inner_returns_col(self):
        # List(String) - inner is not Struct
        schema = pl.List(pl.String)
        expr = build_face_rename_expr(schema)
        lf = pl.LazyFrame({"card_faces": [["a", "b"]]})
        result = lf.select(expr).collect()
        assert "card_faces" in result.columns
