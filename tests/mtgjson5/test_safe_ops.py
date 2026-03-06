"""Tests for v2 pipeline safe_ops: safe_drop, safe_rename, safe_struct_field, require_columns."""

from __future__ import annotations

import logging

import polars as pl
import pytest

from mtgjson5.pipeline.safe_ops import (
    require_columns,
    safe_drop,
    safe_rename,
    safe_struct_field,
)

# ---------------------------------------------------------------------------
# TestSafeDrop
# ---------------------------------------------------------------------------


class TestSafeDrop:
    def test_drop_existing_columns(self):
        lf = pl.LazyFrame({"a": [1], "b": [2], "c": [3]})
        result = safe_drop(lf, ["a", "b"]).collect()
        assert result.columns == ["c"]

    def test_drop_mix_existing_and_missing(self, caplog):
        lf = pl.LazyFrame({"a": [1], "b": [2]})
        with caplog.at_level(logging.DEBUG):
            result = safe_drop(lf, ["a", "missing"]).collect()
        assert result.columns == ["b"]
        assert "missing" in caplog.text

    def test_drop_empty_column_list(self):
        lf = pl.LazyFrame({"a": [1], "b": [2]})
        result = safe_drop(lf, []).collect()
        assert result.columns == ["a", "b"]

    def test_drop_all_columns(self):
        lf = pl.LazyFrame({"a": [1], "b": [2]})
        result = safe_drop(lf, ["a", "b"]).collect()
        assert len(result.columns) == 0


# ---------------------------------------------------------------------------
# TestSafeRename
# ---------------------------------------------------------------------------


class TestSafeRename:
    def test_rename_existing_columns(self):
        lf = pl.LazyFrame({"old_a": [1], "old_b": [2]})
        result = safe_rename(lf, {"old_a": "new_a", "old_b": "new_b"}).collect()
        assert set(result.columns) == {"new_a", "new_b"}

    def test_rename_with_some_missing(self, caplog):
        lf = pl.LazyFrame({"a": [1], "b": [2]})
        with caplog.at_level(logging.DEBUG):
            result = safe_rename(lf, {"a": "x", "missing": "y"}).collect()
        assert "x" in result.columns
        assert "b" in result.columns
        assert "missing" in caplog.text

    def test_rename_empty_mapping(self):
        lf = pl.LazyFrame({"a": [1]})
        result = safe_rename(lf, {}).collect()
        assert result.columns == ["a"]


# ---------------------------------------------------------------------------
# TestSafeStructField
# ---------------------------------------------------------------------------


class TestSafeStructField:
    def test_access_existing_struct_field(self):
        lf = pl.LazyFrame(
            {"s": [{"x": 10, "y": 20}]},
            schema={"s": pl.Struct({"x": pl.Int64, "y": pl.Int64})},
        )
        result = lf.select(safe_struct_field("s", "x").alias("val")).collect()
        assert result["val"][0] == 10

    def test_access_when_struct_is_null(self):
        lf = pl.LazyFrame(
            {"s": [None]},
            schema={"s": pl.Struct({"x": pl.Int64})},
        )
        result = lf.select(safe_struct_field("s", "x").alias("val")).collect()
        assert result["val"][0] is None

    def test_custom_default_expression(self):
        lf = pl.LazyFrame(
            {"s": [None]},
            schema={"s": pl.Struct({"x": pl.Int64})},
        )
        result = lf.select(safe_struct_field("s", "x", default=pl.lit(-1)).alias("val")).collect()
        assert result["val"][0] == -1


# ---------------------------------------------------------------------------
# TestRequireColumns
# ---------------------------------------------------------------------------


class TestRequireColumns:
    def test_all_columns_present(self):
        lf = pl.LazyFrame({"a": [1], "b": [2]})
        result = require_columns(lf, {"a", "b"})
        # Should return same LazyFrame unchanged
        assert result.collect().equals(lf.collect())

    def test_missing_columns_raises(self):
        lf = pl.LazyFrame({"a": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            require_columns(lf, {"a", "b", "c"})

    def test_context_message_included(self):
        lf = pl.LazyFrame({"a": [1]})
        with pytest.raises(ValueError, match="test_stage"):
            require_columns(lf, {"missing"}, context="test_stage")
