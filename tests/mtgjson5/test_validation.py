"""Tests for v2 pipeline validation: StageSchema, validate_stage, pre-defined schemas."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.pipeline.validation import (
    STAGE_POST_EXPLODE,
    STAGE_PRE_SINK,
    ColumnSpec,
    PipelineValidationError,
    StageSchema,
    validate_stage,
)

# ---------------------------------------------------------------------------
# TestStageSchemaValidate
# ---------------------------------------------------------------------------


class TestStageSchemaValidate:
    def test_all_required_present(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a", pl.Int64), ColumnSpec("b", pl.String)],
        )
        lf = pl.LazyFrame({"a": [1], "b": ["x"]})
        warnings = schema.validate(lf)
        assert not warnings

    def test_missing_required_column_raises(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a"), ColumnSpec("missing")],
        )
        lf = pl.LazyFrame({"a": [1]})
        with pytest.raises(PipelineValidationError, match="Missing required columns"):
            schema.validate(lf)

    def test_type_mismatch_raises(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a", pl.String)],
        )
        lf = pl.LazyFrame({"a": [1]})  # Int64, not String
        with pytest.raises(PipelineValidationError, match="Type mismatches"):
            schema.validate(lf)

    def test_numeric_type_compatibility(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a", pl.Int64)],
        )
        lf = pl.LazyFrame({"a": pl.Series([1], dtype=pl.Int32)})
        warnings = schema.validate(lf)
        assert not warnings

    def test_string_type_compatibility(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a", pl.Utf8)],
        )
        lf = pl.LazyFrame({"a": ["x"]})  # String == Utf8 in modern Polars
        warnings = schema.validate(lf)
        assert not warnings

    def test_forbidden_column_returns_warning(self):
        schema = StageSchema(
            name="test",
            required=[],
            forbidden={"_temp"},
        )
        lf = pl.LazyFrame({"a": [1], "_temp": [2]})
        warnings = schema.validate(lf)
        assert len(warnings) == 1
        assert "_temp" in warnings[0]


# ---------------------------------------------------------------------------
# TestValidateStage
# ---------------------------------------------------------------------------


class TestValidateStage:
    def test_strict_with_warnings_raises(self):
        schema = StageSchema(name="test", forbidden={"_temp"})
        lf = pl.LazyFrame({"a": [1], "_temp": [2]})
        with pytest.raises(PipelineValidationError):
            validate_stage(lf, schema, strict=True)

    def test_non_strict_with_warnings_returns_lf(self):
        schema = StageSchema(name="test", forbidden={"_temp"})
        lf = pl.LazyFrame({"a": [1], "_temp": [2]})
        result = validate_stage(lf, schema, strict=False)
        assert result.collect().equals(lf.collect())

    def test_valid_schema_returns_lf(self):
        schema = StageSchema(
            name="test",
            required=[ColumnSpec("a")],
        )
        lf = pl.LazyFrame({"a": [1]})
        result = validate_stage(lf, schema)
        assert result.collect().equals(lf.collect())


# ---------------------------------------------------------------------------
# TestPreDefinedSchemas
# ---------------------------------------------------------------------------


class TestPreDefinedSchemas:
    def test_post_explode_validates_valid_frame(self):
        lf = pl.LazyFrame(
            {
                "_row_id": pl.Series([0], dtype=pl.UInt32),
                "faceId": pl.Series([1], dtype=pl.Int64),
                "side": pl.Series(["a"], dtype=pl.String),
                "_face_data": pl.Series([{"name": "Test"}], dtype=pl.Struct({"name": pl.String})),
            }
        )
        warnings = STAGE_POST_EXPLODE.validate(lf)
        assert not warnings

    def test_pre_sink_rejects_temp_columns(self):
        lf = pl.LazyFrame(
            {
                "uuid": ["u1"],
                "name": ["Test"],
                "setCode": ["TST"],
                "_row_id": [0],
            }
        )
        warnings = STAGE_PRE_SINK.validate(lf)
        assert len(warnings) == 1
        assert "_row_id" in warnings[0]


# ---------------------------------------------------------------------------
# TestPipelineValidationError
# ---------------------------------------------------------------------------


class TestPipelineValidationError:
    def test_error_attributes(self):
        err = PipelineValidationError(
            stage="test_stage",
            message="bad columns",
            missing_columns={"a", "b"},
            type_mismatches={"c": (pl.Int64, pl.String)},
        )
        assert err.stage == "test_stage"
        assert err.missing_columns == {"a", "b"}
        assert "c" in err.type_mismatches
        assert "[test_stage]" in str(err)
