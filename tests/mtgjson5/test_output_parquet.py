"""Tests for Parquet output builder — nested type preservation and round-trip."""

from __future__ import annotations

import polars as pl

# =============================================================================
# TestParquetNestedTypes
# =============================================================================


class TestParquetNestedTypes:
    def test_struct_preserved(self, tmp_path):
        df = pl.DataFrame(
            {"uuid": ["a"], "ids": [{"scryfallId": "sf-001", "multiverseId": "123"}]},
            schema={
                "uuid": pl.String,
                "ids": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String}),
            },
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path, compression="zstd", compression_level=9)
        read_back = pl.read_parquet(path)
        assert isinstance(read_back.schema["ids"], pl.Struct)
        assert read_back["ids"].struct.field("scryfallId")[0] == "sf-001"

    def test_list_preserved(self, tmp_path):
        df = pl.DataFrame(
            {"uuid": ["a"], "colors": [["R", "G"]]},
            schema={"uuid": pl.String, "colors": pl.List(pl.String)},
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path, compression="zstd", compression_level=9)
        read_back = pl.read_parquet(path)
        assert isinstance(read_back.schema["colors"], pl.List)
        assert read_back["colors"][0].to_list() == ["R", "G"]


# =============================================================================
# TestParquetRoundTrip
# =============================================================================


class TestParquetRoundTrip:
    def test_data_matches_after_roundtrip(self, tmp_path):
        df = pl.DataFrame(
            {
                "uuid": ["a", "b"],
                "name": ["Alpha", "Beta"],
                "cmc": [1, 2],
                "price": [1.5, None],
            }
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path, compression="zstd", compression_level=9)
        read_back = pl.read_parquet(path)
        assert read_back.equals(df)

    def test_schema_preserved(self, tmp_path):
        df = pl.DataFrame(
            {
                "uuid": ["a"],
                "name": ["Alpha"],
                "count": [42],
                "flag": [True],
                "ids": [{"a": "1"}],
                "colors": [["R"]],
            },
            schema={
                "uuid": pl.String,
                "name": pl.String,
                "count": pl.Int64,
                "flag": pl.Boolean,
                "ids": pl.Struct({"a": pl.String}),
                "colors": pl.List(pl.String),
            },
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path, compression="zstd", compression_level=9)
        read_back = pl.read_parquet(path)
        assert list(read_back.columns) == list(df.columns)
        for col in df.columns:
            assert read_back.schema[col] == df.schema[col], f"Schema mismatch for {col}"


# =============================================================================
# TestParquetTypeSpecificPreservation
# =============================================================================


class TestParquetTypeSpecificPreservation:
    def test_bool_preserved(self, tmp_path):
        df = pl.DataFrame(
            {"flag": [True, False]},
            schema={"flag": pl.Boolean},
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        read_back = pl.read_parquet(path)
        assert read_back.schema["flag"] == pl.Boolean
        assert read_back["flag"][0] is True
        assert read_back["flag"][1] is False

    def test_int64_with_nulls_preserved(self, tmp_path):
        df = pl.DataFrame(
            {"val": [1, 2, None]},
            schema={"val": pl.Int64},
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        read_back = pl.read_parquet(path)
        assert read_back.schema["val"] == pl.Int64
        assert read_back["val"][2] is None

    def test_null_struct_field_preserved(self, tmp_path):
        df = pl.DataFrame(
            {"ids": [{"a": "1"}, None]},
            schema={"ids": pl.Struct({"a": pl.String})},
        )
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        read_back = pl.read_parquet(path)
        assert isinstance(read_back.schema["ids"], pl.Struct)
        assert read_back["ids"][1] is None
