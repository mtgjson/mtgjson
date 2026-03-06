"""Tests for CSV output builder — _flatten_for_csv and round-trip."""

from __future__ import annotations

import json

import polars as pl

from mtgjson5.build.formats.csv import _flatten_for_csv

# =============================================================================
# TestFlattenForCsv
# =============================================================================


class TestFlattenForCsv:
    def test_struct_to_json_string(self):
        df = pl.DataFrame(
            {"ids": [{"scryfallId": "sf-001", "multiverseId": "123"}]},
            schema={"ids": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String})},
        )
        result = _flatten_for_csv(df)
        val = result["ids"][0]
        parsed = json.loads(val)
        assert parsed["scryfallId"] == "sf-001"
        assert parsed["multiverseId"] == "123"

    def test_no_complex_columns_passthrough(self):
        df = pl.DataFrame({"name": ["Alpha"], "count": [5]})
        result = _flatten_for_csv(df)
        assert result.equals(df)

    def test_struct_only_flattened(self):
        df = pl.DataFrame(
            {
                "name": ["Alpha"],
                "ids": [{"a": "1"}],
            },
            schema={
                "name": pl.String,
                "ids": pl.Struct({"a": pl.String}),
            },
        )
        result = _flatten_for_csv(df)
        assert result.schema["ids"] == pl.String
        assert result.schema["name"] == pl.String


# =============================================================================
# TestCsvRoundTrip
# =============================================================================


class TestCsvRoundTrip:
    def test_roundtrip_column_names(self, tmp_path):
        df = pl.DataFrame(
            {
                "uuid": ["a", "b"],
                "name": ["Alpha", "Beta"],
                "cmc": [1, 2],
            }
        )
        flat = _flatten_for_csv(df)
        path = tmp_path / "test.csv"
        flat.write_csv(path)
        read_back = pl.read_csv(path)
        assert set(read_back.columns) == set(flat.columns)

    def test_scalar_types_survive_roundtrip(self, tmp_path):
        df = pl.DataFrame(
            {
                "name": ["Alpha"],
                "count": [42],
                "price": [3.14],
                "flag": [True],
            }
        )
        flat = _flatten_for_csv(df)
        path = tmp_path / "test.csv"
        flat.write_csv(path)
        read_back = pl.read_csv(path)
        assert read_back["name"][0] == "Alpha"
        assert read_back["count"][0] == 42
        assert abs(read_back["price"][0] - 3.14) < 0.001
        assert read_back["flag"][0] is True


# =============================================================================
# TestCsvListColumnFlattened
# =============================================================================


class TestCsvListColumnFlattened:
    def test_struct_column_flattened_to_string(self):
        df = pl.DataFrame(
            {"ids": [{"a": "1", "b": "2"}]},
            schema={"ids": pl.Struct({"a": pl.String, "b": pl.String})},
        )
        result = _flatten_for_csv(df)
        assert result["ids"].dtype == pl.String


# =============================================================================
# TestCsvSpecialCharacters
# =============================================================================


class TestCsvSpecialCharacters:
    def test_commas_in_values_roundtrip(self, tmp_path):
        df = pl.DataFrame({"name": ["Alpha, Beta"]})
        path = tmp_path / "test.csv"
        df.write_csv(path)
        read_back = pl.read_csv(path)
        assert read_back["name"][0] == "Alpha, Beta"

    def test_quotes_in_values_roundtrip(self, tmp_path):
        df = pl.DataFrame({"name": ['said "hello"']})
        path = tmp_path / "test.csv"
        df.write_csv(path)
        read_back = pl.read_csv(path)
        assert read_back["name"][0] == 'said "hello"'

    def test_newlines_in_values_roundtrip(self, tmp_path):
        df = pl.DataFrame({"text": ["Line1\nLine2"]})
        path = tmp_path / "test.csv"
        df.write_csv(path)
        read_back = pl.read_csv(path)
        assert read_back["text"][0] == "Line1\nLine2"
