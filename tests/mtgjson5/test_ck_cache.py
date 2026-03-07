"""Tests for mtgjson5.providers.cardkingdom.cache: CardKingdomStorage."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from mtgjson5.providers.cardkingdom.cache import CardKingdomStorage


class TestCardKingdomStorage:
    def test_write_and_read_roundtrip(self, tmp_path: Path):
        df = pl.DataFrame({
            "name": ["Lightning Bolt", "Dark Ritual"],
            "price": [1.99, 0.49],
        })
        path = tmp_path / "ck_cache.parquet"
        CardKingdomStorage.write(df, path)
        assert path.exists()

        loaded = CardKingdomStorage.read(path)
        assert loaded.shape == df.shape
        assert loaded["name"].to_list() == ["Lightning Bolt", "Dark Ritual"]

    def test_write_creates_parent_dirs(self, tmp_path: Path):
        df = pl.DataFrame({"name": ["Test"]})
        path = tmp_path / "sub" / "dir" / "cache.parquet"
        result = CardKingdomStorage.write(df, path)
        assert result == path
        assert path.exists()

    def test_write_returns_path(self, tmp_path: Path):
        df = pl.DataFrame({"name": ["Test"]})
        path = tmp_path / "cache.parquet"
        result = CardKingdomStorage.write(df, path)
        assert isinstance(result, Path)
        assert result == path

    def test_exists_true(self, tmp_path: Path):
        path = tmp_path / "exists.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(path)
        assert CardKingdomStorage.exists(path) is True

    def test_exists_false(self, tmp_path: Path):
        path = tmp_path / "missing.parquet"
        assert CardKingdomStorage.exists(path) is False

    def test_write_string_path(self, tmp_path: Path):
        df = pl.DataFrame({"name": ["Test"]})
        path = str(tmp_path / "cache.parquet")
        result = CardKingdomStorage.write(df, path)
        assert Path(path).exists()

    def test_read_string_path(self, tmp_path: Path):
        df = pl.DataFrame({"name": ["Test"]})
        path = tmp_path / "cache.parquet"
        df.write_parquet(path)
        loaded = CardKingdomStorage.read(str(path))
        assert loaded.shape == df.shape
