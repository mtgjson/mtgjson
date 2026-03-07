"""Tests for compress_generator: compression helpers and StreamingCompressor."""

from __future__ import annotations

import bz2
import gzip
import lzma
import pathlib
import zipfile

import pytest

from mtgjson5.compress_generator import (
    StreamingCompressor,
    _compress_file_python,
    _compress_single_format,
    _get_compression_workers,
)

# ---------------------------------------------------------------------------
# TestGetCompressionWorkers
# ---------------------------------------------------------------------------


class TestGetCompressionWorkers:
    def test_returns_int(self):
        assert isinstance(_get_compression_workers(), int)

    def test_minimum_two(self):
        assert _get_compression_workers() >= 2

    def test_maximum_sixteen(self):
        assert _get_compression_workers() <= 16


# ---------------------------------------------------------------------------
# TestCompressSingleFormat
# ---------------------------------------------------------------------------


class TestCompressSingleFormat:
    @pytest.fixture
    def sample_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        f = tmp_path / "test.json"
        f.write_text('{"hello": "world"}')
        return f

    @pytest.mark.parametrize("fmt", ["gz", "bz2", "xz", "zip"])
    def test_compress_creates_output(self, sample_file: pathlib.Path, fmt: str):
        ok, returned_fmt = _compress_single_format(sample_file, fmt)
        assert ok is True
        assert returned_fmt == fmt
        assert pathlib.Path(f"{sample_file}.{fmt}").exists()

    def test_unknown_format_returns_false(self, sample_file: pathlib.Path):
        ok, fmt = _compress_single_format(sample_file, "lz4")
        assert ok is False
        assert fmt == "lz4"

    def test_gz_decompresses_correctly(self, sample_file: pathlib.Path):
        _compress_single_format(sample_file, "gz")
        with gzip.open(f"{sample_file}.gz", "rb") as f:
            assert f.read() == sample_file.read_bytes()

    def test_bz2_decompresses_correctly(self, sample_file: pathlib.Path):
        _compress_single_format(sample_file, "bz2")
        with bz2.open(f"{sample_file}.bz2", "rb") as f:
            assert f.read() == sample_file.read_bytes()

    def test_xz_decompresses_correctly(self, sample_file: pathlib.Path):
        _compress_single_format(sample_file, "xz")
        with lzma.open(f"{sample_file}.xz", "rb") as f:
            assert f.read() == sample_file.read_bytes()

    def test_zip_decompresses_correctly(self, sample_file: pathlib.Path):
        _compress_single_format(sample_file, "zip")
        with zipfile.ZipFile(f"{sample_file}.zip") as zf:
            assert zf.read(sample_file.name) == sample_file.read_bytes()


# ---------------------------------------------------------------------------
# TestCompressFilePython
# ---------------------------------------------------------------------------


class TestCompressFilePython:
    def test_all_four_formats_succeed(self, tmp_path: pathlib.Path):
        f = tmp_path / "data.json"
        f.write_bytes(b"test data " * 100)
        results = _compress_file_python(f)
        assert len(results) == 4
        assert all(ok for ok, _ in results)

    def test_all_output_files_created(self, tmp_path: pathlib.Path):
        f = tmp_path / "data.json"
        f.write_bytes(b"content")
        _compress_file_python(f)
        for ext in ("gz", "bz2", "xz", "zip"):
            assert pathlib.Path(f"{f}.{ext}").exists()


# ---------------------------------------------------------------------------
# TestStreamingCompressor
# ---------------------------------------------------------------------------


class TestStreamingCompressor:
    @pytest.mark.parametrize("fmt", ["gz", "bz2", "xz", "zip"])
    def test_streaming_roundtrip(self, tmp_path: pathlib.Path, fmt: str):
        data = b"streaming test data " * 50
        output = tmp_path / f"output.{fmt}"
        sc = StreamingCompressor(output, fmt, "original.json")
        with sc:
            sc.write(data)

        if fmt == "gz":
            with gzip.open(output, "rb") as f:
                result = f.read()
        elif fmt == "bz2":
            with bz2.open(output, "rb") as f:
                result = f.read()
        elif fmt == "xz":
            with lzma.open(output, "rb") as f:
                result = f.read()
        else:
            with zipfile.ZipFile(output) as zf:
                result = zf.read("original.json")
        assert result == data

    def test_unknown_format_raises(self, tmp_path: pathlib.Path):
        output = tmp_path / "output.lz4"
        sc = StreamingCompressor(output, "lz4", "test.json")
        with pytest.raises(ValueError, match="Unknown format"), sc:
            pass

    def test_multiple_writes(self, tmp_path: pathlib.Path):
        output = tmp_path / "output.gz"
        sc = StreamingCompressor(output, "gz", "test.json")
        with sc:
            sc.write(b"chunk1")
            sc.write(b"chunk2")
            sc.write(b"chunk3")
        with gzip.open(output, "rb") as f:
            assert f.read() == b"chunk1chunk2chunk3"
