"""Tests for Scryfall bulk data download/conversion (JSONL + legacy JSON array)."""

from __future__ import annotations

import gzip
import json

import orjson
import pytest

from mtgjson5.providers.scryfall.provider import ScryfallProvider


@pytest.fixture
def provider():
    return ScryfallProvider()


# =============================================================================
# TestSelectDownloadUri
# =============================================================================


class TestSelectDownloadUri:
    def test_prefers_jsonl_download_uri(self):
        item = {
            "download_uri": "https://data.scryfall.io/x/x.json",
            "jsonl_download_uri": "https://data.scryfall.io/x/x.jsonl.gz",
        }
        assert ScryfallProvider._select_download_uri(item) == "https://data.scryfall.io/x/x.jsonl.gz"

    def test_falls_back_to_download_uri(self):
        item = {"download_uri": "https://data.scryfall.io/x/x.json"}
        assert ScryfallProvider._select_download_uri(item) == "https://data.scryfall.io/x/x.json"

    def test_returns_none_when_neither_present(self):
        assert ScryfallProvider._select_download_uri({"type": "all_cards"}) is None


# =============================================================================
# TestConvertFileToNdjson
# =============================================================================


CARDS = [
    {"id": "a", "name": "Alpha", "cmc": 3},
    {"id": "b", "name": "Beta", "cmc": 0},
]


def _read_ndjson(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class TestConvertFileToNdjson:
    def test_gzipped_jsonl_passthrough(self, provider, tmp_path):
        src = tmp_path / "src.jsonl.gz"
        body = b"\n".join(orjson.dumps(c) for c in CARDS) + b"\n"
        src.write_bytes(gzip.compress(body))
        dest = tmp_path / "out.ndjson"

        count = provider._convert_file_to_ndjson(src, dest)

        assert count == 2
        assert _read_ndjson(dest) == CARDS

    def test_plain_jsonl_passthrough(self, provider, tmp_path):
        src = tmp_path / "src.jsonl"
        src.write_bytes(b"\n".join(orjson.dumps(c) for c in CARDS) + b"\n")
        dest = tmp_path / "out.ndjson"

        count = provider._convert_file_to_ndjson(src, dest)

        assert count == 2
        assert _read_ndjson(dest) == CARDS

    def test_gzipped_json_array_legacy(self, provider, tmp_path):
        # Legacy download_uri fallback: a gzipped JSON array must still convert
        src = tmp_path / "src.json.gz"
        src.write_bytes(gzip.compress(orjson.dumps(CARDS)))
        dest = tmp_path / "out.ndjson"

        count = provider._convert_file_to_ndjson(src, dest)

        assert count == 2
        assert _read_ndjson(dest) == CARDS

    def test_plain_json_array_legacy(self, provider, tmp_path):
        src = tmp_path / "src.json"
        src.write_bytes(orjson.dumps(CARDS))
        dest = tmp_path / "out.ndjson"

        count = provider._convert_file_to_ndjson(src, dest)

        assert count == 2
        assert _read_ndjson(dest) == CARDS

    def test_ignores_blank_lines_in_jsonl(self, provider, tmp_path):
        src = tmp_path / "src.jsonl"
        src.write_bytes(orjson.dumps(CARDS[0]) + b"\n\n" + orjson.dumps(CARDS[1]) + b"\n")
        dest = tmp_path / "out.ndjson"

        count = provider._convert_file_to_ndjson(src, dest)

        assert count == 2
        assert _read_ndjson(dest) == CARDS
