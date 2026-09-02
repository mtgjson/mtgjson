"""Tests for Scryfall bulk data download/conversion (JSONL + legacy JSON array)."""

from __future__ import annotations

import asyncio
import datetime
import gzip
import json
import os
import time

import aiohttp
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


# =============================================================================
# TestParseUpdatedAt
# =============================================================================


class TestParseUpdatedAt:
    def test_parses_offset_timestamp(self):
        parsed = ScryfallProvider._parse_updated_at({"updated_at": "2026-08-20T21:18:12.301+00:00"})
        assert parsed == datetime.datetime(2026, 8, 20, 21, 18, 12, 301000, tzinfo=datetime.UTC)

    def test_parses_zulu_timestamp(self):
        parsed = ScryfallProvider._parse_updated_at({"updated_at": "2026-08-20T21:18:12Z"})
        assert parsed == datetime.datetime(2026, 8, 20, 21, 18, 12, tzinfo=datetime.UTC)

    def test_assumes_utc_when_offset_missing(self):
        parsed = ScryfallProvider._parse_updated_at({"updated_at": "2026-08-20T21:18:12"})
        assert parsed == datetime.datetime(2026, 8, 20, 21, 18, 12, tzinfo=datetime.UTC)

    def test_returns_none_when_absent(self):
        assert ScryfallProvider._parse_updated_at({"type": "all_cards"}) is None

    def test_returns_none_when_unparseable(self):
        assert ScryfallProvider._parse_updated_at({"updated_at": "yesterday"}) is None


# =============================================================================
# TestCachedDumpIsCurrent
# =============================================================================


DUMP_TIME = datetime.datetime(2026, 8, 20, 21, 18, 12, tzinfo=datetime.UTC)


def _write_dump(path, mtime):
    path.write_text("{}\n", encoding="utf-8")
    os.utime(path, (mtime, mtime))
    return path


class TestCachedDumpIsCurrent:
    def test_missing_file_is_not_current(self, provider, tmp_path):
        assert provider._cached_dump_is_current(tmp_path / "all_cards.ndjson", DUMP_TIME) is False

    def test_empty_file_is_not_current(self, provider, tmp_path):
        dest = tmp_path / "all_cards.ndjson"
        dest.write_bytes(b"")
        os.utime(dest, (DUMP_TIME.timestamp(), DUMP_TIME.timestamp()))
        assert provider._cached_dump_is_current(dest, DUMP_TIME) is False

    def test_file_older_than_dump_is_not_current(self, provider, tmp_path):
        # This is the churn case: yesterday's snapshot sitting in the cache
        dest = _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp() - 86400)
        assert provider._cached_dump_is_current(dest, DUMP_TIME) is False

    def test_file_stamped_with_dump_is_current(self, provider, tmp_path):
        dest = _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp())
        assert provider._cached_dump_is_current(dest, DUMP_TIME) is True

    def test_file_downloaded_after_dump_is_current(self, provider, tmp_path):
        dest = _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp() + 3600)
        assert provider._cached_dump_is_current(dest, DUMP_TIME) is True

    def test_falls_back_to_age_when_timestamp_unknown(self, provider, tmp_path):
        fresh = _write_dump(tmp_path / "fresh.ndjson", time.time() - 3600)
        stale = _write_dump(tmp_path / "stale.ndjson", time.time() - 48 * 3600)
        assert provider._cached_dump_is_current(fresh, None) is True
        assert provider._cached_dump_is_current(stale, None) is False


# =============================================================================
# TestDownloadBulkFiles
# =============================================================================


CATALOG = {
    "all_cards": {
        "type": "all_cards",
        "jsonl_download_uri": "https://data.scryfall.io/all-cards.jsonl.gz",
        "updated_at": DUMP_TIME.isoformat(),
    },
    "rulings": {
        "type": "rulings",
        "jsonl_download_uri": "https://data.scryfall.io/rulings.jsonl.gz",
        "updated_at": DUMP_TIME.isoformat(),
    },
}


def _stub_downloads(provider, metadata_error=None):
    """Record which bulk types would be downloaded, without touching the network."""
    downloaded = []

    async def fake_metadata(_session):
        if metadata_error is not None:
            raise metadata_error
        return CATALOG

    async def fake_download(_session, url, destination, updated_at=None):
        downloaded.append(destination.name)
        return destination

    provider.get_bulk_metadata = fake_metadata
    provider.download_to_ndjson = fake_download
    return downloaded


class TestDownloadBulkFiles:
    def test_refreshes_cache_older_than_published_dump(self, provider, tmp_path):
        _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp() - 86400)
        _write_dump(tmp_path / "rulings.ndjson", DUMP_TIME.timestamp() - 86400)
        downloaded = _stub_downloads(provider)

        asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards", "rulings"]))

        assert sorted(downloaded) == ["all_cards.ndjson", "rulings.ndjson"]

    def test_keeps_cache_matching_published_dump(self, provider, tmp_path):
        _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp())
        _write_dump(tmp_path / "rulings.ndjson", DUMP_TIME.timestamp())
        downloaded = _stub_downloads(provider)

        asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards", "rulings"]))

        assert not downloaded

    def test_refreshes_only_the_stale_file(self, provider, tmp_path):
        _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp() - 86400)
        _write_dump(tmp_path / "rulings.ndjson", DUMP_TIME.timestamp())
        downloaded = _stub_downloads(provider)

        asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards", "rulings"]))

        assert downloaded == ["all_cards.ndjson"]

    def test_force_refresh_redownloads_current_cache(self, provider, tmp_path):
        _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp())
        downloaded = _stub_downloads(provider)

        asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards"], force_refresh=True))

        assert downloaded == ["all_cards.ndjson"]

    def test_missing_file_is_downloaded(self, provider, tmp_path):
        downloaded = _stub_downloads(provider)

        asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards"]))

        assert downloaded == ["all_cards.ndjson"]

    def test_unreachable_catalog_falls_back_to_cache(self, provider, tmp_path):
        _write_dump(tmp_path / "all_cards.ndjson", DUMP_TIME.timestamp() - 86400)
        downloaded = _stub_downloads(provider, metadata_error=aiohttp.ClientError("boom"))

        result = asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards"]))

        assert not downloaded
        assert result["all_cards"] == tmp_path / "all_cards.ndjson"

    def test_unreachable_catalog_raises_without_usable_cache(self, provider, tmp_path):
        _stub_downloads(provider, metadata_error=aiohttp.ClientError("boom"))

        with pytest.raises(aiohttp.ClientError):
            asyncio.run(provider.download_bulk_files(tmp_path, ["all_cards"]))

    def test_unknown_bulk_type_raises(self, provider, tmp_path):
        _stub_downloads(provider)

        with pytest.raises(ValueError, match="Unknown bulk type"):
            asyncio.run(provider.download_bulk_files(tmp_path, ["nonexistent"]))


# =============================================================================
# TestDownloadStamping
# =============================================================================


class _FakeContent:
    def __init__(self, body):
        self._body = body

    async def iter_chunked(self, _size):
        yield self._body


class _FakeResponse:
    def __init__(self, body):
        self.content = _FakeContent(body)
        self.headers = {"Content-Length": str(len(body))}

    def raise_for_status(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _FakeSession:
    """Minimal stand-in for aiohttp.ClientSession serving one fixed body."""

    def __init__(self, body):
        self._body = body

    def get(self, _url):
        return _FakeResponse(self._body)


class TestDownloadStamping:
    def test_download_stamps_file_with_dump_time(self, provider, tmp_path):
        body = gzip.compress(b"\n".join(orjson.dumps(c) for c in CARDS) + b"\n")
        dest = tmp_path / "all_cards.ndjson"

        asyncio.run(
            provider.download_to_ndjson(
                _FakeSession(body),
                "https://data.scryfall.io/all-cards.jsonl.gz",
                dest,
                DUMP_TIME,
            )
        )

        assert _read_ndjson(dest) == CARDS
        assert dest.stat().st_mtime == pytest.approx(DUMP_TIME.timestamp())
        # A build that finds this file must recognise it as the current dump
        assert provider._cached_dump_is_current(dest, DUMP_TIME) is True

    def test_download_leaves_mtime_alone_without_dump_time(self, provider, tmp_path):
        body = gzip.compress(b"\n".join(orjson.dumps(c) for c in CARDS) + b"\n")
        dest = tmp_path / "all_cards.ndjson"

        asyncio.run(
            provider.download_to_ndjson(
                _FakeSession(body),
                "https://data.scryfall.io/all-cards.jsonl.gz",
                dest,
            )
        )

        assert _read_ndjson(dest) == CARDS
        assert dest.stat().st_mtime == pytest.approx(time.time(), abs=60)
