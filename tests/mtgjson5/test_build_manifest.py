"""Tests for build manifest generation."""

from __future__ import annotations

import json
import pathlib

from mtgjson5.utils import generate_build_manifest


class TestGenerateBuildManifest:
    def test_creates_manifest_file(self, tmp_path: pathlib.Path):
        """Manifest file is created in the output directory."""
        (tmp_path / "AllPrintings.json").write_text('{"data": {}}')
        (tmp_path / "10E.json").write_text('{"data": {}}')

        generate_build_manifest(tmp_path, assembly_results={})

        manifest_path = tmp_path / "BuildManifest.json"
        assert manifest_path.exists()

    def test_manifest_contains_meta(self, tmp_path: pathlib.Path):
        """Manifest meta section has required fields."""
        (tmp_path / "AllPrintings.json").write_text('{"data": {}}')

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        meta = manifest["meta"]
        assert "date" in meta
        assert "generated_at" in meta
        assert "total_files" in meta
        assert "total_size_bytes" in meta
        assert meta["total_files"] == 1

    def test_manifest_lists_all_files(self, tmp_path: pathlib.Path):
        """Every non-excluded file appears in the manifest."""
        (tmp_path / "AllPrintings.json").write_bytes(b"x" * 100)
        (tmp_path / "10E.json").write_bytes(b"y" * 50)
        (tmp_path / "AllPrintings.json.sha256").write_text("abc123")

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        files = manifest["files"]
        assert "AllPrintings.json" in files
        assert "10E.json" in files
        assert "AllPrintings.json.sha256" not in files
        assert files["AllPrintings.json"]["size_bytes"] == 100
        assert files["10E.json"]["size_bytes"] == 50

    def test_manifest_includes_subdirectory_files(self, tmp_path: pathlib.Path):
        """Files in subdirectories use forward-slash relative paths."""
        decks = tmp_path / "decks"
        decks.mkdir()
        (decks / "SomeDeck.json").write_bytes(b"d" * 30)

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        assert "decks/SomeDeck.json" in manifest["files"]

    def test_manifest_excludes_special_files(self, tmp_path: pathlib.Path):
        """Log files, profile files, sha256 files, and the manifest itself are excluded."""
        (tmp_path / "AllPrintings.json").write_bytes(b"x" * 10)
        (tmp_path / "mtgjson_2026.log").write_text("log")
        (tmp_path / "profile_report.json").write_text("{}")
        (tmp_path / "profile_summary.log").write_text("summary")
        (tmp_path / "AllMTGJSONTypes.ts").write_text("types")
        (tmp_path / "BuildManifest.json").write_text("old manifest")
        dm = tmp_path / "data-models"
        dm.mkdir()
        (dm / "page.md").write_text("docs")
        types_dir = tmp_path / "types"
        types_dir.mkdir()
        (types_dir / "Card.ts").write_text("type")

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        files = manifest["files"]
        assert len(files) == 1
        assert "AllPrintings.json" in files

    def test_manifest_includes_record_counts(self, tmp_path: pathlib.Path):
        """Assembly results are stored in record_counts."""
        (tmp_path / "AllPrintings.json").write_bytes(b"x")

        results = {"AllPrintings": 882, "AllIdentifiers": 117449, "sets": 882}
        generate_build_manifest(tmp_path, assembly_results=results)

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        assert manifest["record_counts"] == results

    def test_manifest_total_size(self, tmp_path: pathlib.Path):
        """total_size_bytes is the sum of all included file sizes."""
        (tmp_path / "a.json").write_bytes(b"x" * 100)
        (tmp_path / "b.json").write_bytes(b"y" * 200)

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        assert manifest["meta"]["total_size_bytes"] == 300

    def test_manifest_files_sorted(self, tmp_path: pathlib.Path):
        """File entries are sorted by path."""
        (tmp_path / "ZNR.json").write_bytes(b"z")
        (tmp_path / "10E.json").write_bytes(b"a")
        (tmp_path / "AllPrintings.json").write_bytes(b"b")

        generate_build_manifest(tmp_path, assembly_results={})

        manifest = json.loads((tmp_path / "BuildManifest.json").read_text())
        keys = list(manifest["files"].keys())
        assert keys == sorted(keys)
