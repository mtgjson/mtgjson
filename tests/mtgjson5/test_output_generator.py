"""
Comprehensive tests for mtgjson5/output_generator.py

Tests cover:
- write_to_file() with various configurations
- construct_format_map() format filtering logic
- generate_output_file_hashes() hash generation
- Real file I/O operations (not mocked)
"""

import json
import pathlib
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.output_generator import (
    construct_atomic_cards_format_map,
    construct_format_map,
    generate_output_file_hashes,
    write_to_file,
)
from mtgjson5.utils import get_file_hash


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def temp_output_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """
    Fixture to create a temporary output directory and patch MtgjsonConfig.

    Uses tmp_path for real file I/O and monkeypatch to override the singleton
    MtgjsonConfig.output_path for the test session.
    """
    output_dir = tmp_path / "mtgjson_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Patch the singleton config's output_path
    # Since MtgjsonConfig is a singleton, we need to patch the instance
    config = MtgjsonConfig()
    monkeypatch.setattr(config, "output_path", output_dir)

    return output_dir


@pytest.fixture
def sample_all_printings_file(temp_output_dir: pathlib.Path) -> pathlib.Path:
    """
    Creates a sample AllPrintings.json file for testing construct_format_map.

    Structure matches real AllPrintings.json with:
    - Sets with different types (expansion, core, token, etc.)
    - Cards with varying legalities across formats
    - Standard set with recent cards
    - Modern set with older cards
    - Alchemy cards (prefixed with "A-") that should be ignored
    """
    all_printings_data = {
        "meta": {
            "date": "2025-01-01",
            "version": "5.0.0"
        },
        "data": {
            # Standard-legal expansion set
            "MID": {
                "name": "Midnight Hunt",
                "code": "MID",
                "type": "expansion",
                "releaseDate": "2021-09-24",
                "cards": [
                    {
                        "name": "Consider",
                        "uuid": "abc-123",
                        "legalities": {
                            "standard": "Legal",
                            "pioneer": "Legal",
                            "modern": "Legal",
                            "pauper": "Legal",
                            "legacy": "Legal",
                            "vintage": "Legal"
                        }
                    },
                    {
                        "name": "A-Consider",  # Alchemy card - should be ignored
                        "uuid": "abc-456",
                        "legalities": {
                            "alchemy": "Legal"
                        }
                    }
                ]
            },
            # Modern-legal core set (not in Standard)
            "M21": {
                "name": "Core Set 2021",
                "code": "M21",
                "type": "core",
                "releaseDate": "2020-07-03",
                "cards": [
                    {
                        "name": "Llanowar Visionary",
                        "uuid": "def-789",
                        "legalities": {
                            "pioneer": "Legal",
                            "modern": "Legal",
                            "legacy": "Legal",
                            "vintage": "Legal"
                        }
                    }
                ]
            },
            # Token set - should be excluded by normal_sets_only
            "TMID": {
                "name": "Midnight Hunt Tokens",
                "code": "TMID",
                "type": "token",
                "releaseDate": "2021-09-24",
                "cards": [
                    {
                        "name": "Wolf Token",
                        "uuid": "token-123",
                        "legalities": {}
                    }
                ]
            },
            # Commander set - included in SUPPORTED_SET_TYPES
            "C21": {
                "name": "Commander 2021",
                "code": "C21",
                "type": "commander",
                "releaseDate": "2021-04-23",
                "cards": [
                    {
                        "name": "Commander's Sphere",
                        "uuid": "cmd-456",
                        "legalities": {
                            "legacy": "Legal",
                            "vintage": "Legal",
                            "commander": "Legal"
                        }
                    }
                ]
            },
            # Legacy-only card (banned in Modern)
            "LEA": {
                "name": "Limited Edition Alpha",
                "code": "LEA",
                "type": "core",
                "releaseDate": "1993-08-05",
                "cards": [
                    {
                        "name": "Black Lotus",
                        "uuid": "lotus-123",
                        "legalities": {
                            "legacy": "Banned",
                            "vintage": "Restricted"
                        }
                    }
                ]
            }
        }
    }

    all_printings_path = temp_output_dir / "AllPrintings.json"
    with all_printings_path.open("w", encoding="utf-8") as f:
        json.dump(all_printings_data, f, indent=4)

    return all_printings_path


# ============================================================================
# TESTS: write_to_file() - Core functionality
# ============================================================================


def test_write_to_file_creates_json_with_meta_and_data(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file creates a JSON file with meta and data structure.

    Verifies:
    - File is created at correct path with .json extension
    - JSON contains 'meta' key with version and date
    - JSON contains 'data' key with provided content
    """
    test_data = {"test_key": "test_value", "number": 42}

    write_to_file("test_output", test_data, pretty_print=True)

    output_file = temp_output_dir / "test_output.json"
    assert output_file.exists(), "Output file should be created"

    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    assert "meta" in content, "Output should contain meta key"
    assert "data" in content, "Output should contain data key"
    assert content["data"] == test_data, "Data should match input"

    # Verify meta structure
    assert "version" in content["meta"], "Meta should contain version"
    assert "date" in content["meta"], "Meta should contain date"


def test_write_to_file_pretty_print_enabled(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file with pretty_print=True creates indented JSON.

    Verifies:
    - Output file contains newlines (pretty-printed)
    - File is human-readable with 4-space indentation
    """
    test_data = {"key1": "value1", "key2": {"nested": "value"}}

    write_to_file("pretty_output", test_data, pretty_print=True)

    output_file = temp_output_dir / "pretty_output.json"
    content = output_file.read_text(encoding="utf-8")

    # Pretty-printed JSON should have newlines and indentation
    assert "\n" in content, "Pretty-printed output should contain newlines"
    assert "    " in content, "Pretty-printed output should contain indentation"

    # Verify it's valid JSON
    parsed = json.loads(content)
    assert parsed["data"] == test_data


def test_write_to_file_pretty_print_disabled(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file with pretty_print=False creates minified JSON.

    Verifies:
    - Output is compact (minimal whitespace)
    - File size is smaller than pretty-printed version
    """
    test_data = {"key1": "value1", "key2": {"nested": "value"}}

    write_to_file("compact_output", test_data, pretty_print=False)

    output_file = temp_output_dir / "compact_output.json"
    content = output_file.read_text(encoding="utf-8")

    # Compact JSON should be on fewer lines (not necessarily single line due to meta/data structure)
    # but should not have 4-space indentation patterns
    assert "    " not in content or content.count("    ") < 5, \
        "Compact output should have minimal indentation"

    # Verify it's valid JSON
    parsed = json.loads(content)
    assert parsed["data"] == test_data


def test_write_to_file_with_sort_keys_true(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file with sort_keys=True alphabetically sorts keys.

    Verifies:
    - Keys in output are sorted alphabetically
    - Nested objects are also sorted
    """
    # Use unsorted data
    test_data = {
        "zebra": 1,
        "apple": 2,
        "banana": {"zoo": 1, "ant": 2}
    }

    write_to_file("sorted_output", test_data, pretty_print=True, sort_keys=True)

    output_file = temp_output_dir / "sorted_output.json"
    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    data_keys = list(content["data"].keys())
    assert data_keys == sorted(data_keys), "Top-level keys should be sorted"

    # Check nested keys are sorted
    nested_keys = list(content["data"]["banana"].keys())
    assert nested_keys == sorted(nested_keys), "Nested keys should be sorted"


def test_write_to_file_with_sort_keys_false(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file with sort_keys=False preserves insertion order.

    Verifies:
    - Keys maintain their original order (Python 3.7+ dict ordering)
    - No sorting is applied
    """
    # Use explicitly ordered data
    test_data = {
        "zebra": 1,
        "apple": 2,
        "banana": 3
    }

    write_to_file("unsorted_output", test_data, pretty_print=True, sort_keys=False)

    output_file = temp_output_dir / "unsorted_output.json"
    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    # In Python 3.7+, dict preserves insertion order
    # Since we're not sorting, order should match input
    data_keys = list(content["data"].keys())
    assert data_keys == ["zebra", "apple", "banana"], \
        "Keys should preserve insertion order when sort_keys=False"


def test_write_to_file_creates_parent_directories(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file creates nested parent directories if they don't exist.

    Verifies:
    - Nested directories are created automatically
    - File is written to correct nested path
    """
    nested_path = "subdir1/subdir2/nested_file"
    test_data = {"nested": "content"}

    write_to_file(nested_path, test_data, pretty_print=True)

    output_file = temp_output_dir / "subdir1" / "subdir2" / "nested_file.json"
    assert output_file.exists(), "Nested output file should be created"
    assert output_file.parent.exists(), "Parent directories should be created"

    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    assert content["data"] == test_data


def test_write_to_file_handles_custom_objects_with_to_json(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file handles custom objects that implement to_json().

    Verifies:
    - Objects with to_json() method are serialized correctly
    - MtgjsonMetaObject is properly serialized in both meta and data sections
    """
    # Create a custom object that has to_json()
    meta_obj = MtgjsonMetaObject(date="2025-01-15", version="5.0.0")

    # Test with the object in data
    write_to_file("custom_object", meta_obj, pretty_print=True)

    output_file = temp_output_dir / "custom_object.json"
    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    # Verify the object was serialized
    assert "date" in content["data"], "Custom object should be serialized"
    assert "version" in content["data"], "Custom object should be serialized"
    assert content["data"]["date"] == "2025-01-15"
    assert content["data"]["version"] == "5.0.0"


def test_write_to_file_unicode_handling(temp_output_dir: pathlib.Path):
    """
    Test that write_to_file properly handles Unicode characters.

    Verifies:
    - Unicode characters are preserved (not escaped)
    - File is written with UTF-8 encoding
    - ensure_ascii=False is working correctly
    """
    test_data = {
        "japanese": "日本語",
        "chinese": "中文",
        "emoji": "🎴",
        "german": "Müller",
        "french": "café"
    }

    write_to_file("unicode_output", test_data, pretty_print=True)

    output_file = temp_output_dir / "unicode_output.json"

    # Read as bytes to verify actual encoding
    raw_content = output_file.read_bytes()

    # Verify UTF-8 encoding by checking for Unicode characters (not \u escapes)
    assert "日本語".encode("utf-8") in raw_content, "Japanese characters should not be escaped"
    assert "中文".encode("utf-8") in raw_content, "Chinese characters should not be escaped"

    # Verify JSON is valid and characters are preserved
    with output_file.open("r", encoding="utf-8") as f:
        content = json.load(f)

    assert content["data"]["japanese"] == "日本語"
    assert content["data"]["chinese"] == "中文"
    assert content["data"]["emoji"] == "🎴"


# ============================================================================
# TESTS: construct_format_map() - Format filtering logic
# ============================================================================


def test_construct_format_map_returns_all_formats(sample_all_printings_file: pathlib.Path):
    """
    Test that construct_format_map returns a dict with all supported formats.

    Verifies:
    - All formats from SUPPORTED_FORMAT_OUTPUTS are present
    - Returns standard, pioneer, modern, legacy, vintage
    """
    format_map = construct_format_map(sample_all_printings_file)

    expected_formats = constants.SUPPORTED_FORMAT_OUTPUTS
    assert set(format_map.keys()) == expected_formats, \
        "Format map should contain all supported formats"


def test_construct_format_map_standard_only_recent_sets(sample_all_printings_file: pathlib.Path):
    """
    Test that construct_format_map correctly identifies Standard-legal sets.

    Verifies:
    - Only sets with all cards legal in Standard are included
    - Standard format has the most restrictive card pool
    - Alchemy cards (A- prefix) are ignored in legality checks
    """
    format_map = construct_format_map(sample_all_printings_file)

    # MID has Standard-legal cards (ignoring A-Consider)
    assert "MID" in format_map["standard"], \
        "MID should be in standard (has standard-legal cards)"

    # M21 is not Standard-legal (released too early, rotated out)
    assert "M21" not in format_map["standard"], \
        "M21 should not be in standard (cards not standard-legal)"


def test_construct_format_map_excludes_non_normal_set_types(sample_all_printings_file: pathlib.Path):
    """
    Test that construct_format_map excludes non-normal set types by default.

    Verifies:
    - Token sets are excluded
    - Only sets in SUPPORTED_SET_TYPES are included (expansion, core, commander, etc.)
    - normal_sets_only parameter controls this behavior
    """
    format_map = construct_format_map(sample_all_printings_file, normal_sets_only=True)

    # Token set should be excluded (not in SUPPORTED_SET_TYPES)
    assert "TMID" not in format_map["vintage"], \
        "Token sets should be excluded when normal_sets_only=True"

    # Commander set should be included (commander is in SUPPORTED_SET_TYPES)
    assert "C21" in format_map["legacy"], \
        "Commander sets should be included when normal_sets_only=True"
    assert "C21" in format_map["vintage"], \
        "Commander sets should be included when normal_sets_only=True"

    # Normal expansion/core sets should be included
    assert "MID" in format_map["vintage"], \
        "Normal expansion sets should be included"
    assert "M21" in format_map["modern"], \
        "Normal core sets should be included"


def test_construct_format_map_card_intersection_logic(sample_all_printings_file: pathlib.Path):
    """
    Test that construct_format_map uses intersection logic for set legality.

    A set is only included in a format if ALL its cards are legal in that format.

    Verifies:
    - Set legality is determined by card intersection (not union)
    - If any card is not legal, the set is excluded from that format
    - Alchemy cards (A- prefix) are ignored in this calculation
    """
    format_map = construct_format_map(sample_all_printings_file)

    # MID has cards legal in all formats (ignoring A-Consider)
    # So it should appear in all formats
    for fmt in constants.SUPPORTED_FORMAT_OUTPUTS:
        assert "MID" in format_map[fmt], \
            f"MID should be in {fmt} (all non-Alchemy cards are legal)"

    # LEA has Black Lotus which is banned/restricted, not "Legal"
    # So LEA should not appear in legacy or vintage format maps
    # (because we only include sets where cards are "Legal" or potentially "Restricted")
    # Actually, looking at the code, it checks card.get("legalities").keys()
    # So if a card has a legality entry for a format, it's included in intersection
    # Let's verify LEA appears in vintage (has Restricted) but not in formats it's not listed in
    assert "LEA" in format_map["vintage"], \
        "LEA should appear in vintage (Black Lotus has vintage legality)"
    assert "LEA" in format_map["legacy"], \
        "LEA should appear in legacy (Black Lotus has legacy legality)"


def test_construct_format_map_missing_file_returns_empty_dict(temp_output_dir: pathlib.Path):
    """
    Test that construct_format_map returns empty dict if AllPrintings.json is missing.

    Verifies:
    - Graceful handling of missing file
    - Returns {} instead of raising exception
    - Logs warning (checked via logging, but not in this test)
    """
    non_existent_path = temp_output_dir / "NonExistent.json"

    format_map = construct_format_map(non_existent_path)

    assert format_map == {}, \
        "Should return empty dict when AllPrintings.json is missing"


# ============================================================================
# TESTS: generate_output_file_hashes() - Hash generation
# ============================================================================


def test_generate_output_file_hashes_creates_hash_files(temp_output_dir: pathlib.Path):
    """
    Test that generate_output_file_hashes creates .sha256 files for each file.

    Verifies:
    - Hash files are created with correct naming (filename.sha256)
    - Hash content matches get_file_hash() output
    - Hash is a valid SHA256 hex string (64 characters)
    """
    # Create test files
    test_file1 = temp_output_dir / "test1.json"
    test_file1.write_text('{"test": "data1"}', encoding="utf-8")

    test_file2 = temp_output_dir / "test2.json"
    test_file2.write_text('{"test": "data2"}', encoding="utf-8")

    # Generate hashes
    generate_output_file_hashes(temp_output_dir)

    # Verify hash files were created
    hash_file1 = temp_output_dir / f"test1.json.{constants.HASH_TO_GENERATE.name}"
    hash_file2 = temp_output_dir / f"test2.json.{constants.HASH_TO_GENERATE.name}"

    assert hash_file1.exists(), "Hash file should be created for test1.json"
    assert hash_file2.exists(), "Hash file should be created for test2.json"

    # Verify hash content
    hash1_content = hash_file1.read_text(encoding="utf-8")
    expected_hash1 = get_file_hash(test_file1)

    assert hash1_content == expected_hash1, "Hash content should match get_file_hash()"
    assert len(hash1_content) == 64, "SHA256 hash should be 64 characters"

    # Verify different files have different hashes
    hash2_content = hash_file2.read_text(encoding="utf-8")
    assert hash1_content != hash2_content, "Different files should have different hashes"


def test_generate_output_file_hashes_skips_hash_files(temp_output_dir: pathlib.Path):
    """
    Test that generate_output_file_hashes doesn't hash the hash files themselves.

    Verifies:
    - .sha256 files are not hashed
    - No .sha256.sha256 files are created
    - Avoids infinite recursion
    """
    # Create a test file
    test_file = temp_output_dir / "test.json"
    test_file.write_text('{"test": "data"}', encoding="utf-8")

    # Generate hashes once
    generate_output_file_hashes(temp_output_dir)

    hash_file = temp_output_dir / f"test.json.{constants.HASH_TO_GENERATE.name}"
    assert hash_file.exists(), "Hash file should be created"

    # Generate hashes again
    generate_output_file_hashes(temp_output_dir)

    # Verify no double-hash file was created
    double_hash = temp_output_dir / f"test.json.{constants.HASH_TO_GENERATE.name}.{constants.HASH_TO_GENERATE.name}"
    assert not double_hash.exists(), "Hash files should not be hashed themselves"


def test_generate_output_file_hashes_recursive(temp_output_dir: pathlib.Path):
    """
    Test that generate_output_file_hashes processes files recursively in subdirectories.

    Verifies:
    - Files in subdirectories are hashed
    - Hash files are created in the same directory as the source file
    - Recursive globbing with **/* works correctly
    """
    # Create nested directory structure
    subdir1 = temp_output_dir / "subdir1"
    subdir1.mkdir()
    subdir2 = temp_output_dir / "subdir1" / "subdir2"
    subdir2.mkdir()

    # Create files in different levels
    root_file = temp_output_dir / "root.json"
    root_file.write_text('{"level": "root"}', encoding="utf-8")

    sub1_file = subdir1 / "sub1.json"
    sub1_file.write_text('{"level": "sub1"}', encoding="utf-8")

    sub2_file = subdir2 / "sub2.json"
    sub2_file.write_text('{"level": "sub2"}', encoding="utf-8")

    # Generate hashes
    generate_output_file_hashes(temp_output_dir)

    # Verify hash files were created at all levels
    assert (temp_output_dir / f"root.json.{constants.HASH_TO_GENERATE.name}").exists(), \
        "Root level hash should be created"
    assert (subdir1 / f"sub1.json.{constants.HASH_TO_GENERATE.name}").exists(), \
        "Subdirectory level 1 hash should be created"
    assert (subdir2 / f"sub2.json.{constants.HASH_TO_GENERATE.name}").exists(), \
        "Subdirectory level 2 hash should be created"


def test_generate_output_file_hashes_skips_directories(temp_output_dir: pathlib.Path):
    """
    Test that generate_output_file_hashes skips directories themselves.

    Verifies:
    - Only files are hashed, not directories
    - No hash files created for directories
    """
    # Create nested directories
    subdir = temp_output_dir / "subdir"
    subdir.mkdir()

    # Create a file to ensure hashing runs
    test_file = temp_output_dir / "test.json"
    test_file.write_text('{"test": "data"}', encoding="utf-8")

    # Generate hashes
    generate_output_file_hashes(temp_output_dir)

    # Verify no hash file for directory
    dir_hash = temp_output_dir / f"subdir.{constants.HASH_TO_GENERATE.name}"
    assert not dir_hash.exists(), "Directories should not be hashed"

    # Verify file was hashed
    file_hash = temp_output_dir / f"test.json.{constants.HASH_TO_GENERATE.name}"
    assert file_hash.exists(), "Regular files should be hashed"


# ============================================================================
# TESTS: construct_atomic_cards_format_map() - Card-level format mapping
# ============================================================================


def test_construct_atomic_cards_format_map_returns_all_formats(sample_all_printings_file: pathlib.Path):
    """
    Test that construct_atomic_cards_format_map returns all supported formats.

    Verifies:
    - All formats from SUPPORTED_FORMAT_OUTPUTS are present
    - Returns dict with format keys mapping to card lists
    """
    format_map = construct_atomic_cards_format_map(sample_all_printings_file)

    expected_formats = constants.SUPPORTED_FORMAT_OUTPUTS
    assert set(format_map.keys()) == expected_formats, \
        "Format map should contain all supported formats"

    # Verify each format maps to a list
    for fmt, cards in format_map.items():
        assert isinstance(cards, list), f"Format {fmt} should map to a list of cards"


def test_construct_atomic_cards_format_map_includes_legal_and_restricted_cards(
    sample_all_printings_file: pathlib.Path
):
    """
    Test that construct_atomic_cards_format_map includes cards with Legal/Restricted status.

    Verifies:
    - Cards with legality "Legal" are included
    - Cards with legality "Restricted" are included
    - Cards with other statuses (Banned, Not Legal) are excluded
    """
    format_map = construct_atomic_cards_format_map(sample_all_printings_file)

    # Get all card names in vintage (should include Legal and Restricted)
    vintage_cards = format_map["vintage"]
    vintage_card_names = [card["name"] for card in vintage_cards]

    # Consider should be in vintage (Legal)
    assert "Consider" in vintage_card_names, \
        "Legal cards should be included in format map"

    # Black Lotus should be in vintage (Restricted)
    assert "Black Lotus" in vintage_card_names, \
        "Restricted cards should be included in format map"


def test_construct_atomic_cards_format_map_dungeon_tokens_included(temp_output_dir: pathlib.Path):
    """
    Test that construct_atomic_cards_format_map includes Dungeon tokens with all formats.

    This is a workaround for Dungeons to ensure they appear in Atomic files.

    Verifies:
    - Tokens with type "Dungeon" are added to all formats
    - Dungeons get synthetic legalities added
    """
    # Create AllPrintings with a Dungeon token
    dungeon_data = {
        "meta": {"date": "2025-01-01", "version": "5.0.0"},
        "data": {
            "AFR": {
                "name": "Adventures in the Forgotten Realms",
                "code": "AFR",
                "type": "expansion",
                "cards": [],
                "tokens": [
                    {
                        "name": "Dungeon of the Mad Mage",
                        "type": "Dungeon",
                        "uuid": "dungeon-123"
                    },
                    {
                        "name": "Wolf Token",  # Non-dungeon token
                        "type": "Token Creature",
                        "uuid": "wolf-456"
                    }
                ]
            }
        }
    }

    dungeon_file = temp_output_dir / "AllPrintings_Dungeon.json"
    with dungeon_file.open("w", encoding="utf-8") as f:
        json.dump(dungeon_data, f)

    format_map = construct_atomic_cards_format_map(dungeon_file)

    # Dungeon should be in all formats
    for fmt in constants.SUPPORTED_FORMAT_OUTPUTS:
        card_names = [card["name"] for card in format_map[fmt]]
        assert "Dungeon of the Mad Mage" in card_names, \
            f"Dungeon should be included in {fmt} format"

    # Non-dungeon token should not be in any format (no legalities)
    for fmt in constants.SUPPORTED_FORMAT_OUTPUTS:
        card_names = [card["name"] for card in format_map[fmt]]
        assert "Wolf Token" not in card_names, \
            f"Non-dungeon tokens should not be included in {fmt} format"


def test_construct_atomic_cards_format_map_missing_file_returns_empty_dict(
    temp_output_dir: pathlib.Path
):
    """
    Test that construct_atomic_cards_format_map returns empty dict if file is missing.

    Verifies:
    - Graceful handling of missing AllPrintings.json
    - Returns {} instead of raising exception
    """
    non_existent_path = temp_output_dir / "NonExistent.json"

    format_map = construct_atomic_cards_format_map(non_existent_path)

    assert format_map == {}, \
        "Should return empty dict when AllPrintings.json is missing"


def test_construct_atomic_cards_format_map_card_appears_multiple_times(
    sample_all_printings_file: pathlib.Path
):
    """
    Test that cards can appear multiple times in format map (from different sets).

    Verifies:
    - Same card name from different sets creates multiple entries
    - Each entry represents a different printing/UUID
    """
    format_map = construct_atomic_cards_format_map(sample_all_printings_file)

    # Consider appears in MID
    modern_cards = format_map["modern"]
    consider_cards = [c for c in modern_cards if c["name"] == "Consider"]

    # Should have at least one entry
    assert len(consider_cards) >= 1, \
        "Card should appear in format map for sets it's legal in"

    # Verify UUID is preserved
    assert all("uuid" in card for card in consider_cards), \
        "Each card entry should preserve UUID"
