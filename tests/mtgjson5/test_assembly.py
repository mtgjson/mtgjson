"""Tests for v2 assembly utilities."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.build.assemble import Assembler, AtomicCardsAssembler, DeckAssembler, DeckListAssembler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_assembler():
    """Create an Assembler with a minimal mock context."""

    class FakeCtx:
        parquet_dir = None
        tokens_dir = None
        set_meta = {}
        decks_df = None
        sealed_df = None
        booster_configs = {}
        token_products = {}
        keyword_data = {}
        card_type_data = {}
        super_types = []
        planar_types = []

    return Assembler(FakeCtx())


def _make_decks_df() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "setCode": "TST",
                "code": "TST",
                "name": "Source Deck",
                "type": "Commander",
                "releaseDate": "2025-01-01",
                "source": "https://example.com/source-deck",
                "sealedProductUuids": ["sealed-001"],
                "sourceSetCodes": ["TST"],
                "mainBoard": [{"uuid": "uuid-001", "count": 1}],
                "sideBoard": [],
                "commander": [],
                "displayCommander": [],
                "tokens": [],
                "planes": [],
                "schemes": [],
            }
        ]
    )


# ---------------------------------------------------------------------------
# TestStripNoneRecursive
# ---------------------------------------------------------------------------


class TestStripNoneRecursive:
    def test_basic_dict(self):
        result = AtomicCardsAssembler._strip_none_recursive({"a": 1, "b": None})
        assert result == {"a": 1}

    def test_deeply_nested(self):
        result = AtomicCardsAssembler._strip_none_recursive({"a": {"b": {"c": None, "d": 1}, "e": None}})
        assert result == {"a": {"b": {"d": 1}}}

    def test_list_with_nones(self):
        result = AtomicCardsAssembler._strip_none_recursive([1, None, 3])
        assert result == [1, 3]

    def test_mixed_nested(self):
        result = AtomicCardsAssembler._strip_none_recursive({"a": [1, None, {"b": None, "c": 2}]})
        assert result == {"a": [1, {"c": 2}]}

    def test_primitive_passthrough(self):
        assert AtomicCardsAssembler._strip_none_recursive(42) == 42
        assert AtomicCardsAssembler._strip_none_recursive("hello") == "hello"

    def test_empty_dict(self):
        assert AtomicCardsAssembler._strip_none_recursive({}) == {}

    def test_all_none_dict(self):
        assert AtomicCardsAssembler._strip_none_recursive({"a": None, "b": None}) == {}


# ---------------------------------------------------------------------------
# TestBuildLanguages
# ---------------------------------------------------------------------------


class TestBuildLanguages:
    @pytest.mark.parametrize(
        ("cards", "expected_langs"),
        [
            # Empty cards list — only English
            ([], ["English"]),
            # Cards with no foreignData — only English
            ([{"name": "Test"}], ["English"]),
            # Cards with duplicate languages — deduped and sorted
            (
                [
                    {"name": "A", "foreignData": [{"language": "Japanese"}, {"language": "Japanese"}]},
                    {"name": "B", "foreignData": [{"language": "French"}]},
                ],
                ["English", "French", "Japanese"],
            ),
            # Cards with only English (foreignData has empty language) — only English
            ([{"name": "Test", "foreignData": [{"language": ""}]}], ["English"]),
        ],
        ids=["empty_cards", "no_foreign_data", "duplicates_deduped", "empty_language_skipped"],
    )
    def test_build_languages(self, cards, expected_langs):
        assembler = _make_assembler()
        result = assembler.build_languages(cards)
        assert result == expected_langs
        assert result == sorted(result)

    def test_unicode_language_names(self):
        """Verify sort handles accented characters correctly."""
        assembler = _make_assembler()
        cards = [
            {"name": "A", "foreignData": [{"language": "Portuguese (Brazil)"}]},
            {"name": "B", "foreignData": [{"language": "Chinese Simplified"}]},
            {"name": "C", "foreignData": [{"language": "German"}]},
        ]
        result = assembler.build_languages(cards)
        assert result == sorted(result)
        assert "English" in result
        assert len(result) == 4  # English + 3 foreign


class TestDeckAssemblers:
    def test_build_minimal_decks_preserves_source(self):
        class FakeCtx:
            parquet_dir = None
            tokens_dir = None
            set_meta = {}
            decks_df = _make_decks_df()
            sealed_df = None
            booster_configs = {}
            token_products = {}
            keyword_data = {}
            card_type_data = {}
            super_types = []
            planar_types = []

        decks = Assembler(FakeCtx()).build_minimal_decks("TST")
        assert decks is not None
        assert decks[0]["source"] == "https://example.com/source-deck"

    def test_deck_list_preserves_source(self):
        class FakeCtx:
            parquet_dir = None
            tokens_dir = None
            set_meta = {}
            decks_df = _make_decks_df()
            sealed_df = None
            booster_configs = {}
            token_products = {}
            keyword_data = {}
            card_type_data = {}
            super_types = []
            planar_types = []

        deck_list = DeckListAssembler(FakeCtx()).build()
        assert deck_list[0]["source"] == "https://example.com/source-deck"

    def test_full_deck_build_preserves_source(self, monkeypatch):
        class FakeCtx:
            parquet_dir = None
            tokens_dir = None
            set_meta = {}
            decks_df = _make_decks_df()
            sealed_df = None
            booster_configs = {}
            token_products = {}
            keyword_data = {}
            card_type_data = {}
            super_types = []
            planar_types = []

        assembler = DeckAssembler(FakeCtx())
        monkeypatch.setattr(assembler, "expand_card_list", lambda refs: refs)
        built = assembler.build(_make_decks_df().to_dicts()[0])
        assert built["source"] == "https://example.com/source-deck"
