"""Tests for the set-level isUniversesBeyond flag."""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Any

import polars as pl
import pytest

from mtgjson5.build.context import AssemblyContext, _enrich_set_metadata, _universes_beyond_set_codes
from mtgjson5.data.context import PipelineContext

UB = "universesbeyond"
SCHEMA = {
    "setCode": pl.String,
    "number": pl.String,
    "isRebalanced": pl.Boolean,
    "promoTypes": pl.List(pl.String),
}


def _write_partition(root: pathlib.Path, set_code: str, promo_types: list[list[str] | None]) -> None:
    """Write one set partition the way sink_cards lays them out."""
    partition = root / f"setCode={set_code}"
    partition.mkdir(parents=True)
    rows = [
        {"setCode": set_code, "number": str(i), "isRebalanced": False, "promoTypes": promos}
        for i, promos in enumerate(promo_types, start=1)
    ]
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(partition / "0.parquet")


@pytest.fixture
def dirs(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    cards, tokens = tmp_path / "_parquet", tmp_path / "_parquet_tokens"
    cards.mkdir()
    tokens.mkdir()
    return cards, tokens


class TestUniversesBeyondSetCodes:
    def test_set_where_every_card_is_tagged_is_flagged(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "WHOLE", [[UB], [UB, "boosterfun"], [UB]])
        assert _universes_beyond_set_codes(cards, tokens) == {"WHOLE"}

    def test_mixed_set_is_not_flagged(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "MIXED", [[UB], ["boosterfun"], [UB]])
        assert _universes_beyond_set_codes(cards, tokens) == set()

    def test_card_with_null_or_empty_promo_types_breaks_the_set(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "NULLS", [[UB], None])
        _write_partition(cards, "EMPTY", [[UB], []])
        assert _universes_beyond_set_codes(cards, tokens) == set()

    def test_only_complete_sets_are_returned(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "WHOLE", [[UB]])
        _write_partition(cards, "MIXED", [[UB], []])
        _write_partition(cards, "PLAIN", [["boosterfun"]])
        assert _universes_beyond_set_codes(cards, tokens) == {"WHOLE"}

    def test_token_only_set_is_judged_on_its_tokens(self, dirs):
        cards, tokens = dirs
        _write_partition(tokens, "TOK", [[UB], [UB]])
        _write_partition(tokens, "MIXTOK", [[UB], []])
        assert _universes_beyond_set_codes(cards, tokens) == {"TOK"}

    def test_tokens_sharing_the_set_code_count_toward_the_set(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "SHARED", [[UB]])
        _write_partition(tokens, "SHARED", [[]])
        assert _universes_beyond_set_codes(cards, tokens) == set()

    def test_linked_token_set_does_not_affect_its_parent(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "WHOLE", [[UB]])
        _write_partition(tokens, "TWHOLE", [[]])
        assert _universes_beyond_set_codes(cards, tokens) == {"WHOLE"}

    def test_no_parquet_means_no_flags(self, tmp_path):
        assert _universes_beyond_set_codes(tmp_path / "missing_cards", tmp_path / "missing_tokens") == set()

    def test_missing_promo_types_column_means_no_flags(self, dirs):
        cards, tokens = dirs
        partition = cards / "setCode=OLD"
        partition.mkdir()
        pl.DataFrame({"setCode": ["OLD"]}).write_parquet(partition / "0.parquet")
        assert _universes_beyond_set_codes(cards, tokens) == set()


class TestEnrichSetMetadata:
    def test_flag_is_set_only_on_wholly_universes_beyond_sets(self, dirs):
        cards, tokens = dirs
        _write_partition(cards, "WHOLE", [[UB], [UB]])
        _write_partition(cards, "MIXED", [[UB], []])
        set_meta: dict[str, dict[str, Any]] = {
            "WHOLE": {"code": "WHOLE", "name": "Whole"},
            "MIXED": {"code": "MIXED", "name": "Mixed"},
            "EMPTY": {"code": "EMPTY", "name": "No Cards"},
        }
        _enrich_set_metadata(PipelineContext.for_testing(), set_meta, cards, tokens)
        assert set_meta["WHOLE"]["isUniversesBeyond"] is True
        assert "isUniversesBeyond" not in set_meta["MIXED"]
        assert "isUniversesBeyond" not in set_meta["EMPTY"]


class TestPublishedOutput:
    """Only True is published, matching the other optional set booleans."""

    @staticmethod
    def _with_flag(ctx: AssemblyContext, value: bool | None) -> AssemblyContext:
        meta = {code: dict(m) for code, m in ctx.set_meta.items()}
        meta["TST"]["isUniversesBeyond"] = value
        return dataclasses.replace(ctx, set_meta=meta)

    def test_flagged_set_publishes_the_flag(self, assembly_ctx: AssemblyContext) -> None:
        ctx = self._with_flag(assembly_ctx, True)
        assert ctx.sets.build("TST", include_decks=False, include_sealed=False)["isUniversesBeyond"] is True
        entry = next(s for s in ctx.set_list.build() if s["code"] == "TST")
        assert entry["isUniversesBeyond"] is True

    @pytest.mark.parametrize("value", [None, False])
    def test_unflagged_set_omits_the_flag(self, assembly_ctx: AssemblyContext, value: bool | None) -> None:
        ctx = self._with_flag(assembly_ctx, value)
        assert "isUniversesBeyond" not in ctx.sets.build("TST", include_decks=False, include_sealed=False)
        entry = next(s for s in ctx.set_list.build() if s["code"] == "TST")
        assert "isUniversesBeyond" not in entry
