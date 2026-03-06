"""Golden file tests for the assembly layer.

Tests SetAssembler, AtomicCardsAssembler, and SetListAssembler against
committed JSON snapshots. Run with --update-golden to regenerate snapshots
when output intentionally changes.
"""

from __future__ import annotations

import pathlib
from typing import Any

import orjson
import pytest

from mtgjson5.build.context import AssemblyContext

GOLDEN_DIR = pathlib.Path(__file__).parent / "golden"


def _assert_or_update_golden(
    result: Any,
    golden_path: pathlib.Path,
    update: bool,
) -> None:
    """Compare result to golden file, or update it."""
    if update:
        golden_path.parent.mkdir(parents=True, exist_ok=True)
        golden_path.write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
        pytest.skip("Golden file updated -- re-run to verify")

    assert golden_path.exists(), f"Golden file missing: {golden_path}. Run with --update-golden to generate."
    expected = orjson.loads(golden_path.read_bytes())
    assert result == expected


# =============================================================================
# SetAssembler
# =============================================================================


class TestSetAssemblerGolden:
    def test_set_tst_golden(self, assembly_ctx: AssemblyContext, update_golden: bool) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        _assert_or_update_golden(result, GOLDEN_DIR / "set_TST.json", update_golden)

    def test_set_ts2_golden(self, assembly_ctx: AssemblyContext, update_golden: bool) -> None:
        result = assembly_ctx.sets.build("TS2", include_decks=False, include_sealed=False)
        _assert_or_update_golden(result, GOLDEN_DIR / "set_TS2.json", update_golden)

    def test_cards_sorted_by_number(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        numbers = [c["number"] for c in result["cards"]]
        # Collector number sort: 1, 2a, 2b, 3
        assert numbers == ["1", "2a", "2b", "3"]

    def test_optional_bools_omitted_when_false(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        bolt = next(c for c in result["cards"] if c["name"] == "Lightning Bolt")
        funny = next(c for c in result["cards"] if c["name"] == "Funny Card")
        # isFunny should be absent on non-funny cards, present on funny ones
        assert "isFunny" not in bolt
        assert funny.get("isFunny") is True

    def test_split_card_faces_linked(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        faces = [c for c in result["cards"] if c["name"] == "Wear // Tear"]
        assert len(faces) == 2
        for face in faces:
            assert "otherFaceIds" in face
            assert len(face["otherFaceIds"]) == 1

    def test_tokens_loaded(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        assert len(result["tokens"]) >= 1
        token_names = [t["name"] for t in result["tokens"]]
        assert "Zombie" in token_names

    def test_set_metadata_fields(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.sets.build("TST", include_decks=False, include_sealed=False)
        assert result["code"] == "TST"
        assert result["name"] == "Test Set"
        assert result["type"] == "expansion"
        assert result["releaseDate"] == "2025-01-01"
        assert isinstance(result["baseSetSize"], int)
        assert isinstance(result["totalSetSize"], int)


# =============================================================================
# AtomicCardsAssembler
# =============================================================================


class TestAtomicCardsGolden:
    def test_atomic_golden(self, assembly_ctx: AssemblyContext, update_golden: bool) -> None:
        result = assembly_ctx.atomic_cards.build()
        _assert_or_update_golden(result, GOLDEN_DIR / "atomic_cards.json", update_golden)

    def test_dedup_across_sets(self, assembly_ctx: AssemblyContext) -> None:
        """Lightning Bolt in TST and TS2 should produce one atomic entry."""
        result = assembly_ctx.atomic_cards.build()
        bolts = result.get("Lightning Bolt", [])
        assert len(bolts) == 1

    def test_printing_identifiers_stripped(self, assembly_ctx: AssemblyContext) -> None:
        """Atomic cards should only keep scryfallOracleId in identifiers."""
        result = assembly_ctx.atomic_cards.build()
        for name, cards in result.items():
            for card in cards:
                ids = card.get("identifiers", {})
                for key in ids:
                    assert key == "scryfallOracleId", f"{name}: unexpected identifier key '{key}' in atomic output"

    def test_legalities_merged_across_printings(self, assembly_ctx: AssemblyContext) -> None:
        """Lightning Bolt's legalities should include pauper from the TS2 printing."""
        result = assembly_ctx.atomic_cards.build()
        bolts = result.get("Lightning Bolt", [])
        assert len(bolts) == 1
        leg = bolts[0].get("legalities", {})
        # TST has vintage/legacy/modern/commander; TS2 adds pauper
        assert leg.get("pauper") == "Legal"
        assert leg.get("vintage") == "Legal"


# =============================================================================
# SetListAssembler
# =============================================================================


class TestSetListGolden:
    def test_set_list_golden(self, assembly_ctx: AssemblyContext, update_golden: bool) -> None:
        result = assembly_ctx.set_list.build()
        _assert_or_update_golden(result, GOLDEN_DIR / "set_list.json", update_golden)

    def test_sorted_by_code(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.set_list.build()
        codes = [s["code"] for s in result]
        assert codes == sorted(codes)

    def test_set_list_fields(self, assembly_ctx: AssemblyContext) -> None:
        result = assembly_ctx.set_list.build()
        assert len(result) >= 2
        for entry in result:
            assert "code" in entry
            assert "name" in entry
            assert "type" in entry
            assert "releaseDate" in entry
