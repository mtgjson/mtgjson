"""Tests for pinned token references in relatedCards.tokens."""

from __future__ import annotations

import json

import polars as pl

from mtgjson5.pipeline.stages.relationships import add_token_ids
from mtgjson5.pipeline.stages.token_references import (
    build_pins,
    build_token_pins,
    dump_pins,
    extract_token_references,
    load_pins,
    pin_stats,
)

CARD = "11111111-1111-1111-1111-111111111111"
CARD_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ORACLE = "99999999-9999-9999-9999-999999999999"
OLD_PRINTING = "22222222-2222-2222-2222-222222222222"
NEW_PRINTING = "33333333-3333-3333-3333-333333333333"

_ALL_PARTS = pl.List(pl.Struct({"component": pl.String, "id": pl.String}))


def _cards_lf(rows: list[dict]) -> pl.LazyFrame:
    """Minimal Scryfall-shaped frame: id, oracle_id, all_parts."""
    return pl.LazyFrame(
        rows,
        schema={"id": pl.String, "oracle_id": pl.String, "all_parts": _ALL_PARTS},
    )


def _token(printing: str) -> list[dict]:
    return [{"component": "token", "id": printing}]


def _card_lf(printing: str) -> pl.LazyFrame:
    """The pipeline frame add_token_ids() consumes."""
    return pl.LazyFrame(
        [{"uuid": CARD_UUID, "scryfallId": CARD, "_all_parts": _token(printing)}],
        schema={"uuid": pl.String, "scryfallId": pl.String, "_all_parts": _ALL_PARTS},
    )


def _uuid_map(*printings: str) -> pl.LazyFrame:
    return pl.LazyFrame(
        {"scryfallId": list(printings), "uuid": [f"uuid-of-{p[:8]}" for p in printings]},
        schema={"scryfallId": pl.String, "uuid": pl.String},
    )


def _resolve(lf: pl.LazyFrame, uuid_map: pl.LazyFrame, pins) -> list[str]:
    df = add_token_ids(lf, uuid_map, pins).collect()
    return sorted(df["_token_uuids"][0])


class TestExtractTokenReferences:
    def test_pairs_each_card_with_its_token_oracle(self):
        cards = _cards_lf(
            [
                {"id": CARD, "oracle_id": "card-oracle", "all_parts": _token(OLD_PRINTING)},
                {"id": OLD_PRINTING, "oracle_id": ORACLE, "all_parts": None},
            ]
        )

        refs = extract_token_references(cards)

        assert refs.to_dicts() == [{"_card_sid": CARD, "_tok_oracle": ORACLE, "_tok_sid": OLD_PRINTING}]

    def test_ignores_non_token_components(self):
        cards = _cards_lf(
            [
                {"id": CARD, "oracle_id": "o", "all_parts": [{"component": "meld_result", "id": OLD_PRINTING}]},
                {"id": OLD_PRINTING, "oracle_id": ORACLE, "all_parts": None},
            ]
        )

        assert extract_token_references(cards).height == 0

    def test_drops_references_to_tokens_missing_from_the_dump(self):
        """Scryfall occasionally points at an id it no longer serves."""
        cards = _cards_lf([{"id": CARD, "oracle_id": "o", "all_parts": _token("gone")}])

        assert extract_token_references(cards).height == 0


class TestBuildPins:
    def _refs(self, printing: str) -> pl.DataFrame:
        return pl.DataFrame([{"_card_sid": CARD, "_tok_oracle": ORACLE, "_tok_sid": printing}])

    def test_seeds_unpinned_references(self):
        pins = build_pins(self._refs(OLD_PRINTING), {}, {OLD_PRINTING})

        assert pins == {CARD: {ORACLE: OLD_PRINTING}}

    def test_existing_pin_survives_a_scryfall_repoint(self):
        existing = {CARD: {ORACLE: OLD_PRINTING}}

        # Scryfall now names NEW_PRINTING, but OLD_PRINTING still exists.
        refs = pl.DataFrame(
            [
                {"_card_sid": CARD, "_tok_oracle": ORACLE, "_tok_sid": NEW_PRINTING},
                {"_card_sid": "other", "_tok_oracle": ORACLE, "_tok_sid": OLD_PRINTING},
            ]
        )
        pins = build_pins(refs, existing, {OLD_PRINTING, NEW_PRINTING})

        assert pins[CARD][ORACLE] == OLD_PRINTING

    def test_pin_survives_when_no_card_references_it_any_more(self):
        """The repoint case: OLD_PRINTING still exists, nothing names it now."""
        existing = {CARD: {ORACLE: OLD_PRINTING}}

        pins = build_pins(self._refs(NEW_PRINTING), existing, {OLD_PRINTING, NEW_PRINTING})

        assert pins[CARD][ORACLE] == OLD_PRINTING

    def test_pin_to_a_retired_printing_is_repointed(self):
        """Nothing left to preserve once the pinned printing leaves Scryfall."""
        existing = {CARD: {ORACLE: OLD_PRINTING}}

        pins = build_pins(self._refs(NEW_PRINTING), existing, {NEW_PRINTING})

        assert pins[CARD][ORACLE] == NEW_PRINTING

    def test_round_trip_is_idempotent(self, tmp_path):
        first = build_pins(self._refs(OLD_PRINTING), {}, {OLD_PRINTING})
        path = tmp_path / "pins.json"
        dump_pins(first, path)

        assert build_pins(self._refs(OLD_PRINTING), load_pins(path, refresh=True), {OLD_PRINTING}) == first
        assert json.loads(path.read_text()) == first

    def test_pin_stats_counts_cards_and_references(self):
        assert pin_stats({CARD: {ORACLE: OLD_PRINTING, "b": "c"}}) == {"cards": 1, "references": 2}


class TestAddTokenIds:
    def _pins(self, pin_map: dict) -> object:
        cards = _cards_lf(
            [
                {"id": OLD_PRINTING, "oracle_id": ORACLE, "all_parts": None},
                {"id": NEW_PRINTING, "oracle_id": ORACLE, "all_parts": None},
            ]
        )
        return build_token_pins(cards, pin_map)

    def test_without_pins_follows_scryfall(self):
        result = _resolve(_card_lf(NEW_PRINTING), _uuid_map(OLD_PRINTING, NEW_PRINTING), None)

        assert result == [f"uuid-of-{NEW_PRINTING[:8]}"]

    def test_pin_overrides_a_repointed_reference(self):
        pins = self._pins({CARD: {ORACLE: OLD_PRINTING}})

        result = _resolve(_card_lf(NEW_PRINTING), _uuid_map(OLD_PRINTING, NEW_PRINTING), pins)

        assert result == [f"uuid-of-{OLD_PRINTING[:8]}"]

    def test_pin_agreeing_with_scryfall_is_a_no_op(self):
        pins = self._pins({CARD: {ORACLE: OLD_PRINTING}})

        result = _resolve(_card_lf(OLD_PRINTING), _uuid_map(OLD_PRINTING, NEW_PRINTING), pins)

        assert result == [f"uuid-of-{OLD_PRINTING[:8]}"]

    def test_pin_to_a_vanished_printing_falls_back_to_live(self):
        """A pin must never strand a reference."""
        pins = self._pins({CARD: {ORACLE: OLD_PRINTING}})

        # OLD_PRINTING is no longer resolvable to a UUID.
        result = _resolve(_card_lf(NEW_PRINTING), _uuid_map(NEW_PRINTING), pins)

        assert result == [f"uuid-of-{NEW_PRINTING[:8]}"]

    def test_unpinned_card_uses_live_reference(self):
        pins = self._pins({"some-other-card": {ORACLE: OLD_PRINTING}})

        result = _resolve(_card_lf(NEW_PRINTING), _uuid_map(OLD_PRINTING, NEW_PRINTING), pins)

        assert result == [f"uuid-of-{NEW_PRINTING[:8]}"]

    def test_empty_pin_file_disables_pinning(self):
        assert build_token_pins(_cards_lf([]), {}) is None

    def test_pins_apply_to_the_camel_cased_pipeline_frame(self):
        """ctx.cards_lf is normalized to camelCase before the pipeline sees it."""
        cards = _cards_lf(
            [
                {"id": OLD_PRINTING, "oracle_id": ORACLE, "all_parts": None},
                {"id": NEW_PRINTING, "oracle_id": ORACLE, "all_parts": None},
            ]
        ).rename({"oracle_id": "oracleId", "all_parts": "allParts"})
        pins = build_token_pins(cards, {CARD: {ORACLE: OLD_PRINTING}})

        assert pins is not None
        result = _resolve(_card_lf(NEW_PRINTING), _uuid_map(OLD_PRINTING, NEW_PRINTING), pins)

        assert result == [f"uuid-of-{OLD_PRINTING[:8]}"]

    def test_unresolvable_reference_yields_an_empty_list(self):
        df = add_token_ids(_card_lf(NEW_PRINTING), _uuid_map(OLD_PRINTING), None).collect()

        assert df["_token_uuids"].to_list() == [[]]

    def test_all_parts_is_dropped(self):
        df = add_token_ids(_card_lf(NEW_PRINTING), _uuid_map(NEW_PRINTING), None).collect()

        assert "_all_parts" not in df.columns
        assert "_token_scryfall_ids" not in df.columns


class TestShippedPinFile:
    def test_is_populated_and_well_formed(self):
        pins = load_pins()
        assert pins, "token_references.json is missing or empty"

        for card_sid, entries in pins.items():
            assert isinstance(entries, dict), f"{card_sid} is not a mapping"
            assert entries, f"{card_sid} has no pinned references"
            for oracle, printing in entries.items():
                assert isinstance(oracle, str), f"{card_sid} has a non-string oracle id"
                assert isinstance(printing, str), f"{card_sid}/{oracle} is not a string"
                assert len(printing) == 36, f"{card_sid}/{oracle} is not a Scryfall id"
