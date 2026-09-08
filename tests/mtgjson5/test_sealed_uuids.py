"""Tests for stable sealed product UUID pinning."""

from __future__ import annotations

import json
import logging

import polars as pl

from mtgjson5.pipeline.stages.sealed_uuids import (
    build_pins,
    dump_pins,
    load_pins,
    name_uuid,
    qualified_name,
    resolve_sealed_uuids,
    sealed_uuid_expr,
    serialize_pins,
)

BUNDLE = "Test Set Bundle"
BUNDLE_RENAMED = "Test Set Collector Bundle"
BOX = "Test Set Booster Box"


def _pins(**products: dict) -> dict:
    return {"TST": products}


def _pin(name: str, **identifiers: str) -> dict:
    entry: dict = {"uuid": name_uuid(name)}
    if identifiers:
        entry["identifiers"] = identifiers
    return entry


class TestResolveSealedUuids:
    def test_unpinned_product_uses_name_formula(self):
        products = {"tst": {BOX: {"identifiers": {"tcgplayerProductId": "1"}}}}

        resolved = resolve_sealed_uuids(products, {})

        assert resolved[("TST", BOX)] == name_uuid(BOX)

    def test_set_code_is_normalised_to_upper(self):
        products: dict = {"tst": {BOX: {}}}

        resolved = resolve_sealed_uuids(products, {})

        assert ("TST", BOX) in resolved

    def test_pin_wins_over_name_formula(self):
        pins = _pins(**{BOX: {"uuid": "pinned-uuid"}})
        products: dict = {"TST": {BOX: {}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BOX)] == "pinned-uuid"

    def test_rename_keeps_original_uuid(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123", mcmId="456")})
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"tcgplayerProductId": "123", "mcmId": "456"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE)

    def test_rename_matches_on_a_single_shared_identifier(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123", mcmId="456")})
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"mcmId": "456", "csiId": "new"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE)

    def test_new_product_does_not_steal_a_live_products_uuid(self):
        """A pin whose name is still in the build was not vacated by a rename."""
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123")})
        products = {
            "TST": {
                BUNDLE: {"identifiers": {"tcgplayerProductId": "123"}},
                BOX: {"identifiers": {"tcgplayerProductId": "123"}},
            }
        }

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE)] == name_uuid(BUNDLE)
        assert resolved[("TST", BOX)] == name_uuid(BOX)

    def test_ambiguous_match_falls_back_to_name_formula(self):
        """Two retired pins sharing an identifier must not hand out a UUID."""
        pins = _pins(
            **{
                BUNDLE: _pin(BUNDLE, tcgplayerProductId="123"),
                BOX: _pin(BOX, tcgplayerProductId="123"),
            }
        )
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"tcgplayerProductId": "123"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE_RENAMED)

    def test_two_products_claiming_one_pin_both_fall_back(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123", mcmId="456")})
        products = {
            "TST": {
                "Split A": {"identifiers": {"tcgplayerProductId": "123"}},
                "Split B": {"identifiers": {"mcmId": "456"}},
            }
        }

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", "Split A")] == name_uuid("Split A")
        assert resolved[("TST", "Split B")] == name_uuid("Split B")

    def test_pins_are_scoped_per_set(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123")})
        products = {"OTH": {BUNDLE_RENAMED: {"identifiers": {"tcgplayerProductId": "123"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("OTH", BUNDLE_RENAMED)] == name_uuid(BUNDLE_RENAMED)

    def test_conflicting_identifier_blocks_a_rename_match(self):
        """A shared id is not enough when another id says these are different products."""
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="123", mcmId="456")})
        products = {"TST": {BOX: {"identifiers": {"tcgplayerProductId": "123", "mcmId": "999"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BOX)] == name_uuid(BOX)

    def test_rename_still_matches_when_identifiers_only_get_added(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, mcmId="456")})
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"mcmId": "456", "csiId": "new"}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE)

    def test_malformed_product_entries_are_skipped(self):
        """The products frame drops non-mapping bodies, so resolution must too."""
        products = {"TST": {BOX: {"identifiers": {"mcmId": "1"}}, BUNDLE: "not a mapping"}}

        resolved = resolve_sealed_uuids(products, {})

        assert resolved == {("TST", BOX): name_uuid(BOX)}

    def test_malformed_set_body_is_skipped(self):
        products = {"TST": ["not a mapping"], "OTH": {BOX: {}}}

        resolved = resolve_sealed_uuids(products, {})

        assert resolved == {("OTH", BOX): name_uuid(BOX)}

    def test_reused_retired_name_does_not_duplicate_a_uuid(self):
        """Upstream re-adding a name a rename carried forward must not collide."""
        pins = _pins(**{BUNDLE_RENAMED: {"uuid": name_uuid(BUNDLE), "originalName": BUNDLE}})
        products = {
            "TST": {
                BUNDLE_RENAMED: {"identifiers": {"mcmId": "456"}},
                BUNDLE: {"identifiers": {"mcmId": "789"}},
            }
        }

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE)
        assert resolved[("TST", BUNDLE)] == name_uuid(qualified_name("TST", BUNDLE))
        assert len(set(resolved.values())) == 2

    def test_cross_set_name_reuse_does_not_duplicate_a_pinned_uuid(self):
        pins = _pins(**{BUNDLE_RENAMED: {"uuid": name_uuid(BUNDLE), "originalName": BUNDLE}})
        products: dict = {"TST": {BUNDLE_RENAMED: {}}, "OTH": {BUNDLE: {}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE)
        assert resolved[("OTH", BUNDLE)] == name_uuid(qualified_name("OTH", BUNDLE))

    def test_unpinned_duplicate_names_keep_the_published_uuid(self):
        """Two sets sharing a product name collide in the historical formula too.

        Nothing was pinned here, so both UUIDs are already published as-is and
        re-minting one would move it.  Report it, do not rewrite it.
        """
        products: dict = {"TST": {BOX: {}}, "OTH": {BOX: {}}}

        resolved = resolve_sealed_uuids(products, {})

        assert resolved[("TST", BOX)] == name_uuid(BOX)
        assert resolved[("OTH", BOX)] == name_uuid(BOX)

    def test_identifier_drift_under_a_stable_name_is_reported(self, caplog):
        """A name swap resolves by name; the pin's identifiers are the only tell."""
        pins = _pins(**{BOX: _pin(BOX, mcmId="456")})
        products = {"TST": {BOX: {"identifiers": {"mcmId": "999"}}}}

        with caplog.at_level(logging.WARNING):
            resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BOX)] == name_uuid(BOX)
        assert "mcmId: 456 -> 999" in caplog.text

    def test_added_identifiers_are_not_reported_as_drift(self, caplog):
        pins = _pins(**{BOX: _pin(BOX, mcmId="456")})
        products = {"TST": {BOX: {"identifiers": {"mcmId": "456", "csiId": "new"}}}}

        with caplog.at_level(logging.WARNING):
            resolve_sealed_uuids(products, pins)

        assert "disagrees" not in caplog.text

    def test_empty_identifiers_never_match(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, tcgplayerProductId="")})
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"mcmId": None}}}}

        resolved = resolve_sealed_uuids(products, pins)

        assert resolved[("TST", BUNDLE_RENAMED)] == name_uuid(BUNDLE_RENAMED)


class TestBuildPins:
    def test_seeds_every_product(self):
        products = {"tst": {BOX: {"identifiers": {"tcgplayerProductId": "1"}}}}

        pins = build_pins(products, {})

        assert pins["TST"][BOX]["uuid"] == name_uuid(BOX)
        assert pins["TST"][BOX]["identifiers"] == {"tcgplayerProductId": "1"}

    def test_rename_refiles_pin_under_new_name(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, mcmId="456")})
        products = {"TST": {BUNDLE_RENAMED: {"identifiers": {"mcmId": "456"}}}}

        updated = build_pins(products, pins)

        assert BUNDLE not in updated["TST"]
        assert updated["TST"][BUNDLE_RENAMED]["uuid"] == name_uuid(BUNDLE)
        assert updated["TST"][BUNDLE_RENAMED]["originalName"] == BUNDLE

    def test_original_name_survives_a_second_rename(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, mcmId="456")})
        once = build_pins({"TST": {BUNDLE_RENAMED: {"identifiers": {"mcmId": "456"}}}}, pins)
        twice = build_pins({"TST": {BOX: {"identifiers": {"mcmId": "456"}}}}, once)

        assert twice["TST"][BOX]["uuid"] == name_uuid(BUNDLE)
        assert twice["TST"][BOX]["originalName"] == BUNDLE

    def test_retired_product_keeps_its_pin(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, mcmId="456"), BOX: _pin(BOX, mcmId="789")})
        products = {"TST": {BOX: {"identifiers": {"mcmId": "789"}}}}

        updated = build_pins(products, pins)

        assert updated["TST"][BUNDLE]["uuid"] == name_uuid(BUNDLE)

    def test_live_product_keeps_its_pin_when_a_name_is_reused(self):
        """The refile loop must not hand a live product's pin to a namesake."""
        pins = _pins(**{BUNDLE_RENAMED: {"uuid": name_uuid(BUNDLE), "originalName": BUNDLE}})
        products = {
            "TST": {
                BUNDLE: {"identifiers": {"mcmId": "789"}},
                BUNDLE_RENAMED: {"identifiers": {"mcmId": "456"}},
            }
        }

        updated = build_pins(products, pins)

        assert updated["TST"][BUNDLE_RENAMED]["uuid"] == name_uuid(BUNDLE)
        assert updated["TST"][BUNDLE_RENAMED]["originalName"] == BUNDLE
        assert updated["TST"][BUNDLE]["uuid"] == name_uuid(qualified_name("TST", BUNDLE))
        assert updated["TST"][BUNDLE]["originalName"] == qualified_name("TST", BUNDLE)

    def test_every_pin_traces_back_to_the_name_it_was_minted_from(self):
        """The invariant TestShippedPinFile enforces has to survive a collision."""
        pins = _pins(**{BUNDLE_RENAMED: {"uuid": name_uuid(BUNDLE), "originalName": BUNDLE}})
        products: dict = {"TST": {BUNDLE: {}, BUNDLE_RENAMED: {}, BOX: {}}}

        updated = build_pins(products, pins)

        for name, entry in updated["TST"].items():
            assert entry["uuid"] == name_uuid(entry.get("originalName", name))

    def test_malformed_entries_are_not_pinned(self):
        products = {"TST": {BOX: {}, BUNDLE: "not a mapping"}}

        updated = build_pins(products, {})

        assert set(updated["TST"]) == {BOX}

    def test_round_trip_is_idempotent(self, tmp_path):
        products = {"tst": {BOX: {"identifiers": {"tcgplayerProductId": "1"}}, BUNDLE: {}}}

        first = build_pins(products, {})
        path = tmp_path / "pins.json"
        dump_pins(first, path)
        second = build_pins(products, load_pins(path, refresh=True))

        assert first == second
        assert json.loads(path.read_text()) == first


class TestSealedUuidExpr:
    @staticmethod
    def _apply(products_lf: pl.LazyFrame, pins: dict) -> list:
        return products_lf.with_columns(sealed_uuid_expr(products_lf, pins).alias("uuid")).collect()["uuid"].to_list()

    def test_resolves_each_row(self):
        products_lf = pl.LazyFrame(
            {
                "setCode": ["TST", "TST"],
                "productName": [BOX, BUNDLE],
                "identifiers": [{"mcmId": "1"}, {"mcmId": "2"}],
            }
        )

        assert self._apply(products_lf, {}) == [name_uuid(BOX), name_uuid(BUNDLE)]

    def test_applies_pins_and_renames(self):
        pins = _pins(**{BUNDLE: _pin(BUNDLE, mcmId="2")})
        products_lf = pl.LazyFrame(
            {
                "setCode": ["TST"],
                "productName": [BUNDLE_RENAMED],
                "identifiers": [{"mcmId": "2"}],
            }
        )

        assert self._apply(products_lf, pins) == [name_uuid(BUNDLE)]

    def test_works_without_an_identifiers_column(self):
        products_lf = pl.LazyFrame({"setCode": ["TST"], "productName": [BOX]})

        assert self._apply(products_lf, {}) == [name_uuid(BOX)]

    def test_preserves_row_order(self):
        """sealedProduct array order is the frame's row order, so it must hold."""
        names = [f"Product {i:03d}" for i in range(50)]
        products_lf = pl.LazyFrame({"setCode": ["TST"] * 50, "productName": list(reversed(names))})

        assert self._apply(products_lf, {}) == [name_uuid(n) for n in reversed(names)]

    def test_lower_case_set_codes_still_resolve(self):
        """Pins are keyed upper case, so the row key has to be normalised too."""
        pins = _pins(**{BOX: {"uuid": "pinned-uuid"}})
        products_lf = pl.LazyFrame({"setCode": ["tst"], "productName": [BOX]})

        assert self._apply(products_lf, pins) == ["pinned-uuid"]

    def test_unknown_row_resolves_to_null(self):
        """Rows absent from the resolved map fall through to the caller."""
        products_lf = pl.LazyFrame({"setCode": ["TST"], "productName": [BOX]})
        other_lf = pl.LazyFrame({"setCode": ["TST"], "productName": ["Not In Products"]})

        result = other_lf.with_columns(sealed_uuid_expr(products_lf, {}).alias("uuid")).collect()

        assert result["uuid"].to_list() == [None]

    def test_empty_input_yields_null(self):
        products_lf = pl.LazyFrame(schema={"setCode": pl.String, "productName": pl.String})

        expr = sealed_uuid_expr(products_lf, {})
        result = pl.LazyFrame({"setCode": ["TST"], "productName": [BOX]}).with_columns(expr.alias("uuid")).collect()

        assert result["uuid"].to_list() == [None]
        assert result.schema["uuid"] == pl.String


class TestShippedPinFile:
    def test_matches_the_published_name_based_uuids(self):
        """Every pin must trace back to the name its UUID was minted from.

        For a product that has never been renamed that is its own name; once a
        rename carries a pin forward the entry records ``originalName``, which
        is the name the published UUID came from.  Either way the pin must
        still be reproducible from the historical formula, so no pin can quietly
        move an already-published UUID.
        """
        pins = load_pins()
        assert pins, "sealed_product_uuids.json is missing or empty"

        for set_code, entries in pins.items():
            for name, entry in entries.items():
                source = entry.get("originalName", name)
                assert entry["uuid"] == name_uuid(source), f"{set_code}/{name} pin diverges from '{source}'"

    def test_serialisation_is_byte_stable(self, tmp_path):
        """--check compares bytes, so dump and serialize must not drift."""
        pins = load_pins()
        path = tmp_path / "pins.json"
        dump_pins(pins, path)

        assert path.read_text(encoding="utf-8") == serialize_pins(pins)

    def test_no_uuid_is_shared_by_two_products(self):
        pins = load_pins()
        seen: dict[str, tuple[str, str]] = {}

        for set_code, entries in pins.items():
            for name, entry in entries.items():
                assert entry["uuid"] not in seen, f"{set_code}/{name} collides with {seen.get(entry['uuid'])}"
                seen[entry["uuid"]] = (set_code, name)
