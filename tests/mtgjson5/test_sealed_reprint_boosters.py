"""Tests for boosters derived for a set reprinting its parent in another language.

The Japanese Chronicles pack opens a BCHR booster, which magic-sealed-data
does not carry: BCHR reprints CHR card for card in Japanese, so its booster is
CHR's with every card swapped for the BCHR printing at the same number.
"""

from __future__ import annotations

import copy
import logging

import polars as pl

from mtgjson5.pipeline.stages.sealed import (
    add_reprint_boosters,
    build_pipeline_view,
    compile_card_to_products,
    compile_contents,
    compile_products,
    derive_reprint_boosters,
    requested_boosters,
    set_parents,
)
from mtgjson5.pipeline.stages.sealed_uuids import resolve_sealed_uuids
from mtgjson5.providers.github.provider import _build_boosters_records

PARENT_BOOSTER = {
    "default": {
        "languages": ["en"],
        "boosters": [{"weight": 1, "contents": {"common": 2, "uncommon": 1}}],
        "boostersTotalWeight": 1,
        "sheets": {
            "common": {"cards": {"en-1": 3, "en-2": 1}, "foil": False, "totalWeight": 4},
            "uncommon": {"cards": {"en-3": 1}, "foil": False, "totalWeight": 1},
        },
        "sourceSetCodes": ["CHR"],
    }
}


def _uuid_map(reprints: dict | None = None) -> dict:
    return {
        "chr": {"cards": {"1": ("en-1", "Abu Ja'far"), "2": ("en-2", "Akron Legionnaire"), "3": ("en-3", "Arcades")}},
        "bchr": {
            "cards": reprints
            if reprints is not None
            else {"1": ("ja-1", "Abu Ja'far"), "2": ("ja-2", "Akron Legionnaire"), "3": ("ja-3", "Arcades")}
        },
    }


REQUESTED = {("bchr", "default"): {"Japanese"}}
PARENTS = {"bchr": "chr"}


class TestDeriveReprintBoosters:
    def test_parent_booster_with_every_card_swapped_for_its_reprint(self):
        derived = derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, _uuid_map(), PARENTS)

        booster = derived["BCHR"]["default"]
        assert booster["sheets"]["common"]["cards"] == {"ja-1": 3, "ja-2": 1}
        assert booster["sheets"]["uncommon"]["cards"] == {"ja-3": 1}
        assert booster["sheets"]["common"]["totalWeight"] == 4
        assert booster["boosters"] == PARENT_BOOSTER["default"]["boosters"]
        assert booster["sourceSetCodes"] == ["BCHR"]
        assert booster["languages"] == ["jp"]

    def test_parent_booster_is_left_as_it_was(self):
        derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, _uuid_map(), PARENTS)

        assert PARENT_BOOSTER["default"]["sheets"]["common"]["cards"] == {"en-1": 3, "en-2": 1}
        assert PARENT_BOOSTER["default"]["sourceSetCodes"] == ["CHR"]

    def test_nothing_when_no_product_opens_the_set(self):
        """Today the Japanese pack still opens CHR, which has its booster."""
        requested = {("chr", "default"): {"Japanese"}}

        assert not derive_reprint_boosters(requested, {"CHR": PARENT_BOOSTER}, _uuid_map(), PARENTS)

    def test_a_booster_the_set_already_has_is_kept(self):
        own = {"default": {"sheets": {}, "sourceSetCodes": ["BCHR"]}}

        assert not derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER, "BCHR": own}, _uuid_map(), PARENTS)

    def test_nothing_without_a_parent_or_a_parent_booster(self):
        assert not derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, _uuid_map(), {})
        assert not derive_reprint_boosters(REQUESTED, {}, _uuid_map(), PARENTS)
        assert not derive_reprint_boosters(
            {("bchr", "jumpstart"): set()}, {"CHR": PARENT_BOOSTER}, _uuid_map(), PARENTS
        )

    def test_a_card_without_a_reprint_stops_the_whole_booster(self, caplog):
        reprints = {"1": ("ja-1", "Abu Ja'far"), "2": ("ja-2", "Akron Legionnaire")}

        with caplog.at_level(logging.WARNING, logger="mtgjson5.pipeline.stages.sealed"):
            derived = derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, _uuid_map(reprints), PARENTS)

        assert not derived
        assert any("CHR 3 Arcades has no reprint" in r.getMessage() for r in caplog.records)

    def test_a_reprint_printed_in_several_languages_is_not_guessed(self, caplog):
        """4BB is printed in Japanese, Korean, Chinese, Spanish and Portuguese:
        which of them a pack held is unknown, and the map's pick for a number
        is not necessarily the card MTGJSON publishes for it."""
        uuid_map = _uuid_map()
        uuid_map["bchr"]["cards_by_language"] = {
            "2": {"Japanese": ("ja-2", "Akron Legionnaire"), "Korean": ("ko-2", "Akron Legionnaire")}
        }

        with caplog.at_level(logging.WARNING, logger="mtgjson5.pipeline.stages.sealed"):
            derived = derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, uuid_map, PARENTS)

        assert not derived
        assert any("BCHR 2 Akron Legionnaire is printed in several languages" in r.getMessage() for r in caplog.records)

    def test_a_different_card_at_the_same_number_is_not_a_reprint(self):
        reprints = {"1": ("ja-1", "Abu Ja'far"), "2": ("ja-2", "Akron Legionnaire"), "3": ("ja-3", "Something Else")}

        assert not derive_reprint_boosters(REQUESTED, {"CHR": PARENT_BOOSTER}, _uuid_map(reprints), PARENTS)

    def test_no_language_claimed_when_the_product_names_none(self):
        """The parent's English is not the reprint's language."""
        derived = derive_reprint_boosters({("bchr", "default"): set()}, {"CHR": PARENT_BOOSTER}, _uuid_map(), PARENTS)

        assert "languages" not in derived["BCHR"]["default"]


class TestRequestedBoosters:
    def test_every_pack_a_product_opens_with_its_language(self, tmp_path):
        (tmp_path / "BCHR.yaml").write_text(
            """
code: bchr
products:
  Japanese Booster Pack:
    language: Japanese
    contents:
      pack:
      - {code: default, set: bchr}
  Japanese Booster Pack Again:
    language: Japanese
    contents:
      copy: Japanese Booster Pack
  Mixed Box:
    contents:
      variable:
      - chance: 1
        pack:
        - {code: default, set: chr}
  Box:
    contents:
      sealed:
      - {count: 45, name: Japanese Booster Pack, set: bchr}
""",
            encoding="utf-8",
        )

        requested = requested_boosters(tmp_path, None, compile_products(tmp_path))

        assert requested == {("bchr", "default"): {"Japanese"}, ("chr", "default"): set()}


class TestSetParents:
    def test_lowercase_code_to_parent(self):
        sets = pl.DataFrame({"code": ["BCHR", "CHR"], "parent_set_code": ["chr", None]})

        assert set_parents(sets) == {"bchr": "chr"}
        assert set_parents(None) == {}


BCHR_YAML = """
code: bchr
products:
  Chronicles Foreign Black Border Japanese Booster Pack:
    category: BOOSTER_PACK
    language: Japanese
    contents:
      card_count: 3
      pack:
      - {code: default, set: %s}
"""


class TestAddReprintBoosters:
    """The whole path the sealed compilation takes, from the YAML on."""

    def _run(self, tmp_path, opens: str):
        (tmp_path / "BCHR.yaml").write_text(BCHR_YAML % opens, encoding="utf-8")
        products = compile_products(tmp_path)
        boosters_raw = {"CHR": copy.deepcopy(PARENT_BOOSTER)}
        uuid_map = _uuid_map()
        uuid_map["chr"]["booster"] = {"default"}
        uuid_map["bchr"]["sealedProduct"] = {name: uuid for (_, name), uuid in resolve_sealed_uuids(products).items()}
        sets = pl.DataFrame({"code": ["BCHR"], "parent_set_code": ["chr"]})
        added = add_reprint_boosters(boosters_raw, uuid_map, tmp_path, None, products, sets)
        return added, boosters_raw, uuid_map, products

    def test_japanese_pack_opens_and_lists_the_japanese_cards(self, tmp_path, caplog):
        added, boosters_raw, uuid_map, products = self._run(tmp_path, "bchr")

        assert added
        assert boosters_raw["BCHR"]["default"]["sheets"]["common"]["cards"] == {"ja-1": 3, "ja-2": 1}
        assert "default" in uuid_map["bchr"]["booster"]
        # published like magic-sealed-data's own boosters
        assert "BCHR" in {record["setCode"] for record in _build_boosters_records(boosters_raw)}

        with caplog.at_level(logging.WARNING, logger="mtgjson5.pipeline.stages.sealed"):
            contents, _ = compile_contents(tmp_path, None, uuid_map)
        assert not [r for r in caplog.records if "Booster code" in r.getMessage()]

        view = build_pipeline_view(contents, boosters_raw, [], {}, products, uuid_map)
        sources = compile_card_to_products(view)
        pack = resolve_sealed_uuids(products)[("BCHR", "Chronicles Foreign Black Border Japanese Booster Pack")]
        for uuid in ("ja-1", "ja-2", "ja-3"):
            assert pack in sources[uuid]["nonfoil"]
        assert not any(uuid.startswith("en-") for uuid in sources)

    def test_pack_still_opening_chr_changes_nothing(self, tmp_path):
        added, boosters_raw, uuid_map, _ = self._run(tmp_path, "chr")

        assert not added
        assert set(boosters_raw) == {"CHR"}
        assert "booster" not in uuid_map["bchr"]
