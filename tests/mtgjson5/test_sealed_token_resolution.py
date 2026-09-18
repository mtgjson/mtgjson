"""Tests for how sealed content resolves ``token: true`` card entries.

Two things decide whether such an entry finds its card, and
build_uuid_map_from_pipeline() got both wrong:

1. What counts as a token. MTGJSON splits a set into cards[] and tokens[]
   with output.filter_out_tokens(), which takes layout in TOKEN_LAYOUTS, a
   "Dungeon" type, a type containing "Token", or a bare "Card" type. The
   UUID map checked ``layout == "token"`` only, so the double-faced tokens
   (MID:19 "Day // Night", CLB:20 "Undercity // The Initiative") and the AFR
   dungeons -- Scryfall ships those with layout "normal" and nothing but a
   "Dungeon" type line -- never made it into the tokens index.

2. Which set code they hang off. Most sets keep their tokens in a separate
   Scryfall set (TAFR for AFR, TCLB for CLB, TMID for MID), but
   assemble.load_set_tokens() remaps them onto the parent set, so
   AllPrintings publishes the AFR dungeons as AFR tokens numbered 20-22 and
   mtg-sealed-content names them "set: afr, number: 20, token: true". The map
   keyed them under "tafr" and left "afr" without them.

Both failures land in the same place: the entry falls through to the parent
set's cards[] index, hits a real card at that number under a different name,
and resolves to no UUID at all. SealedProductCard.uuid is required, so that
one entry then fails validation for every sealed product in the set -- which
is how a handful of dungeons took down a whole build.

The expected UUIDs are the ones mtgjson.com actually publishes in each set's
tokens[], not values derived from this test's fixture.
"""

from __future__ import annotations

import logging

import polars as pl

from mtgjson5.pipeline.stages.sealed import build_uuid_map_from_pipeline, card, product

# (scryfall id, published MTGJSON UUID for side "a")
AFR20_DUNGEON = ("6f509dbe-6ec7-4438-ab36-e20be46c9922", "aa281821-2a70-5b23-8a0d-a5874c753e6c")
CLB20_UNDERCITY = ("2c65185b-6cf0-451d-985e-56aa45d9a57d", "3e24568b-33d4-5e56-8e89-8a2e545ae88c")
MID19_DAY_NIGHT = ("9c0f7843-4cbb-4d0f-8887-ec823a9238da", "5b43434b-ed87-5548-bbe4-3f4bc4525c87")
# AFR:20 in the *card* set -- the row a dungeon entry wrongly fell through to.
AFR20_CARD = ("26b0e68a-9d4e-42ea-8dac-c47cbed0c6ad", "c65c30e4-9046-584a-bfd8-d73f27140c89")

CARD_ROWS = [
    # (id, set, number, name, layout, type_line)
    (AFR20_CARD[0], "afr", "20", "Icingdeath, Frost Tyrant", "normal", "Legendary Creature — Dragon"),
    (AFR20_DUNGEON[0], "tafr", "20", "Dungeon of the Mad Mage", "normal", "Dungeon"),
    (
        CLB20_UNDERCITY[0],
        "tclb",
        "20",
        "Undercity // The Initiative",
        "double_faced_token",
        "Dungeon — Undercity // Card",
    ),
    (MID19_DAY_NIGHT[0], "tmid", "19", "Day // Night", "double_faced_token", "Card // Card"),
    # A token printed inside its own set rather than a token set: SLD keeps
    # these in sld, and they resolved correctly even before the fix.
    ("8e6b1c7b-ef76-446a-9d05-6a5fb34fa68b", "sld", "918", "Food", "token", "Token Artifact — Food"),
]

TOKEN_SETS = [("TAFR", "afr"), ("TCLB", "clb"), ("TMID", "mid")]


def _cards_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        [
            {
                "id": row[0],
                "set": row[1],
                "collector_number": row[2],
                "name": row[3],
                "layout": row[4],
                "type_line": row[5],
                "lang": "en",
            }
            for row in CARD_ROWS
        ],
        schema={
            "id": pl.String,
            "set": pl.String,
            "collector_number": pl.String,
            "name": pl.String,
            "layout": pl.String,
            "type_line": pl.String,
            "lang": pl.String,
        },
    )


def _sets_lf() -> pl.LazyFrame:
    """Scryfall set metadata, shaped as GlobalCache._load_sets_metadata leaves it.

    Codes uppercased, parent_set_code left in Scryfall's lowercase.
    """
    return pl.LazyFrame(
        [{"code": code, "set_type": "token", "parent_set_code": parent} for code, parent in TOKEN_SETS]
        + [
            {"code": "AFR", "set_type": "expansion", "parent_set_code": None},
            {"code": "SLD", "set_type": "box", "parent_set_code": None},
        ],
        schema={"code": pl.String, "set_type": pl.String, "parent_set_code": pl.String},
    )


def _uuid_cache_lf() -> pl.LazyFrame:
    pairs = [AFR20_DUNGEON, CLB20_UNDERCITY, MID19_DAY_NIGHT, AFR20_CARD]
    return pl.LazyFrame(
        [{"scryfallId": sid, "side": "a", "cachedUuid": uuid} for sid, uuid in pairs],
        schema={"scryfallId": pl.String, "side": pl.String, "cachedUuid": pl.String},
    )


def _build_uuid_map(with_sets: bool = True) -> dict:
    return build_uuid_map_from_pipeline(
        cards_lf=_cards_lf(),
        uuid_cache_lf=_uuid_cache_lf(),
        boosters_raw={},
        decks_raw=[],
        products_dict={},
        sets_lf=_sets_lf() if with_sets else None,
    )


class TestTokenIndex:
    def test_dungeon_lands_in_the_parent_sets_tokens(self):
        """Scryfall gives the AFR dungeons layout "normal" and set "tafr"; the
        output publishes them as AFR tokens, and so must the map."""
        uuid_map = _build_uuid_map()
        assert uuid_map["afr"]["tokens"]["20"] == (AFR20_DUNGEON[1], "Dungeon of the Mad Mage")

    def test_double_faced_tokens_land_in_the_parent_sets_tokens(self):
        uuid_map = _build_uuid_map()
        assert uuid_map["clb"]["tokens"]["20"] == (CLB20_UNDERCITY[1], "Undercity // The Initiative")
        assert uuid_map["mid"]["tokens"]["19"] == (MID19_DAY_NIGHT[1], "Day // Night")

    def test_parent_cards_index_is_untouched(self):
        """Folding tokens in must not shadow the real card at that number."""
        uuid_map = _build_uuid_map()
        assert uuid_map["afr"]["cards"]["20"] == (AFR20_CARD[1], "Icingdeath, Frost Tyrant")

    def test_in_set_token_still_resolves(self):
        uuid_map = _build_uuid_map()
        assert uuid_map["sld"]["tokens"]["918"][1] == "Food"

    def test_without_set_metadata_nothing_is_folded(self):
        """sets_lf is optional; leaving it out costs the fold, not the build."""
        uuid_map = _build_uuid_map(with_sets=False)
        assert "20" not in uuid_map["afr"]["tokens"]
        assert uuid_map["tafr"]["tokens"]["20"][1] == "Dungeon of the Mad Mage"


class TestCardEntryResolution:
    def test_dungeon_entry_resolves_quietly(self, caplog):
        """A dungeon is written as set: afr, number: 20, token: true in
        mtg-sealed-content, and should resolve without a word of complaint."""
        uuid_map = _build_uuid_map()
        entry = card({"name": "Dungeon of the Mad Mage", "set": "afr", "number": 20, "token": True, "foil": True})
        with caplog.at_level(logging.WARNING):
            entry.get_uuids(uuid_map)
        assert entry.uuid == AFR20_DUNGEON[1]
        assert not caplog.records

    def test_day_night_entry_resolves(self):
        uuid_map = _build_uuid_map()
        entry = card({"name": "Day // Night", "set": "mid", "number": 19, "token": True})
        entry.get_uuids(uuid_map)
        assert entry.uuid == MID19_DAY_NIGHT[1]

    def test_non_token_entry_at_the_same_number_is_unaffected(self):
        uuid_map = _build_uuid_map()
        entry = card({"name": "Icingdeath, Frost Tyrant", "set": "afr", "number": 20})
        entry.get_uuids(uuid_map)
        assert entry.uuid == AFR20_CARD[1]


class TestUnresolvedCardsAreDropped:
    def test_unresolved_entry_is_left_out_of_contents(self, caplog):
        """SealedProductCard.uuid is required. One entry nobody can resolve
        used to fail validation for every product in the set."""
        uuid_map = _build_uuid_map()
        p = product(
            {
                "card": [
                    {"name": "Icingdeath, Frost Tyrant", "set": "afr", "number": 20},
                    {"name": "Nothing Like This", "set": "afr", "number": 9999},
                ]
            },
            "afr",
            "Test Product",
        )
        p.get_uuids(uuid_map)
        with caplog.at_level(logging.WARNING):
            data = p.toJson()
        assert [c["name"] for c in data["card"]] == ["Icingdeath, Frost Tyrant"]
        assert all(c.get("uuid") for c in data["card"])
        assert any("unresolved card entries" in r.message for r in caplog.records)

    def test_card_key_is_omitted_when_nothing_resolves(self):
        uuid_map = _build_uuid_map()
        p = product({"card": [{"name": "Nothing Like This", "set": "afr", "number": 9999}]}, "afr", "Test Product")
        p.get_uuids(uuid_map)
        assert "card" not in p.toJson()
