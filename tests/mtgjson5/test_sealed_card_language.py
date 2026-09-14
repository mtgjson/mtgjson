"""Tests for language-aware card: entry resolution in sealed content.

Scryfall's all_cards data carries one row per (set, collector_number,
language) for any card printed in more than one language -- e.g. WAR:5
"Battlefield Promotion" has a distinct Scryfall object (and Scryfall ID) for
its English, French, German, Italian, Japanese, Korean, Portuguese, and
Russian printings, all sharing the same set and collector number. Before this
fix, build_uuid_map_from_pipeline() collapsed every (set, number) to a single
row (English when available), so a sealed-content card: entry had no way to
request anything but the default-language printing: it always got the
English UUID even when the product it belonged to was explicitly the German,
Japanese, or other-language release.

Crucially, a non-default-language printing's MTGJSON UUID is *not* a fresh
hash of its own Scryfall ID: AllPrintings.json folds it into foreignData[] on
the default-language card, and that entry's UUID is
uuid5(defaultScryfallId + "a" + "_" + language, NAMESPACE_DNS) -- derived
from the *default* printing's Scryfall ID, not the foreign printing's own.
The expected UUIDs below are copied verbatim from a real, published
AllPrintings.json's WAR:5 foreignData[] and Grave Pact's foreignData[] (an
SLD reprint of a Kamigawa card), so this test fails if the formula ever
drifts from what actually ships.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.pipeline.stages.sealed import build_uuid_map_from_pipeline, card

# WAR:5 "Battlefield Promotion" -- Scryfall IDs and the real, published
# MTGJSON UUIDs from AllPrintings.json's foreignData[] for each language.
WAR5_SCRYFALL_IDS = {
    "en": "89f8e970-b90c-4829-8970-0a3364027bbb",
    "fr": "cf20ecf0-f10b-41d1-8907-85b942fcbb4c",
    "de": "bd45c9e5-3ed2-487b-9370-ea52959454bb",
    "it": "4f985d4c-d761-4f4a-9285-20f0c92e1e3b",
    "ja": "36d55473-c87d-49b8-a414-d935a63f1835",
    "ko": "ac1da003-d45a-43be-b3b3-b0ee6d5a8960",
    "pt": "6c239c1b-d35e-4130-a664-2244f57cf955",
    "ru": "69dc24e9-bb3a-4ca8-91f2-460b65819685",
}
# The English entry's UUID isn't asserted against a fixed value: in a real
# build it comes from the persisted UUID cache (build_uuid_map_from_pipeline
# coalesces cachedUuid before falling back to a fresh hash), which this test
# doesn't populate, so what it gets here is whatever the no-cache fallback
# produces -- correct for this test's own internal consistency checks, but
# not itself a value to pin.
WAR5_EXPECTED_UUID_BY_LANGUAGE = {
    "French": "3f45f285-9eab-5406-a32c-3d32e8132e5a",
    "German": "84af5c42-c5c7-561c-9e4b-1108861107e6",
    "Italian": "de64d785-94a4-5fb1-a339-292e1f819ea2",
    "Japanese": "91eb12a0-b2be-5803-831c-dfb50d6a5420",
    "Korean": "88a05fb3-f063-5230-b0e1-b8896bc89248",
    "Portuguese (Brazil)": "0d5d041e-6f58-51f4-87f0-1b16c2d98aa1",
    "Russian": "c99f8d5d-466d-531f-9c52-d56382dd43c0",
}

# SLD:560 "Swamp" is Phyrexian-only -- no English row at that number at all.
SLD560_PH_SCRYFALL_ID = "084efea7-2a22-4cca-a1f9-b47aad2ebcac"


def _cards_lf(rows: list[dict]) -> pl.LazyFrame:
    """Minimal Scryfall-shaped frame: id, set, collector_number, name, layout, lang."""
    return pl.LazyFrame(
        rows,
        schema={
            "id": pl.String,
            "set": pl.String,
            "collector_number": pl.String,
            "name": pl.String,
            "layout": pl.String,
            "lang": pl.String,
        },
    )


def _build_uuid_map() -> dict:
    rows = [
        {
            "id": scryfall_id,
            "set": "war",
            "collector_number": "5",
            "name": "Battlefield Promotion",
            "layout": "normal",
            "lang": lang,
        }
        for lang, scryfall_id in WAR5_SCRYFALL_IDS.items()
    ]
    rows.append(
        {
            "id": SLD560_PH_SCRYFALL_ID,
            "set": "sld",
            "collector_number": "560",
            "name": "Swamp",
            "layout": "normal",
            "lang": "ph",
        }
    )
    rows.append(
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "set": "abc",
            "collector_number": "1",
            "name": "Test Card",
            "layout": "normal",
            "lang": "en",
        }
    )
    return build_uuid_map_from_pipeline(
        cards_lf=_cards_lf(rows),
        uuid_cache_lf=None,
        boosters_raw={},
        decks_raw=[],
        products_dict={},
    )


class TestBuildUuidMapFromPipelineLanguages:
    def test_by_language_index_reproduces_the_real_published_uuids(self):
        """The decisive check: these UUIDs are copied from a real, published
        AllPrintings.json, not derived from this test's own fixture data --
        this fails if the hash formula (or its inputs) ever drifts from what
        the rest of the pipeline actually publishes in foreignData[].uuid."""
        uuid_map = _build_uuid_map()
        by_lang = uuid_map["war"]["cards_by_language"]["5"]
        for language, expected_uuid in WAR5_EXPECTED_UUID_BY_LANGUAGE.items():
            assert by_lang[language][0] == expected_uuid, language

    def test_default_pick_matches_the_by_language_english_entry(self):
        uuid_map = _build_uuid_map()
        assert uuid_map["war"]["cards"]["5"] == uuid_map["war"]["cards_by_language"]["5"]["English"]

    def test_by_language_index_is_empty_for_a_single_language_number(self):
        """The common case (no ambiguity) shouldn't carry a redundant index entry."""
        uuid_map = _build_uuid_map()
        assert uuid_map["sld"]["cards_by_language"] == {}
        assert uuid_map["abc"]["cards_by_language"] == {}

    def test_single_language_number_still_resolves_via_the_default_map(self):
        uuid_map = _build_uuid_map()
        assert "560" in uuid_map["sld"]["cards"]


class TestCardGetUuidsLanguage:
    def test_no_language_resolves_to_the_default_english_pick(self):
        """Regression guard: entries without a language field must be unaffected."""
        uuid_map = _build_uuid_map()
        expected = uuid_map["war"]["cards"]["5"][0]
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5"})
        c.get_uuids(uuid_map)
        assert c.uuid == expected

    def test_explicit_language_resolves_to_the_real_published_uuid(self):
        uuid_map = _build_uuid_map()
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5", "language": "German"})
        c.get_uuids(uuid_map)
        assert c.uuid == WAR5_EXPECTED_UUID_BY_LANGUAGE["German"]
        assert c.uuid != uuid_map["war"]["cards"]["5"][0]

    def test_explicit_language_does_not_leak_into_a_different_language(self):
        uuid_map = _build_uuid_map()
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5", "language": "Japanese"})
        c.get_uuids(uuid_map)
        assert c.uuid == WAR5_EXPECTED_UUID_BY_LANGUAGE["Japanese"]

    def test_unresolvable_language_falls_back_to_the_default_pick(self):
        """A language that doesn't exist for that number must still resolve to
        something (with a warning), not silently produce None."""
        uuid_map = _build_uuid_map()
        expected_en = uuid_map["war"]["cards"]["5"][0]
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5", "language": "Klingon"})
        c.get_uuids(uuid_map)
        assert c.uuid == expected_en

    def test_single_language_number_unaffected_by_language_awareness(self):
        uuid_map = _build_uuid_map()
        expected = uuid_map["sld"]["cards"]["560"][0]
        c = card({"name": "Swamp", "set": "sld", "number": "560"})
        c.get_uuids(uuid_map)
        assert c.uuid == expected

    def test_to_json_emits_the_language_field_when_set(self):
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5", "language": "German"})
        assert c.toJson()["language"] == "German"

    def test_to_json_omits_language_when_unset(self):
        c = card({"name": "Battlefield Promotion", "set": "war", "number": "5"})
        assert "language" not in c.toJson()
