"""Default pick for a number printed in several languages, none of them English.

4BB (Fourth Edition Black Bordered) exists in Spanish, Portuguese, Japanese,
Korean and Traditional Chinese, and FBB (Foreign Black Border) in French,
German and Italian. MTGJSON publishes the printing Scryfall's default_cards
carries -- Spanish for every 4BB number, French for every FBB one -- and
folds the others into its foreignData[]. The UUIDs below are copied from the
published 4BB.json and FBB.json (MTGJSON 5.3.0+20261023), so the uuid map has
to land on exactly the cards AllPrintings carries.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.pipeline.stages.sealed import build_card_finishes_lookup, build_uuid_map_from_pipeline, card

# Scryfall IDs per language, the published printing last so that it is never
# the first non-English row an English-first rule would happen to keep.
ALABASTER_POTION_4BB_1 = {
    "zht": "2e4b4e52-a061-43f4-99dd-98d3a0e91899",
    "ko": "411c951a-4568-4af6-84f9-30d1b67b4c07",
    "ja": "82eff756-e0b7-4d86-a156-f57be50e2a17",
    "pt": "c9980836-50ba-4c80-9b38-5e0793a001e9",
    "es": "d79196ec-038e-4a59-bd27-77a132c6596c",
}
WALL_OF_WATER_FBB_91 = {
    "de": "40d8d1d1-16f8-4d45-a77e-7a83fef7c6e6",
    "it": "518d3448-6012-4961-8015-462e0bb52fb0",
    "fr": "66b2284d-b508-45e5-abab-0371ad812d00",
}

# 4BB.json / FBB.json: the card's uuid, and its foreignData[].uuid per language.
PUBLISHED_4BB_1 = "3b9c696b-8457-595b-a991-b0545c49b293"
PUBLISHED_FBB_91 = "91be728b-6a0d-5518-9907-7e23700c2dae"
FOREIGN_DATA_4BB_1 = {
    "Chinese Traditional": "2f37bb86-11ec-5e7b-8c93-8e415c9d74c8",
    "Japanese": "bd17f969-71cc-5c09-a486-307cf7a5bd04",
    "Korean": "eebcaebd-b88c-5bd8-b15d-ab3ca3e84eea",
    "Portuguese (Brazil)": "be9cffd5-145d-532e-a47a-5352a3253021",
}
FOREIGN_DATA_FBB_91 = {
    "German": "9b15ee1b-6815-5c81-81a7-38625636c6a9",
    "Italian": "c8573ac3-50dc-5922-8b70-52a58295023d",
}

# legacy_mtgjson_v5_uuid_mapping.json entries for the two published printings.
UUID_CACHE = {
    ALABASTER_POTION_4BB_1["es"]: PUBLISHED_4BB_1,
    WALL_OF_WATER_FBB_91["fr"]: PUBLISHED_FBB_91,
}


def _rows() -> list[dict]:
    rows = []
    for set_code, number, name, ids in (
        ("4bb", "1", "Alabaster Potion", ALABASTER_POTION_4BB_1),
        ("fbb", "91", "Wall of Water", WALL_OF_WATER_FBB_91),
    ):
        for lang, scryfall_id in ids.items():
            rows.append(
                {
                    "id": scryfall_id,
                    "set": set_code,
                    "collector_number": number,
                    "name": name,
                    "layout": "normal",
                    "lang": lang,
                    "finishes": ["nonfoil"],
                }
            )
    return rows


def _cards_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        _rows(),
        schema={
            "id": pl.String,
            "set": pl.String,
            "collector_number": pl.String,
            "name": pl.String,
            "layout": pl.String,
            "lang": pl.String,
            "finishes": pl.List(pl.String),
        },
    )


def _uuid_cache_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        [{"scryfallId": sid, "side": "a", "cachedUuid": uuid} for sid, uuid in UUID_CACHE.items()],
        schema={"scryfallId": pl.String, "side": pl.String, "cachedUuid": pl.String},
    )


def _languages_lf() -> pl.LazyFrame:
    """default_cards as GlobalCache._load_languages() reads it: one row per published printing."""
    return pl.LazyFrame(
        [
            {"scryfallId": ALABASTER_POTION_4BB_1["es"], "language": "Spanish"},
            {"scryfallId": WALL_OF_WATER_FBB_91["fr"], "language": "French"},
        ]
    )


def _build_uuid_map(languages_lf: pl.LazyFrame | None) -> dict:
    return build_uuid_map_from_pipeline(
        cards_lf=_cards_lf(),
        uuid_cache_lf=_uuid_cache_lf(),
        boosters_raw={},
        decks_raw=[],
        products_dict={},
        languages_lf=languages_lf,
    )


class TestDefaultPickFollowsDefaultCards:
    def test_default_pick_is_the_published_card(self):
        uuid_map = _build_uuid_map(_languages_lf())
        assert uuid_map["4bb"]["cards"]["1"] == (PUBLISHED_4BB_1, "Alabaster Potion")
        assert uuid_map["fbb"]["cards"]["91"] == (PUBLISHED_FBB_91, "Wall of Water")

    def test_other_languages_resolve_to_the_published_foreign_data(self):
        """foreignData UUIDs hash the published printing's Scryfall ID, so
        they only come out right once the default pick is that printing."""
        uuid_map = _build_uuid_map(_languages_lf())
        by_language = uuid_map["4bb"]["cards_by_language"]["1"]
        assert {lang: by_language[lang][0] for lang in FOREIGN_DATA_4BB_1} == FOREIGN_DATA_4BB_1
        assert by_language["Spanish"][0] == PUBLISHED_4BB_1
        by_language = uuid_map["fbb"]["cards_by_language"]["91"]
        assert {lang: by_language[lang][0] for lang in FOREIGN_DATA_FBB_91} == FOREIGN_DATA_FBB_91
        assert by_language["French"][0] == PUBLISHED_FBB_91

    def test_card_entry_resolves_to_the_published_card(self):
        uuid_map = _build_uuid_map(_languages_lf())
        untagged = card({"name": "Alabaster Potion", "set": "4bb", "number": "1"})
        untagged.get_uuids(uuid_map)
        assert untagged.uuid == PUBLISHED_4BB_1
        japanese = card({"name": "Alabaster Potion", "set": "4bb", "number": "1", "language": "Japanese"})
        japanese.get_uuids(uuid_map)
        assert japanese.uuid == FOREIGN_DATA_4BB_1["Japanese"]

    def test_english_ranks_first_where_default_cards_has_no_row(self):
        """Cards default_cards does not list (preview sets fetched on their
        own) and builds without languages_lf keep the English-first pick."""
        rows = [
            {"id": "11111111-1111-1111-1111-111111111111", "lang": "de"},
            {"id": "22222222-2222-2222-2222-222222222222", "lang": "en"},
        ]
        cards_lf = pl.LazyFrame(
            [{"set": "abc", "collector_number": "1", "name": "Test Card", "layout": "normal", **row} for row in rows]
        )
        for languages_lf in (None, _languages_lf()):
            uuid_map = build_uuid_map_from_pipeline(cards_lf, None, {}, [], {}, languages_lf=languages_lf)
            assert uuid_map["abc"]["cards"]["1"] == uuid_map["abc"]["cards_by_language"]["1"]["English"]

    def test_finishes_lookup_is_keyed_by_the_published_card(self):
        """card_to_products looks finishes up by the UUID boosters and decks
        carry, which is the published card's."""
        finishes = build_card_finishes_lookup(_cards_lf(), _uuid_cache_lf(), _languages_lf())
        assert {PUBLISHED_4BB_1, PUBLISHED_FBB_91} <= finishes.keys()
        assert len(finishes) == 2
