"""Tests for language-aware deck: content resolution in card_to_products.

A deck object (mainBoard/sideBoard/etc.) is shared verbatim by every sealed
product that references it by name -- its card UUIDs are always the deck's
own default-language (English) printings. Before this fix, a language-tagged
product (e.g. "Secret Lair Drop Special Guest Junji Ito Japanese Etched")
whose contents reference a deck: entry got attributed, in card_to_products,
to the exact same English UUIDs as its English counterpart, because nothing
in the deck-resolution path ever consulted the requesting product's own
language. _ctp_get_cards_in_deck now substitutes each deck card's uuid for
its language's entry in cards_by_language/tokens_by_language (built in
build_uuid_map_from_pipeline(), see test_sealed_card_language.py) when a
*language* is passed in, and leaves the deck's own uuid untouched otherwise
-- critically, for a *different*, non-tagged product that references the
very same deck object.
"""

from __future__ import annotations

import logging

from mtgjson5.pipeline.stages.sealed import _ctp_get_cards_in_deck, _CTPCard

DECK_NAME = "Special Guest: Junji Ito Foil Etched Edition"

EN_UUID_1 = "11111111-1111-1111-1111-111111111111"
EN_UUID_2 = "22222222-2222-2222-2222-222222222222"
JA_UUID_1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
JA_UUID_2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
TOKEN_EN_UUID = "33333333-3333-3333-3333-333333333333"
TOKEN_JA_UUID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


def _data() -> dict:
    return {
        "SLD": {
            "cards": [
                {"uuid": EN_UUID_1, "finishes": ["nonfoil", "etched"]},
                {"uuid": EN_UUID_2, "finishes": ["nonfoil", "etched"]},
            ],
            "tokens": [
                {"uuid": TOKEN_EN_UUID, "finishes": ["nonfoil"]},
            ],
            "cards_by_language": {
                "1114": {"Japanese": (JA_UUID_1, "カリオンフィーダー")},
                "1115": {"Japanese": (JA_UUID_2, "何か")},
            },
            "tokens_by_language": {
                "1116": {"Japanese": (TOKEN_JA_UUID, "兵士")},
            },
            "decks": [
                {
                    "name": DECK_NAME,
                    "sourceSetCodes": ["SLD"],
                    "mainBoard": [
                        {
                            "uuid": EN_UUID_1,
                            "isEtched": True,
                            "_set": "SLD",
                            "_number": "1114",
                        },
                        {
                            "uuid": EN_UUID_2,
                            "isEtched": True,
                            "_set": "SLD",
                            "_number": "1115",
                        },
                    ],
                    "tokens": [
                        {
                            "uuid": TOKEN_EN_UUID,
                            "_set": "SLD",
                            "_number": "1116",
                        },
                    ],
                }
            ],
        }
    }


class TestCtpGetCardsInDeckLanguage:
    def test_no_language_returns_the_decks_own_default_uuids(self):
        """Regression guard: the English product referencing this exact same
        deck object must keep getting the deck's own (English) uuids."""
        cards = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME)
        assert set(cards) == {
            _CTPCard(EN_UUID_1, "etched"),
            _CTPCard(EN_UUID_2, "etched"),
            _CTPCard(TOKEN_EN_UUID, "nonfoil"),
        }

    def test_language_substitutes_the_foreign_uuid_for_each_card(self):
        """The actual bug: a Japanese product sharing this same deck object
        must resolve to the Japanese uuids, not the deck's English ones."""
        cards = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME, "Japanese")
        assert set(cards) == {
            _CTPCard(JA_UUID_1, "etched"),
            _CTPCard(JA_UUID_2, "etched"),
            _CTPCard(TOKEN_JA_UUID, "nonfoil"),
        }

    def test_the_same_deck_object_resolves_differently_per_product_language(self):
        """The exact Junji Ito English vs. Japanese scenario: two products
        pointing at the identical deck: entry must not collide."""
        english = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME)
        japanese = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME, "Japanese")
        assert english != japanese
        assert {c.uuid for c in english}.isdisjoint({c.uuid for c in japanese})

    def test_finish_is_still_determined_from_the_original_english_uuid(self):
        """card_finishes is only indexed by the default-language uuid, so
        finish-matching must happen before the uuid is swapped."""
        cards = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME, "Japanese")
        by_uuid = {c.uuid: c.finish for c in cards}
        assert by_uuid[JA_UUID_1] == "etched"
        assert by_uuid[JA_UUID_2] == "etched"

    def test_missing_foreign_entry_falls_back_to_the_default_uuid_and_warns(self, caplog):
        """Scryfall hasn't transcribed every printing -- when this exact card
        has no entry for the requested language, fall back rather than drop
        the card or crash."""
        data = _data()
        del data["SLD"]["cards_by_language"]["1115"]["Japanese"]
        with caplog.at_level(logging.WARNING, logger="mtgjson5.pipeline.stages.sealed"):
            cards = _ctp_get_cards_in_deck(data, "SLD", DECK_NAME, "Japanese")
        by_uuid = {c.uuid for c in cards}
        assert EN_UUID_2 in by_uuid
        assert JA_UUID_2 not in by_uuid
        assert any("has no Japanese printing" in r.getMessage() for r in caplog.records)

    def test_unknown_language_leaves_every_card_at_its_default_uuid(self):
        cards = _ctp_get_cards_in_deck(_data(), "SLD", DECK_NAME, "Klingon")
        assert {c.uuid for c in cards} == {EN_UUID_1, EN_UUID_2, TOKEN_EN_UUID}

    def test_unknown_deck_name_still_returns_nothing(self):
        assert not _ctp_get_cards_in_deck(_data(), "SLD", "Not A Real Deck", "Japanese")
