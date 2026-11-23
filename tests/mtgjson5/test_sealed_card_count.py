"""cardCount is derived from what a product contains, not read from its YAML.

The uuid map carries each booster's and deck's size from the booster export
and decks_v2.json; compile_contents counts a product's cards, packs and decks
from it, and leaves its nested sealed products to count their own.
"""

from __future__ import annotations

import polars as pl
import yaml

from mtgjson5.pipeline.stages.sealed import build_uuid_map_from_pipeline, compile_contents

BOOSTERS = {
    "TST": {
        "draft": {
            "boosters": [
                {"weight": 3, "contents": {"common": 10, "uncommon": 3, "rare": 1}},
                {"weight": 1, "contents": {"common": 9, "uncommon": 3, "rare": 1, "foil": 1}},
            ],
        }
    }
}

DECKS = [
    {
        "set_code": "tst",
        "name": "Land Pack",
        "cards": [{"count": 20}, {"count": 10}],
        "commander": [],
        "displayCommander": [{"count": 1}],
        "tokens": [{"count": 2}],
    }
]


def _uuid_map() -> dict:
    cards = pl.LazyFrame(
        [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "set": "tst",
                "collector_number": "1",
                "name": "Promo",
                "layout": "normal",
                "lang": "en",
            }
        ],
        schema={
            "id": pl.String,
            "set": pl.String,
            "collector_number": pl.String,
            "name": pl.String,
            "layout": pl.String,
            "lang": pl.String,
        },
    )
    return build_uuid_map_from_pipeline(
        cards_lf=cards, uuid_cache_lf=None, boosters_raw=BOOSTERS, decks_raw=DECKS, products_dict={}
    )


def test_uuid_map_carries_booster_and_deck_sizes():
    uuid_map = _uuid_map()

    assert uuid_map["tst"]["boosterCardCount"] == {"draft": 14}
    assert uuid_map["tst"]["deckCardCount"] == {"Land Pack": 30}


def test_card_count_is_derived_from_the_contents(tmp_path):
    products = {
        # The written 1 is not read: a promo, a 14-card pack and a 30-card deck.
        "Test Bundle": {
            "contents": {
                "card_count": 1,
                "card": [{"set": "tst", "number": 1, "name": "Promo", "foil": True}],
                "pack": [{"set": "tst", "code": "draft"}],
                "deck": [{"set": "tst", "name": "Land Pack"}],
                "sealed": [{"set": "tst", "count": 8, "name": "Test Booster Pack"}],
            }
        },
        "Test Booster Pack": {"contents": {"pack": [{"set": "tst", "code": "draft"}]}},
        "Test Booster Box": {"contents": {"sealed": [{"set": "tst", "count": 36, "name": "Test Booster Pack"}]}},
        "Test Unknown Deck": {"contents": {"deck": [{"set": "tst", "name": "Missing"}]}},
    }
    (tmp_path / "TST.yaml").write_text(yaml.safe_dump({"code": "tst", "products": products}))

    contents, _ = compile_contents(tmp_path, None, _uuid_map())

    published = {name: product.get("card_count") for name, product in contents["tst"].items()}
    assert published == {
        "Test Bundle": 45,
        "Test Booster Pack": 14,
        "Test Booster Box": None,
        "Test Unknown Deck": None,
    }
