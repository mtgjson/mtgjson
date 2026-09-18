"""Tests for sourceProducts on foreignData[] entries.

add_source_products previously joined card_to_products only against the
outer card's own uuid, so foreignData[] entries -- each a distinct printing
with its own uuid (see mtgjson5.data.context._build_foreign_data_df) -- never
got a sourceProducts of their own. mtgmatcher needs this to know which
sealed products a specific foreign-language printing is found in, which is a
different question from which products the card's own (usually English)
uuid is found in.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.pipeline.stages.derived import _add_foreign_data_source_products

_SOURCE_PRODUCTS_STRUCT = pl.Struct(
    [
        pl.Field("etched", pl.List(pl.String)),
        pl.Field("foil", pl.List(pl.String)),
        pl.Field("nonfoil", pl.List(pl.String)),
    ]
)

_FOREIGN_DATA_STRUCT = pl.Struct(
    {
        "faceName": pl.String,
        "flavorText": pl.String,
        "identifiers": pl.Struct({"multiverseId": pl.String, "scryfallId": pl.String}),
        "language": pl.String,
        "multiverseId": pl.Int64,
        "name": pl.String,
        "text": pl.String,
        "type": pl.String,
        "uuid": pl.String,
    }
)


def _fd(uuid: str, language: str, name: str) -> dict:
    return {
        "faceName": None,
        "flavorText": None,
        "identifiers": None,
        "language": language,
        "multiverseId": None,
        "name": name,
        "text": None,
        "type": None,
        "uuid": uuid,
    }


def _cards_lf(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={"uuid": pl.String, "foreignData": pl.List(_FOREIGN_DATA_STRUCT)},
    ).lazy()


def _card_to_products(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={
            "uuid": pl.String,
            "foil": pl.List(pl.String),
            "nonfoil": pl.List(pl.String),
            "etched": pl.List(pl.String),
        },
    ).lazy()


def _run(lf: pl.LazyFrame, card_to_products_df) -> list[dict]:
    return _add_foreign_data_source_products(lf, card_to_products_df, _SOURCE_PRODUCTS_STRUCT).collect().to_dicts()


class TestAddForeignDataSourceProducts:
    def test_foreign_entry_gets_its_own_source_products_distinct_from_the_parent(self):
        cards = _cards_lf(
            [
                {
                    "uuid": "card-en",
                    "foreignData": [_fd("fd-de", "German", "Aasgeier"), _fd("fd-ja", "Japanese", "カリオンフィーダー")],
                }
            ]
        )
        card_to_products = _card_to_products(
            [
                {"uuid": "card-en", "foil": ["prod-A"], "nonfoil": [], "etched": []},
                {"uuid": "fd-de", "foil": [], "nonfoil": ["prod-B"], "etched": []},
                {"uuid": "fd-ja", "foil": [], "nonfoil": [], "etched": ["prod-C"]},
            ]
        )

        rows = _run(cards, card_to_products)

        foreign_data = {fd["uuid"]: fd for fd in rows[0]["foreignData"]}
        assert foreign_data["fd-de"]["sourceProducts"] == {"foil": [], "nonfoil": ["prod-B"], "etched": []}
        assert foreign_data["fd-ja"]["sourceProducts"] == {"foil": [], "nonfoil": [], "etched": ["prod-C"]}

    def test_foreign_fields_survive_untouched(self):
        cards = _cards_lf([{"uuid": "card-en", "foreignData": [_fd("fd-de", "German", "Aasgeier")]}])
        card_to_products = _card_to_products([{"uuid": "fd-de", "foil": [], "nonfoil": ["prod-B"], "etched": []}])

        rows = _run(cards, card_to_products)

        fd = rows[0]["foreignData"][0]
        assert fd["language"] == "German"
        assert fd["name"] == "Aasgeier"
        assert fd["uuid"] == "fd-de"

    def test_foreign_entry_with_no_matching_product_gets_null_lists(self):
        """Same left-join shape as the parent card's own sourceProducts: an
        unmatched uuid gets null fields, not fabricated empty lists."""
        cards = _cards_lf([{"uuid": "card-en", "foreignData": [_fd("fd-de", "German", "Aasgeier")]}])
        card_to_products = _card_to_products([{"uuid": "card-en", "foil": ["prod-A"], "nonfoil": [], "etched": []}])

        rows = _run(cards, card_to_products)

        assert rows[0]["foreignData"][0]["sourceProducts"] == {"foil": None, "nonfoil": None, "etched": None}

    def test_empty_foreign_data_list_stays_empty(self):
        cards = _cards_lf([{"uuid": "card-en", "foreignData": []}])
        card_to_products = _card_to_products([{"uuid": "card-en", "foil": ["prod-A"], "nonfoil": [], "etched": []}])

        rows = _run(cards, card_to_products)

        assert rows[0]["foreignData"] == []

    def test_none_card_to_products_df_still_attaches_a_null_source_products(self):
        cards = _cards_lf([{"uuid": "card-en", "foreignData": [_fd("fd-de", "German", "Aasgeier")]}])

        rows = _run(cards, None)

        assert rows[0]["foreignData"][0]["sourceProducts"] is None

    def test_none_card_to_products_df_with_empty_foreign_data_stays_empty(self):
        cards = _cards_lf([{"uuid": "card-en", "foreignData": []}])

        rows = _run(cards, None)

        assert rows[0]["foreignData"] == []

    def test_row_order_and_identity_are_preserved_across_multiple_cards(self):
        cards = _cards_lf(
            [
                {"uuid": "card-1", "foreignData": [_fd("fd-1", "German", "Eins")]},
                {"uuid": "card-2", "foreignData": []},
                {
                    "uuid": "card-3",
                    "foreignData": [_fd("fd-3a", "French", "Trois-A"), _fd("fd-3b", "Italian", "Tre-B")],
                },
            ]
        )
        card_to_products = _card_to_products(
            [
                {"uuid": "fd-1", "foil": ["p1"], "nonfoil": [], "etched": []},
                {"uuid": "fd-3a", "foil": [], "nonfoil": ["p3a"], "etched": []},
                {"uuid": "fd-3b", "foil": [], "nonfoil": [], "etched": ["p3b"]},
            ]
        )

        rows = _run(cards, card_to_products)

        assert [r["uuid"] for r in rows] == ["card-1", "card-2", "card-3"]
        assert rows[1]["foreignData"] == []
        by_uuid = {fd["uuid"]: fd for fd in rows[2]["foreignData"]}
        assert by_uuid["fd-3a"]["sourceProducts"]["nonfoil"] == ["p3a"]
        assert by_uuid["fd-3b"]["sourceProducts"]["etched"] == ["p3b"]

    def test_accepts_an_eager_dataframe_for_card_to_products(self):
        cards = _cards_lf([{"uuid": "card-en", "foreignData": [_fd("fd-de", "German", "Aasgeier")]}])
        card_to_products = _card_to_products(
            [{"uuid": "fd-de", "foil": [], "nonfoil": ["prod-B"], "etched": []}]
        ).collect()

        rows = _run(cards, card_to_products)

        assert rows[0]["foreignData"][0]["sourceProducts"] == {"foil": [], "nonfoil": ["prod-B"], "etched": []}
