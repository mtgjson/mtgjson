"""Tests for token product uuids and the normalized token product tables (#1760)."""

from __future__ import annotations

import copy
import uuid as uuid_lib
from typing import Any

from mtgjson5.build.assemble import TableAssembler
from mtgjson5.build.token_products import (
    add_token_product_uuids,
    build_token_product_tables,
    token_product_hash_input,
    token_product_uuids,
)

# BLC Eldrazi // Clue double-faced token
ELDRAZI = "3c0ee4a0-0554-5751-9425-fdcd54ec07fb"
CLUE = "48394044-7565-5d50-a917-1fc2e26b8592"
ELDRAZI_CLUE_UUID = "313a62ae-628e-5e7e-88c1-4ab9b37f6f34"


def _product(parts: list[dict[str, Any]], pid: str = "1", url: str | None = "https://tcg.example/1") -> dict[str, Any]:
    product: dict[str, Any] = {"identifiers": {"tcgplayerProductId": pid}, "tokenParts": parts}
    if url:
        product["purchaseUrls"] = {"tcgplayer": url}
    return product


def _uuid(product: dict[str, Any]) -> str:
    return token_product_uuids([token_product_hash_input(product)])[0]


class TestTokenProductUuid:
    def test_reference_value(self):
        product = _product([{"uuid": ELDRAZI}, {"uuid": CLUE}])
        assert _uuid(product) == ELDRAZI_CLUE_UUID

    def test_matches_python_uuid5_dns(self):
        product = _product([{"uuid": ELDRAZI}, {"uuid": CLUE}])
        expected = str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, token_product_hash_input(product)))
        assert _uuid(product) == expected

    def test_hash_input_format(self):
        product = _product([{"uuid": CLUE, "faceAttribute": ["Gold Stamped"]}, {"uuid": ELDRAZI}])
        assert token_product_hash_input(product) == f'[["{ELDRAZI}",[]],["{CLUE}",["Gold Stamped"]]]'

    def test_part_order_does_not_matter(self):
        forward = _product([{"uuid": ELDRAZI}, {"uuid": CLUE, "faceAttribute": ["Gold Stamped"]}])
        reverse = _product([{"uuid": CLUE, "faceAttribute": ["Gold Stamped"]}, {"uuid": ELDRAZI}])
        assert _uuid(forward) == _uuid(reverse)

    def test_face_attribute_order_does_not_matter(self):
        a = _product([{"uuid": ELDRAZI, "faceAttribute": ["Surge Foil", "Gold Stamped"]}])
        b = _product([{"uuid": ELDRAZI, "faceAttribute": ["Gold Stamped", "Surge Foil"]}])
        assert _uuid(a) == _uuid(b)

    def test_gold_stamped_differs_from_plain(self):
        plain = _product([{"uuid": ELDRAZI}, {"uuid": CLUE}])
        stamped = _product(
            [{"uuid": ELDRAZI, "faceAttribute": ["Gold Stamped"]}, {"uuid": CLUE, "faceAttribute": ["Gold Stamped"]}]
        )
        assert _uuid(plain) != _uuid(stamped)

    def test_missing_and_empty_face_attribute_are_equal(self):
        assert _uuid(_product([{"uuid": ELDRAZI}])) == _uuid(_product([{"uuid": ELDRAZI, "faceAttribute": []}]))

    def test_duplicate_parts_are_deduped(self):
        # Upstream lists a double_faced_token's own uuid twice (e.g. OTJ 22eec444-...)
        single = _product([{"uuid": ELDRAZI}])
        doubled = _product([{"uuid": ELDRAZI}, {"uuid": ELDRAZI}])
        assert token_product_hash_input(doubled) == token_product_hash_input(single)
        assert _uuid(doubled) == _uuid(single)

    def test_vendor_id_does_not_affect_uuid(self):
        a = _product([{"uuid": ELDRAZI}], pid="100", url="https://tcg.example/100")
        b = _product([{"uuid": ELDRAZI}], pid="200", url="https://tcg.example/200")
        assert _uuid(a) == _uuid(b)

    def test_parts_without_uuid_are_kept_and_distinct(self):
        base = _product([{"uuid": ELDRAZI}])
        zombie = _product([{"uuid": ELDRAZI}, {"faceId": "9", "faceName": "Zombie", "tokenType": "Token"}])
        skeleton = _product([{"uuid": ELDRAZI}, {"faceId": "6", "faceName": "Skeleton", "tokenType": "Token"}])
        assert len({_uuid(base), _uuid(zombie), _uuid(skeleton)}) == 3
        assert token_product_hash_input(zombie) == f'[["{ELDRAZI}",[]],["[\\"9\\",\\"Zombie\\"]",[]]]'

    def test_add_uuids_same_product_on_every_token(self):
        token_products = {
            ELDRAZI: [_product([{"uuid": ELDRAZI}, {"uuid": CLUE}])],
            CLUE: [_product([{"uuid": CLUE}, {"uuid": ELDRAZI}])],
        }
        add_token_product_uuids(token_products)
        assert token_products[ELDRAZI][0]["uuid"] == ELDRAZI_CLUE_UUID
        assert token_products[CLUE][0]["uuid"] == ELDRAZI_CLUE_UUID

    def test_add_uuids_is_idempotent(self):
        token_products = {ELDRAZI: [_product([{"uuid": ELDRAZI}, {"uuid": CLUE}])]}
        add_token_product_uuids(token_products)
        again = copy.deepcopy(token_products)
        add_token_product_uuids(again)
        assert again == token_products


def _sample_token_products() -> dict[str, list[dict[str, Any]]]:
    plain_a = _product([{"uuid": ELDRAZI}, {"uuid": CLUE}], pid="300", url="https://tcg.example/300")
    plain_b = _product([{"uuid": CLUE}, {"uuid": ELDRAZI}], pid="100", url="https://tcg.example/100")
    stamped = _product(
        [{"uuid": ELDRAZI, "faceAttribute": ["Gold Stamped"]}, {"uuid": CLUE}],
        pid="200",
        url="https://tcg.example/200",
    )
    other_set = _product(
        [{"uuid": ELDRAZI}, {"faceId": "9", "faceName": "Zombie", "tokenType": "Token"}],
        pid="400",
        url=None,
    )
    token_products = {
        # Same products are listed under both faces.
        ELDRAZI: [plain_a, plain_b, stamped, other_set],
        CLUE: copy.deepcopy([plain_a, plain_b, stamped]),
    }
    add_token_product_uuids(token_products)
    return token_products


class TestTokenProductTables:
    def test_empty(self):
        assert build_token_product_tables(None) == {}
        assert build_token_product_tables({}) == {}

    def test_token_products_table(self):
        tp = _sample_token_products()
        df = build_token_product_tables(tp)["tokenProducts"]
        assert df.columns == ["uuid", "faceAttribute"]
        assert df["uuid"].is_unique().all()
        assert df["uuid"].to_list() == sorted(df["uuid"].to_list())
        stamped_uuid = tp[ELDRAZI][2]["uuid"]
        assert len(df) == 3
        assert set(df["uuid"]) == {ELDRAZI_CLUE_UUID, stamped_uuid, tp[ELDRAZI][3]["uuid"]}
        attrs = dict(zip(df["uuid"], df["faceAttribute"].to_list(), strict=True))
        assert attrs[stamped_uuid] == ["Gold Stamped"]
        assert attrs[ELDRAZI_CLUE_UUID] is None

    def test_token_product_parts_table(self):
        tp = _sample_token_products()
        df = build_token_product_tables(tp)["tokenProductParts"]
        assert df.columns == ["tokenProductUuid", "uuid"]
        rows = df.rows()
        assert rows == sorted(rows)
        assert len(rows) == len(set(rows))
        assert (ELDRAZI_CLUE_UUID, ELDRAZI) in rows
        assert (ELDRAZI_CLUE_UUID, CLUE) in rows
        # The uuid-less Zombie face has no row; the Eldrazi face does.
        zombie_rows = [r for r in rows if r[0] == tp[ELDRAZI][3]["uuid"]]
        assert zombie_rows == [(tp[ELDRAZI][3]["uuid"], ELDRAZI)]
        assert len(rows) == 5

    def test_token_product_identifiers_table(self):
        tp = _sample_token_products()
        df = build_token_product_tables(tp)["tokenProductIdentifiers"]
        assert df.columns == ["tokenProductUuid", "tcgplayerProductId", "tcgplayerPurchaseUrl"]
        rows = df.rows()
        assert rows == sorted(rows)
        # One row per (product, listing) even though each listing appears under two tokens.
        assert [r for r in rows if r[0] == ELDRAZI_CLUE_UUID] == [
            (ELDRAZI_CLUE_UUID, "100", "https://tcg.example/100"),
            (ELDRAZI_CLUE_UUID, "300", "https://tcg.example/300"),
        ]
        assert (tp[ELDRAZI][3]["uuid"], "400", None) in rows
        assert len(rows) == 4

    def test_foreign_keys_resolve(self):
        tables = build_token_product_tables(_sample_token_products())
        product_uuids = set(tables["tokenProducts"]["uuid"])
        assert set(tables["tokenProductParts"]["tokenProductUuid"]) <= product_uuids
        assert set(tables["tokenProductIdentifiers"]["tokenProductUuid"]) <= product_uuids

    def test_build_all_includes_serialized_tables(self):
        import polars as pl

        cards_df = pl.DataFrame({"uuid": ["c1"], "name": ["X"], "setCode": ["TST"]})
        tp = _sample_token_products()
        tables = TableAssembler.build_all(cards_df, token_products=tp)
        assert {"tokenProducts", "tokenProductParts", "tokenProductIdentifiers"} <= tables.keys()
        products = tables["tokenProducts"]
        assert products.schema["faceAttribute"] == pl.String
        attrs = dict(zip(products["uuid"], products["faceAttribute"], strict=True))
        assert attrs[tp[ELDRAZI][2]["uuid"]] == "Gold Stamped"
        assert attrs[ELDRAZI_CLUE_UUID] is None

    def test_build_all_without_token_products(self):
        import polars as pl

        cards_df = pl.DataFrame({"uuid": ["c1"], "name": ["X"], "setCode": ["TST"]})
        tables = TableAssembler.build_all(cards_df)
        assert not {"tokenProducts", "tokenProductParts", "tokenProductIdentifiers"} & tables.keys()
