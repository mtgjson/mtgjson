"""Tests for TableAssembler — build_all normalization and build_boosters."""

from __future__ import annotations

from typing import Any

import polars as pl

from mtgjson5.build.assemble import TableAssembler
from mtgjson5.models.schemas import (
    CARDS_TABLE_EXCLUDE,
    SETS_TABLE_EXCLUDE,
    TOKENS_TABLE_EXCLUDE,
)

# =============================================================================
# Helpers — synthetic DataFrames
# =============================================================================


def _make_cards_df(rows: list[dict[str, Any]] | None = None) -> pl.DataFrame:
    """Build a minimal cards DataFrame exercising all normalization paths."""
    if rows is None:
        rows = [
            {
                "uuid": "uuid-001",
                "name": "Lightning Bolt",
                "setCode": "M10",
                "number": "1",
                "type": "Instant",
                "colors": ["R"],
                "identifiers": {"scryfallId": "sf-001", "multiverseId": "12345"},
                "legalities": {"vintage": "Legal", "modern": "Legal"},
                "rulings": [
                    {"publishedAt": "2020-01-01", "comment": "Good card.", "source": "wotc"},
                ],
                "foreignData": [
                    {"language": "German", "name": "Blitzschlag", "uuid": "foreign-uuid-should-drop"},
                ],
                "purchaseUrls": {"tcgplayer": "https://tcg.example.com"},
            },
            {
                "uuid": "uuid-002",
                "name": "Giant Growth",
                "setCode": "M10",
                "number": "2",
                "type": "Instant",
                "colors": ["G"],
                "identifiers": {"scryfallId": "sf-002", "multiverseId": "12346"},
                "legalities": {"vintage": "Legal", "modern": "Legal"},
                "rulings": [],
                "foreignData": [],
                "purchaseUrls": {"tcgplayer": "https://tcg2.example.com"},
            },
        ]
    return pl.DataFrame(
        rows,
        schema={
            "uuid": pl.String,
            "name": pl.String,
            "setCode": pl.String,
            "number": pl.String,
            "type": pl.String,
            "colors": pl.List(pl.String),
            "identifiers": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String}),
            "legalities": pl.Struct({"vintage": pl.String, "modern": pl.String}),
            "rulings": pl.List(pl.Struct({"publishedAt": pl.String, "comment": pl.String, "source": pl.String})),
            "foreignData": pl.List(pl.Struct({"language": pl.String, "name": pl.String, "uuid": pl.String})),
            "purchaseUrls": pl.Struct({"tcgplayer": pl.String}),
        },
    )


def _make_tokens_df() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "uuid": "tok-001",
                "name": "Soldier Token",
                "setCode": "M10",
                "number": "T1",
                "type": "Token Creature — Soldier",
                "colors": ["W"],
                "identifiers": {"scryfallId": "sf-tok-001"},
            },
        ],
        schema={
            "uuid": pl.String,
            "name": pl.String,
            "setCode": pl.String,
            "number": pl.String,
            "type": pl.String,
            "colors": pl.List(pl.String),
            "identifiers": pl.Struct({"scryfallId": pl.String}),
        },
    )


def _make_sets_df() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "code": "M10",
                "name": "Magic 2010",
                "type": "core",
                "releaseDate": "2009-07-17",
                "translations": {"French": "Magic 2010", "German": "Hauptset 2010"},
                "sealedProduct": [
                    {
                        "uuid": "sealed-001",
                        "name": "Booster Box",
                        "category": "booster_box",
                        "subtype": None,
                        "releaseDate": "2009-07-17",
                        "cardCount": 36,
                        "productSize": None,
                        "identifiers": {"tcgplayerProductId": "12345"},
                        "purchaseUrls": {"tcgplayer": "https://tcg.example.com"},
                        "contents": '{"card": []}',
                    },
                ],
                "decks": [
                    {
                        "code": "M10",
                        "name": "Intro Pack Red",
                        "type": "Intro Pack",
                        "releaseDate": "2009-07-17",
                        "sealedProductUuids": ["sealed-001"],
                        "sourceSetCodes": None,
                        "mainBoard": [{"uuid": "uuid-001", "count": 1}],
                        "sideBoard": [],
                        "commander": [],
                        "displayCommander": [],
                        "tokens": [],
                        "planes": [],
                        "schemes": [],
                    },
                ],
                "languages": ["English", "French", "German"],
            },
        ],
        schema={
            "code": pl.String,
            "name": pl.String,
            "type": pl.String,
            "releaseDate": pl.String,
            "translations": pl.Struct({"French": pl.String, "German": pl.String}),
            "sealedProduct": pl.List(
                pl.Struct(
                    {
                        "uuid": pl.String,
                        "name": pl.String,
                        "category": pl.String,
                        "subtype": pl.String,
                        "releaseDate": pl.String,
                        "cardCount": pl.Int64,
                        "productSize": pl.Int64,
                        "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
                        "purchaseUrls": pl.Struct({"tcgplayer": pl.String}),
                        "contents": pl.String,
                    }
                )
            ),
            "decks": pl.List(
                pl.Struct(
                    {
                        "code": pl.String,
                        "name": pl.String,
                        "type": pl.String,
                        "releaseDate": pl.String,
                        "sealedProductUuids": pl.List(pl.String),
                        "sourceSetCodes": pl.List(pl.String),
                        "mainBoard": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "sideBoard": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "commander": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "displayCommander": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "tokens": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "planes": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                        "schemes": pl.List(pl.Struct({"uuid": pl.String, "count": pl.Int64})),
                    }
                )
            ),
            "languages": pl.List(pl.String),
        },
    )


def _make_booster_config() -> dict[str, dict[str, Any]]:
    return {
        "TST": {
            "default": {
                "sheets": {
                    "common": {
                        "foil": False,
                        "balanceColors": True,
                        "totalWeight": 100,
                        "cards": {"uuid-001": 10, "uuid-002": 10},
                    },
                    "rare": {
                        "foil": True,
                        "balanceColors": False,
                        "totalWeight": 50,
                        "cards": {"uuid-003": 50},
                    },
                },
                "boosters": [
                    {
                        "weight": 5,
                        "contents": {"common": 10, "rare": 1},
                    },
                    {
                        "weight": 1,
                        "contents": {"rare": 2},
                    },
                ],
            },
        },
    }


# =============================================================================
# TestBuildAllCards
# =============================================================================


class TestBuildAllCards:
    def test_cards_table_excludes_normalized_columns(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cards" in tables
        for excluded in CARDS_TABLE_EXCLUDE:
            assert excluded not in tables["cards"].columns

    def test_uuid_deduplication(self):
        row = {
            "uuid": "uuid-dup",
            "name": "Dup Card",
            "setCode": "TST",
            "number": "1",
            "type": "Creature",
            "colors": ["W"],
            "identifiers": {"scryfallId": "sf-dup", "multiverseId": None},
            "legalities": {"vintage": "Legal", "modern": None},
            "rulings": [],
            "foreignData": [],
            "purchaseUrls": {"tcgplayer": None},
        }
        df = _make_cards_df([row, row])
        tables = TableAssembler.build_all(df)
        assert len(tables["cards"]) == 1

    def test_complex_types_serialized(self):
        tables = TableAssembler.build_all(_make_cards_df())
        # colors should be serialized to a string (comma-separated)
        assert tables["cards"].schema["colors"] == pl.String

    def test_internal_columns_excluded(self):
        df = _make_cards_df()
        df = df.with_columns(pl.lit("internal").alias("_debug"))
        tables = TableAssembler.build_all(df)
        assert "_debug" not in tables["cards"].columns


# =============================================================================
# TestBuildAllCardIdentifiers
# =============================================================================


class TestBuildAllCardIdentifiers:
    def test_card_identifiers_created(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cardIdentifiers" in tables
        assert "uuid" in tables["cardIdentifiers"].columns
        assert "scryfallId" in tables["cardIdentifiers"].columns

    def test_struct_unnested(self):
        tables = TableAssembler.build_all(_make_cards_df())
        ci = tables["cardIdentifiers"]
        # identifiers struct should be unnested into separate columns
        assert "identifiers" not in ci.columns
        assert "multiverseId" in ci.columns

    def test_null_identifiers_filtered(self):
        df = _make_cards_df()
        # Set identifiers to null for one row
        df = df.with_columns(
            pl.when(pl.col("uuid") == "uuid-002")
            .then(pl.lit(None, dtype=df.schema["identifiers"]))
            .otherwise(pl.col("identifiers"))
            .alias("identifiers")
        )
        tables = TableAssembler.build_all(df)
        assert len(tables["cardIdentifiers"]) == 1


# =============================================================================
# TestBuildAllCardLegalities
# =============================================================================


class TestBuildAllCardLegalities:
    def test_card_legalities_created(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cardLegalities" in tables
        assert "uuid" in tables["cardLegalities"].columns

    def test_format_columns(self):
        tables = TableAssembler.build_all(_make_cards_df())
        cl = tables["cardLegalities"]
        assert "vintage" in cl.columns
        assert "modern" in cl.columns


# =============================================================================
# TestBuildAllCardRulings
# =============================================================================


class TestBuildAllCardRulings:
    def test_card_rulings_created(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cardRulings" in tables
        # Only the first card has rulings
        assert len(tables["cardRulings"]) == 1

    def test_column_renaming(self):
        tables = TableAssembler.build_all(_make_cards_df())
        cr = tables["cardRulings"]
        assert "date" in cr.columns
        assert "text" in cr.columns
        assert "publishedAt" not in cr.columns
        assert "comment" not in cr.columns

    def test_source_column_dropped(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "source" not in tables["cardRulings"].columns

    def test_uuid_fk_preserved(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert tables["cardRulings"]["uuid"][0] == "uuid-001"


# =============================================================================
# TestBuildAllCardForeignData
# =============================================================================


class TestBuildAllCardForeignData:
    def test_card_foreign_data_created(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cardForeignData" in tables
        assert len(tables["cardForeignData"]) == 1

    def test_uuid_conflict_resolution(self):
        tables = TableAssembler.build_all(_make_cards_df())
        fd = tables["cardForeignData"]
        # struct's own uuid field should be dropped, card's uuid kept
        assert fd["uuid"][0] == "uuid-001"
        assert "foreign-uuid-should-drop" not in fd["uuid"].to_list()


# =============================================================================
# TestBuildAllCardPurchaseUrls
# =============================================================================


class TestBuildAllCardPurchaseUrls:
    def test_card_purchase_urls_created(self):
        tables = TableAssembler.build_all(_make_cards_df())
        assert "cardPurchaseUrls" in tables
        assert "uuid" in tables["cardPurchaseUrls"].columns

    def test_struct_unnested(self):
        tables = TableAssembler.build_all(_make_cards_df())
        pu = tables["cardPurchaseUrls"]
        assert "tcgplayer" in pu.columns
        assert "purchaseUrls" not in pu.columns


# =============================================================================
# TestBuildAllTokens
# =============================================================================


class TestBuildAllTokens:
    def test_tokens_table_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), tokens_df=_make_tokens_df())
        assert "tokens" in tables
        for excluded in TOKENS_TABLE_EXCLUDE:
            if excluded in _make_tokens_df().columns:
                assert excluded not in tables["tokens"].columns

    def test_token_identifiers_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), tokens_df=_make_tokens_df())
        assert "tokenIdentifiers" in tables
        assert "uuid" in tables["tokenIdentifiers"].columns
        assert "scryfallId" in tables["tokenIdentifiers"].columns


# =============================================================================
# TestBuildAllSets
# =============================================================================


class TestBuildAllSets:
    def test_sets_table_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "sets" in tables
        for excluded in SETS_TABLE_EXCLUDE:
            if excluded in _make_sets_df().columns:
                assert excluded not in tables["sets"].columns

    def test_set_translations_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "setTranslations" in tables
        st = tables["setTranslations"]
        assert "code" in st.columns
        assert "language" in st.columns
        assert "translation" in st.columns


# =============================================================================
# TestBuildAllSealedProducts
# =============================================================================


class TestBuildAllSealedProducts:
    def test_sealed_products_table_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "sealedProducts" in tables

    def test_sealed_products_columns(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sp = tables["sealedProducts"]
        for col in ["setCode", "uuid", "name", "category"]:
            assert col in sp.columns

    def test_sealed_product_excluded_from_sets(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "sealedProduct" not in tables["sets"].columns

    def test_sealed_products_set_code_from_parent(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sp = tables["sealedProducts"]
        assert sp["setCode"][0] == "M10"

    def test_sealed_products_identifiers_serialized(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sp = tables["sealedProducts"]
        assert sp.schema["identifiers"] == pl.String
        import json
        parsed = json.loads(sp["identifiers"][0])
        assert parsed["tcgplayerProductId"] == "12345"


# =============================================================================
# TestBuildAllSetDecks
# =============================================================================


class TestBuildAllSetDecks:
    def test_set_decks_table_created(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "setDecks" in tables

    def test_set_decks_columns(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sd = tables["setDecks"]
        for col in ["setCode", "name", "type"]:
            assert col in sd.columns

    def test_decks_excluded_from_sets(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        assert "decks" not in tables["sets"].columns

    def test_set_decks_boards_serialized_as_json(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sd = tables["setDecks"]
        assert sd.schema["mainBoard"] == pl.String
        import json
        parsed = json.loads(sd["mainBoard"][0])
        assert isinstance(parsed, list)
        assert parsed[0]["uuid"] == "uuid-001"

    def test_set_decks_set_code_from_parent(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sd = tables["setDecks"]
        assert sd["setCode"][0] == "M10"


# =============================================================================
# TestLanguagesOnSetsTable
# =============================================================================


class TestLanguagesOnSetsTable:
    def test_languages_column_on_sets_table(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sets = tables["sets"]
        assert "languages" in sets.columns

    def test_languages_serialized_as_csv(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        sets = tables["sets"]
        assert sets.schema["languages"] == pl.String
        assert "English" in sets["languages"][0]
        assert "French" in sets["languages"][0]


# =============================================================================
# TestBuildBoosters
# =============================================================================


class TestBuildBoosters:
    def test_four_tables_created(self):
        tables = TableAssembler.build_boosters(_make_booster_config())
        assert "setBoosterSheets" in tables
        assert "setBoosterSheetCards" in tables
        assert "setBoosterContents" in tables
        assert "setBoosterContentWeights" in tables

    def test_sheet_fields(self):
        tables = TableAssembler.build_boosters(_make_booster_config())
        sheets = tables["setBoosterSheets"]
        for col in ["setCode", "boosterName", "sheetName", "sheetIsFoil", "sheetHasBalanceColors", "sheetTotalWeight"]:
            assert col in sheets.columns
        assert len(sheets) == 2  # common + rare

    def test_sheet_cards(self):
        tables = TableAssembler.build_boosters(_make_booster_config())
        sc = tables["setBoosterSheetCards"]
        for col in ["setCode", "boosterName", "sheetName", "cardUuid", "cardWeight"]:
            assert col in sc.columns
        assert len(sc) == 3  # uuid-001, uuid-002 in common + uuid-003 in rare

    def test_empty_config_returns_empty_dataframes(self):
        tables = TableAssembler.build_boosters({})
        assert len(tables) == 4
        for df in tables.values():
            assert len(df) == 0


# =============================================================================
# TestRelationalIntegrity
# =============================================================================


class TestRelationalIntegrity:
    def test_normalized_tables_have_uuid_fk(self):
        tables = TableAssembler.build_all(_make_cards_df())
        card_uuids = set(tables["cards"]["uuid"].to_list())
        for tname in ["cardIdentifiers", "cardLegalities", "cardPurchaseUrls"]:
            if tname in tables:
                fk_uuids = set(tables[tname]["uuid"].to_list())
                assert fk_uuids.issubset(card_uuids), f"{tname} uuids not subset of cards.uuid"

    def test_set_translations_code_fk(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        set_codes = set(tables["sets"]["code"].to_list())
        trans_codes = set(tables["setTranslations"]["code"].to_list())
        assert trans_codes.issubset(set_codes)

    def test_sealed_products_set_code_in_sets(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        if "sealedProducts" in tables and "sets" in tables:
            set_codes = set(tables["sets"]["code"].to_list())
            sealed_codes = set(tables["sealedProducts"]["setCode"].to_list())
            assert sealed_codes.issubset(set_codes)

    def test_set_decks_set_code_in_sets(self):
        tables = TableAssembler.build_all(_make_cards_df(), sets_df=_make_sets_df())
        if "setDecks" in tables and "sets" in tables:
            set_codes = set(tables["sets"]["code"].to_list())
            deck_codes = set(tables["setDecks"]["setCode"].to_list())
            assert deck_codes.issubset(set_codes)
