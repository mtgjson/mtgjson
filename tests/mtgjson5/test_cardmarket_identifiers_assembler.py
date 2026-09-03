"""Tests for the CardmarketIdentifiers.json assembler."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5 import constants
from mtgjson5.build.assemble import CardmarketIdentifiersAssembler, CompiledListAssembler
from mtgjson5.consts.outputs import COMPILED_OUTPUT_NAMES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(set_meta: dict | None = None):
    meta = set_meta or {}

    class FakeCtx:
        parquet_dir = None
        tokens_dir = None
        set_meta: dict = meta
        decks_df = None
        sealed_df = None
        booster_configs: dict = {}
        token_products: dict = {}
        keyword_data: dict = {}
        card_type_data: dict = {}
        super_types: list = []
        planar_types: list = []

    return FakeCtx()


def _write_catalog(cache_dir, rows: list[dict]) -> None:
    pl.DataFrame(
        rows,
        schema={
            "mcmId": pl.Int64,
            "mcmMetaId": pl.Int64,
            "name": pl.String,
            "number": pl.String,
            "expansionId": pl.Int64,
            "expansionName": pl.String,
        },
    ).write_parquet(cache_dir / "mkm_cards.parquet")


def _write_uuid_map(cache_dir, rows: list[dict]) -> None:
    pl.DataFrame(
        rows,
        schema={"uuid": pl.String, "mcmId": pl.String},
    ).write_parquet(cache_dir / "cardmarket_to_uuid.parquet")


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    from mtgjson5.data import GLOBAL_CACHE

    monkeypatch.setattr(GLOBAL_CACHE, "cardmarket_to_uuid_lf", None)
    return tmp_path


# ---------------------------------------------------------------------------
# TestCardmarketIdentifiersAssembler
# ---------------------------------------------------------------------------


class TestCardmarketIdentifiersAssembler:
    def test_builds_expansion_and_product_maps(self, cache_dir):
        _write_catalog(
            cache_dir,
            [
                {
                    "mcmId": 287295,
                    "mcmMetaId": 221432,
                    "name": "Fall of the Titans",
                    "number": "167",
                    "expansionId": 1676,
                    "expansionName": "Oath of the Gatewatch",
                },
            ],
        )
        _write_uuid_map(cache_dir, [{"uuid": "uuid-a", "mcmId": "287295"}])
        set_meta = {"OGW": {"code": "OGW", "mcmId": 1676, "mcmName": "Oath of the Gatewatch"}}

        result = CardmarketIdentifiersAssembler(_make_ctx(set_meta)).build()

        assert result["expansions"] == {
            "1676": {"name": "Oath of the Gatewatch", "setCodes": ["OGW"]},
        }
        assert result["products"] == {
            "287295": {
                "name": "Fall of the Titans",
                "number": "167",
                "expansionId": 1676,
                "uuids": ["uuid-a"],
            },
        }

    def test_extras_expansion_maps_via_mcm_id_extras(self, cache_dir):
        _write_catalog(
            cache_dir,
            [
                {
                    "mcmId": 400001,
                    "mcmMetaId": 300001,
                    "name": "Garruk, Cursed Huntsman (V.2)",
                    "number": "270",
                    "expansionId": 3625,
                    "expansionName": "Throne of Eldraine: Extras",
                },
            ],
        )
        set_meta = {"ELD": {"code": "ELD", "mcmId": 3624, "mcmIdExtras": 3625}}

        result = CardmarketIdentifiersAssembler(_make_ctx(set_meta)).build()

        assert result["expansions"]["3625"] == {
            "name": "Throne of Eldraine: Extras",
            "setCodes": ["ELD"],
        }

    def test_product_without_uuid_or_number_omits_fields(self, cache_dir):
        _write_catalog(
            cache_dir,
            [
                {
                    "mcmId": 999,
                    "mcmMetaId": None,
                    "name": "Mystery Product",
                    "number": "",
                    "expansionId": 42,
                    "expansionName": "Unknown Set",
                },
            ],
        )

        result = CardmarketIdentifiersAssembler(_make_ctx()).build()

        assert result["products"]["999"] == {
            "name": "Mystery Product",
            "expansionId": 42,
        }
        assert result["expansions"]["42"] == {"name": "Unknown Set", "setCodes": []}

    def test_multiple_uuids_are_sorted(self, cache_dir):
        _write_catalog(
            cache_dir,
            [
                {
                    "mcmId": 100,
                    "mcmMetaId": 1,
                    "name": "Doubled Card",
                    "number": "1",
                    "expansionId": 7,
                    "expansionName": "Some Set",
                },
            ],
        )
        _write_uuid_map(
            cache_dir,
            [
                {"uuid": "uuid-b", "mcmId": "100"},
                {"uuid": "uuid-a", "mcmId": "100"},
            ],
        )

        result = CardmarketIdentifiersAssembler(_make_ctx()).build()

        assert result["products"]["100"]["uuids"] == ["uuid-a", "uuid-b"]

    def test_missing_catalog_returns_empty_maps(self, cache_dir):
        result = CardmarketIdentifiersAssembler(_make_ctx()).build()

        assert result == {"expansions": {}, "products": {}}

    def test_output_keys_sorted_numerically(self, cache_dir):
        _write_catalog(
            cache_dir,
            [
                {
                    "mcmId": 1000,
                    "mcmMetaId": 1,
                    "name": "B",
                    "number": "2",
                    "expansionId": 20,
                    "expansionName": "Set B",
                },
                {
                    "mcmId": 99,
                    "mcmMetaId": 2,
                    "name": "A",
                    "number": "1",
                    "expansionId": 3,
                    "expansionName": "Set A",
                },
            ],
        )

        result = CardmarketIdentifiersAssembler(_make_ctx()).build()

        assert list(result["products"]) == ["99", "1000"]
        assert list(result["expansions"]) == ["3", "20"]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestCardmarketIdentifiersRegistration:
    def test_in_compiled_output_names(self):
        assert "CardmarketIdentifiers" in COMPILED_OUTPUT_NAMES

    def test_in_compiled_list(self):
        assert "CardmarketIdentifiers" in CompiledListAssembler.COMPILED_FILES

    def test_file_model_registered(self):
        from mtgjson5.models.files import CardmarketIdentifiersFile, Files

        assert Files.CardmarketIdentifiersFile is CardmarketIdentifiersFile
