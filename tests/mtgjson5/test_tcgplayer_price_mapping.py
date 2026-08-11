"""Regression tests for TCGplayer alternative-product price mapping."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import polars as pl
import pytest

from mtgjson5 import constants
from mtgjson5.build.prices.price_builder import PolarsPriceBuilder, PriceBuilderContext
from mtgjson5.build.prices.price_writers import stream_write_all_prices_json, stream_write_today_prices_json
from mtgjson5.data import GLOBAL_CACHE
from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.output import _build_id_mappings
from mtgjson5.providers.tcgplayer import prices as tcg_prices_module
from mtgjson5.providers.tcgplayer.prices import TCGPlayerPriceProvider

TODAY = "2026-08-07"
JAWS_UUID = "373dab28-3a38-5b34-abdb-f77d1801a6d2"
FLIPPED_UUID = "face0000-0000-0000-0000-000000000001"
ETCHED_UUID = "etched00-0000-0000-0000-000000000001"
BASE_COLLISION_UUID = "base0000-0000-0000-0000-000000000001"
ETCHED_COLLISION_UUID = "etched00-0000-0000-0000-000000000002"
OVERLAP_UUID = "base0000-0000-0000-0000-000000000002"
SHARED_PRODUCT_ETCHED_UUID = "etched00-0000-0000-0000-000000000003"
CROSS_GROUP_UUID = "cross000-0000-0000-0000-000000000001"
CROSS_GROUP_ETCHED_UUID = "cross000-0000-0000-0000-000000000002"
ALT_TIE_UUID = "alttie00-0000-0000-0000-000000000001"

BASE_MAP = {
    "656542": {JAWS_UUID},
    "700": {FLIPPED_UUID},
    "900": {BASE_COLLISION_UUID},
    "910": {OVERLAP_UUID},
}
ETCHED_MAP = {
    "800": {ETCHED_UUID},
    "901": {ETCHED_COLLISION_UUID},
    # The exact base overlap is dropped, while the different card is retained.
    "910": {OVERLAP_UUID, SHARED_PRODUCT_ETCHED_UUID},
}
ALT_MAP = {
    "656544": {JAWS_UUID},
    "701": {FLIPPED_UUID},
    "900": {"wrong-alt-base-collision"},
    "901": {"wrong-alt-etched-collision"},
    "902": {"ambiguous-one", "ambiguous-two"},
    "700000": {JAWS_UUID},
    "904": {"null-price"},
}
RAW_PRICES = [
    # Put the duplicate alternative first to prove selection is deterministic.
    {"productId": "700000", "subTypeName": "Foil", "marketPrice": 999.0},
    {"productId": "656544", "subTypeName": "Foil", "marketPrice": 22.0},
    {"productId": "656542", "subTypeName": "Normal", "marketPrice": 11.0},
    {"productId": "700", "subTypeName": "Foil", "marketPrice": 31.0},
    {"productId": "701", "subTypeName": "Normal", "marketPrice": 32.0},
    {"productId": "800", "subTypeName": "Foil", "marketPrice": 40.0},
    {"productId": "900", "subTypeName": "Normal", "marketPrice": 50.0},
    {"productId": "901", "subTypeName": "Foil", "marketPrice": 60.0},
    {"productId": "902", "subTypeName": "Foil", "marketPrice": 70.0},
    {"productId": "904", "subTypeName": "Foil", "marketPrice": None},
    {"productId": "910", "subTypeName": "Foil", "marketPrice": 80.0},
]


class _FakeTcgClient:
    def __init__(self, results: list[dict[str, Any]] | dict[int, list[dict[str, Any]]]) -> None:
        self.results = results

    async def __aenter__(self) -> _FakeTcgClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get(self, endpoint: str, versioned: bool = True) -> dict[str, list[dict[str, Any]]]:
        assert versioned
        if isinstance(self.results, dict):
            group_id = int(endpoint.rsplit("/", maxsplit=1)[-1])
            return {"results": self.results[group_id]}
        return {"results": self.results}


def _write_mapping(path, id_column: str, mapping: dict[str, set[str]]) -> None:
    rows = [{id_column: product_id, "uuid": uuid} for product_id, uuids in mapping.items() for uuid in uuids]
    pl.DataFrame(rows, schema={id_column: pl.String, "uuid": pl.String}).write_parquet(path)


@pytest.fixture
def mapped_price_frames(tmp_path, monkeypatch) -> tuple[pl.DataFrame, pl.DataFrame]:
    fake_client = _FakeTcgClient(RAW_PRICES)
    monkeypatch.setattr(tcg_prices_module, "TcgPlayerClient", lambda _config: fake_client)

    provider = TCGPlayerPriceProvider(output_path=tmp_path / "live_prices.parquet")
    provider.today_date = TODAY
    provider._config = object()  # type: ignore[assignment]

    async def _group_ids(_client: object) -> list[tuple[int, str]]:
        return [(1, "Regression fixtures")]

    monkeypatch.setattr(provider, "_get_magic_set_ids", _group_ids)
    live = asyncio.run(provider.fetch_all_prices(BASE_MAP, ETCHED_MAP, ALT_MAP))

    pl.DataFrame(
        RAW_PRICES,
        schema={"productId": pl.String, "subTypeName": pl.String, "marketPrice": pl.Float64},
    ).write_parquet(tmp_path / "tcg_raw_prices.parquet")
    _write_mapping(tmp_path / "tcg_to_uuid.parquet", "tcgplayerProductId", BASE_MAP)
    _write_mapping(tmp_path / "tcg_etched_to_uuid.parquet", "tcgplayerEtchedProductId", ETCHED_MAP)
    _write_mapping(
        tmp_path / "tcg_alt_foil_to_uuid.parquet",
        "tcgplayerAlternativeFoilProductId",
        ALT_MAP,
    )

    builder = PolarsPriceBuilder()
    builder.today_date = TODAY
    raw = builder.map_raw_to_today_df(tmp_path)
    return live, raw


def test_live_and_raw_paths_map_alternative_products_equivalently(mapped_price_frames):
    live, raw = mapped_price_frames
    sort_columns = ["uuid", "finish", "price"]

    assert live.sort(sort_columns).rows(named=True) == raw.sort(sort_columns).rows(named=True)

    actual = {(row["uuid"], row["finish"]): row["price"] for row in raw.iter_rows(named=True)}
    assert actual == {
        (JAWS_UUID, "normal"): 11.0,
        (JAWS_UUID, "foil"): 22.0,
        (FLIPPED_UUID, "foil"): 31.0,
        (FLIPPED_UUID, "normal"): 32.0,
        (ETCHED_UUID, "etched"): 40.0,
        (BASE_COLLISION_UUID, "normal"): 50.0,
        (ETCHED_COLLISION_UUID, "etched"): 60.0,
        (OVERLAP_UUID, "foil"): 80.0,
        (SHARED_PRODUCT_ETCHED_UUID, "etched"): 80.0,
    }


def test_base_overlap_only_suppresses_same_card_etched_mapping(mapped_price_frames):
    for prices in mapped_price_frames:
        actual = {(row["uuid"], row["finish"]): row["price"] for row in prices.iter_rows(named=True)}

        assert actual[(OVERLAP_UUID, "foil")] == 80.0
        assert (OVERLAP_UUID, "etched") not in actual
        assert actual[(SHARED_PRODUCT_ETCHED_UUID, "etched")] == 80.0


@pytest.mark.parametrize(
    "group_ids",
    [
        [(1, "Alternatives"), (2, "Base products")],
        [(2, "Base products"), (1, "Alternatives")],
    ],
)
def test_live_mapping_priority_applies_across_groups(tmp_path, monkeypatch, group_ids):
    grouped_prices = {
        1: [
            {"productId": "200", "subTypeName": "Foil", "marketPrice": 20.0},
            {"productId": "201", "subTypeName": "Normal", "marketPrice": 21.0},
            {"productId": "1000", "subTypeName": "Foil", "marketPrice": 31.0},
        ],
        2: [
            {"productId": "100", "subTypeName": "Foil", "marketPrice": 10.0},
            {"productId": "101", "subTypeName": "Normal", "marketPrice": 11.0},
            {"productId": "999", "subTypeName": "Foil", "marketPrice": 30.0},
        ],
    }
    base_map = {"100": {CROSS_GROUP_UUID}}
    etched_map = {"101": {CROSS_GROUP_ETCHED_UUID}}
    alt_map = {
        "200": {CROSS_GROUP_UUID},
        "201": {CROSS_GROUP_ETCHED_UUID},
        "999": {ALT_TIE_UUID},
        "1000": {ALT_TIE_UUID},
    }

    fake_client = _FakeTcgClient(grouped_prices)
    monkeypatch.setattr(tcg_prices_module, "TcgPlayerClient", lambda _config: fake_client)

    provider = TCGPlayerPriceProvider(output_path=tmp_path / "live_prices.parquet")
    provider.today_date = TODAY
    provider._config = object()  # type: ignore[assignment]

    async def _group_ids(_client: object) -> list[tuple[int, str]]:
        return group_ids

    monkeypatch.setattr(provider, "_get_magic_set_ids", _group_ids)
    live = asyncio.run(provider.fetch_all_prices(base_map, etched_map, alt_map))

    raw_prices = [price for group_id, _name in group_ids for price in grouped_prices[group_id]]
    pl.DataFrame(
        raw_prices,
        schema={"productId": pl.String, "subTypeName": pl.String, "marketPrice": pl.Float64},
    ).write_parquet(tmp_path / "tcg_raw_prices.parquet")
    _write_mapping(tmp_path / "tcg_to_uuid.parquet", "tcgplayerProductId", base_map)
    _write_mapping(tmp_path / "tcg_etched_to_uuid.parquet", "tcgplayerEtchedProductId", etched_map)
    _write_mapping(
        tmp_path / "tcg_alt_foil_to_uuid.parquet",
        "tcgplayerAlternativeFoilProductId",
        alt_map,
    )

    builder = PolarsPriceBuilder()
    builder.today_date = TODAY
    raw = builder.map_raw_to_today_df(tmp_path)

    sort_columns = ["uuid", "finish", "price"]
    assert live.sort(sort_columns).rows(named=True) == raw.sort(sort_columns).rows(named=True)
    assert {(row["uuid"], row["finish"]): row["price"] for row in live.iter_rows(named=True)} == {
        (CROSS_GROUP_UUID, "foil"): 10.0,
        (CROSS_GROUP_ETCHED_UUID, "normal"): 11.0,
        (ALT_TIE_UUID, "foil"): 30.0,
    }


def test_alternative_mapping_does_not_duplicate_or_override_authoritative_products(mapped_price_frames):
    live, raw = mapped_price_frames
    key_columns = ["uuid", "date", "source", "provider", "price_type", "finish"]

    assert live.unique(subset=key_columns).height == live.height
    assert raw.unique(subset=key_columns).height == raw.height
    assert not set(raw["uuid"]).intersection(
        {
            "wrong-alt-base-collision",
            "wrong-alt-etched-collision",
            "ambiguous-one",
            "ambiguous-two",
            "null-price",
        }
    )


def test_alternative_price_serializes_in_today_and_history_outputs(tmp_path, mapped_price_frames):
    _live, raw = mapped_price_frames
    output_paths = [tmp_path / "AllPricesToday.json", tmp_path / "AllPrices.json"]

    stream_write_today_prices_json(raw, output_paths[0], TODAY)
    stream_write_all_prices_json(raw.lazy(), output_paths[1], TODAY)

    for output_path in output_paths:
        output = json.loads(output_path.read_text(encoding="utf-8"))
        retail = output["data"][JAWS_UUID]["paper"]["tcgplayer"]["retail"]
        assert retail == {
            "foil": {TODAY: 22.0},
            "normal": {TODAY: 11.0},
        }


def test_standalone_price_context_loads_persisted_alternative_mapping(tmp_path, monkeypatch):
    mapping_attrs = [
        "tcg_to_uuid_lf",
        "tcg_etched_to_uuid_lf",
        "tcg_alt_foil_to_uuid_lf",
        "mtgo_to_uuid_lf",
        "scryfall_to_uuid_lf",
        "cardmarket_to_uuid_lf",
    ]
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "cache_path", tmp_path)
    for attr in mapping_attrs:
        monkeypatch.setattr(GLOBAL_CACHE, attr, None)

    cards = pl.DataFrame(
        {
            "uuid": [JAWS_UUID],
            "identifiers": [
                {
                    "tcgplayerProductId": "656542",
                    "tcgplayerEtchedProductId": None,
                    "tcgplayerAlternativeFoilProductId": "656544",
                    "mtgoId": None,
                    "scryfallId": "c6d16a9e-98c0-46e0-987c-f0de0915a204",
                    "mcmId": None,
                }
            ],
        }
    )
    pipeline_ctx = PipelineContext(_cache=GLOBAL_CACHE)
    _build_id_mappings(pipeline_ctx, cards.lazy())

    persisted = pl.read_parquet(tmp_path / "tcg_alt_foil_to_uuid.parquet")
    assert persisted.to_dicts() == [
        {
            "uuid": JAWS_UUID,
            "tcgplayerAlternativeFoilProductId": "656544",
        }
    ]

    # Simulate the clean process used for a standalone --price-build run.
    for attr in mapping_attrs:
        monkeypatch.setattr(GLOBAL_CACHE, attr, None)

    price_ctx = PriceBuilderContext.from_cache()
    assert price_ctx.tcg_to_uuid == {"656542": {JAWS_UUID}}
    assert price_ctx.tcg_alt_foil_to_uuid == {"656544": {JAWS_UUID}}
