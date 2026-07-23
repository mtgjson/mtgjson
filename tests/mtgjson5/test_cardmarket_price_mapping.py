"""Regression tests for Cardmarket product/finish price identity."""

from __future__ import annotations

import json

import polars as pl

from mtgjson5.build.prices.price_builder import PolarsPriceBuilder
from mtgjson5.build.prices.price_writers import stream_write_today_prices_json
from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.output import _build_mcm_price_mapping_cache

FORCE_UUID = "3353f32a-1938-5e04-b9ba-f31da9b5924d"


def _cards() -> pl.DataFrame:
    rows = [
        (FORCE_UUID, "H1R", "Force of Negation", "9", "566136", ["foil", "etched"]),
        ("ambiguous", "TST", "Ambiguous", "1", "900", ["foil", "etched"]),
        ("normal-foil", "TST", "Normal Foil", "2", "100", ["nonfoil", "foil"]),
        ("foil-only", "TST", "Foil Only", "3", "101", ["foil"]),
        ("etched-only", "TST", "Etched Only", "4", "102", ["etched"]),
        ("all-three", "TST", "All Three", "5", "103", ["nonfoil", "foil", "etched"]),
    ]
    return pl.DataFrame(
        {
            "uuid": [row[0] for row in rows],
            "setCode": [row[1] for row in rows],
            "name": [row[2] for row in rows],
            "number": [row[3] for row in rows],
            "identifiers": [{"mcmId": row[4]} for row in rows],
            "finishes": [row[5] for row in rows],
        }
    )


def _price_candidates() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mcmId": ["566136", "567745"],
            "mcmMetaId": ["270840", "270840"],
            "expansionId": [4275, 4275],
            "setCode": ["H1R", "H1R"],
            "nameLower": ["force of negation", "force of negation"],
            "number": ["9", "9"],
            "mcmVariant": ["1", "2"],
        }
    )


def test_cardmarket_lookup_preserves_variants_for_prices():
    ctx = PipelineContext(
        resource_path=None,
        _test_data={
            "_mcm_lookup_lf": pl.DataFrame(
                {
                    "mcmId": ["566136", "567745"],
                    "mcmMetaId": ["270840", "270840"],
                    "expansionId": [4275, 4275],
                    "expansionName": ["Modern Horizons 1 Timeshifts"] * 2,
                    "name": ["Force of Negation (V.1)", "Force of Negation (V.2)"],
                    "number": ["9", "9"],
                }
            ).lazy(),
            "_sets_lf": pl.DataFrame({"code": ["H1R"], "name": ["Modern Horizons 1 Timeshifts"]}).lazy(),
        },
    )
    ctx._build_mcm_lookup()

    assert ctx.mcm_lookup_lf is not None
    assert ctx.mcm_price_lookup_lf is not None
    assert ctx.mcm_lookup_lf.collect()["mcmId"].to_list() == ["566136"]
    candidates = ctx.mcm_price_lookup_lf.collect().sort("mcmId")
    assert candidates["mcmId"].to_list() == ["566136", "567745"]
    assert candidates["mcmVariant"].to_list() == ["1", "2"]


def _build_mapping(tmp_path, monkeypatch) -> pl.DataFrame:
    from mtgjson5 import constants

    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    ctx = PipelineContext()
    ctx._mcm_price_lookup_enriched = _price_candidates().lazy()
    _build_mcm_price_mapping_cache(ctx, _cards().lazy())
    return pl.read_parquet(tmp_path / "mcm_price_mappings.parquet")


def test_finish_mapping_is_explicit_and_conservative(tmp_path, monkeypatch):
    mapping = _build_mapping(tmp_path, monkeypatch)
    actual = set(mapping.rows())

    assert (FORCE_UUID, "566136", "trend", "foil") in actual
    assert (FORCE_UUID, "567745", "trend", "etched") in actual
    assert not mapping.filter((pl.col("uuid") == FORCE_UUID) & (pl.col("finish") == "normal")).height
    assert not mapping.filter(pl.col("uuid") == "ambiguous").height
    assert set(mapping.filter(pl.col("uuid") == "normal-foil")["finish"]) == {"normal", "foil"}
    assert set(mapping.filter(pl.col("uuid") == "foil-only")["finish"]) == {"foil"}
    assert set(mapping.filter(pl.col("uuid") == "etched-only")["finish"]) == {"etched"}
    assert set(mapping.filter(pl.col("uuid") == "all-three")["finish"]) == {"normal"}


def test_force_prices_use_both_products_and_serialize_without_normal(tmp_path, monkeypatch):
    mapping = _build_mapping(tmp_path, monkeypatch)
    raw = pl.DataFrame(
        {
            "productId": ["566136", "567745"],
            "trend": [111.11, 66.66],
            "trend_foil": [144.44, 77.77],
        }
    )
    builder = PolarsPriceBuilder()
    builder.today_date = "2026-07-10"
    prices = builder._map_cardmarket_frames(raw, mapping)

    force = prices.filter(pl.col("uuid") == FORCE_UUID)
    assert dict(zip(force["finish"], force["price"], strict=False)) == {"foil": 111.11, "etched": 66.66}

    output_path = tmp_path / "AllPricesToday.json"
    stream_write_today_prices_json(prices, output_path, builder.today_date)
    output = json.loads(output_path.read_text(encoding="utf-8"))
    retail = output["data"][FORCE_UUID]["paper"]["cardmarket"]["retail"]
    assert retail == {
        "etched": {"2026-07-10": 66.66},
        "foil": {"2026-07-10": 111.11},
    }


def test_ordinary_and_single_finish_price_fields(tmp_path, monkeypatch):
    mapping = _build_mapping(tmp_path, monkeypatch)
    raw = pl.DataFrame(
        {
            "productId": ["100", "101", "102", "103", "900"],
            "trend": [10.0, 20.0, 30.0, 40.0, 50.0],
            "trend_foil": [11.0, 21.0, 31.0, 41.0, 51.0],
        }
    )
    prices = PolarsPriceBuilder()._map_cardmarket_frames(raw, mapping)
    by_uuid = {
        uuid: dict(zip(group["finish"], group["price"], strict=False)) for (uuid,), group in prices.group_by("uuid")
    }

    assert by_uuid["normal-foil"] == {"normal": 10.0, "foil": 11.0}
    assert by_uuid["foil-only"] == {"foil": 20.0}
    assert by_uuid["etched-only"] == {"etched": 30.0}
    assert by_uuid["all-three"] == {"normal": 40.0}
    assert "ambiguous" not in by_uuid


def test_all_three_finishes_when_separate_product_identity_is_known():
    mapping = pl.DataFrame(
        {
            "uuid": ["three"] * 3,
            "productId": ["base", "base", "etched-product"],
            "priceColumn": ["trend", "trend_foil", "trend"],
            "finish": ["normal", "foil", "etched"],
        }
    )
    raw = pl.DataFrame(
        {
            "productId": ["base", "etched-product"],
            "trend": [10.0, 30.0],
            "trend_foil": [20.0, 99.0],
        }
    )
    prices = PolarsPriceBuilder()._map_cardmarket_frames(raw, mapping)
    assert dict(zip(prices["finish"], prices["price"], strict=False)) == {
        "normal": 10.0,
        "foil": 20.0,
        "etched": 30.0,
    }


def test_every_mapped_finish_is_supported(tmp_path, monkeypatch):
    mapping = _build_mapping(tmp_path, monkeypatch)
    supported = {row["uuid"]: set(row["finishes"]) for row in _cards().iter_rows(named=True)}
    for row in mapping.iter_rows(named=True):
        card_finish = "nonfoil" if row["finish"] == "normal" else row["finish"]
        assert card_finish in supported[row["uuid"]]
