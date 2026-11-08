"""Regression tests for Cardmarket card identity mapping."""

from __future__ import annotations

import polars as pl

from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.identifiers import join_cardmarket_ids


def _mcm_lookup() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "mcmId": ["750355", "123456"],
            "mcmMetaId": ["7595", "654321"],
            "setCode": ["RVR", "TST"],
            "nameLower": ["hallowed fountain", "fallback card"],
            "number": ["404", "1"],
        }
    )


def test_scryfall_cardmarket_id_preserves_rvr_variant_identity():
    cards = pl.LazyFrame(
        {
            "name": ["Hallowed Fountain", "Hallowed Fountain"],
            "setCode": ["RVR", "RVR"],
            "number": ["404", "404z"],
            "cardmarketId": [748753, 750355],
        }
    )
    ctx = PipelineContext(_test_data={"_mcm_lookup_lf": _mcm_lookup()})

    result = join_cardmarket_ids(cards, ctx).collect()

    assert result["mcmId"].to_list() == ["748753", "750355"]
    assert result["mcmMetaId"].to_list() == ["7595", "7595"]


def test_mcm_meta_id_follows_product_dropped_from_number_lookup():
    """RVR #376z: its product shares #376's number, so only the full product list has it (#1615)."""
    cards = pl.LazyFrame(
        {
            "name": ["Niv-Mizzet, Parun", "Niv-Mizzet, Parun"],
            "setCode": ["RVR", "RVR"],
            "number": ["376", "376z"],
            "cardmarketId": [748729, 750333],
        }
    )
    number_lookup = pl.LazyFrame(
        {
            "mcmId": ["748729"],
            "mcmMetaId": ["265254"],
            "setCode": ["RVR"],
            "nameLower": ["niv-mizzet, parun"],
            "number": ["376"],
        }
    )
    all_products = pl.LazyFrame(
        {
            "mcmId": ["748729", "750333"],
            "mcmMetaId": ["265254", "265254"],
            "setCode": ["RVR", "RVR"],
            "nameLower": ["niv-mizzet, parun", "niv-mizzet, parun"],
            "number": ["376", "376"],
        }
    )
    ctx = PipelineContext(
        _test_data={
            "_mcm_lookup_lf": number_lookup,
            "_mcm_price_lookup_lf": all_products,
        }
    )

    result = join_cardmarket_ids(cards, ctx).collect()

    assert result["mcmId"].to_list() == ["748729", "750333"]
    assert result["mcmMetaId"].to_list() == ["265254", "265254"]


def test_cardmarket_lookup_fills_missing_scryfall_id():
    cards = pl.LazyFrame(
        {
            "name": ["Fallback Card"],
            "setCode": ["TST"],
            "number": ["1"],
            "cardmarketId": [None],
        },
        schema={
            "name": pl.String,
            "setCode": pl.String,
            "number": pl.String,
            "cardmarketId": pl.Int64,
        },
    )
    ctx = PipelineContext(_test_data={"_mcm_lookup_lf": _mcm_lookup()})

    result = join_cardmarket_ids(cards, ctx).collect()

    assert result["mcmId"].to_list() == ["123456"]
    assert result["mcmMetaId"].to_list() == ["654321"]
