"""Regression tests for dropping absurd Manapool listing prices (issue #1664).

Manapool occasionally reports placeholder listings in the tens of millions of
dollars (2,237,677,959 cents, past INT32 max). Anything above the ceiling is
dropped for the day instead of being published.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from mtgjson5.build.prices.price_builder import PolarsPriceBuilder
from mtgjson5.providers.manapool.provider import MANAPOOL_MAX_PRICE_CENTS, _clean_cents


def test_clean_cents_drops_values_over_ceiling() -> None:
    card = {"scryfall_id": "x", "price_cents": 2_237_677_959, "price_cents_foil": 1234}

    assert _clean_cents(card, "price_cents") == 0
    assert _clean_cents(card, "price_cents_foil") == 1234
    assert _clean_cents(card, "price_cents_etched") == 0
    assert _clean_cents({"price_cents": MANAPOOL_MAX_PRICE_CENTS}, "price_cents") == MANAPOOL_MAX_PRICE_CENTS


def test_map_manapool_raw_drops_values_over_ceiling(tmp_path: Path) -> None:
    raw_path = tmp_path / "manapool_raw.parquet"
    uuid_path = tmp_path / "scryfall_to_uuid.parquet"
    pl.DataFrame(
        {
            "scryfallId": ["sf-a", "sf-b"],
            "price_cents": [2_237_677_959, MANAPOOL_MAX_PRICE_CENTS],
            "price_cents_foil": [3402, MANAPOOL_MAX_PRICE_CENTS + 1],
            "price_cents_etched": [0, 0],
        }
    ).write_parquet(raw_path)
    pl.DataFrame({"uuid": ["uuid-a", "uuid-b"], "scryfallId": ["sf-a", "sf-b"]}).write_parquet(uuid_path)

    result = PolarsPriceBuilder()._map_manapool_raw(raw_path, uuid_path)

    assert sorted(result.select("uuid", "finish", "price").rows()) == [
        ("uuid-a", "foil", 34.02),
        ("uuid-b", "normal", 5_000_000.0),
    ]
