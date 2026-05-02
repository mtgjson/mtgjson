"""Tests for identifier lookup consolidation and struct assembly."""

from __future__ import annotations

import polars as pl

from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.identifiers import add_identifiers_struct


def test_build_identifiers_lookup_joins_cardtrader_ids() -> None:
    ctx = PipelineContext.for_testing(
        uuid_cache_lf=pl.LazyFrame(
            [
                {"scryfallId": "sf-001", "side": "a", "cachedUuid": "uuid-001"},
                {"scryfallId": "sf-002", "side": "a", "cachedUuid": "uuid-002"},
            ],
            schema={
                "scryfallId": pl.String,
                "side": pl.String,
                "cachedUuid": pl.String,
            },
        ),
        cardtrader_lf=pl.LazyFrame(
            [{"scryfallId": "sf-001", "cardtraderId": "ct-101"}],
            schema={
                "scryfallId": pl.String,
                "cardtraderId": pl.String,
            },
        ),
        meld_triplets={},
        manual_overrides={},
    )

    ctx._build_identifiers_lookup()
    assert ctx.identifiers_lf is not None

    rows = ctx.identifiers_lf.collect().sort("scryfallId").to_dicts()
    assert rows == [
        {
            "scryfallId": "sf-001",
            "side": "a",
            "cachedUuid": "uuid-001",
            "cardtraderId": "ct-101",
        },
        {
            "scryfallId": "sf-002",
            "side": "a",
            "cachedUuid": "uuid-002",
            "cardtraderId": None,
        },
    ]


def test_add_identifiers_struct_includes_cardtrader_id() -> None:
    lf = pl.LazyFrame(
        [
            {
                "scryfallId": "sf-001",
                "_face_data": None,
                "oracleId": "oracle-001",
                "illustrationId": "ill-001",
                "cardBackId": "back-001",
                "mcmId": None,
                "mcmMetaId": None,
                "arenaId": None,
                "mtgoId": None,
                "mtgoFoilId": None,
                "multiverseIds": ["123"],
                "faceId": 0,
                "tcgplayerId": None,
                "tcgplayerEtchedId": None,
                "tcgplayerAlternativeFoilProductId": None,
                "cardKingdomId": None,
                "cardKingdomFoilId": None,
                "cardKingdomEtchedId": None,
                "cardtraderId": "ct-101",
                "cardsphereId": None,
                "cardsphereFoilId": None,
                "deckboxId": None,
            }
        ],
        schema={
            "scryfallId": pl.String,
            "_face_data": pl.Struct({"oracle_id": pl.String, "illustration_id": pl.String}),
            "oracleId": pl.String,
            "illustrationId": pl.String,
            "cardBackId": pl.String,
            "mcmId": pl.String,
            "mcmMetaId": pl.String,
            "arenaId": pl.String,
            "mtgoId": pl.String,
            "mtgoFoilId": pl.String,
            "multiverseIds": pl.List(pl.String),
            "faceId": pl.Int64,
            "tcgplayerId": pl.String,
            "tcgplayerEtchedId": pl.String,
            "tcgplayerAlternativeFoilProductId": pl.String,
            "cardKingdomId": pl.String,
            "cardKingdomFoilId": pl.String,
            "cardKingdomEtchedId": pl.String,
            "cardtraderId": pl.String,
            "cardsphereId": pl.String,
            "cardsphereFoilId": pl.String,
            "deckboxId": pl.String,
        },
    )

    row = add_identifiers_struct(lf).collect().to_dicts()[0]["identifiers"]

    assert row["cardtraderId"] == "ct-101"
    assert row["scryfallId"] == "sf-001"
