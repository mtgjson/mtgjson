"""Tests for Art Series signature handling in add_signatures_combined."""

from __future__ import annotations

import polars as pl

from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.signatures import add_signatures_combined


def _art_card(set_code: str, set_name: str) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "setCode": [set_code],
            "setName": [set_name],
            "setType": ["memorabilia"],
            "number": ["1"],
            "borderColor": ["borderless"],
            "artist": ["Michael C. Hayes"],
            "finishes": [["nonfoil"]],
            "_wc_signature": [None],
        },
        schema_overrides={"_wc_signature": pl.String},
    )


def test_art_series_gets_artist_signature(simple_ctx: PipelineContext) -> None:
    lf = _art_card("AMH2", "Modern Horizons 2 Art Series")
    result = add_signatures_combined(lf, simple_ctx).collect()
    assert result["signature"][0] == "Michael C. Hayes"
    assert result["finishes"][0].to_list() == ["nonfoil", "signed"]


def test_modern_horizons_art_series_not_signed(simple_ctx: PipelineContext) -> None:
    lf = _art_card("AMH1", "Modern Horizons Art Series")
    result = add_signatures_combined(lf, simple_ctx).collect()
    assert result["signature"][0] is None
    assert result["finishes"][0].to_list() == ["nonfoil"]
