"""Tests that is_foil_only flows from sealed YAML records to the published product."""

import polars as pl
import pytest

from mtgjson5.data.context import PipelineContext
from mtgjson5.models.sealed import SealedProduct
from mtgjson5.pipeline.stages.metadata import build_sealed_products_lf
from mtgjson5.pipeline.stages.sealed import product
from mtgjson5.providers.github.provider import (
    SCHEMAS,
    _build_sealed_contents_records,
    _build_sealed_products_records,
    _to_lazyframe,
)


@pytest.mark.parametrize("column", ["is_foil_only", "isFoilOnly"])
def test_is_foil_only_reaches_the_published_product(column):
    definition = {"category": "BOX_SET", "subtype": "SECRET_LAIR", "identifiers": {"tcgplayerProductId": "1"}}
    records = _build_sealed_products_records({"SLD": {"Flagged": {**definition, column: True}, "Plain": definition}})
    products = _to_lazyframe(records, "sealed_products", "sealed_products").with_columns(
        pl.col("productName").alias("uuid")
    )
    entry = {"name": "Lurking Crocodile", "set": "sld", "number": "590", "uuid": "card-id"}
    contents = {"SLD": {name: product({"card": [entry]}, "SLD", name).toJson() for name in ("Flagged", "Plain")}}
    ctx = PipelineContext(
        _test_data={
            "_sealed_products_lf": products,
            # A whole build has a column for every content type, not just cards
            "_sealed_contents_lf": pl.concat(
                [
                    pl.LazyFrame(schema=SCHEMAS["sealed_contents"]),
                    _to_lazyframe(_build_sealed_contents_records(contents), "sealed_contents", "sealed_contents"),
                ],
                how="diagonal_relaxed",
            ),
        }
    )
    frame = build_sealed_products_lf(ctx).collect()
    published = {
        p["name"]: p for p in (m.to_polars_dict(exclude_none=True) for m in SealedProduct.from_dataframe(frame))
    }
    assert published["Flagged"]["isFoilOnly"] is True
    assert "isFoilOnly" not in published["Plain"]
