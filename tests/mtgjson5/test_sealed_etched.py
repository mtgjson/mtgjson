"""Etched bonuses retain their finish through sealed output and card mapping."""

import polars as pl
import pytest

from mtgjson5.data.context import PipelineContext
from mtgjson5.models.sealed import SealedProduct, SealedProductAssembler
from mtgjson5.pipeline.stages.metadata import build_sealed_products_lf
from mtgjson5.pipeline.stages.sealed import _ctp_get_cards_in_content_type, card, product
from mtgjson5.providers.github.provider import SCHEMAS, _build_sealed_contents_records, _to_lazyframe


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({}, "nonfoil"),
        ({"foil": True}, "foil"),
        ({"etched": True}, "etched"),
        ({"foil": True, "etched": True}, "etched"),
    ],
)
@pytest.mark.parametrize("variable", [False, True])
def test_finish_survives_model_and_mapper(flags, expected, variable):
    source = card({"name": "Fellwar Stone", "set": "sld", "number": "708", "uuid": "card-id", **flags}).toJson()
    contents = {"card": [source]}
    if variable:
        contents = {"variable": [{"configs": [contents]}]}
    output = SealedProduct(uuid="product-id", name="Test", contents=contents).model_dump(by_alias=True)["contents"]
    entry = output["variable"][0]["configs"][0]["card"][0] if variable else output["card"][0]
    assert entry["finishes"] == [expected]
    assert entry.get("etched", False) == flags.get("etched", False)
    key = "variable" if variable else "card"
    mapped = _ctp_get_cards_in_content_type({}, key, output[key][0])
    assert [(c.uuid, c.finish) for c in mapped] == [("card-id", expected)]


def test_dataframe_assembly_preserves_etched():
    frame = pl.DataFrame(
        [
            {
                "productUuid": "product-id",
                "contentType": "card",
                "uuid": "card-id",
                "name": "Fellwar Stone",
                "number": "708",
                "set": "sld",
                "etched": True,
            }
        ]
    )
    contents = SealedProductAssembler.assemble_contents(frame, "product-id")
    assert contents["card"][0]["finishes"] == ["etched"]
    assert contents["card"][0]["etched"] is True


def test_sealed_products_frame_preserves_etched():
    """Set output takes its contents from build_sealed_products_lf, not assemble_contents."""
    flags = {"Plain": {}, "Foil": {"foil": True}, "Etched": {"etched": True}}
    entry = {"name": "Lurking Crocodile", "set": "sld", "number": "590", "uuid": "card-id"}
    contents = {
        "SLD": {name: product({"card": [{**entry, **flag}]}, "SLD", name).toJson() for name, flag in flags.items()}
    }
    ctx = PipelineContext(
        _test_data={
            "_sealed_products_lf": pl.LazyFrame(
                [
                    {"setCode": "SLD", "productName": name, "category": "box_set", "subtype": "secret_lair"}
                    for name in flags
                ]
            ).with_columns(pl.struct(pl.lit("1").alias("tcgplayerProductId")).alias("identifiers")),
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
    published = [m.to_polars_dict(exclude_none=True) for m in SealedProduct.from_dataframe(frame)]
    entries = {p["name"]: p["contents"]["card"][0] for p in published}
    assert {name: e["finishes"] for name, e in entries.items()} == {
        "Plain": ["nonfoil"],
        "Foil": ["foil"],
        "Etched": ["etched"],
    }
    assert {name: e.get("etched", False) for name, e in entries.items()} == {
        "Plain": False,
        "Foil": False,
        "Etched": True,
    }
