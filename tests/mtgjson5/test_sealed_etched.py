"""Etched bonuses retain their finish through sealed output and card mapping."""

import polars as pl
import pytest

from mtgjson5.models.sealed import SealedProduct, SealedProductAssembler
from mtgjson5.pipeline.stages.sealed import _ctp_get_cards_in_content_type, card


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
