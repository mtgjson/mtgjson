"""Independent Battle Pack choices survive compilation and published JSON.

Compact fixtures reproduce the M12/M13 choice structure without catalog data.
"""

import copy
import itertools
import json

import polars as pl
import pytest
import yaml

from mtgjson5.data.context import PipelineContext
from mtgjson5.models.sealed import SealedProduct
from mtgjson5.pipeline.stages.metadata import build_sealed_products_lf
from mtgjson5.pipeline.stages.sealed import compile_contents, product
from mtgjson5.providers.github.provider import SCHEMAS, _build_sealed_contents_records, _to_lazyframe


def outcomes(contents):
    rows = [contents.get("deck", [])]
    for group in contents.get("variable", []):
        choices = [row for config in group["configs"] for row in outcomes(config)]
        rows = [a + b for a, b in itertools.product(rows, choices)]
    return rows


def test_repeated_component_preserves_independent_choices():
    source = {
        "variable": [{"variable": [{"deck": [{"set": "tst", "name": name}]} for name in ("A", "B")]}],
        "variable_mode": {"count": 2, "replacement": True},
    }
    original = copy.deepcopy(source)
    compiled = product(source)
    compiled.get_uuids({"tst": {"decks": {"A": {}, "B": {}}}})
    assert source == original
    assert [[d["name"] for d in row] for row in outcomes(compiled.toJson())] == [
        ["A", "A"],
        ["A", "B"],
        ["B", "A"],
        ["B", "B"],
    ]
    assert product(source).toJson() == compiled.toJson()


@pytest.mark.parametrize(("code", "expected_count"), [("M12", 80), ("M13", 1600)])
def test_battle_pack_published_outcomes(code, expected_count, tmp_path, caplog):
    name = f"{code} Battle Pack"
    colors = ("White", "Blue", "Black", "Red", "Green")
    deck_names = [f"Booster Battle Pack: {color} {version}" for color in colors for version in ("A", "B")]
    choices = [
        {
            "variable": [
                {"deck": [{"set": code.lower(), "name": f"Booster Battle Pack: {color} {version}"}]}
                for version in ("A", "B")
            ]
        }
        for color in colors
    ]
    selection = {"variable": choices, "variable_mode": {"count": 4 if code == "M12" else 2}}
    if code == "M13":
        selection = {"variable": [selection], "variable_mode": {"count": 2, "replacement": True}}
    definition = {
        "contents": {"card_count": 70, "sealed": [{"set": code.lower(), "name": "Booster", "count": 2}], **selection}
    }
    products_dir = tmp_path / "products"
    products_dir.mkdir()
    (products_dir / f"{code}.yaml").write_text(yaml.safe_dump({"code": code.lower(), "products": {name: definition}}))
    uuid_map = {
        code.lower(): {
            "decks": {name: {} for name in deck_names},
            "sealedProduct": {name: "product-id", definition["contents"]["sealed"][0]["name"]: "booster-id"},
        }
    }
    compiled, _ = compile_contents(products_dir, None, uuid_map)
    assert "not found" not in caplog.text
    frame = pl.concat(
        [
            pl.LazyFrame(schema=SCHEMAS["sealed_contents"]),
            _to_lazyframe(_build_sealed_contents_records(compiled), "sealed_contents", "sealed_contents"),
        ],
        how="diagonal_relaxed",
    ).collect()
    cache = tmp_path / "contents.parquet"
    frame.write_parquet(cache)
    ctx = PipelineContext(
        _test_data={
            "_sealed_products_lf": pl.LazyFrame(
                [
                    {
                        "setCode": code,
                        "productName": name,
                        "category": "multi_deck",
                        "subtype": "battle",
                        "identifiers": {"tcgplayerProductId": "1"},
                    }
                ]
            ),
            "_sealed_contents_lf": pl.scan_parquet(cache),
        }
    )
    output = build_sealed_products_lf(ctx).collect()
    model = SealedProduct.from_dataframe(output)[0]
    published = json.loads(model.model_dump_json(by_alias=True, exclude_none=True))
    contents = published["contents"]
    rows = outcomes(contents)
    assert len(rows) == expected_count
    assert rows == outcomes(compiled[code.lower()][name])
    assert published["cardCount"] == 70
    assert contents["sealed"][0]["count"] == 2
    for row in rows:
        assert len(row) == 4
        selected_colors = [d["name"].split(": ")[1].rsplit(" ", 1)[0] for d in row]
        if code == "M12":
            assert len(set(selected_colors)) == 4
        else:
            assert len(set(selected_colors[:2])) == len(set(selected_colors[2:])) == 2
    if code == "M13":
        assert any(row[:2] == row[2:] for row in rows)


def test_nested_choices_reach_card_to_products():
    """Cards in nested alternatives must reach the published reverse index."""
    from mtgjson5.pipeline.stages.sealed import compile_card_to_products

    contents = {
        "variable": [
            {
                "configs": [
                    {
                        "variable": [
                            {
                                "configs": [
                                    {"card": [{"uuid": "first", "foil": True}]},
                                    {"card": [{"uuid": "second", "etched": True}]},
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    view = {"TST": {"sealedProduct": [{"uuid": "product", "contents": contents}]}}
    assert compile_card_to_products(view) == {
        "first": {"foil": ["product"]},
        "second": {"etched": ["product"]},
    }
