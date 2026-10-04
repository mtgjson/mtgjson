"""Choice compilation contract against a separately checked-out sealed repo.

Set SEALED_SOURCE_PATH to the checkout matching the installed dependency pin
to compare real product recipes without keeping another fixture copy.
UUID resolution and language selection remain owned by each consumer.
"""

import copy
import importlib.util
import os
from pathlib import Path

import pytest
import yaml

from mtgjson5.pipeline.stages.sealed import product


@pytest.fixture(scope="module")
def sealed_source():
    path = os.environ.get("SEALED_SOURCE_PATH")
    if not path:
        pytest.skip("Set SEALED_SOURCE_PATH to check the upstream choice compiler")
    root = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("sealed_contract_source", root / "scripts/product_classes.py")
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return root, module.product


def prepared(value):
    """Supply placeholder card UUIDs; exclude consumer-specific language lookup."""
    if isinstance(value, list):
        return [prepared(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: prepared(item) for key, item in value.items() if key != "language"}
    for card in result.get("card", []):
        card["uuid"] = f"{card['set']}:{card['number']}"
    return result


def assert_parity(source_product, source, name):
    source = prepared(source)
    original = copy.deepcopy(source)
    left = source_product(source, name=name).toJson()
    assert source == original, f"sealed compiler mutated {name}"
    right = product(source, name=name).toJson()
    assert source == original, f"MTGJSON compiler mutated {name}"
    assert left == right, f"choice compilation differs for {name}"


def test_current_product_choices(sealed_source, tmp_path, monkeypatch):
    root, source_product = sealed_source
    # The source compiler writes unknown-bonus diagnostics to status.txt.
    monkeypatch.chdir(tmp_path)
    count = 0
    for path in sorted((root / "data/products").glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        for name, definition in data["products"].items():
            contents = definition.get("contents", {})
            if not isinstance(contents, dict) or "variable" not in contents:
                continue
            assert_parity(source_product, contents, f"{data['code']}: {name}")
            count += 1
    assert count > 0, "No variable products checked; source layout may have changed"


@pytest.mark.parametrize("replacement", [False, True])
@pytest.mark.parametrize("count", [1, 2, 3])
def test_weighted_nested_choices(sealed_source, replacement, count):
    _, source_product = sealed_source
    choices = [
        {
            "chance": weight,
            "variable": [{"deck": [{"set": "tst", "name": f"{color}-{version}"}]} for version in ("A", "B")],
        }
        for color, weight in (("White", 1), ("Blue", 2), ("Black", 3))
    ]
    source = {"variable": choices, "variable_mode": {"count": count, "replacement": replacement}}
    assert_parity(source_product, source, "weighted nested choices")


def test_invalid_weight_rejected_by_both(sealed_source):
    _, source_product = sealed_source
    for compiler in (source_product, product):
        with pytest.raises(ValueError, match="Weight incorrectly assigned"):
            compiler({"variable": [{"chance": 2}], "variable_mode": {"weight": 1}})
