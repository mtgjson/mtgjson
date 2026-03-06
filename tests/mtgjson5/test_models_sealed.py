"""Tests for v2 SealedProduct model validator."""

from __future__ import annotations

from mtgjson5.models.sealed import SealedProduct


class TestSealedProductValidator:
    def test_foil_true_populates_finishes(self):
        data = {
            "uuid": "sp-001",
            "name": "Test Product",
            "contents": {"card": [{"uuid": "card-1", "name": "Card A", "number": "1", "set": "TST", "foil": True}]},
        }
        product = SealedProduct(**data)
        card = product.contents["card"][0]
        assert card["finishes"] == ["foil"]

    def test_foil_false_populates_nonfoil(self):
        data = {
            "uuid": "sp-002",
            "name": "Test Product",
            "contents": {"card": [{"uuid": "card-1", "name": "Card A", "number": "1", "set": "TST", "foil": False}]},
        }
        product = SealedProduct(**data)
        card = product.contents["card"][0]
        assert card["finishes"] == ["nonfoil"]

    def test_existing_finishes_not_overwritten(self):
        data = {
            "uuid": "sp-003",
            "name": "Test Product",
            "contents": {
                "card": [{"uuid": "card-1", "name": "Card A", "number": "1", "set": "TST", "finishes": ["etched"]}]
            },
        }
        product = SealedProduct(**data)
        card = product.contents["card"][0]
        assert card["finishes"] == ["etched"]

    def test_no_contents_no_error(self):
        product = SealedProduct(uuid="sp-004", name="Empty Product")
        assert product.contents is None

    def test_identifiers_default_empty_dict(self):
        product = SealedProduct(uuid="sp-005", name="Test Product")
        assert product.identifiers == {}

    def test_identifiers_roundtrip(self):
        product = SealedProduct(
            uuid="sp-006",
            name="Test Product",
            identifiers={"tcgplayerProductId": "12345"},
        )
        d = product.to_polars_dict(exclude_none=True)
        assert d["identifiers"] == {"tcgplayerProductId": "12345"}

    def test_exclude_none_retains_required(self):
        product = SealedProduct(uuid="sp-007", name="Minimal Product")
        d = product.to_polars_dict(exclude_none=True)
        assert d["uuid"] == "sp-007"
        assert d["name"] == "Minimal Product"
        assert d["identifiers"] == {}

    def test_contents_with_multiple_types(self):
        data = {
            "uuid": "sp-008",
            "name": "Bundle",
            "contents": {
                "card": [{"uuid": "card-1", "name": "Card A", "number": "1", "set": "TST", "foil": False}],
                "sealed": [{"uuid": "sealed-1", "name": "Booster", "count": 10, "set": "TST"}],
            },
        }
        product = SealedProduct(**data)
        assert "card" in product.contents
        assert "sealed" in product.contents
        assert len(product.contents["card"]) == 1
        assert len(product.contents["sealed"]) == 1
