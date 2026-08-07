"""Tests for PipelineContext._build_tcg_alt_foil_lookup.

Covers the base <-> alt-foil TCGPlayer product pairing, including the "flipped"
case (MSC Surge Foil) where Scryfall anchors the card on the *suffixed* product
and the non-suffixed "Regular" product would otherwise be orphaned.
See https://github.com/mtgjson/mtgjson/issues/1676.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.data.context import PipelineContext


def _build_lookup(products: list[dict], known_tcg_ids: list[int]) -> dict[str, str]:
    """Run _build_tcg_alt_foil_lookup against fake data and return the mapping.

    Args:
        products: rows for the fake tcg_skus frame (productId, name, groupId).
        known_tcg_ids: TCGPlayer product IDs that appear as a card's Scryfall
            tcgplayerId (i.e. the products Scryfall anchors cards on).

    Returns:
        dict of tcgplayerProductId -> tcgplayerAlternativeFoilProductId.
    """
    tcg_skus = pl.LazyFrame(products, schema={"productId": pl.Int64, "name": pl.String, "groupId": pl.Int64})
    cards = pl.LazyFrame({"tcgplayerId": known_tcg_ids}, schema={"tcgplayerId": pl.Int64})
    # uuid_cache must be non-None; leave it empty so all known IDs come from cards.
    uuid_cache = pl.LazyFrame({"tcgplayerId": []}, schema={"tcgplayerId": pl.Int64})

    ctx = PipelineContext.for_testing(cards_lf=cards, uuid_cache_lf=uuid_cache)
    ctx._test_data["_tcg_skus_lf"] = tcg_skus
    ctx._build_tcg_alt_foil_lookup()

    if ctx.tcg_alt_foil_lf is None:
        return {}
    df = ctx.tcg_alt_foil_lf.collect()
    return dict(
        zip(
            df["tcgplayerProductId"].to_list(),
            df["tcgplayerAlternativeFoilProductId"].to_list(),
            strict=True,
        )
    )


class TestBuildTcgAltFoilLookup:
    def test_normal_case_base_is_anchor(self) -> None:
        """Scryfall points at the non-suffixed base -> map base to alt-foil."""
        mapping = _build_lookup(
            products=[
                {"productId": 1000, "name": "Normal Card", "groupId": 2},
                {"productId": 1001, "name": "Normal Card (Rainbow Foil)", "groupId": 2},
            ],
            known_tcg_ids=[1000],
        )
        assert mapping == {"1000": "1001"}

    def test_flipped_case_surge_foil_is_anchor(self) -> None:
        """MSC case: Scryfall points at the suffixed Surge Foil product.

        The orphaned Regular product must be attached via the flipped mapping so
        its non-foil SKUs join to the card.
        """
        mapping = _build_lookup(
            products=[
                {"productId": 698032, "name": "Tombstone, Career Criminal", "groupId": 1},
                {"productId": 698031, "name": "Tombstone, Career Criminal (Surge Foil)", "groupId": 1},
            ],
            known_tcg_ids=[698031],
        )
        # Key is the Scryfall-anchored surge product; value is the regular product.
        assert mapping == {"698031": "698032"}

    def test_both_products_known_emits_no_mapping(self) -> None:
        """When both products are distinct Scryfall cards, emit no pairing."""
        mapping = _build_lookup(
            products=[
                {"productId": 2000, "name": "Dual Card", "groupId": 3},
                {"productId": 2001, "name": "Dual Card (Foil)", "groupId": 3},
            ],
            known_tcg_ids=[2000, 2001],
        )
        assert not mapping

    def test_mixed_sets_resolve_independently(self) -> None:
        """Normal and flipped pairs in one build resolve to correct directions."""
        mapping = _build_lookup(
            products=[
                {"productId": 1000, "name": "Normal Card", "groupId": 2},
                {"productId": 1001, "name": "Normal Card (Rainbow Foil)", "groupId": 2},
                {"productId": 698032, "name": "Tombstone, Career Criminal", "groupId": 1},
                {"productId": 698031, "name": "Tombstone, Career Criminal (Surge Foil)", "groupId": 1},
            ],
            known_tcg_ids=[1000, 698031],
        )
        assert mapping == {"1000": "1001", "698031": "698032"}
