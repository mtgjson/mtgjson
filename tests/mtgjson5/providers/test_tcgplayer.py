"""Test the TCGPlayer provider."""
from typing import Tuple, List

import pytest

from mtgjson5.classes import MtgjsonSealedProductCategory, MtgjsonSealedProductObject

testdata: List[Tuple[str, MtgjsonSealedProductCategory]] = [
    ("Double Masters - Booster Box Case", MtgjsonSealedProductCategory.CASE),
    ("Innistrad - Booster Box Case (6 boxes)", MtgjsonSealedProductCategory.CASE),
    ("Kamigawa: Neon Dynasty - Bundle Case", MtgjsonSealedProductCategory.CASE),
    ("Modern Horizons 2 - Collector Booster Display Case", MtgjsonSealedProductCategory.CASE),
    ("Fate Reforged - Intro Pack Display", MtgjsonSealedProductCategory.CASE),
    ("Shards of Alara Premium Foil Booster Pack Box", MtgjsonSealedProductCategory.CASE),
    ("Theros Beyond Death - Prerelease Pack Case", MtgjsonSealedProductCategory.CASE),
    ("Urza's Saga Tournament Pack Display ", MtgjsonSealedProductCategory.CASE),
    ("Double Masters - VIP Edition Box Case", MtgjsonSealedProductCategory.CASE),
    ("Exodus - Booster Box", MtgjsonSealedProductCategory.BOOSTER_BOX),
    ("Commander Legends - Collector Booster Display", MtgjsonSealedProductCategory.BOOSTER_BOX),
    ("Guilds of Ravnica: Mythic Edition", MtgjsonSealedProductCategory.BOOSTER_BOX),
    ("Legends - Booster Pack", MtgjsonSealedProductCategory.BOOSTER_PACK),
    ("Kaladesh - Prerelease Kit", MtgjsonSealedProductCategory.PRERELEASE_PACK),
    ("Battlebond Blister Pack", MtgjsonSealedProductCategory.DRAFT_SET),
    ("Gatecrash - Booster Battle Pack", MtgjsonSealedProductCategory.TWO_PLAYER_STARTER_SET),
    ("Throne of Eldraine Deluxe Collection", MtgjsonSealedProductCategory.BOX_SET),
    ("Throne of Eldraine - Brawl Deck [Set of 4]", MtgjsonSealedProductCategory.CASE),
    ('Throne of Eldraine - Brawl Deck "Faerie Schemes"', MtgjsonSealedProductCategory.COMMANDER_DECK),
    ("World Championship Deck Box: 2004 San Francisco", MtgjsonSealedProductCategory.DECK_BOX),
    ("Challenger Deck 2018: Counter Surge", MtgjsonSealedProductCategory.DECK),
    ("Kaladesh - Bundle", MtgjsonSealedProductCategory.BUNDLE),
    ("Magic 2011 (M11) Land Station", MtgjsonSealedProductCategory.LAND_STATION)
]


@pytest.mark.parametrize("product_name,expected", testdata)
def test_determine_mtgjson_sealed_product_category(product_name, expected):
    """Test the function that decides what the type is for a sealed product"""
    assert MtgjsonSealedProductObject().determine_mtgjson_sealed_product_category(product_name.lower()) == expected
