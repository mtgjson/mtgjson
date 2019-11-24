"""
MTGJSON container for pricing data
"""
from typing import Dict


class MtgjsonPricesObject:
    """
    Pricing Container
    """

    paper: Dict[str, float]
    paper_foil: Dict[str, float]
    mtgo: Dict[str, float]
    mtgo_foil: Dict[str, float]

    def __init__(self):
        pass
