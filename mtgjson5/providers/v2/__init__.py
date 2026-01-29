"""
V2 providers module for MTGJSON.

This module exports the second generation of data providers including
CardKingdom, GitHub, EDHREC Salt, BulkData, and pricing providers.
"""

from .cardkingdom import CKProvider
from .cardmarket import CardMarketProvider
from .cardhoarder import CardHoarderPriceProvider
from .github import SealedDataProvider
from .manapool import ManapoolPriceProvider
from .salt import EdhrecSaltProvider
from .scryfall import BulkDataProvider
from .tcgplayer import TCGPlayerPriceProvider, TCGProvider

__all__ = [
    "BulkDataProvider",
    "CardHoarderPriceProvider",
    "CardMarketProvider",
    "CKProvider",
    "EdhrecSaltProvider",
    "ManapoolPriceProvider",
    "SealedDataProvider",
    "TCGPlayerPriceProvider",
    "TCGProvider",
]
