"""
V2 providers module for MTGJSON.

This module exports the second generation of data providers including
CardKingdom, GitHub, EDHREC Salt, Scryfall, and pricing providers.
"""

from .cardkingdom import CKProvider
from .cardmarket import CardMarketProvider
from .cardhoarder import CardHoarderPriceProvider
from .github import SealedDataProvider
from .manapool import ManapoolPriceProvider
from .salt import EdhrecSaltProvider
from .scryfall import BulkDataProvider, ScryfallProvider
from .tcgplayer import TCGPlayerPriceProvider, TCGProvider

__all__ = [
    "BulkDataProvider",
    "CardHoarderPriceProvider",
    "CardMarketProvider",
    "CKProvider",
    "EdhrecSaltProvider",
    "ManapoolPriceProvider",
    "ScryfallProvider",
    "SealedDataProvider",
    "TCGPlayerPriceProvider",
    "TCGProvider",
]
