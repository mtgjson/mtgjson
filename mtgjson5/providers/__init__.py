"""
V2 providers module for MTGJSON.

This module exports the second generation of data providers including
CardKingdom, GitHub, EDHREC Salt, Scryfall, and pricing providers.
"""

from .cardhoarder import CardHoarderPriceProvider
from .cardkingdom import CKProvider
from .cardmarket import CardMarketProvider
from .gatherer import GathererProvider
from .github import SealedDataProvider
from .manapool import ManapoolPriceProvider
from .mtgwiki import SecretLairProvider
from .salt import EdhrecSaltProvider
from .scryfall import BulkDataProvider, OrientationDetector, ScryfallProvider
from .tcgplayer import TCGPlayerPriceProvider, TCGProvider
from .whats_in_standard import WhatsInStandardProvider
from .wizards import WizardsProvider

__all__ = [
    "BulkDataProvider",
    "CKProvider",
    "CardHoarderPriceProvider",
    "CardMarketProvider",
    "EdhrecSaltProvider",
    "GathererProvider",
    "ManapoolPriceProvider",
    "OrientationDetector",
    "ScryfallProvider",
    "SealedDataProvider",
    "SecretLairProvider",
    "TCGPlayerPriceProvider",
    "TCGProvider",
    "WhatsInStandardProvider",
    "WizardsProvider",
]
