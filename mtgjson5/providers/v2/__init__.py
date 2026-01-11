"""
V2 providers module for MTGJSON.

This module exports the second generation of data providers including
CardKingdom, GitHub, EDHREC Salt, and BulkData providers.
"""

from .cardkingdom import CKProvider
from .cardmarket import CardMarketProvider
from .github import SealedDataProvider
from .salt import EdhrecSaltProvider
from .scryfall import BulkDataProvider
from .tcgplayer import TCGProvider


__all__ = [
	"BulkDataProvider",
	"CKProvider",
	"CardMarketProvider",
	"EdhrecSaltProvider",
	"SealedDataProvider",
	"TCGProvider",
]
