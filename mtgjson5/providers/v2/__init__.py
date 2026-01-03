"""
V2 providers module for MTGJSON.

This module exports the second generation of data providers including
CardKingdom, GitHub, EDHREC Salt, and BulkData providers.
"""

from .bulk import BulkDataProvider
from .cardkingdom import CKProvider
from .salt import EdhrecSaltProvider
from .sealed import SealedDataProvider
from .tcgplayer import TCGProvider


__all__ = [
	"BulkDataProvider",
	"CKProvider",
	"EdhrecSaltProvider",
	"SealedDataProvider",
	"TCGProvider",
]
