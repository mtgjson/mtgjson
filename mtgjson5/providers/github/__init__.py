"""
GitHub provider package for MTGJSON data sources.

This package contains provider implementations that fetch data from various
GitHub repositories including boosters, sealed products, decks, and MTGSqlite data.
"""

from .github_boosters import GitHubBoostersProvider
from .github_card_sealed_products import GitHubCardSealedProductsProvider
from .github_decks import GitHubDecksProvider
from .github_mtgsqlite import GitHubMTGSqliteProvider
from .github_sealed import GitHubSealedProvider

__all__ = [
    "GitHubCardSealedProductsProvider",
    "GitHubBoostersProvider",
    "GitHubDecksProvider",
    "GitHubSealedProvider",
    "GitHubMTGSqliteProvider",
]
