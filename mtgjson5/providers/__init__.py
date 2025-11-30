"""
Provider Dispatcher
"""

from .cardhoarder import CardHoarderProvider
from .cardkingdom import CardKingdomProvider
from .cardmarket.monolith import CardMarketProvider
from .edhrec.card_ranks import EdhrecProviderCardRanks
from .gatherer import GathererProvider
from .github.github_boosters import GitHubBoostersProvider
from .github.github_card_sealed_products import GitHubCardSealedProductsProvider
from .github.github_decks import GitHubDecksProvider
from .github.github_mtgsqlite import GitHubMTGSqliteProvider
from .github.github_sealed import GitHubSealedProvider
from .manapool.manapool_prices import ManapoolPricesProvider
from .mtgwiki.secret_lair import MtgWikiProviderSecretLair
from .multiversebridge import MultiverseBridgeProvider
from .scryfall.monolith import ScryfallProvider
from .scryfall.orientation_detector import ScryfallProviderOrientationDetector
from .scryfall.set_language_detector import ScryfallProviderSetLanguageDetector
from .tcgplayer import TCGPlayerProvider
from .uuid_cache import UuidCacheProvider
from .whats_in_standard import WhatsInStandardProvider
from .wizards import WizardsProvider


from .v2 import CardKingdomProviderV2
from .v2 import GitHubDataProvider
from .v2 import EdhrecSaltProvider


__all__ = [
    "CardHoarderProvider",
    "CardKingdomProvider",
    "CardMarketProvider",
    "EdhrecProviderCardRanks",
    "GathererProvider",
    "GitHubBoostersProvider",
    "GitHubCardSealedProductsProvider",
    "GitHubDecksProvider",
    "GitHubMTGSqliteProvider",
    "GitHubSealedProvider",
    "ManapoolPricesProvider",
    "MtgWikiProviderSecretLair",
    "MultiverseBridgeProvider",
    "ScryfallProvider",
    "ScryfallProviderOrientationDetector",
    "ScryfallProviderSetLanguageDetector",
    "TCGPlayerProvider",
    "UuidCacheProvider",
    "WhatsInStandardProvider",
    "WizardsProvider",
    "CardKingdomProviderV2",
    "GitHubDataProvider",
    "EdhrecSaltProvider",
]