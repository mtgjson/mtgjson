"""
Provider Dispatcher

Legacy providers are imported here for backwards compatibility.
V2 providers have moved to mtgjson5.v2.providers.
"""

# Re-export v2 providers for backwards compatibility with legacy pipeline
from mtgjson5.v2.providers import (
    BulkDataProvider,
    CardMarketProvider,
    CKProvider,
    EdhrecSaltProvider,
    SealedDataProvider,
    TCGProvider,
)

from .cardhoarder import CardHoarderProvider
from .cardkingdom import CardKingdomProvider
from .edhrec.card_ranks import EdhrecProviderCardRanks
from .enrichment_provider import EnrichmentProvider
from .gatherer import GathererProvider
from .github.github_boosters import GitHubBoostersProvider
from .github.github_card_sealed_products import GitHubCardSealedProductsProvider
from .github.github_decks import GitHubDecksProvider
from .github.github_mtgsqlite import GitHubMTGSqliteProvider
from .github.github_sealed import GitHubSealedProvider
from .github.github_token_products import GitHubTokenProductsProvider
from .manapool.manapool_prices import ManapoolPricesProvider
from .mtgwiki.secret_lair import MtgWikiProviderSecretLair
from .scryfall.monolith import ScryfallProvider
from .scryfall.orientation_detector import ScryfallProviderOrientationDetector
from .scryfall.set_language_detector import ScryfallProviderSetLanguageDetector
from .tcgplayer import TCGPlayerProvider
from .uuid_cache import UuidCacheProvider
from .whats_in_standard import WhatsInStandardProvider
from .wizards import WizardsProvider
