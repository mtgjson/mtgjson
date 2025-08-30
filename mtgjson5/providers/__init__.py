"""
Provider Dispatcher
"""

from .cardhoarder import CardHoarderProvider
from .cardkingdom import CardKingdomProvider
from .cardmarket.monolith import CardMarketProvider
from .edhrec.card_ranks import EdhrecProviderCardRanks
from .gatherer import GathererProvider
from .github_boosters import GitHubBoostersProvider
from .github_card_sealed_products import GitHubCardSealedProductsProvider
from .github_decks import GitHubDecksProvider
from .github_mtgsqlite import GitHubMTGSqliteProvider
from .github_sealed import GitHubSealedProvider
from .manapool.manapool_prices import ManapoolPricesProvider
from .mtgwiki.secret_lair import MtgWikiProviderSecretLair
from .multiversebridge import MultiverseBridgeProvider
from .scryfall.monolith import ScryfallProvider
from .scryfall.orientation_detector import ScryfallProviderOrientationDetector
from .scryfall.set_language_detector import ScryfallProviderSetLanguageDetector
from .tcgplayer import TCGPlayerProvider
from .whats_in_standard import WhatsInStandardProvider
from .wizards import WizardsProvider
