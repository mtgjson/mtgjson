"""
Provider Dispatcher
"""

from .cardhoarder import CardHoarderProvider
from .cardkingdom import CardKingdomProvider, CardKingdomSealedProvider
from .cardmarket.monolith import CardMarketProvider
from .cardmarket.set_name_translations import CardMarketProviderSetNameTranslations
from .edhrec.card_ranks import EdhrecProviderCardRanks
from .fandom.secret_lair import FandomProviderSecretLair
from .gatherer import GathererProvider
from .github_boosters import GitHubBoostersProvider
from .github_decks import GitHubDecksProvider
from .github_mtgsqlite import GitHubMTGSqliteProvider
from .github_sealed import GitHubSealedProvider
from .mtgban import MTGBanProvider
from .multiversebridge import MultiverseBridgeProvider
from .scryfall.monolith import ScryfallProvider
from .scryfall.orientation_detector import ScryfallProviderOrientationDetector
from .scryfall.set_language_detector import ScryfallProviderSetLanguageDetector
from .tcgplayer import TCGPlayerProvider
from .whats_in_standard import WhatsInStandardProvider
from .wizards import WizardsProvider
