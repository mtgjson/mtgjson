from dataclasses import dataclass, field
from .github_decks import GitHubDecksProvider
from .github_sealed import GitHubSealedProvider
from .github_boosters import GitHubBoostersProvider
from .github_mtgsqlite import GitHubMTGSqliteProvider
from .github_card_sealed_products import GitHubCardSealedProductsProvider

type GithubProviderType = (
    GitHubDecksProvider
    | GitHubSealedProvider
    | GitHubBoostersProvider
    | GitHubMTGSqliteProvider
    | GitHubCardSealedProductsProvider
)

@dataclass
class GithubProvider:
    """
    GitHubProvider container
    """

    decks: GitHubDecksProvider = field(
        default_factory=GitHubDecksProvider,
    )
    sealed: GitHubSealedProvider = field(
        default_factory=GitHubSealedProvider,
    )
    boosters: GitHubBoostersProvider = field(
        default_factory=GitHubBoostersProvider,
    )
    mtgsqlite: GitHubMTGSqliteProvider = field(
        default_factory=GitHubMTGSqliteProvider,
    )
    card_sealed_products: GitHubCardSealedProductsProvider = field(
        default_factory=GitHubCardSealedProductsProvider,
    )
    
    def get_provider(
        self, provider_name: str
    ) -> GithubProviderType:
        """
        Get a specific GitHub provider by name
        :param provider_name: Name of the provider to get
        :return: GitHub provider instance
        """
        provider_map: dict[str, GithubProviderType] = {
            "decks": self.decks,
            "sealed": self.sealed,
            "boosters": self.boosters,
            "mtgsqlite": self.mtgsqlite,
            "card_sealed_products": self.card_sealed_products,
        }
        return provider_map[provider_name]
    
    def get_all_providers(self) -> list[GithubProviderType]:
        """
        Get all GitHub providers
        :return: List of all GitHub provider instances
        """
        return [
            self.decks,
            self.sealed,
            self.boosters,
            self.mtgsqlite,
            self.card_sealed_products,
        ]
    
github_provider = GithubProvider()