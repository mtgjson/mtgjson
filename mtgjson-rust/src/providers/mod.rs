use pyo3::prelude::*;
use thiserror::Error;

/// Provider error types
#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Authentication error: {0}")]
    AuthError(String),
    #[error("Rate limit exceeded")]
    RateLimitError,
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Processing error: {0}")]
    ProcessingError(String),
}

/// Result type for provider operations
pub type ProviderResult<T> = Result<T, ProviderError>;

// Core provider modules
pub mod provider_base;
pub mod third_party;

// Subdirectory provider modules
pub mod cardmarket;
pub mod edhrec;
pub mod github;
pub mod mtgwiki;
pub mod scryfall;

// Re-export main provider types and implementations from third_party
pub use provider_base::{AbstractProvider, BaseProvider, RateLimiter};
pub use third_party::cardhoarder::CardHoarderProvider;
pub use third_party::cardkingdom::CardKingdomProvider;
pub use third_party::gatherer::GathererProvider;
pub use third_party::mtgban::MTGBanProvider;
pub use third_party::multiverse_bridge::MultiverseBridgeProvider;
pub use third_party::tcgplayer::TCGPlayerProvider;
pub use third_party::whats_in_standard::WhatsInStandardProvider;
pub use third_party::wizards::WizardsProvider;

// Re-export providers from subdirectories
pub use cardmarket::monolith::CardMarketProvider;
pub use edhrec::card_ranks::EdhrecProviderCardRanks;
pub use github::boosters::GitHubBoostersProvider;
pub use github::card_sealed_products::GitHubCardSealedProductsProvider;
pub use github::decks::GitHubDecksProvider;
pub use github::mtgsqlite::GitHubMTGSqliteProvider;
pub use github::sealed::GitHubSealedProvider;
pub use mtgwiki::secret_lair::MtgWikiProviderSecretLair;
pub use scryfall::monolith::ScryfallProvider;
pub use scryfall::orientation_detector::ScryfallProviderOrientationDetector;

/// Convert ProviderError to PyErr for PyO3 compatibility
impl From<ProviderError> for pyo3::PyErr {
    fn from(err: ProviderError) -> Self {
        match err {
            ProviderError::NetworkError(msg) => pyo3::exceptions::PyConnectionError::new_err(msg),
            ProviderError::ParseError(msg) => pyo3::exceptions::PyValueError::new_err(msg),
            ProviderError::AuthError(msg) => pyo3::exceptions::PyPermissionError::new_err(msg),
            ProviderError::RateLimitError => {
                pyo3::exceptions::PyRuntimeError::new_err("Rate limit exceeded")
            }
            ProviderError::ConfigurationError(msg) => {
                pyo3::exceptions::PyRuntimeError::new_err(msg)
            }
            ProviderError::ProcessingError(msg) => pyo3::exceptions::PyRuntimeError::new_err(msg),
        }
    }
}

/// Add all provider classes to Python module
pub fn add_provider_classes_to_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Core providers from third_party
    m.add_class::<CardHoarderProvider>()?;
    m.add_class::<CardKingdomProvider>()?;
    m.add_class::<GathererProvider>()?;
    m.add_class::<MTGBanProvider>()?;
    m.add_class::<MultiverseBridgeProvider>()?;
    m.add_class::<TCGPlayerProvider>()?;
    m.add_class::<WhatsInStandardProvider>()?;
    m.add_class::<WizardsProvider>()?;

    // Subdirectory providers
    m.add_class::<CardMarketProvider>()?;
    m.add_class::<EdhrecProviderCardRanks>()?;
    m.add_class::<GitHubBoostersProvider>()?;
    m.add_class::<GitHubCardSealedProductsProvider>()?;
    m.add_class::<GitHubDecksProvider>()?;
    m.add_class::<GitHubMTGSqliteProvider>()?;
    m.add_class::<GitHubSealedProvider>()?;
    m.add_class::<MtgWikiProviderSecretLair>()?;
    m.add_class::<ScryfallProvider>()?;
    m.add_class::<ScryfallProviderOrientationDetector>()?;

    Ok(())
}
