use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// PyO3-compatible wrapper for JSON values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[pyclass(name = "JsonValue")]
pub struct JsonValue {
    #[pyo3(get, set)]
    pub value: String,
}

#[pymethods]
impl JsonValue {
    #[new]
    pub fn new(value: String) -> Self {
        Self { value }
    }
    
    /// Convert to JSON string
    pub fn to_json(&self) -> String {
        self.value.clone()
    }
}

// Classes module
mod classes;
// Providers module
mod providers;
// Compiled classes module
mod compiled_classes;
// Performance modules
mod builders;

// Import all classes
use classes::{
    MtgjsonCardObject, MtgjsonDeckObject, MtgjsonDeckHeaderObject, MtgjsonForeignDataObject,
    MtgjsonGameFormatsObject, MtgjsonIdentifiers, MtgjsonLeadershipSkillsObject,
    MtgjsonLegalitiesObject, MtgjsonMetaObject, MtgjsonPricesObject, MtgjsonPurchaseUrls,
    MtgjsonRelatedCardsObject, MtgjsonRulingObject, MtgjsonSealedProductObject,
    SealedProductCategory, SealedProductSubtype, MtgjsonSetObject, MtgjsonTranslations
};

// Import all provider classes
use providers::{
    CardHoarderProvider, CardKingdomProvider, CardMarketProvider, EdhrecProviderCardRanks,
    GathererProvider, GitHubBoostersProvider, GitHubCardSealedProductsProvider,
    GitHubDecksProvider, GitHubMTGSqliteProvider, GitHubSealedProvider,
    MTGBanProvider, MtgWikiProviderSecretLair, MultiverseBridgeProvider,
    ScryfallProvider, ScryfallProviderOrientationDetector,
    TCGPlayerProvider, WhatsInStandardProvider, WizardsProvider
};

// Import all compiled classes
use compiled_classes::{
    MtgjsonStructures, MtgjsonCompiledList, MtgjsonDeckObjectList, 
    MtgjsonKeywords, MtgjsonAllIdentifiers, MtgjsonAllPrintings,
    MtgjsonAtomicCards, MtgjsonCardTypesObject, MtgjsonEnumValues,
    MtgjsonSetObjectList, MtgjsonTcgplayerSkus
};

// Import all performance modules
use builders::{
    OutputGenerator, PriceBuilder, ParallelProcessor, ParallelIterator
};

// Export everything
pub use classes::*;
pub use providers::*;
pub use compiled_classes::*;
pub use builders::*;

/// Python module definition
#[pymodule]
fn mtgjson_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add the JSON value wrapper
    m.add_class::<JsonValue>()?;
    
    // Add all MTGJSON classes
    m.add_class::<MtgjsonCardObject>()?;
    m.add_class::<MtgjsonDeckObject>()?;
    m.add_class::<MtgjsonDeckHeaderObject>()?;
    m.add_class::<MtgjsonForeignDataObject>()?;
    m.add_class::<MtgjsonGameFormatsObject>()?;
    m.add_class::<MtgjsonIdentifiers>()?;
    m.add_class::<MtgjsonLeadershipSkillsObject>()?;
    m.add_class::<MtgjsonLegalitiesObject>()?;
    m.add_class::<MtgjsonMetaObject>()?;
    m.add_class::<MtgjsonPricesObject>()?;
    m.add_class::<MtgjsonPurchaseUrls>()?;
    m.add_class::<MtgjsonRelatedCardsObject>()?;
    m.add_class::<MtgjsonRulingObject>()?;
    m.add_class::<MtgjsonSealedProductObject>()?;
    m.add_class::<MtgjsonSetObject>()?;
    m.add_class::<MtgjsonTranslations>()?;
    
    // Add enums
    m.add_class::<SealedProductCategory>()?;
    m.add_class::<SealedProductSubtype>()?;
    
    // Add compiled classes
    m.add_class::<MtgjsonStructures>()?;
    m.add_class::<MtgjsonCompiledList>()?;
    m.add_class::<MtgjsonDeckObjectList>()?;
    m.add_class::<MtgjsonKeywords>()?;
    m.add_class::<MtgjsonAllIdentifiers>()?;
    m.add_class::<MtgjsonAllPrintings>()?;
    m.add_class::<MtgjsonAtomicCards>()?;
    m.add_class::<MtgjsonCardTypesObject>()?;
    m.add_class::<MtgjsonEnumValues>()?;
    m.add_class::<MtgjsonSetObjectList>()?;
    m.add_class::<MtgjsonTcgplayerSkus>()?;
    
    // Add high-performance classes
    m.add_class::<OutputGenerator>()?;
    m.add_class::<PriceBuilder>()?;
    m.add_class::<ParallelProcessor>()?;
    m.add_class::<ParallelIterator>()?;
    
    // Add set_builder module functions
    m.add_function(wrap_pyfunction!(set_builder_functions::parse_card_types, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::get_card_colors, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::get_card_cmc, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::is_number, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::parse_legalities, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::build_mtgjson_set, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::parse_foreign, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::parse_printings, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::parse_rulings, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::mark_duel_decks, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::enhance_cards_with_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(set_builder_functions::build_base_mtgjson_cards, m)?)?;
    
    // Add utility functions (when implemented)
    // m.add_function(wrap_pyfunction!(utils_functions::to_camel_case, m)?)?;
    // m.add_function(wrap_pyfunction!(utils_functions::make_windows_safe_filename, m)?)?;
    // m.add_function(wrap_pyfunction!(utils_functions::clean_card_number, m)?)?;
    
    // Add all provider classes for 100% Python API coverage
    providers::add_provider_classes_to_module(m)?;
    
    Ok(())
}