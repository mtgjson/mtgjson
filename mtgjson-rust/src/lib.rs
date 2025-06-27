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

// Import all modules  
mod base;
mod card;
mod deck;
mod foreign_data;
mod game_formats;
mod identifiers;
mod leadership_skills;
mod legalities;
mod meta;
mod prices;
mod purchase_urls;
mod related_cards;
mod rulings;
mod sealed_product;
mod set;
mod translations;
mod utils;

// High-computational modules
mod output_generator;
mod parallel_call;
mod price_builder;
mod set_builder;

// Wrapper module for set_builder functions to expose as PyO3 functions
mod set_builder_functions;

// Wrapper module for utility functions
mod utils_functions;

// Compiled classes
mod compiled_classes;

// Import all the structs
use card::MtgjsonCardObject;
use deck::{MtgjsonDeckObject, MtgjsonDeckHeaderObject};
use foreign_data::MtgjsonForeignDataObject;
use game_formats::MtgjsonGameFormatsObject;
use identifiers::MtgjsonIdentifiers;
use leadership_skills::MtgjsonLeadershipSkillsObject;
use legalities::MtgjsonLegalitiesObject;
use meta::MtgjsonMetaObject;
use prices::MtgjsonPricesObject;
use purchase_urls::MtgjsonPurchaseUrls;
use related_cards::MtgjsonRelatedCardsObject;
use rulings::MtgjsonRulingObject;
use sealed_product::{MtgjsonSealedProductObject, SealedProductCategory, SealedProductSubtype};
use set::MtgjsonSetObject;
use translations::MtgjsonTranslations;

// Import compiled classes
use compiled_classes::{
    MtgjsonStructures, MtgjsonCompiledList, MtgjsonDeckObjectList, 
    MtgjsonKeywords, MtgjsonAllIdentifiers, MtgjsonAllPrintings,
    MtgjsonAtomicCards, MtgjsonCardObjectTypes, MtgjsonEnumValues,
    MtgjsonSetObjectList, MtgjsonTcgplayerSkus
};

// Re-export for tests and external usage  
pub use output_generator::OutputGenerator;
pub use price_builder::PriceBuilder;
pub use parallel_call::{ParallelProcessor, ParallelIterator};
pub use set_builder_functions::*;

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
    m.add_class::<MtgjsonCardObjectTypes>()?;
    m.add_class::<MtgjsonEnumValues>()?;
    m.add_class::<MtgjsonSetObjectList>()?;
    m.add_class::<MtgjsonTcgplayerSkus>()?;
    
    // Add high-performance classes
    m.add_class::<output_generator::OutputGenerator>()?;
    m.add_class::<price_builder::PriceBuilder>()?;
    m.add_class::<parallel_call::ParallelProcessor>()?;
    m.add_class::<parallel_call::ParallelIterator>()?;
    
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
    
    // Add utility functions
    m.add_function(wrap_pyfunction!(utils_functions::to_camel_case, m)?)?;
    m.add_function(wrap_pyfunction!(utils_functions::make_windows_safe_filename, m)?)?;
    m.add_function(wrap_pyfunction!(utils_functions::clean_card_number, m)?)?;
    
    Ok(())
}