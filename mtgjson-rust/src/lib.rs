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

// Compiled classes
mod compiled_classes;

// Import all the structs
use card::MtgjsonCard;
use deck::{MtgjsonDeck, MtgjsonDeckHeader};
use foreign_data::MtgjsonForeignData;
use game_formats::MtgjsonGameFormats;
use identifiers::MtgjsonIdentifiers;
use leadership_skills::MtgjsonLeadershipSkills;
use legalities::MtgjsonLegalities;
use meta::MtgjsonMeta;
use prices::MtgjsonPrices;
use purchase_urls::MtgjsonPurchaseUrls;
use related_cards::MtgjsonRelatedCards;
use rulings::MtgjsonRuling;
use sealed_product::{MtgjsonSealedProduct, SealedProductCategory, SealedProductSubtype};
use set::MtgjsonSet;
use translations::MtgjsonTranslations;

// Import compiled classes
use compiled_classes::{
    MtgjsonStructures, MtgjsonCompiledList, MtgjsonDeckList, 
    MtgjsonKeywords, MtgjsonAllIdentifiers, MtgjsonAllPrintings,
    MtgjsonAtomicCards, MtgjsonCardTypes, MtgjsonEnumValues,
    MtgjsonSetList, MtgjsonTcgplayerSkus
};

/// Python module definition
#[pymodule]
fn mtgjson_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add the JSON value wrapper
    m.add_class::<JsonValue>()?;
    
    // Add all MTGJSON classes
    m.add_class::<MtgjsonCard>()?;
    m.add_class::<MtgjsonDeck>()?;
    m.add_class::<MtgjsonDeckHeader>()?;
    m.add_class::<MtgjsonForeignData>()?;
    m.add_class::<MtgjsonGameFormats>()?;
    m.add_class::<MtgjsonIdentifiers>()?;
    m.add_class::<MtgjsonLeadershipSkills>()?;
    m.add_class::<MtgjsonLegalities>()?;
    m.add_class::<MtgjsonMeta>()?;
    m.add_class::<MtgjsonPrices>()?;
    m.add_class::<MtgjsonPurchaseUrls>()?;
    m.add_class::<MtgjsonRelatedCards>()?;
    m.add_class::<MtgjsonRuling>()?;
    m.add_class::<MtgjsonSealedProduct>()?;
    m.add_class::<MtgjsonSet>()?;
    m.add_class::<MtgjsonTranslations>()?;
    
    // Add enums
    m.add_class::<SealedProductCategory>()?;
    m.add_class::<SealedProductSubtype>()?;
    
    // Add compiled classes
    m.add_class::<MtgjsonStructures>()?;
    m.add_class::<MtgjsonCompiledList>()?;
    m.add_class::<MtgjsonDeckList>()?;
    m.add_class::<MtgjsonKeywords>()?;
    m.add_class::<MtgjsonAllIdentifiers>()?;
    m.add_class::<MtgjsonAllPrintings>()?;
    m.add_class::<MtgjsonAtomicCards>()?;
    m.add_class::<MtgjsonCardTypes>()?;
    m.add_class::<MtgjsonEnumValues>()?;
    m.add_class::<MtgjsonSetList>()?;
    m.add_class::<MtgjsonTcgplayerSkus>()?;
    
    // Add high-performance classes
    m.add_class::<output_generator::OutputGenerator>()?;
    m.add_class::<price_builder::PriceBuilder>()?;
    m.add_class::<parallel_call::ParallelProcessor>()?;
    m.add_class::<parallel_call::ParallelIterator>()?;
    
    Ok(())
}