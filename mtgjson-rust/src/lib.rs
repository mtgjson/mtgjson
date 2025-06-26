use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::collections::HashMap;

// PyO3-compatible wrapper for JSON values
#[pyclass(name = "JsonValue")]
#[derive(Debug, Clone)]
pub struct PyJsonValue {
    #[pyo3(get, set)]
    pub value: String,
}

#[pymethods]
impl PyJsonValue {
    #[new]
    pub fn new(value: String) -> Self {
        Self { value }
    }
    
    pub fn __str__(&self) -> String {
        self.value.clone()
    }
    
    pub fn __repr__(&self) -> String {
        format!("JsonValue(\"{}\")", self.value)
    }
}

impl From<serde_json::Value> for PyJsonValue {
    fn from(value: serde_json::Value) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl From<PyJsonValue> for serde_json::Value {
    fn from(py_value: PyJsonValue) -> Self {
        serde_json::from_str(&py_value.value).unwrap_or(serde_json::Value::Null)
    }
}

// Re-export all modules
pub mod base;
pub mod card;
pub mod deck;
pub mod foreign_data;
pub mod game_formats;
pub mod identifiers;
pub mod leadership_skills;
pub mod legalities;
pub mod meta;
pub mod prices;
pub mod purchase_urls;
pub mod related_cards;
pub mod rulings;
pub mod sealed_product;
pub mod set;
pub mod translations;
pub mod utils;
pub mod set_builder;
pub mod compiled_classes;

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
    m.add_class::<PyJsonValue>()?;
    
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
    
    Ok(())
}