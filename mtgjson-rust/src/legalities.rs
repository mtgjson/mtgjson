use crate::base::{skip_if_empty_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON Singular Card.Legalities Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonLegalities")]
pub struct MtgjsonLegalities {
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub brawl: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub commander: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub duel: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub future: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub frontier: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub legacy: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub modern: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub pauper: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub penny: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub pioneer: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub standard: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_string")]
    #[pyo3(get, set)]
    pub vintage: String,
}

#[pymethods]
impl MtgjsonLegalities {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Get all legal formats - optimized with pre-allocated vector
    pub fn get_legal_formats(&self) -> Vec<String> {
        let mut legal_formats = Vec::with_capacity(12); // Pre-allocate for max possible formats
        
        // Use a macro to reduce code duplication and improve performance
        macro_rules! check_format {
            ($field:expr, $name:literal) => {
                if $field == "Legal" {
                    legal_formats.push($name.to_string());
                }
            };
        }
        
        check_format!(self.brawl, "brawl");
        check_format!(self.commander, "commander");
        check_format!(self.duel, "duel");
        check_format!(self.future, "future");
        check_format!(self.frontier, "frontier");
        check_format!(self.legacy, "legacy");
        check_format!(self.modern, "modern");
        check_format!(self.pauper, "pauper");
        check_format!(self.penny, "penny");
        check_format!(self.pioneer, "pioneer");
        check_format!(self.standard, "standard");
        check_format!(self.vintage, "vintage");
        
        legal_formats
    }

    /// Convert to dictionary for Python compatibility - optimized
    pub fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut result = HashMap::with_capacity(12); // Pre-allocate capacity
        
        // Use a macro to reduce code duplication
        macro_rules! add_if_not_empty {
            ($field:expr, $key:literal) => {
                if !$field.is_empty() {
                    result.insert($key.to_string(), $field.clone());
                }
            };
        }
        
        add_if_not_empty!(self.brawl, "brawl");
        add_if_not_empty!(self.commander, "commander");
        add_if_not_empty!(self.duel, "duel");
        add_if_not_empty!(self.future, "future");
        add_if_not_empty!(self.frontier, "frontier");
        add_if_not_empty!(self.legacy, "legacy");
        add_if_not_empty!(self.modern, "modern");
        add_if_not_empty!(self.pauper, "pauper");
        add_if_not_empty!(self.penny, "penny");
        add_if_not_empty!(self.pioneer, "pioneer");
        add_if_not_empty!(self.standard, "standard");
        add_if_not_empty!(self.vintage, "vintage");
        
        Ok(result)
    }
}

impl JsonObject for MtgjsonLegalities {}