use crate::base::{skip_if_empty_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

    /// Get all legal formats
    pub fn get_legal_formats(&self) -> Vec<String> {
        let mut legal_formats = Vec::new();
        
        if self.brawl == "Legal" {
            legal_formats.push("brawl".to_string());
        }
        if self.commander == "Legal" {
            legal_formats.push("commander".to_string());
        }
        if self.duel == "Legal" {
            legal_formats.push("duel".to_string());
        }
        if self.future == "Legal" {
            legal_formats.push("future".to_string());
        }
        if self.frontier == "Legal" {
            legal_formats.push("frontier".to_string());
        }
        if self.legacy == "Legal" {
            legal_formats.push("legacy".to_string());
        }
        if self.modern == "Legal" {
            legal_formats.push("modern".to_string());
        }
        if self.pauper == "Legal" {
            legal_formats.push("pauper".to_string());
        }
        if self.penny == "Legal" {
            legal_formats.push("penny".to_string());
        }
        if self.pioneer == "Legal" {
            legal_formats.push("pioneer".to_string());
        }
        if self.standard == "Legal" {
            legal_formats.push("standard".to_string());
        }
        if self.vintage == "Legal" {
            legal_formats.push("vintage".to_string());
        }
        
        legal_formats
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut result = HashMap::new();
        
        if !self.brawl.is_empty() {
            result.insert("brawl".to_string(), self.brawl.clone());
        }
        if !self.commander.is_empty() {
            result.insert("commander".to_string(), self.commander.clone());
        }
        if !self.duel.is_empty() {
            result.insert("duel".to_string(), self.duel.clone());
        }
        if !self.future.is_empty() {
            result.insert("future".to_string(), self.future.clone());
        }
        if !self.frontier.is_empty() {
            result.insert("frontier".to_string(), self.frontier.clone());
        }
        if !self.legacy.is_empty() {
            result.insert("legacy".to_string(), self.legacy.clone());
        }
        if !self.modern.is_empty() {
            result.insert("modern".to_string(), self.modern.clone());
        }
        if !self.pauper.is_empty() {
            result.insert("pauper".to_string(), self.pauper.clone());
        }
        if !self.penny.is_empty() {
            result.insert("penny".to_string(), self.penny.clone());
        }
        if !self.pioneer.is_empty() {
            result.insert("pioneer".to_string(), self.pioneer.clone());
        }
        if !self.standard.is_empty() {
            result.insert("standard".to_string(), self.standard.clone());
        }
        if !self.vintage.is_empty() {
            result.insert("vintage".to_string(), self.vintage.clone());
        }
        
        Ok(result)
    }
}

impl JsonObject for MtgjsonLegalities {}