//! MtgjsonLegalities

use crate::base::{skip_if_empty_optional_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MtgjsonLegalities
///
/// This struct represents the legalities for a single Magic: The Gathering card.
///
/// It is used to store all the data for a single card's legalities, including
/// its brawl, commander, duel, future, frontier, legacy, modern, pauper, penny,
/// pioneer, and vintage.
///
/// Note: All fields are required
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonLegalities")]
pub struct MtgjsonLegalities {
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub brawl: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub commander: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub duel: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub future: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub frontier: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub legacy: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub modern: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub pauper: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub penny: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub pioneer: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub standard: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub vintage: Option<String>,
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
        
        if let Some(ref status) = self.brawl {
            if status == "Legal" {
                legal_formats.push("brawl".to_string());
            }
        }
        if let Some(ref status) = self.commander {
            if status == "Legal" {
                legal_formats.push("commander".to_string());
            }
        }
        if let Some(ref status) = self.duel {
            if status == "Legal" {
                legal_formats.push("duel".to_string());
            }
        }
        if let Some(ref status) = self.future {
            if status == "Legal" {
                legal_formats.push("future".to_string());
            }
        }
        if let Some(ref status) = self.frontier {
            if status == "Legal" {
                legal_formats.push("frontier".to_string());
            }
        }
        if let Some(ref status) = self.legacy {
            if status == "Legal" {
                legal_formats.push("legacy".to_string());
            }
        }
        if let Some(ref status) = self.modern {
            if status == "Legal" {
                legal_formats.push("modern".to_string());
            }
        }
        if let Some(ref status) = self.pauper {
            if status == "Legal" {
                legal_formats.push("pauper".to_string());
            }
        }
        if let Some(ref status) = self.penny {
            if status == "Legal" {
                legal_formats.push("penny".to_string());
            }
        }
        if let Some(ref status) = self.pioneer {
            if status == "Legal" {
                legal_formats.push("pioneer".to_string());
            }
        }
        if let Some(ref status) = self.standard {
            if status == "Legal" {
                legal_formats.push("standard".to_string());
            }
        }
        if let Some(ref status) = self.vintage {
            if status == "Legal" {
                legal_formats.push("vintage".to_string());
            }
        }
        
        legal_formats
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut result = HashMap::new();
        
        if let Some(ref val) = self.brawl {
            result.insert("brawl".to_string(), val.clone());
        }
        if let Some(ref val) = self.commander {
            result.insert("commander".to_string(), val.clone());
        }
        if let Some(ref val) = self.duel {
            result.insert("duel".to_string(), val.clone());
        }
        if let Some(ref val) = self.future {
            result.insert("future".to_string(), val.clone());
        }
        if let Some(ref val) = self.frontier {
            result.insert("frontier".to_string(), val.clone());
        }
        if let Some(ref val) = self.legacy {
            result.insert("legacy".to_string(), val.clone());
        }
        if let Some(ref val) = self.modern {
            result.insert("modern".to_string(), val.clone());
        }
        if let Some(ref val) = self.pauper {
            result.insert("pauper".to_string(), val.clone());
        }
        if let Some(ref val) = self.penny {
            result.insert("penny".to_string(), val.clone());
        }
        if let Some(ref val) = self.pioneer {
            result.insert("pioneer".to_string(), val.clone());
        }
        if let Some(ref val) = self.standard {
            result.insert("standard".to_string(), val.clone());
        }
        if let Some(ref val) = self.vintage {
            result.insert("vintage".to_string(), val.clone());
        }
        
        Ok(result)
    }
}

impl JsonObject for MtgjsonLegalities {}