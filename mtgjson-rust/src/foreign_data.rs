//! MTGJSON Singular Card.ForeignData Object
use crate::base::{skip_if_empty_optional_string, JsonObject};
use crate::identifiers::MtgjsonIdentifiers;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

//! MtgjsonForeignData
//!
//! This struct represents a single Magic: The Gathering card's foreign data.
//! It is used to store all the data for a single card's foreign data, including
//! its language, multiverse id, identifiers, face name, flavor text, name, text,
//! and type.
//!
//! Note: Language and MtgjsonIdentifiers are required, all other fields are optional.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonForeignData")]
pub struct MtgjsonForeignData {
    #[pyo3(get, set)]
    pub language: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub multiverse_id: Option<i32>,
    
    #[pyo3(get, set)]
    pub identifiers: MtgjsonIdentifiers,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub face_name: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub flavor_text: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub name: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub text: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub type_: Option<String>,
}

#[pymethods]
impl MtgjsonForeignData {
    #[new]
    pub fn new(language: String) -> Self {
        Self {
            language,
            multiverse_id: None,
            identifiers: MtgjsonIdentifiers::new(),
            face_name: None,
            flavor_text: None,
            name: None,
            text: None,
            type_: None,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let mut result = HashMap::new();
        
        result.insert("language".to_string(), serde_json::Value::String(self.language.clone()));
        
        if let Some(val) = self.multiverse_id {
            result.insert("multiverseId".to_string(), serde_json::Value::Number(val.into()));
        }
        
        // Include identifiers
        if let Ok(identifiers_json) = serde_json::to_value(&self.identifiers) {
            result.insert("identifiers".to_string(), identifiers_json);
        }
        
        if let Some(ref val) = self.face_name {
            if !val.is_empty() {
                result.insert("faceName".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.flavor_text {
            if !val.is_empty() {
                result.insert("flavorText".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.name {
            if !val.is_empty() {
                result.insert("name".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.text {
            if !val.is_empty() {
                result.insert("text".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.type_ {
            if !val.is_empty() {
                result.insert("type".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        Ok(result.into_py(py))
    }

    /// Check if foreign data has meaningful content
    pub fn has_content(&self) -> bool {
        self.name.is_some() || 
        self.text.is_some() || 
        self.flavor_text.is_some() ||
        self.type_.is_some() ||
        self.face_name.is_some()
    }

    /// Get display name (face_name if available, otherwise name)
    pub fn get_display_name(&self) -> Option<String> {
        self.face_name.clone().or_else(|| self.name.clone())
    }
}

impl Default for MtgjsonForeignData {
    fn default() -> Self {
        Self::new("English".to_string())
    }
}

impl JsonObject for MtgjsonForeignData {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("url".to_string());
        keys_to_skip.insert("number".to_string());
        keys_to_skip.insert("set_code".to_string());
        keys_to_skip
    }
}