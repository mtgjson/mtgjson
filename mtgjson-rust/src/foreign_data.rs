use crate::base::{skip_if_empty_optional_string, JsonObject};
use crate::identifiers::MtgjsonIdentifiers;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Card.ForeignData Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonForeignDataObject")]
pub struct MtgjsonForeignDataObject {
    #[pyo3(get, set)]
    pub language: String,
    
    /// Deprecated - Remove in 5.4.0
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
impl MtgjsonForeignDataObject {
    #[new]
    pub fn new() -> Self {
        Self {
            face_name: None,
            flavor_text: None,
            language: String::new(),
            multiverse_id: None,
            name: None,
            text: None,
            type_: None,
            identifiers: MtgjsonIdentifiers::new(),
        }
    }

    /// Convert to JSON string
    pub fn to_json_string(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self) -> PyResult<HashMap<String, PyObject>> {
        Python::with_gil(|py| {
            let mut result = HashMap::new();
            
            result.insert("language".to_string(), self.language.to_object(py));
            
            if let Some(val) = self.multiverse_id {
                result.insert("multiverseId".to_string(), val.to_object(py));
            }
            
            // Include identifiers as dict
            let identifiers_dict = self.identifiers.to_dict()?;
            result.insert("identifiers".to_string(), identifiers_dict.to_object(py));
            
            if let Some(ref val) = self.face_name {
                if !val.is_empty() {
                    result.insert("faceName".to_string(), val.to_object(py));
                }
            }
            
            if let Some(ref val) = self.flavor_text {
                if !val.is_empty() {
                    result.insert("flavorText".to_string(), val.to_object(py));
                }
            }
            
            if let Some(ref val) = self.name {
                if !val.is_empty() {
                    result.insert("name".to_string(), val.to_object(py));
                }
            }
            
            if let Some(ref val) = self.text {
                if !val.is_empty() {
                    result.insert("text".to_string(), val.to_object(py));
                }
            }
            
            if let Some(ref val) = self.type_ {
                if !val.is_empty() {
                    result.insert("type".to_string(), val.to_object(py));
                }
            }
            
            Ok(result)
        })
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

impl Default for MtgjsonForeignDataObject {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonForeignDataObject {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("url".to_string());
        keys_to_skip.insert("number".to_string());
        keys_to_skip.insert("set_code".to_string());
        keys_to_skip
    }
}