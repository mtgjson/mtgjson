use chrono::Utc;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use crate::base::JsonObject;
use std::collections::HashSet;

/// MTGJSON Meta Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonMetaObject")]
pub struct MtgjsonMetaObject {
    #[pyo3(get, set)]
    pub date: String,
    
    #[pyo3(get, set)]
    pub version: String,
}

#[pymethods]
impl MtgjsonMetaObject {
    #[new]
    #[pyo3(signature = (date = None, version = None))]
    pub fn new(date: Option<String>, version: Option<String>) -> Self {
        let date = date.unwrap_or_else(|| {
            Utc::now().format("%Y-%m-%d").to_string()
        });
        let version = version.unwrap_or_else(|| "5.2.2".to_string());
        
        Self { date, version }
    }

    /// Create with current date
    #[staticmethod]
    #[pyo3(signature = (version=None))]
    pub fn with_current_date(version: Option<String>) -> Self {
        Self::new(None, version)
    }

    /// Create with specific date
    #[staticmethod]
    #[pyo3(signature = (date, version=None))]
    pub fn with_date(date: String, version: Option<String>) -> Self {
        Self::new(Some(date), version)
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Get date as datetime object
    pub fn get_datetime(&self) -> PyResult<String> {
        // For Python compatibility, just return the string
        // Python side can parse it as needed
        Ok(self.date.clone())
    }
}

impl Default for MtgjsonMetaObject {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl JsonObject for MtgjsonMetaObject {}