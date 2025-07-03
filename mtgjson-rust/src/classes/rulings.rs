use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// MTGJSON Singular Card.Rulings Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonRulingObject")]
pub struct MtgjsonRulingObject {
    #[pyo3(get, set)]
    pub date: String,

    #[pyo3(get, set)]
    pub text: String,
}

#[pymethods]
impl MtgjsonRulingObject {
    #[new]
    pub fn new(date: String, text: String) -> Self {
        Self { date, text }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Check if ruling is valid (has both date and text)
    pub fn is_valid(&self) -> bool {
        !self.date.is_empty() && !self.text.is_empty()
    }

    /// Get ruling summary (first 100 characters of text)
    pub fn get_summary(&self) -> String {
        if self.text.len() <= 100 {
            self.text.clone()
        } else {
            format!("{}...", &self.text[..97])
        }
    }

    /// Compare rulings by date (returns -1, 0, or 1)
    pub fn compare_by_date(&self, other: &MtgjsonRulingObject) -> i32 {
        match self.date.cmp(&other.date) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

impl JsonObject for MtgjsonRulingObject {}
