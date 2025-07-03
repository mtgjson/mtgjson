use crate::base::{skip_if_empty_vec, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// MTGJSON Related Cards Container
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonRelatedCardsObject")]
pub struct MtgjsonRelatedCardsObject {
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub reverse_related: Vec<String>,

    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub spellbook: Vec<String>,
}

#[pymethods]
impl MtgjsonRelatedCardsObject {
    #[new]
    pub fn new() -> Self {
        Self {
            reverse_related: Vec::new(),
            spellbook: Vec::new(),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Determine if this object contains any values
    pub fn present(&self) -> bool {
        !self.reverse_related.is_empty() || !self.spellbook.is_empty()
    }

    /// Add a reverse related card
    pub fn add_reverse_related(&mut self, card_id: String) {
        if !self.reverse_related.contains(&card_id) {
            self.reverse_related.push(card_id);
        }
    }

    /// Add a spellbook card
    pub fn add_spellbook(&mut self, card_id: String) {
        if !self.spellbook.contains(&card_id) {
            self.spellbook.push(card_id);
        }
    }

    /// Remove a reverse related card
    pub fn remove_reverse_related(&mut self, card_id: &str) -> bool {
        if let Some(pos) = self.reverse_related.iter().position(|x| x == card_id) {
            self.reverse_related.remove(pos);
            true
        } else {
            false
        }
    }

    /// Remove a spellbook card
    pub fn remove_spellbook(&mut self, card_id: &str) -> bool {
        if let Some(pos) = self.spellbook.iter().position(|x| x == card_id) {
            self.spellbook.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get total count of related cards
    pub fn total_count(&self) -> usize {
        self.reverse_related.len() + self.spellbook.len()
    }

    /// Clear all related cards
    pub fn clear(&mut self) {
        self.reverse_related.clear();
        self.spellbook.clear();
    }
}

impl JsonObject for MtgjsonRelatedCardsObject {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();

        if self.reverse_related.is_empty() {
            keys_to_skip.insert("reverse_related".to_string());
        }
        if self.spellbook.is_empty() {
            keys_to_skip.insert("spellbook".to_string());
        }

        keys_to_skip
    }
}
