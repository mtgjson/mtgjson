//! MTGJSON Singular Deck Object
use crate::base::{skip_if_empty_optional_string, skip_if_empty_vec, JsonObject};
use crate::card::MtgjsonCard;
use crate::sealed_product::MtgjsonSealedProduct;
use crate::utils::MtgjsonUtils;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MtgjsonDeck
//!
//! This struct represents a single Magic: The Gathering deck.
//! It is used to store all the data for a single deck, including
//! its name, main board, side board, and other relevant information.
//!
//! Note: All fields are optional, so we must manually check for empty values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonDeck")]
pub struct MtgjsonDeck {
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub main_board: Vec<serde_json::Value>, // Could be MtgjsonCard or dict
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub side_board: Vec<serde_json::Value>, // Could be MtgjsonCard or dict
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub display_commander: Vec<serde_json::Value>, // Could be MtgjsonCard or dict
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub commander: Vec<serde_json::Value>, // Could be MtgjsonCard or dict
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub planes: Vec<serde_json::Value>, // Could be MtgjsonCard or dict
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub schemes: Vec<serde_json::Value>, // Could be MtgjsonCard or dict

    #[pyo3(get, set)]
    pub code: String,
    
    #[pyo3(get, set)]
    pub name: String,
    
    #[pyo3(get, set)]
    pub release_date: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub sealed_product_uuids: Option<Vec<String>>,
    
    #[pyo3(get, set)]
    pub type_: String,
    
    #[serde(skip)]
    #[pyo3(get, set)]
    pub file_name: String,

    // Internal field for deck name matching
    #[serde(skip)]
    alpha_numeric_name: String,
}

#[pymethods]
impl MtgjsonDeck {
    #[new]
    #[pyo3(signature = (deck_name = "", sealed_product_uuids = None))]
    pub fn new(deck_name: &str, sealed_product_uuids: Option<Vec<String>>) -> Self {
        let alpha_numeric_name = MtgjsonUtils::alpha_numeric_only(deck_name);
        
        Self {
            main_board: Vec::new(),
            side_board: Vec::new(),
            display_commander: Vec::new(),
            commander: Vec::new(),
            planes: Vec::new(),
            schemes: Vec::new(),
            code: String::new(),
            name: deck_name.to_string(),
            release_date: String::new(),
            sealed_product_uuids,
            type_: String::new(),
            file_name: String::new(),
            alpha_numeric_name,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Turn an unsanitary file name to a safe one
    pub fn set_sanitized_name(&mut self, name: &str) {
        self.file_name = MtgjsonUtils::sanitize_deck_name(name, &self.code);
    }

    /// Update the UUID for the deck to link back to sealed product, if able
    pub fn add_sealed_product_uuids(&mut self, mtgjson_set_sealed_products: &[MtgjsonSealedProduct]) {
        if self.sealed_product_uuids.is_none() {
            for sealed_product_entry in mtgjson_set_sealed_products {
                let sealed_name = sealed_product_entry.name.to_lowercase();
                if sealed_name.contains(&self.alpha_numeric_name) {
                    self.sealed_product_uuids = Some(vec![sealed_product_entry.uuid.clone()]);
                    break;
                }
            }
        }
    }

    /// Add a card to the main board
    pub fn add_main_board_card(&mut self, card: &MtgjsonCard) -> PyResult<()> {
        let card_value = serde_json::to_value(card).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Card serialization error: {}", e))
        })?;
        self.main_board.push(card_value);
        Ok(())
    }

    /// Add a card to the side board
    pub fn add_side_board_card(&mut self, card: &MtgjsonCard) -> PyResult<()> {
        let card_value = serde_json::to_value(card).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Card serialization error: {}", e))
        })?;
        self.side_board.push(card_value);
        Ok(())
    }

    /// Add a commander card
    pub fn add_commander_card(&mut self, card: &MtgjsonCard) -> PyResult<()> {
        let card_value = serde_json::to_value(card).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Card serialization error: {}", e))
        })?;
        self.commander.push(card_value);
        Ok(())
    }

    /// Get total number of cards in deck
    pub fn get_total_cards(&self) -> usize {
        self.main_board.len() + 
        self.side_board.len() + 
        self.commander.len() + 
        self.display_commander.len() +
        self.planes.len() +
        self.schemes.len()
    }

    /// Get main board card count
    pub fn get_main_board_count(&self) -> usize {
        self.main_board.len()
    }

    /// Get side board card count
    pub fn get_side_board_count(&self) -> usize {
        self.side_board.len()
    }

    /// Check if deck has any cards
    pub fn has_cards(&self) -> bool {
        self.get_total_cards() > 0
    }

    /// Clear all cards from deck
    pub fn clear_all_cards(&mut self) {
        self.main_board.clear();
        self.side_board.clear();
        self.commander.clear();
        self.display_commander.clear();
        self.planes.clear();
        self.schemes.clear();
    }
}

impl Default for MtgjsonDeck {
    fn default() -> Self {
        Self::new("", None)
    }
}

impl JsonObject for MtgjsonDeck {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("file_name".to_string());
        keys_to_skip
    }
}

//! MtgjsonDeckHeader
//!
//! This struct represents a single Magic: The Gathering deck header.
//! It is used to set the header information for a deck
//! Note: All fields are required - we must manually check for empty values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonDeckHeader")]
pub struct MtgjsonDeckHeader {
    #[pyo3(get, set)]
    pub code: String,
    
    #[serde(skip)]
    #[pyo3(get, set)]
    pub file_name: String,
    
    #[pyo3(get, set)]
    pub name: String,
    
    #[pyo3(get, set)]
    pub release_date: String,
    
    #[pyo3(get, set)]
    pub type_: String,
}

#[pymethods]
impl MtgjsonDeckHeader {
    #[new]
    pub fn new(output_deck: &MtgjsonDeck) -> Self {
        Self {
            code: output_deck.code.clone(),
            file_name: output_deck.file_name.clone(),
            name: output_deck.name.clone(),
            release_date: output_deck.release_date.clone(),
            type_: output_deck.type_.clone(),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Create from deck data
    #[staticmethod]
    pub fn from_deck_data(
        code: String,
        name: String,
        release_date: String,
        type_: String,
        file_name: String,
    ) -> Self {
        Self {
            code,
            file_name,
            name,
            release_date,
            type_,
        }
    }

    /// Get display information
    pub fn get_display_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::new();
        info.insert("code".to_string(), self.code.clone());
        info.insert("name".to_string(), self.name.clone());
        info.insert("releaseDate".to_string(), self.release_date.clone());
        info.insert("type".to_string(), self.type_.clone());
        info
    }
}

impl JsonObject for MtgjsonDeckHeader {}