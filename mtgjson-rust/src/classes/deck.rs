use serde::{Deserialize, Serialize};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use crate::classes::{JsonObject, MtgjsonSealedProductObject};
use crate::classes::base::skip_if_empty_vec;

/// MTGJSON Singular Deck Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonDeckObject")]
pub struct MtgjsonDeckObject {
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub main_board: Vec<String>,
    
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub side_board: Vec<String>,
    
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub display_commander: Vec<String>,
    
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub commander: Vec<String>,
    
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub planes: Vec<String>,
    
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub schemes: Vec<String>,

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
impl MtgjsonDeckObject {
    #[new]
    #[pyo3(signature = (deck_name = "", sealed_product_uuids = None))]
    pub fn new(deck_name: &str, sealed_product_uuids: Option<Vec<String>>) -> Self {
        let alpha_numeric_name = deck_name.chars()
            .filter(|c| c.is_alphanumeric())
            .collect::<String>()
            .to_lowercase();
        
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
        // Simple sanitization 
        let sanitized = name.chars()
            .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect::<String>();
        
        self.file_name = format!("{}_{}.json", sanitized, self.code);
    }

    /// Update the UUID for the deck to link back to sealed product, if able
    pub fn add_sealed_product_uuids(&mut self, mtgjson_set_sealed_products: Vec<MtgjsonSealedProductObject>) {
        if self.sealed_product_uuids.is_none() {
            for sealed_product_entry in mtgjson_set_sealed_products {
                if let Some(ref name) = sealed_product_entry.name {
                    let sealed_name = name.to_lowercase();
                    if sealed_name.contains(&self.alpha_numeric_name) {
                        if let Some(uuid) = sealed_product_entry.uuid {
                            self.sealed_product_uuids = Some(vec![uuid]);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Populate deck from API
    pub fn populate_deck_from_api(
        &mut self,
        _mtgjson_deck_header: crate::deck::MtgjsonDeckHeaderObject,
        mtgjson_set_sealed_products: Vec<crate::sealed_product::MtgjsonSealedProductObject>
    ) {
        for sealed_product_entry in mtgjson_set_sealed_products {
            if let Some(ref name) = sealed_product_entry.name {
                let sealed_name = name.to_lowercase();
                if sealed_name.contains("deck") {
                    if let Some(uuid) = sealed_product_entry.uuid {
                        self.sealed_product_uuids = Some(vec![uuid]);
                    }
                }
            }
        }
    }

    /// Add card to main board as JSON string
    pub fn add_main_board_card(&mut self, card_json: String) {
        self.main_board.push(card_json);
    }

    /// Add card to side board as JSON string  
    pub fn add_side_board_card(&mut self, card_json: String) {
        self.side_board.push(card_json);
    }

    /// Add commander card as JSON string
    pub fn add_commander_card(&mut self, card_json: String) {
        self.commander.push(card_json);
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

impl Default for MtgjsonDeckObject {
    fn default() -> Self {
        Self::new("", None)
    }
}

impl JsonObject for MtgjsonDeckObject {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("file_name".to_string());
        keys_to_skip
    }
}

/// MTGJSON Singular Deck Header Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonDeckHeaderObject")]
pub struct MtgjsonDeckHeaderObject {
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
impl MtgjsonDeckHeaderObject {
    #[new]
    pub fn new(output_deck: &MtgjsonDeckObject) -> Self {
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

impl JsonObject for MtgjsonDeckHeaderObject {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deck_header_creation() {
        let header = MtgjsonDeckHeaderObject::new();
        assert_eq!(header.code, "");
        assert_eq!(header.name, "");
        assert_eq!(header.release_date, "");
        assert_eq!(header.type_, "");
        assert_eq!(header.file_name, "");
    }

    #[test]
    fn test_deck_header_default() {
        let header = MtgjsonDeckHeaderObject::default();
        assert_eq!(header.code, "");
        assert_eq!(header.name, "");
        assert_eq!(header.release_date, "");
        assert_eq!(header.type_, "");
        assert_eq!(header.file_name, "");
    }

    #[test]
    fn test_deck_creation() {
        let deck = MtgjsonDeckObject::new();
        assert_eq!(deck.code, "");
        assert_eq!(deck.name, "");
        assert_eq!(deck.release_date, "");
        assert_eq!(deck.type_, "");
        assert_eq!(deck.file_name, "");
        assert!(deck.main_board.is_empty());
        assert!(deck.side_board.is_empty());
        assert!(deck.commander.is_empty());
    }

    #[test]
    fn test_deck_default() {
        let deck = MtgjsonDeckObject::default();
        assert_eq!(deck.code, "");
        assert_eq!(deck.name, "");
        assert!(deck.main_board.is_empty());
        assert!(deck.side_board.is_empty());
        assert!(deck.commander.is_empty());
    }

    #[test]
    fn test_deck_header_json_serialization() {
        let mut header = MtgjsonDeckHeaderObject::new();
        header.code = "TST".to_string();
        header.name = "Test Deck".to_string();
        header.release_date = "2023-01-01".to_string();
        header.type_ = "constructed".to_string();
        
        let json_result = header.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("TST"));
        assert!(json_string.contains("Test Deck"));
        assert!(json_string.contains("2023-01-01"));
        assert!(json_string.contains("constructed"));
    }

    #[test]
    fn test_deck_json_serialization() {
        let mut deck = MtgjsonDeckObject::new();
        deck.code = "TST".to_string();
        deck.name = "Test Deck".to_string();
        deck.release_date = "2023-01-01".to_string();
        deck.type_ = "constructed".to_string();
        
        let json_result = deck.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("TST"));
        assert!(json_string.contains("Test Deck"));
        assert!(json_string.contains("2023-01-01"));
        assert!(json_string.contains("constructed"));
    }

    #[test]
    fn test_deck_header_string_representations() {
        let mut header = MtgjsonDeckHeaderObject::new();
        header.name = "Test Deck Header".to_string();
        header.code = "TST".to_string();
        
        let str_repr = header.__str__();
        assert!(str_repr.contains("Test Deck Header"));
        assert!(str_repr.contains("TST"));
        
        let repr = header.__repr__();
        assert!(repr.contains("Test Deck Header"));
        assert!(repr.contains("TST"));
    }

    #[test]
    fn test_deck_string_representations() {
        let mut deck = MtgjsonDeckObject::new();
        deck.name = "Test Deck".to_string();
        deck.code = "TST".to_string();
        
        let str_repr = deck.__str__();
        assert!(str_repr.contains("Test Deck"));
        assert!(str_repr.contains("TST"));
        
        let repr = deck.__repr__();
        assert!(repr.contains("Test Deck"));
        assert!(repr.contains("TST"));
    }

    #[test]
    fn test_deck_header_equality() {
        let mut header1 = MtgjsonDeckHeaderObject::new();
        let mut header2 = MtgjsonDeckHeaderObject::new();
        
        header1.code = "TST".to_string();
        header2.code = "TST".to_string();
        
        assert!(header1.__eq__(&header2));
        
        header2.code = "DIFF".to_string();
        assert!(!header1.__eq__(&header2));
    }

    #[test]
    fn test_deck_equality() {
        let mut deck1 = MtgjsonDeckObject::new();
        let mut deck2 = MtgjsonDeckObject::new();
        
        deck1.code = "TST".to_string();
        deck2.code = "TST".to_string();
        
        assert!(deck1.__eq__(&deck2));
        
        deck2.code = "DIFF".to_string();
        assert!(!deck1.__eq__(&deck2));
    }

    #[test]
    fn test_deck_header_hash() {
        let mut header = MtgjsonDeckHeaderObject::new();
        header.code = "TST".to_string();
        
        let hash1 = header.__hash__();
        let hash2 = header.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_deck_hash() {
        let mut deck = MtgjsonDeckObject::new();
        deck.code = "TST".to_string();
        
        let hash1 = deck.__hash__();
        let hash2 = deck.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_deck_cards_manipulation() {
        let mut deck = MtgjsonDeckObject::new();
        
        // Add cards to main board
        let card1 = MtgjsonCardObject::new(false);
        let card2 = MtgjsonCardObject::new(false);
        deck.main_board.push(card1);
        deck.main_board.push(card2);
        
        // Add cards to side board
        let card3 = MtgjsonCardObject::new(false);
        deck.side_board.push(card3);
        
        // Add commander
        let commander = MtgjsonCardObject::new(false);
        deck.commander.push(commander);
        
        assert_eq!(deck.main_board.len(), 2);
        assert_eq!(deck.side_board.len(), 1);
        assert_eq!(deck.commander.len(), 1);
    }

    #[test]
    fn test_deck_optional_fields() {
        let mut deck = MtgjsonDeckObject::new();
        
        // Test setting optional fields
        deck.is_foil_override = Some(true);
        deck.tcgplayer_deck_id = Some(12345);
        
        assert_eq!(deck.is_foil_override, Some(true));
        assert_eq!(deck.tcgplayer_deck_id, Some(12345));
    }

    #[test]
    fn test_deck_header_optional_fields() {
        let mut header = MtgjsonDeckHeaderObject::new();
        
        // Test setting optional fields
        header.is_foil_override = Some(true);
        header.tcgplayer_deck_id = Some(67890);
        
        assert_eq!(header.is_foil_override, Some(true));
        assert_eq!(header.tcgplayer_deck_id, Some(67890));
    }

    #[test]
    fn test_deck_file_name() {
        let mut deck = MtgjsonDeckObject::new();
        deck.file_name = "test_deck.json".to_string();
        
        assert_eq!(deck.file_name, "test_deck.json");
    }

    #[test]
    fn test_deck_header_file_name() {
        let mut header = MtgjsonDeckHeaderObject::new();
        header.file_name = "test_header.json".to_string();
        
        assert_eq!(header.file_name, "test_header.json");
    }

    #[test]
    fn test_json_object_trait_deck() {
        let deck = MtgjsonDeckObject::new();
        let keys_to_skip = deck.build_keys_to_skip();
        
        // Keys to skip should be empty for deck unless specifically implemented
        assert!(keys_to_skip.is_empty());
    }

    #[test]
    fn test_json_object_trait_deck_header() {
        let header = MtgjsonDeckHeaderObject::new();
        let keys_to_skip = header.build_keys_to_skip();
        
        // Keys to skip should be empty for deck header unless specifically implemented  
        assert!(keys_to_skip.is_empty());
    }

    #[test]
    fn test_deck_complex_scenario() {
        let mut deck = MtgjsonDeckObject::new();
        
        // Set up a complex deck scenario
        deck.code = "EDH01".to_string();
        deck.name = "Commander Deck Test".to_string();
        deck.type_ = "commander".to_string();
        deck.release_date = "2023-06-01".to_string();
        deck.is_foil_override = Some(false);
        deck.tcgplayer_deck_id = Some(999999);
        
        // Add various cards
        for i in 0..60 {
            let mut card = MtgjsonCardObject::new(false);
            card.name = format!("Card {}", i);
            card.uuid = format!("uuid-{}", i);
            deck.main_board.push(card);
        }
        
        for i in 0..15 {
            let mut card = MtgjsonCardObject::new(false);
            card.name = format!("Sideboard Card {}", i);
            card.uuid = format!("sb-uuid-{}", i);
            deck.side_board.push(card);
        }
        
        let mut commander = MtgjsonCardObject::new(false);
        commander.name = "Test Commander".to_string();
        commander.uuid = "commander-uuid".to_string();
        deck.commander.push(commander);
        
        // Verify the complex setup
        assert_eq!(deck.main_board.len(), 60);
        assert_eq!(deck.side_board.len(), 15);
        assert_eq!(deck.commander.len(), 1);
        assert_eq!(deck.type_, "commander");
        assert_eq!(deck.tcgplayer_deck_id, Some(999999));
        
        // Test JSON serialization with complex data
        let json_result = deck.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("Commander Deck Test"));
        assert!(json_string.contains("EDH01"));
        assert!(json_string.contains("Test Commander"));
        assert!(json_string.contains("Card 0"));
        assert!(json_string.contains("Sideboard Card 0"));
    }
}