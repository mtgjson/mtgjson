use crate::base::JsonObject;
use crate::deck::MtgjsonDeckHeader;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// MTGJSON DeckList Object
/// Rust equivalent of MtgjsonDeckListObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonDeckList")]
pub struct MtgjsonDeckList {
    #[pyo3(get, set)]
    pub decks: Vec<MtgjsonDeckHeader>,
}

#[pymethods]
impl MtgjsonDeckList {
    #[new]
    pub fn new(deck_headers: Vec<MtgjsonDeckHeader>) -> Self {
        Self { 
            decks: deck_headers 
        }
    }

    /// Create empty deck list
    #[staticmethod]
    pub fn empty() -> Self {
        Self { 
            decks: Vec::new() 
        }
    }

    /// Add a deck header to the list
    pub fn add_deck(&mut self, deck_header: MtgjsonDeckHeader) {
        self.decks.push(deck_header);
    }

    /// Add multiple deck headers to the list
    pub fn add_decks(&mut self, deck_headers: Vec<MtgjsonDeckHeader>) {
        self.decks.extend(deck_headers);
    }

    /// Remove a deck by code
    pub fn remove_deck_by_code(&mut self, code: &str) -> bool {
        if let Some(pos) = self.decks.iter().position(|deck| deck.code == code) {
            self.decks.remove(pos);
            true
        } else {
            false
        }
    }

    /// Find a deck by code
    pub fn find_deck_by_code(&self, code: &str) -> Option<&MtgjsonDeckHeader> {
        self.decks.iter().find(|deck| deck.code == code)
    }

    /// Find decks by type
    pub fn find_decks_by_type(&self, deck_type: &str) -> Vec<&MtgjsonDeckHeader> {
        self.decks
            .iter()
            .filter(|deck| deck.type_ == deck_type)
            .collect()
    }

    /// Get deck count
    pub fn deck_count(&self) -> usize {
        self.decks.len()
    }

    /// Sort decks by name
    pub fn sort_by_name(&mut self) {
        self.decks.sort_by(|a, b| a.name.cmp(&b.name));
    }

    /// Sort decks by release date
    pub fn sort_by_release_date(&mut self) {
        self.decks.sort_by(|a, b| a.release_date.cmp(&b.release_date));
    }

    /// Sort decks by code
    pub fn sort_by_code(&mut self) {
        self.decks.sort_by(|a, b| a.code.cmp(&b.code));
    }

    /// Filter decks by release year
    pub fn filter_by_year(&self, year: u16) -> Vec<&MtgjsonDeckHeader> {
        let year_str = year.to_string();
        self.decks
            .iter()
            .filter(|deck| deck.release_date.starts_with(&year_str))
            .collect()
    }

    /// Get all unique deck types
    pub fn get_unique_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self.decks
            .iter()
            .map(|deck| deck.type_.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        types.sort();
        types
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.decks).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Get the decks list (for JSON serialization)
    pub fn get_decks(&self) -> Vec<MtgjsonDeckHeader> {
        self.decks.clone()
    }
}

impl Default for MtgjsonDeckList {
    fn default() -> Self {
        Self::empty()
    }
}

impl JsonObject for MtgjsonDeckList {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_deck_header(code: &str, name: &str, deck_type: &str, release_date: &str) -> MtgjsonDeckHeader {
        MtgjsonDeckHeader::from_deck_data(
            code.to_string(),
            name.to_string(),
            release_date.to_string(),
            deck_type.to_string(),
            format!("{}.json", code),
        )
    }

    #[test]
    fn test_deck_list_creation() {
        let deck_headers = vec![
            create_test_deck_header("TEST1", "Test Deck 1", "standard", "2023-01-01"),
            create_test_deck_header("TEST2", "Test Deck 2", "commander", "2023-02-01"),
        ];
        
        let deck_list = MtgjsonDeckList::new(deck_headers);
        assert_eq!(deck_list.deck_count(), 2);
        assert!(deck_list.find_deck_by_code("TEST1").is_some());
        assert!(deck_list.find_deck_by_code("TEST3").is_none());
    }

    #[test]
    fn test_add_remove_decks() {
        let mut deck_list = MtgjsonDeckList::empty();
        assert_eq!(deck_list.deck_count(), 0);
        
        let deck_header = create_test_deck_header("TEST1", "Test Deck", "standard", "2023-01-01");
        deck_list.add_deck(deck_header);
        assert_eq!(deck_list.deck_count(), 1);
        
        assert!(deck_list.remove_deck_by_code("TEST1"));
        assert_eq!(deck_list.deck_count(), 0);
        assert!(!deck_list.remove_deck_by_code("NONEXISTENT"));
    }

    #[test]
    fn test_filter_by_type() {
        let deck_headers = vec![
            create_test_deck_header("CMDR1", "Commander Deck 1", "commander", "2023-01-01"),
            create_test_deck_header("STD1", "Standard Deck 1", "standard", "2023-02-01"),
            create_test_deck_header("CMDR2", "Commander Deck 2", "commander", "2023-03-01"),
        ];
        
        let deck_list = MtgjsonDeckList::new(deck_headers);
        let commander_decks = deck_list.find_decks_by_type("commander");
        assert_eq!(commander_decks.len(), 2);
        
        let standard_decks = deck_list.find_decks_by_type("standard");
        assert_eq!(standard_decks.len(), 1);
    }

    #[test]
    fn test_unique_types() {
        let deck_headers = vec![
            create_test_deck_header("CMDR1", "Commander Deck 1", "commander", "2023-01-01"),
            create_test_deck_header("STD1", "Standard Deck 1", "standard", "2023-02-01"),
            create_test_deck_header("CMDR2", "Commander Deck 2", "commander", "2023-03-01"),
        ];
        
        let deck_list = MtgjsonDeckList::new(deck_headers);
        let unique_types = deck_list.get_unique_types();
        assert_eq!(unique_types, vec!["commander", "standard"]);
    }
}