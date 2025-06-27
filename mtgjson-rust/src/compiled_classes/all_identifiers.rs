use crate::base::JsonObject;
use crate::card::MtgjsonCardObject;
use crate::set::MtgjsonSetObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON AllIdentifiers Object
/// Rust equivalent of MtgjsonAllIdentifiersObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonAllIdentifiers")]
pub struct MtgjsonAllIdentifiers {
    #[pyo3(get, set)]
    pub all_identifiers_dict: HashMap<String, MtgjsonCardObject>,
}

#[pymethods]
impl MtgjsonAllIdentifiers {
    #[new]
    pub fn new() -> Self {
        Self {
            all_identifiers_dict: HashMap::new(),
        }
    }

    /// Create from AllPrintings data
    #[staticmethod]
    pub fn from_all_printings(all_printings: HashMap<String, MtgjsonSetObject>) -> PyResult<Self> {
        let mut all_identifiers_dict = HashMap::new();
        let mut duplicate_count = 0;

        for (_set_code, set_data) in all_printings {
            // Process regular cards
            for card in set_data.cards {
                if all_identifiers_dict.contains_key(&card.uuid) {
                    duplicate_count += 1;
                    eprintln!(
                        "Duplicate MTGJSON UUID {} detected for card: {}",
                        card.uuid, card.name
                    );
                    continue;
                }
                all_identifiers_dict.insert(card.uuid.clone(), card);
            }

            // Process tokens
            for token in set_data.tokens {
                if all_identifiers_dict.contains_key(&token.uuid) {
                    duplicate_count += 1;
                    eprintln!(
                        "Duplicate MTGJSON UUID {} detected for token: {}",
                        token.uuid, token.name
                    );
                    continue;
                }
                all_identifiers_dict.insert(token.uuid.clone(), token);
            }
        }

        if duplicate_count > 0 {
            eprintln!("Found {} duplicate UUIDs during AllIdentifiers creation", duplicate_count);
        }

        Ok(Self { all_identifiers_dict })
    }

    /// Add a card to the identifiers dictionary
    pub fn add_card(&mut self, card: MtgjsonCardObject) -> PyResult<bool> {
        if self.all_identifiers_dict.contains_key(&card.uuid) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Card with UUID {} already exists",
                card.uuid
            )));
        }
        
        self.all_identifiers_dict.insert(card.uuid.clone(), card);
        Ok(true)
    }

    /// Add a card, replacing if UUID already exists
    pub fn add_or_replace_card(&mut self, card: MtgjsonCardObject) -> bool {
        let was_replacement = self.all_identifiers_dict.contains_key(&card.uuid);
        self.all_identifiers_dict.insert(card.uuid.clone(), card);
        was_replacement
    }

    /// Get a card by UUID
    pub fn get_card_by_uuid(&self, uuid: &str) -> Option<usize> {
        self.all_identifiers_dict.get(uuid).map(|_| 0) // Return index 0 if found
    }

    /// Remove a card by UUID
    pub fn remove_card_by_uuid(&mut self, uuid: &str) -> Option<MtgjsonCardObject> {
        self.all_identifiers_dict.remove(uuid)
    }

    /// Check if a UUID exists
    pub fn contains_uuid(&self, uuid: &str) -> bool {
        self.all_identifiers_dict.contains_key(uuid)
    }

    /// Get all UUIDs
    pub fn get_all_uuids(&self) -> Vec<String> {
        self.all_identifiers_dict.keys().cloned().collect()
    }

    /// Get card count
    pub fn card_count(&self) -> usize {
        self.all_identifiers_dict.len()
    }

    /// Find cards by exact name match
    pub fn find_cards_by_name(&self, name: &str) -> Vec<usize> {
        self.all_identifiers_dict
            .values()
            .enumerate()
            .filter_map(|(i, card)| {
                if card.name == name {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find cards by partial name match
    pub fn find_cards_by_partial_name(&self, partial_name: &str) -> Vec<usize> {
        self.all_identifiers_dict
            .values()
            .enumerate()
            .filter_map(|(i, card)| {
                if card.name.to_lowercase().contains(&partial_name.to_lowercase()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find cards by set code
    pub fn find_cards_by_set(&self, set_code: &str) -> Vec<usize> {
        self.all_identifiers_dict
            .values()
            .enumerate()
            .filter_map(|(i, card)| {
                if card.set_code == set_code {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get statistics about the collection
    pub fn get_statistics(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        let mut token_count = 0;
        let mut card_count = 0;
        let mut set_codes = std::collections::HashSet::new();

        for card in self.all_identifiers_dict.values() {
            if card.is_token {
                token_count += 1;
            } else {
                card_count += 1;
            }
            set_codes.insert(card.set_code.clone());
        }

        stats.insert("total_entities".to_string(), self.all_identifiers_dict.len());
        stats.insert("cards".to_string(), card_count);
        stats.insert("tokens".to_string(), token_count);
        stats.insert("unique_sets".to_string(), set_codes.len());

        stats
    }

    /// Validate all UUIDs are unique (should always be true)
    pub fn validate_unique_uuids(&self) -> bool {
        self.all_identifiers_dict.len() == self.all_identifiers_dict.keys().collect::<std::collections::HashSet<_>>().len()
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.all_identifiers_dict).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Get the identifiers dictionary (for JSON serialization)
    pub fn get_identifiers_dict(&self) -> HashMap<String, MtgjsonCardObject> {
        self.all_identifiers_dict.clone()
    }

    /// Merge with another AllIdentifiers object
    pub fn merge(&mut self, other: &MtgjsonAllIdentifiers) -> usize {
        let mut conflicts = 0;
        
        for (uuid, card) in &other.all_identifiers_dict {
            if self.all_identifiers_dict.contains_key(uuid) {
                conflicts += 1;
                eprintln!("UUID conflict during merge: {}", uuid);
            } else {
                self.all_identifiers_dict.insert(uuid.clone(), card.clone());
            }
        }
        
        conflicts
    }
}

impl Default for MtgjsonAllIdentifiers {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonAllIdentifiers {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_card(uuid: &str, name: &str, set_code: &str) -> MtgjsonCardObject {
        let mut card = MtgjsonCardObject::new(false);
        card.uuid = uuid.to_string();
        card.name = name.to_string();
        card.set_code = set_code.to_string();
        card
    }

    #[test]
    fn test_all_identifiers_creation() {
        let all_identifiers = MtgjsonAllIdentifiers::new();
        assert_eq!(all_identifiers.card_count(), 0);
        assert!(all_identifiers.validate_unique_uuids());
    }

    #[test]
    fn test_add_cards() {
        let mut all_identifiers = MtgjsonAllIdentifiers::new();
        let card1 = create_test_card("uuid1", "Test Card 1", "TST");
        let card2 = create_test_card("uuid2", "Test Card 2", "TST");

        assert!(all_identifiers.add_card(card1).is_ok());
        assert!(all_identifiers.add_card(card2).is_ok());
        assert_eq!(all_identifiers.card_count(), 2);

        // Test duplicate UUID
        let duplicate_card = create_test_card("uuid1", "Duplicate Card", "TST");
        assert!(all_identifiers.add_card(duplicate_card).is_err());
    }

    #[test]
    fn test_find_cards() {
        let mut all_identifiers = MtgjsonAllIdentifiers::new();
        let card1 = create_test_card("uuid1", "Lightning Bolt", "LEA");
        let card2 = create_test_card("uuid2", "Lightning Strike", "M19");
        
        all_identifiers.add_card(card1).unwrap();
        all_identifiers.add_card(card2).unwrap();

        let lightning_cards = all_identifiers.find_cards_by_partial_name("Lightning");
        assert_eq!(lightning_cards.len(), 2);

        let bolt_cards = all_identifiers.find_cards_by_name("Lightning Bolt");
        assert_eq!(bolt_cards.len(), 1);
        assert_eq!(bolt_cards[0], 0);

        let lea_cards = all_identifiers.find_cards_by_set("LEA");
        assert_eq!(lea_cards.len(), 1);
        assert_eq!(lea_cards[0], 0);
    }

    #[test]
    fn test_statistics() {
        let mut all_identifiers = MtgjsonAllIdentifiers::new();
        let card1 = create_test_card("uuid1", "Card 1", "SET1");
        let mut token1 = create_test_card("uuid2", "Token 1", "SET1");
        token1.is_token = true;
        let card2 = create_test_card("uuid3", "Card 2", "SET2");

        all_identifiers.add_card(card1).unwrap();
        all_identifiers.add_card(token1).unwrap();
        all_identifiers.add_card(card2).unwrap();

        let stats = all_identifiers.get_statistics();
        assert_eq!(stats.get("total_entities"), Some(&3));
        assert_eq!(stats.get("cards"), Some(&2));
        assert_eq!(stats.get("tokens"), Some(&1));
        assert_eq!(stats.get("unique_sets"), Some(&2));
    }
}