use crate::base::{skip_if_empty_optional_string, skip_if_empty_vec, JsonObject};
use crate::card::MtgjsonCardObject;
use crate::sealed_product::MtgjsonSealedProductObject;
use crate::utils::MtgjsonUtils;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub fn add_sealed_product_uuids(
        &mut self,
        mtgjson_set_sealed_products: Vec<MtgjsonSealedProductObject>,
    ) {
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
        mtgjson_set_sealed_products: Vec<crate::sealed_product::MtgjsonSealedProductObject>,
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
        self.main_board.len()
            + self.side_board.len()
            + self.commander.len()
            + self.display_commander.len()
            + self.planes.len()
            + self.schemes.len()
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

    /// String representation
    pub fn __str__(&self) -> String {
        format!(
            "MtgjsonDeckObject(name='{}', code='{}', total_cards={})",
            self.name,
            self.code,
            self.get_total_cards()
        )
    }

    /// Repr representation
    pub fn __repr__(&self) -> String {
        format!(
            "MtgjsonDeckObject(name='{}', code='{}', main_board={}, side_board={}, commander={})",
            self.name,
            self.code,
            self.main_board.len(),
            self.side_board.len(),
            self.commander.len()
        )
    }

    /// Equality comparison
    pub fn __eq__(&self, other: &MtgjsonDeckObject) -> bool {
        self.name == other.name
            && self.code == other.code
            && self.release_date == other.release_date
    }

    /// Hash method
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.code.hash(&mut hasher);
        hasher.finish()
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

    /// String representation
    pub fn __str__(&self) -> String {
        format!(
            "MtgjsonDeckHeaderObject(name='{}', code='{}', type='{}')",
            self.name, self.code, self.type_
        )
    }

    /// Repr representation
    pub fn __repr__(&self) -> String {
        format!(
            "MtgjsonDeckHeaderObject(code='{}', name='{}', release_date='{}', type='{}')",
            self.code, self.name, self.release_date, self.type_
        )
    }

    /// Equality comparison
    pub fn __eq__(&self, other: &MtgjsonDeckHeaderObject) -> bool {
        self.name == other.name
            && self.code == other.code
            && self.release_date == other.release_date
            && self.type_ == other.type_
    }

    /// Hash method
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.code.hash(&mut hasher);
        hasher.finish()
    }
}

impl JsonObject for MtgjsonDeckHeaderObject {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deck_header_creation() {
        let deck = MtgjsonDeckObject::new("Test Deck", None);
        let header = MtgjsonDeckHeaderObject::new(&deck);
        assert_eq!(header.code, "");
        assert_eq!(header.name, "Test Deck");
        assert_eq!(header.release_date, "");
        assert_eq!(header.type_, "");
        assert_eq!(header.file_name, "");
    }

    #[test]
    fn test_deck_header_from_deck_data() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test Deck".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test_deck.json".to_string(),
        );

        assert_eq!(header.code, "TST");
        assert_eq!(header.name, "Test Deck");
        assert_eq!(header.release_date, "2023-01-01");
        assert_eq!(header.type_, "constructed");
        assert_eq!(header.file_name, "test_deck.json");
    }

    #[test]
    fn test_deck_header_get_display_info() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test Deck".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test_deck.json".to_string(),
        );

        let info = header.get_display_info();
        assert_eq!(info.get("code"), Some(&"TST".to_string()));
        assert_eq!(info.get("name"), Some(&"Test Deck".to_string()));
        assert_eq!(info.get("releaseDate"), Some(&"2023-01-01".to_string()));
        assert_eq!(info.get("type"), Some(&"constructed".to_string()));
    }

    #[test]
    fn test_deck_creation() {
        let deck = MtgjsonDeckObject::new("My Deck", None);
        assert_eq!(deck.code, "");
        assert_eq!(deck.name, "My Deck");
        assert_eq!(deck.release_date, "");
        assert_eq!(deck.type_, "");
        assert_eq!(deck.file_name, "");
        assert!(deck.main_board.is_empty());
        assert!(deck.side_board.is_empty());
        assert!(deck.commander.is_empty());
        assert!(deck.display_commander.is_empty());
        assert!(deck.planes.is_empty());
        assert!(deck.schemes.is_empty());
    }

    #[test]
    fn test_deck_creation_with_sealed_products() {
        let sealed_uuids = vec!["uuid1".to_string(), "uuid2".to_string()];
        let deck = MtgjsonDeckObject::new("Deck with Products", Some(sealed_uuids.clone()));

        assert_eq!(deck.name, "Deck with Products");
        assert_eq!(deck.sealed_product_uuids, Some(sealed_uuids));
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
    fn test_deck_set_sanitized_name() {
        let mut deck = MtgjsonDeckObject::new("", None);
        deck.code = "TST".to_string();
        deck.set_sanitized_name("Unsafe/Name\\With:Chars");

        // Should sanitize the name
        assert!(!deck.file_name.contains("/"));
        assert!(!deck.file_name.contains("\\"));
        assert!(!deck.file_name.contains(":"));
    }

    #[test]
    fn test_deck_add_sealed_product_uuids() {
        let mut deck = MtgjsonDeckObject::new("Test Deck Name", None);

        let mut sealed_product = MtgjsonSealedProductObject::new();
        sealed_product.name = Some("Test Deck Name Sealed".to_string());
        sealed_product.uuid = Some("test-uuid".to_string());

        let sealed_products = vec![sealed_product];
        deck.add_sealed_product_uuids(sealed_products);

        assert!(deck.sealed_product_uuids.is_some());
        assert_eq!(
            deck.sealed_product_uuids.unwrap(),
            vec!["test-uuid".to_string()]
        );
    }

    #[test]
    fn test_deck_populate_deck_from_api() {
        let mut deck = MtgjsonDeckObject::new("Test", None);
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test Header".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );

        let mut sealed_product = MtgjsonSealedProductObject::new();
        sealed_product.name = Some("Test Deck Product".to_string());
        sealed_product.uuid = Some("deck-uuid".to_string());

        let sealed_products = vec![sealed_product];
        deck.populate_deck_from_api(header, sealed_products);

        assert!(deck.sealed_product_uuids.is_some());
        assert_eq!(
            deck.sealed_product_uuids.unwrap(),
            vec!["deck-uuid".to_string()]
        );
    }

    #[test]
    fn test_deck_add_main_board_card() {
        let mut deck = MtgjsonDeckObject::new("", None);
        let card_json = r#"{"name": "Lightning Bolt", "count": 4}"#.to_string();

        deck.add_main_board_card(card_json.clone());

        assert_eq!(deck.main_board.len(), 1);
        assert_eq!(deck.main_board[0], card_json);
    }

    #[test]
    fn test_deck_add_side_board_card() {
        let mut deck = MtgjsonDeckObject::new("", None);
        let card_json = r#"{"name": "Counterspell", "count": 2}"#.to_string();

        deck.add_side_board_card(card_json.clone());

        assert_eq!(deck.side_board.len(), 1);
        assert_eq!(deck.side_board[0], card_json);
    }

    #[test]
    fn test_deck_add_commander_card() {
        let mut deck = MtgjsonDeckObject::new("", None);
        let card_json = r#"{"name": "Urza, Lord High Artificer", "count": 1}"#.to_string();

        deck.add_commander_card(card_json.clone());

        assert_eq!(deck.commander.len(), 1);
        assert_eq!(deck.commander[0], card_json);
    }

    #[test]
    fn test_deck_get_total_cards() {
        let mut deck = MtgjsonDeckObject::new("", None);

        deck.add_main_board_card(r#"{"name": "Card1"}"#.to_string());
        deck.add_main_board_card(r#"{"name": "Card2"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Card3"}"#.to_string());
        deck.add_commander_card(r#"{"name": "Commander"}"#.to_string());
        deck.display_commander
            .push(r#"{"name": "Display"}"#.to_string());
        deck.planes.push(r#"{"name": "Plane"}"#.to_string());
        deck.schemes.push(r#"{"name": "Scheme"}"#.to_string());

        assert_eq!(deck.get_total_cards(), 7);
    }

    #[test]
    fn test_deck_get_main_board_count() {
        let mut deck = MtgjsonDeckObject::new("", None);

        deck.add_main_board_card(r#"{"name": "Card1"}"#.to_string());
        deck.add_main_board_card(r#"{"name": "Card2"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "SideCard"}"#.to_string());

        assert_eq!(deck.get_main_board_count(), 2);
    }

    #[test]
    fn test_deck_get_side_board_count() {
        let mut deck = MtgjsonDeckObject::new("", None);

        deck.add_main_board_card(r#"{"name": "MainCard"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "SideCard1"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "SideCard2"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "SideCard3"}"#.to_string());

        assert_eq!(deck.get_side_board_count(), 3);
    }

    #[test]
    fn test_deck_has_cards() {
        let mut deck = MtgjsonDeckObject::new("", None);

        assert!(!deck.has_cards());

        deck.add_main_board_card(r#"{"name": "Card1"}"#.to_string());
        assert!(deck.has_cards());
    }

    #[test]
    fn test_deck_clear_all_cards() {
        let mut deck = MtgjsonDeckObject::new("", None);

        deck.add_main_board_card(r#"{"name": "Card1"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Card2"}"#.to_string());
        deck.add_commander_card(r#"{"name": "Commander"}"#.to_string());
        deck.display_commander
            .push(r#"{"name": "Display"}"#.to_string());
        deck.planes.push(r#"{"name": "Plane"}"#.to_string());
        deck.schemes.push(r#"{"name": "Scheme"}"#.to_string());

        assert!(deck.has_cards());

        deck.clear_all_cards();

        assert!(!deck.has_cards());
        assert_eq!(deck.get_total_cards(), 0);
        assert!(deck.main_board.is_empty());
        assert!(deck.side_board.is_empty());
        assert!(deck.commander.is_empty());
        assert!(deck.display_commander.is_empty());
        assert!(deck.planes.is_empty());
        assert!(deck.schemes.is_empty());
    }

    #[test]
    fn test_deck_header_json_serialization() {
        let mut header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test Deck".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test_deck.json".to_string(),
        );

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
        let mut deck = MtgjsonDeckObject::new("Test Deck", None);
        deck.code = "TST".to_string();
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
    fn test_deck_json_object_trait_deck() {
        let deck = MtgjsonDeckObject::new("", None);
        let keys_to_skip = deck.build_keys_to_skip();

        // Should skip file_name
        assert!(keys_to_skip.contains("file_name"));
    }

    #[test]
    fn test_deck_json_object_trait_deck_header() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );
        let keys_to_skip = header.build_keys_to_skip();

        // Should return empty set unless specifically implemented
        assert!(keys_to_skip.is_empty());
    }

    // COMPREHENSIVE ADDITIONAL TESTS FOR FULL COVERAGE

    #[test]
    fn test_deck_alpha_numeric_name_generation() {
        let deck1 = MtgjsonDeckObject::new("Test Deck 123!", None);
        let deck2 = MtgjsonDeckObject::new("Test-Deck@123#", None);

        // Both should generate similar alpha-numeric names for matching
        assert_eq!(deck1.alpha_numeric_name, deck2.alpha_numeric_name);
    }

    #[test]
    fn test_deck_sealed_product_uuid_matching() {
        let mut deck = MtgjsonDeckObject::new("Lightning Deck", None);

        let mut sealed_product1 = MtgjsonSealedProductObject::new();
        sealed_product1.name = Some("Lightning Deck Precon".to_string());
        sealed_product1.uuid = Some("lightning-uuid".to_string());

        let mut sealed_product2 = MtgjsonSealedProductObject::new();
        sealed_product2.name = Some("Thunder Deck".to_string());
        sealed_product2.uuid = Some("thunder-uuid".to_string());

        let sealed_products = vec![sealed_product1, sealed_product2];
        deck.add_sealed_product_uuids(sealed_products);

        // Should match the first one that contains similar alpha-numeric name
        assert!(deck.sealed_product_uuids.is_some());
        assert_eq!(
            deck.sealed_product_uuids.unwrap(),
            vec!["lightning-uuid".to_string()]
        );
    }

    #[test]
    fn test_deck_multiple_card_types() {
        let mut deck = MtgjsonDeckObject::new("Commander Deck", None);

        // Add various types of cards
        for i in 0..60 {
            deck.add_main_board_card(format!(r#"{{"name": "Main Card {}", "count": 1}}"#, i));
        }

        for i in 0..15 {
            deck.add_side_board_card(format!(r#"{{"name": "Side Card {}", "count": 1}}"#, i));
        }

        deck.add_commander_card(r#"{"name": "Commander", "count": 1}"#.to_string());
        deck.display_commander
            .push(r#"{"name": "Display Commander", "count": 1}"#.to_string());

        for i in 0..10 {
            deck.planes
                .push(format!(r#"{{"name": "Plane {}", "count": 1}}"#, i));
        }

        for i in 0..20 {
            deck.schemes
                .push(format!(r#"{{"name": "Scheme {}", "count": 1}}"#, i));
        }

        assert_eq!(deck.get_main_board_count(), 60);
        assert_eq!(deck.get_side_board_count(), 15);
        assert_eq!(deck.commander.len(), 1);
        assert_eq!(deck.display_commander.len(), 1);
        assert_eq!(deck.planes.len(), 10);
        assert_eq!(deck.schemes.len(), 20);
        assert_eq!(deck.get_total_cards(), 107); // 60 + 15 + 1 + 1 + 10 + 20
    }

    #[test]
    fn test_deck_string_representations() {
        let mut deck = MtgjsonDeckObject::new("My Test Deck", None);
        deck.code = "TST".to_string();

        let str_repr = deck.__str__();
        assert!(str_repr.contains("My Test Deck"));
        assert!(str_repr.contains("TST"));

        let repr = deck.__repr__();
        assert!(repr.contains("My Test Deck"));
        assert!(repr.contains("TST"));
    }

    #[test]
    fn test_deck_header_string_representations() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test Deck Header".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test_header.json".to_string(),
        );

        let str_repr = header.__str__();
        assert!(str_repr.contains("Test Deck Header"));
        assert!(str_repr.contains("TST"));

        let repr = header.__repr__();
        assert!(repr.contains("Test Deck Header"));
        assert!(repr.contains("TST"));
    }

    #[test]
    fn test_deck_equality() {
        let mut deck1 = MtgjsonDeckObject::new("Test", None);
        let mut deck2 = MtgjsonDeckObject::new("Test", None);

        deck1.code = "TST".to_string();
        deck2.code = "TST".to_string();

        assert!(deck1.__eq__(&deck2));

        deck2.code = "DIFF".to_string();
        assert!(!deck1.__eq__(&deck2));
    }

    #[test]
    fn test_deck_header_equality() {
        let header1 = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );

        let header2 = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );

        assert!(header1.__eq__(&header2));

        let header3 = MtgjsonDeckHeaderObject::from_deck_data(
            "DIFF".to_string(),
            "Test".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );

        assert!(!header1.__eq__(&header3));
    }

    #[test]
    fn test_deck_hash() {
        let mut deck = MtgjsonDeckObject::new("Test", None);
        deck.code = "TST".to_string();

        let hash1 = deck.__hash__();
        let hash2 = deck.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_deck_header_hash() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "TST".to_string(),
            "Test".to_string(),
            "2023-01-01".to_string(),
            "constructed".to_string(),
            "test.json".to_string(),
        );

        let hash1 = header.__hash__();
        let hash2 = header.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_deck_clone_trait() {
        let mut deck = MtgjsonDeckObject::new("Original Deck", None);
        deck.code = "ORIG".to_string();
        deck.type_ = "constructed".to_string();
        deck.add_main_board_card(r#"{"name": "Card1"}"#.to_string());

        let cloned_deck = deck.clone();

        assert_eq!(deck.name, cloned_deck.name);
        assert_eq!(deck.code, cloned_deck.code);
        assert_eq!(deck.type_, cloned_deck.type_);
        assert_eq!(deck.main_board, cloned_deck.main_board);
    }

    #[test]
    fn test_deck_edge_cases() {
        let mut deck = MtgjsonDeckObject::new("", None);

        // Test empty values
        assert_eq!(deck.name, "");
        assert_eq!(deck.code, "");
        assert_eq!(deck.release_date, "");
        assert_eq!(deck.type_, "");
        assert_eq!(deck.file_name, "");

        // Test special characters in deck name
        let special_deck = MtgjsonDeckObject::new("Æther Vial™ Deck", None);
        assert_eq!(special_deck.name, "Æther Vial™ Deck");
    }

    #[test]
    fn test_deck_large_collections() {
        let mut deck = MtgjsonDeckObject::new("Massive Deck", None);

        // Add many cards to each section
        for i in 0..250 {
            deck.add_main_board_card(format!(r#"{{"name": "Main {}", "count": 1}}"#, i));
        }

        for i in 0..100 {
            deck.add_side_board_card(format!(r#"{{"name": "Side {}", "count": 1}}"#, i));
        }

        for i in 0..50 {
            deck.planes
                .push(format!(r#"{{"name": "Plane {}", "count": 1}}"#, i));
        }

        assert_eq!(deck.get_main_board_count(), 250);
        assert_eq!(deck.get_side_board_count(), 100);
        assert_eq!(deck.planes.len(), 50);
        assert_eq!(deck.get_total_cards(), 400);
        assert!(deck.has_cards());
    }

    #[test]
    fn test_deck_serialization_deserialization() {
        let mut deck = MtgjsonDeckObject::new("Serialization Test Deck", None);
        deck.code = "SER".to_string();
        deck.release_date = "2023-01-01".to_string();
        deck.type_ = "constructed".to_string();
        deck.sealed_product_uuids = Some(vec!["uuid1".to_string(), "uuid2".to_string()]);

        deck.add_main_board_card(r#"{"name": "Lightning Bolt", "count": 4}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Counterspell", "count": 2}"#.to_string());
        deck.add_commander_card(r#"{"name": "Urza", "count": 1}"#.to_string());

        let json_result = deck.to_json();
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();

        // Test that serialization contains expected fields
        assert!(json_str.contains("Serialization Test Deck"));
        assert!(json_str.contains("SER"));
        assert!(json_str.contains("2023-01-01"));
        assert!(json_str.contains("constructed"));
        assert!(json_str.contains("Lightning Bolt"));

        // Test deserialization
        let deserialized: Result<MtgjsonDeckObject, _> = serde_json::from_str(&json_str);
        assert!(deserialized.is_ok());

        let deserialized_deck = deserialized.unwrap();
        assert_eq!(deserialized_deck.name, "Serialization Test Deck");
        assert_eq!(deserialized_deck.code, "SER");
        assert_eq!(deserialized_deck.release_date, "2023-01-01");
        assert_eq!(deserialized_deck.type_, "constructed");
    }

    #[test]
    fn test_deck_complex_integration_scenario() {
        let mut deck = MtgjsonDeckObject::new("Atraxa, Praetors' Voice EDH", None);

        // Set up a complex deck scenario
        deck.code = "C16".to_string();
        deck.release_date = "2016-11-11".to_string();
        deck.type_ = "commander".to_string();
        deck.file_name = "atraxa_praetors_voice.json".to_string();

        // Add commander
        deck.add_commander_card(
            r#"{"name": "Atraxa, Praetors' Voice", "count": 1, "uuid": "atraxa-uuid"}"#.to_string(),
        );
        deck.display_commander
            .push(r#"{"name": "Atraxa, Praetors' Voice", "count": 1}"#.to_string());

        // Add main deck cards (99 cards for EDH)
        let card_names = vec![
            "Sol Ring",
            "Command Tower",
            "Cultivate",
            "Kodama's Reach",
            "Counterspell",
            "Swords to Plowshares",
            "Path to Exile",
            "Toxic Deluge",
            "Wrath of God",
            "Cyclonic Rift",
            "Mystical Tutor",
            "Enlightened Tutor",
            "Vampiric Tutor",
            "Demonic Tutor",
            "Green Sun's Zenith",
            "Chord of Calling",
            "Craterhoof Behemoth",
            "Avenger of Zendikar",
            "Elspeth, Knight-Errant",
            "Jace, the Mind Sculptor",
            "Liliana of the Veil",
            "Garruk Wildspeaker",
            "Doubling Season",
            "Parallel Lives",
            "Anointed Procession",
            "Hardened Scales",
            "Cathars' Crusade",
            "Master Biomancer",
            "Forgotten Ancient",
            "Deepglow Skate",
            "Vorel of the Hull Clade",
            "Ezuri, Claw of Progress",
        ];

        for (i, card_name) in card_names.iter().enumerate() {
            deck.add_main_board_card(format!(
                r#"{{"name": "{}", "count": 1, "uuid": "uuid-{}", "manaCost": "{{X}}", "cmc": {}}}"#,
                card_name, i, i % 10 + 1
            ));
        }

        // Fill remaining slots to reach 99 cards
        for i in card_names.len()..99 {
            deck.add_main_board_card(format!(
                r#"{{"name": "Basic Land {}", "count": 1, "uuid": "land-uuid-{}", "types": ["Land"]}}"#,
                i, i
            ));
        }

        // Add some sealed product associations
        let mut sealed_product = MtgjsonSealedProductObject::new();
        sealed_product.name = Some("Commander 2016: Breed Lethality".to_string());
        sealed_product.uuid = Some("c16-breed-lethality".to_string());

        deck.add_sealed_product_uuids(vec![sealed_product]);

        // Test all the complex interactions
        assert_eq!(deck.name, "Atraxa, Praetors' Voice EDH");
        assert_eq!(deck.code, "C16");
        assert_eq!(deck.type_, "commander");
        assert_eq!(deck.get_main_board_count(), 99);
        assert_eq!(deck.commander.len(), 1);
        assert_eq!(deck.display_commander.len(), 1);
        assert_eq!(deck.get_total_cards(), 101); // 99 main + 1 commander + 1 display
        assert!(deck.has_cards());

        // Test that sealed product UUID was set
        assert!(deck.sealed_product_uuids.is_some());
        assert_eq!(
            deck.sealed_product_uuids.as_ref().unwrap(),
            &vec!["c16-breed-lethality".to_string()]
        );

        // Test JSON serialization of complex deck
        let json_result = deck.to_json();
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        assert!(json_str.contains("Atraxa, Praetors' Voice EDH"));
        assert!(json_str.contains("C16"));
        assert!(json_str.contains("commander"));
        assert!(json_str.contains("Sol Ring"));
        assert!(json_str.contains("Command Tower"));

        // Create and test deck header
        let header = MtgjsonDeckHeaderObject::new(&deck);
        assert_eq!(header.name, "Atraxa, Praetors' Voice EDH");
        assert_eq!(header.code, "C16");
        assert_eq!(header.type_, "commander");

        let header_info = header.get_display_info();
        assert_eq!(
            header_info.get("name"),
            Some(&"Atraxa, Praetors' Voice EDH".to_string())
        );
        assert_eq!(header_info.get("type"), Some(&"commander".to_string()));
    }

    #[test]
    fn test_deck_empty_collections_handling() {
        let deck = MtgjsonDeckObject::new("Empty Deck", None);

        // Test methods on empty deck
        assert_eq!(deck.get_total_cards(), 0);
        assert_eq!(deck.get_main_board_count(), 0);
        assert_eq!(deck.get_side_board_count(), 0);
        assert!(!deck.has_cards());
    }

    #[test]
    fn test_deck_partial_eq_trait() {
        let mut deck1 = MtgjsonDeckObject::new("Test Deck", None);
        let mut deck2 = MtgjsonDeckObject::new("Test Deck", None);

        deck1.code = "TST".to_string();
        deck1.type_ = "constructed".to_string();

        deck2.code = "TST".to_string();
        deck2.type_ = "constructed".to_string();

        assert_eq!(deck1, deck2);

        deck2.name = "Different Deck".to_string();
        assert_ne!(deck1, deck2);
    }

    #[test]
    fn test_deck_with_all_card_types_populated() {
        let mut deck = MtgjsonDeckObject::new("Complete Deck", None);

        // Populate all possible card collections
        deck.main_board.push(r#"{"name": "Main1"}"#.to_string());
        deck.main_board.push(r#"{"name": "Main2"}"#.to_string());

        deck.side_board.push(r#"{"name": "Side1"}"#.to_string());

        deck.commander.push(r#"{"name": "Commander1"}"#.to_string());

        deck.display_commander
            .push(r#"{"name": "DisplayCmd"}"#.to_string());

        deck.planes.push(r#"{"name": "Plane1"}"#.to_string());
        deck.planes.push(r#"{"name": "Plane2"}"#.to_string());
        deck.planes.push(r#"{"name": "Plane3"}"#.to_string());

        deck.schemes.push(r#"{"name": "Scheme1"}"#.to_string());
        deck.schemes.push(r#"{"name": "Scheme2"}"#.to_string());

        assert_eq!(deck.get_total_cards(), 9);
        assert_eq!(deck.get_main_board_count(), 2);
        assert_eq!(deck.get_side_board_count(), 1);
        assert_eq!(deck.commander.len(), 1);
        assert_eq!(deck.display_commander.len(), 1);
        assert_eq!(deck.planes.len(), 3);
        assert_eq!(deck.schemes.len(), 2);
        assert!(deck.has_cards());
    }

    #[test]
    fn test_deck_clear_specific_sections() {
        let mut deck = MtgjsonDeckObject::new("Test", None);

        // Populate all sections
        deck.add_main_board_card(r#"{"name": "Main"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Side"}"#.to_string());
        deck.add_commander_card(r#"{"name": "Commander"}"#.to_string());
        deck.display_commander
            .push(r#"{"name": "Display"}"#.to_string());
        deck.planes.push(r#"{"name": "Plane"}"#.to_string());
        deck.schemes.push(r#"{"name": "Scheme"}"#.to_string());

        assert_eq!(deck.get_total_cards(), 6);

        // Clear only main board
        deck.main_board.clear();
        assert_eq!(deck.get_total_cards(), 5);
        assert_eq!(deck.get_main_board_count(), 0);

        // Clear only side board
        deck.side_board.clear();
        assert_eq!(deck.get_total_cards(), 4);
        assert_eq!(deck.get_side_board_count(), 0);

        // Clear everything else
        deck.clear_all_cards();
        assert_eq!(deck.get_total_cards(), 0);
        assert!(!deck.has_cards());
    }
}
