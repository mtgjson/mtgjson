use crate::base::{skip_if_empty_optional_string, skip_if_empty_vec, JsonObject};
use crate::card::MtgjsonCardObject;
use crate::deck::MtgjsonDeckObject;
use crate::sealed_product::MtgjsonSealedProductObject;
use crate::translations::MtgjsonTranslations;
use crate::utils::MtgjsonUtils;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Set Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonSetObject")]
pub struct MtgjsonSetObject {
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_set_size: Option<i32>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub booster: Option<String>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub cards: Vec<crate::card::MtgjsonCardObject>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardsphere_set_id: Option<i32>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_v3: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub decks: Vec<MtgjsonDeckObject>,

    #[pyo3(get, set)]
    pub is_foreign_only: bool,

    #[pyo3(get, set)]
    pub is_foil_only: bool,

    #[pyo3(get, set)]
    pub is_non_foil_only: bool,

    #[pyo3(get, set)]
    pub is_online_only: bool,

    #[pyo3(get, set)]
    pub is_partial_preview: bool,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub keyrune_code: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub languages: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub mcm_id: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub mcm_id_extras: Option<i32>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mcm_name: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgo_code: Option<String>,

    #[pyo3(get, set)]
    pub name: String,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub parent_code: Option<String>,

    #[pyo3(get, set)]
    pub release_date: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub tcgplayer_group_id: Option<i32>,

    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub sealed_product: Vec<MtgjsonSealedProductObject>,

    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub tokens: Vec<MtgjsonCardObject>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub token_set_code: Option<String>,

    #[pyo3(get, set)]
    pub total_set_size: i32,

    #[pyo3(get, set)]
    pub translations: MtgjsonTranslations,

    #[pyo3(get, set)]
    pub type_: String,

    // Internal fields not published in JSON
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    pub extra_tokens: Vec<String>,

    #[serde(skip)]
    #[pyo3(get, set)]
    pub search_uri: String,
}

#[pymethods]
impl MtgjsonSetObject {
    #[new]
    pub fn new() -> Self {
        Self {
            base_set_size: None,
            booster: None,
            cards: Vec::new(),
            cardsphere_set_id: None,
            code: None,
            code_v3: None,
            decks: Vec::new(),
            is_foreign_only: false,
            is_foil_only: false,
            is_non_foil_only: false,
            is_online_only: false,
            is_partial_preview: false,
            keyrune_code: None,
            languages: Vec::new(),
            mcm_id: None,
            mcm_id_extras: None,
            mcm_name: None,
            mtgo_code: None,
            name: String::new(),
            parent_code: None,
            release_date: String::new(),
            tcgplayer_group_id: None,
            sealed_product: Vec::new(),
            tokens: Vec::new(),
            token_set_code: None,
            total_set_size: 0,
            translations: MtgjsonTranslations::new(None),
            type_: String::new(),
            extra_tokens: Vec::new(),
            search_uri: String::new(),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Python string representation
    pub fn __str__(&self) -> String {
        format!("{} ({})", self.name, self.code.as_deref().unwrap_or("???"))
    }

    /// Python repr representation
    pub fn __repr__(&self) -> String {
        format!(
            "MtgjsonSetObject(code={:?}, name={:?})",
            self.code, self.name
        )
    }

    /// Python equality method
    pub fn __eq__(&self, other: &MtgjsonSetObject) -> bool {
        self.code == other.code
    }

    /// Python hash method
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        if let Some(ref code) = self.code {
            code.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Get the Windows-safe set code
    pub fn get_windows_safe_set_code(&self) -> String {
        MtgjsonUtils::make_windows_safe_filename(&self.code.as_ref().unwrap_or(&String::new()))
    }

    /// Add a card to the set
    pub fn add_card(&mut self, card: MtgjsonCardObject) {
        self.cards.push(card);
    }

    /// Add a token to the set
    pub fn add_token(&mut self, token: MtgjsonCardObject) {
        self.tokens.push(token);
    }

    /// Add a deck to the set
    pub fn add_deck(&mut self, deck: MtgjsonDeckObject) {
        self.decks.push(deck);
    }

    /// Add a sealed product to the set
    pub fn add_sealed_product(&mut self, product: MtgjsonSealedProductObject) {
        self.sealed_product.push(product);
    }

    /// Sort cards in the set
    pub fn sort_cards(&mut self) {
        self.cards
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    /// Sort tokens in the set
    pub fn sort_tokens(&mut self) {
        self.tokens
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    /// Get total number of cards (including tokens)
    pub fn get_total_cards(&self) -> usize {
        self.cards.len() + self.tokens.len()
    }

    /// Get card count by rarity
    pub fn get_cards_by_rarity(&self) -> HashMap<String, i32> {
        let mut rarity_counts = HashMap::new();

        for card in &self.cards {
            let count = rarity_counts.entry(card.rarity.clone()).or_insert(0);
            *count += 1;
        }

        rarity_counts
    }

    /// Get all unique languages in the set
    pub fn get_unique_languages(&self) -> Vec<String> {
        let mut languages = HashSet::new();

        for card in &self.cards {
            if !card.language.is_empty() {
                languages.insert(card.language.clone());
            }
        }

        for token in &self.tokens {
            if !token.language.is_empty() {
                languages.insert(token.language.clone());
            }
        }

        let mut lang_vec: Vec<String> = languages.into_iter().collect();
        lang_vec.sort();
        lang_vec
    }

    /// Find card by name
    pub fn find_card_by_name(&self, name: &str) -> Option<usize> {
        self.cards.iter().position(|card| card.name == name)
    }

    /// Find card by UUID
    pub fn find_card_by_uuid(&self, uuid: &str) -> Option<usize> {
        self.cards.iter().position(|card| card.uuid == uuid)
    }

    /// Get cards of specific rarity
    pub fn get_cards_of_rarity(&self, rarity: &str) -> Vec<usize> {
        self.cards
            .iter()
            .enumerate()
            .filter_map(
                |(i, card)| {
                    if card.rarity == rarity {
                        Some(i)
                    } else {
                        None
                    }
                },
            )
            .collect()
    }

    /// Check if set contains any foil cards
    pub fn has_foil_cards(&self) -> bool {
        self.cards
            .iter()
            .any(|card| card.finishes.contains(&"foil".to_string()))
            || self
                .tokens
                .iter()
                .any(|token| token.finishes.contains(&"foil".to_string()))
    }

    /// Check if set contains any non-foil cards
    pub fn has_non_foil_cards(&self) -> bool {
        self.cards
            .iter()
            .any(|card| card.finishes.contains(&"nonfoil".to_string()))
            || self
                .tokens
                .iter()
                .any(|token| token.finishes.contains(&"nonfoil".to_string()))
    }

    /// Get set statistics
    pub fn get_statistics(&self) -> String {
        let mut stats = std::collections::HashMap::new();
        stats.insert("total_cards", self.cards.len());
        stats.insert("base_set_size", self.base_set_size.unwrap_or(0) as usize);

        serde_json::to_string(&stats).unwrap_or_default()
    }

    /// Update set size calculations
    pub fn update_set_sizes(&mut self) {
        self.total_set_size = self.cards.len() as i32;

        // Base set size is typically the number of non-token, non-special cards
        // This is a simplified calculation - the real logic would be more complex
        self.base_set_size = Some(
            self.cards
                .iter()
                .filter(|card| {
                    !card.is_token
                        && card.rarity != "special"
                        && !card.types.contains(&"Token".to_string())
                })
                .count() as i32,
        );
    }

    /// Validate set integrity
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.code.is_none() {
            errors.push("Set code is required".to_string());
        }

        if self.name.is_empty() {
            errors.push("Set name is required".to_string());
        }

        if self.release_date.is_empty() {
            errors.push("Release date is required".to_string());
        }

        if self.type_.is_empty() {
            errors.push("Set type is required".to_string());
        }

        // Check for duplicate card UUIDs
        let mut seen_uuids = HashSet::new();
        for card in &self.cards {
            if !card.uuid.is_empty() && !seen_uuids.insert(card.uuid.clone()) {
                errors.push(format!("Duplicate card UUID found: {}", card.uuid));
            }
        }

        // Check for duplicate token UUIDs
        for token in &self.tokens {
            if !token.uuid.is_empty() && !seen_uuids.insert(token.uuid.clone()) {
                errors.push(format!("Duplicate token UUID found: {}", token.uuid));
            }
        }

        errors
    }
}

impl Default for MtgjsonSetObject {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonSetObject {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut excluded_keys = HashSet::new();

        excluded_keys.insert("added_scryfall_tokens".to_string());
        excluded_keys.insert("search_uri".to_string());
        excluded_keys.insert("extra_tokens".to_string());

        // Allow certain falsy values
        let _allow_if_falsey = [
            "cards",
            "tokens",
            "is_foil_only",
            "is_online_only",
            "base_set_size",
            "total_set_size",
        ];

        // Skip empty values that aren't in the allow list
        if self.booster.is_none() {
            excluded_keys.insert("booster".to_string());
        }
        if self.cardsphere_set_id.is_none() {
            excluded_keys.insert("cardsphere_set_id".to_string());
        }
        if self.code_v3.is_none() {
            excluded_keys.insert("code_v3".to_string());
        }
        if self.keyrune_code.is_none() {
            excluded_keys.insert("keyrune_code".to_string());
        }
        if self.mcm_id.is_none() {
            excluded_keys.insert("mcm_id".to_string());
        }
        if self.mcm_id_extras.is_none() {
            excluded_keys.insert("mcm_id_extras".to_string());
        }
        if self.mcm_name.is_none() {
            excluded_keys.insert("mcm_name".to_string());
        }
        if self.mtgo_code.is_none() {
            excluded_keys.insert("mtgo_code".to_string());
        }
        if self.parent_code.is_none() {
            excluded_keys.insert("parent_code".to_string());
        }
        if self.tcgplayer_group_id.is_none() {
            excluded_keys.insert("tcgplayer_group_id".to_string());
        }
        if self.token_set_code.is_none() {
            excluded_keys.insert("token_set_code".to_string());
        }

        excluded_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_creation() {
        let set = MtgjsonSetObject::new();
        assert_eq!(set.base_set_size, None);
        assert_eq!(set.total_set_size, 0);
        assert!(set.cards.is_empty());
        assert!(set.tokens.is_empty());
        assert_eq!(set.name, "");
        assert_eq!(set.code, None);
    }

    #[test]
    fn test_set_default() {
        let set = MtgjsonSetObject::default();
        assert_eq!(set.base_set_size, None);
        assert_eq!(set.total_set_size, 0);
        assert!(set.cards.is_empty());
        assert!(set.tokens.is_empty());
    }

    #[test]
    fn test_add_card() {
        let mut set = MtgjsonSetObject::new();
        let card = MtgjsonCardObject::new(false);
        set.add_card(card);
        assert_eq!(set.cards.len(), 1);
    }

    #[test]
    fn test_add_token() {
        let mut set = MtgjsonSetObject::new();
        let token = MtgjsonCardObject::new(true);
        set.add_token(token);
        assert_eq!(set.tokens.len(), 1);
    }

    #[test]
    fn test_add_deck() {
        let mut set = MtgjsonSetObject::new();
        let deck = MtgjsonDeckObject::new("Test Deck", None);
        set.add_deck(deck);
        assert_eq!(set.decks.len(), 1);
    }

    #[test]
    fn test_add_sealed_product() {
        let mut set = MtgjsonSetObject::new();
        let product = MtgjsonSealedProductObject::new();
        set.add_sealed_product(product);
        assert_eq!(set.sealed_product.len(), 1);
    }

    #[test]
    fn test_sort_cards() {
        let mut set = MtgjsonSetObject::new();

        let mut card1 = MtgjsonCardObject::new(false);
        card1.name = "Beta".to_string();
        card1.number = "2".to_string();

        let mut card2 = MtgjsonCardObject::new(false);
        card2.name = "Alpha".to_string();
        card2.number = "1".to_string();

        set.add_card(card1);
        set.add_card(card2);

        set.sort_cards();

        assert_eq!(set.cards[0].name, "Alpha");
        assert_eq!(set.cards[1].name, "Beta");
    }

    #[test]
    fn test_sort_tokens() {
        let mut set = MtgjsonSetObject::new();

        let mut token1 = MtgjsonCardObject::new(true);
        token1.name = "Zombie".to_string();
        token1.number = "T2".to_string();

        let mut token2 = MtgjsonCardObject::new(true);
        token2.name = "Angel".to_string();
        token2.number = "T1".to_string();

        set.add_token(token1);
        set.add_token(token2);

        set.sort_tokens();

        assert_eq!(set.tokens[0].name, "Angel");
        assert_eq!(set.tokens[1].name, "Zombie");
    }

    #[test]
    fn test_get_total_cards() {
        let mut set = MtgjsonSetObject::new();

        set.add_card(MtgjsonCardObject::new(false));
        set.add_card(MtgjsonCardObject::new(false));
        set.add_token(MtgjsonCardObject::new(true));

        assert_eq!(set.get_total_cards(), 3);
    }

    #[test]
    fn test_get_cards_by_rarity() {
        let mut set = MtgjsonSetObject::new();

        let mut common = MtgjsonCardObject::new(false);
        common.rarity = "common".to_string();

        let mut rare = MtgjsonCardObject::new(false);
        rare.rarity = "rare".to_string();

        let mut another_common = MtgjsonCardObject::new(false);
        another_common.rarity = "common".to_string();

        set.add_card(common);
        set.add_card(rare);
        set.add_card(another_common);

        let rarity_counts = set.get_cards_by_rarity();
        assert_eq!(rarity_counts.get("common"), Some(&2));
        assert_eq!(rarity_counts.get("rare"), Some(&1));
    }

    #[test]
    fn test_get_unique_languages() {
        let mut set = MtgjsonSetObject::new();

        let mut english_card = MtgjsonCardObject::new(false);
        english_card.language = "English".to_string();

        let mut japanese_card = MtgjsonCardObject::new(false);
        japanese_card.language = "Japanese".to_string();

        let mut another_english = MtgjsonCardObject::new(false);
        another_english.language = "English".to_string();

        set.add_card(english_card);
        set.add_card(japanese_card);
        set.add_card(another_english);

        let languages = set.get_unique_languages();
        assert_eq!(languages.len(), 2);
        assert!(languages.contains(&"English".to_string()));
        assert!(languages.contains(&"Japanese".to_string()));
    }

    #[test]
    fn test_find_card_by_name() {
        let mut set = MtgjsonSetObject::new();

        let mut card = MtgjsonCardObject::new(false);
        card.name = "Lightning Bolt".to_string();

        set.add_card(card);

        let index = set.find_card_by_name("Lightning Bolt");
        assert_eq!(index, Some(0));

        let not_found = set.find_card_by_name("Nonexistent Card");
        assert_eq!(not_found, None);
    }

    #[test]
    fn test_find_card_by_uuid() {
        let mut set = MtgjsonSetObject::new();

        let mut card = MtgjsonCardObject::new(false);
        card.uuid = "test-uuid-123".to_string();

        set.add_card(card);

        let index = set.find_card_by_uuid("test-uuid-123");
        assert_eq!(index, Some(0));

        let not_found = set.find_card_by_uuid("nonexistent-uuid");
        assert_eq!(not_found, None);
    }

    #[test]
    fn test_get_cards_of_rarity() {
        let mut set = MtgjsonSetObject::new();

        let mut common1 = MtgjsonCardObject::new(false);
        common1.rarity = "common".to_string();

        let mut rare = MtgjsonCardObject::new(false);
        rare.rarity = "rare".to_string();

        let mut common2 = MtgjsonCardObject::new(false);
        common2.rarity = "common".to_string();

        set.add_card(common1);
        set.add_card(rare);
        set.add_card(common2);

        let common_indices = set.get_cards_of_rarity("common");
        assert_eq!(common_indices.len(), 2);
        assert!(common_indices.contains(&0));
        assert!(common_indices.contains(&2));

        let rare_indices = set.get_cards_of_rarity("rare");
        assert_eq!(rare_indices.len(), 1);
        assert!(rare_indices.contains(&1));
    }

    #[test]
    fn test_has_foil_cards() {
        let mut set = MtgjsonSetObject::new();

        let mut foil_card = MtgjsonCardObject::new(false);
        foil_card.finishes = vec!["foil".to_string()];

        set.add_card(foil_card);
        assert!(set.has_foil_cards());
    }

    #[test]
    fn test_has_non_foil_cards() {
        let mut set = MtgjsonSetObject::new();

        let mut nonfoil_card = MtgjsonCardObject::new(false);
        nonfoil_card.finishes = vec!["nonfoil".to_string()];

        set.add_card(nonfoil_card);
        assert!(set.has_non_foil_cards());
    }

    #[test]
    fn test_get_statistics() {
        let mut set = MtgjsonSetObject::new();
        set.base_set_size = Some(100);

        set.add_card(MtgjsonCardObject::new(false));
        set.add_card(MtgjsonCardObject::new(false));

        let stats = set.get_statistics();
        assert!(stats.contains("total_cards"));
        assert!(stats.contains("base_set_size"));
    }

    #[test]
    fn test_update_set_sizes() {
        let mut set = MtgjsonSetObject::new();

        let mut normal_card = MtgjsonCardObject::new(false);
        normal_card.rarity = "common".to_string();

        let mut token = MtgjsonCardObject::new(true);
        token.types = vec!["Token".to_string()];

        set.add_card(normal_card);
        set.add_card(token);

        set.update_set_sizes();

        assert_eq!(set.total_set_size, 2);
        assert!(set.base_set_size.is_some());
    }

    #[test]
    fn test_validate() {
        let mut set = MtgjsonSetObject::new();

        // Empty set should have errors
        let errors = set.validate();
        assert!(errors.len() > 0);
        assert!(errors.iter().any(|e| e.contains("Set code is required")));
        assert!(errors.iter().any(|e| e.contains("Set name is required")));

        // Fill required fields
        set.code = Some("TST".to_string());
        set.name = "Test Set".to_string();
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();

        let errors = set.validate();
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_validate_duplicate_uuids() {
        let mut set = MtgjsonSetObject::new();

        set.code = Some("TST".to_string());
        set.name = "Test Set".to_string();
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();

        let mut card1 = MtgjsonCardObject::new(false);
        card1.uuid = "duplicate-uuid".to_string();

        let mut card2 = MtgjsonCardObject::new(false);
        card2.uuid = "duplicate-uuid".to_string();

        set.add_card(card1);
        set.add_card(card2);

        let errors = set.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("Duplicate card UUID found")));
    }

    #[test]
    fn test_get_windows_safe_set_code() {
        let mut set = MtgjsonSetObject::new();
        set.code = Some("C:\\CON".to_string());

        let safe_code = set.get_windows_safe_set_code();
        assert!(!safe_code.contains("\\"));
        assert!(!safe_code.contains(":"));
    }

    #[test]
    fn test_json_serialization() {
        let mut set = MtgjsonSetObject::new();
        set.name = "Test Set".to_string();
        set.code = Some("TST".to_string());
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();
        set.total_set_size = 100;

        let json_result = set.to_json();
        assert!(json_result.is_ok());

        let json_string = json_result.unwrap();
        assert!(json_string.contains("Test Set"));
        assert!(json_string.contains("TST"));
        assert!(json_string.contains("2023-01-01"));
        assert!(json_string.contains("expansion"));
    }

    #[test]
    fn test_string_representations() {
        let mut set = MtgjsonSetObject::new();
        set.name = "Test Set".to_string();
        set.code = Some("TST".to_string());

        let str_repr = set.__str__();
        assert!(str_repr.contains("Test Set"));
        assert!(str_repr.contains("TST"));

        let repr = set.__repr__();
        assert!(repr.contains("Test Set"));
        assert!(repr.contains("TST"));
    }

    #[test]
    fn test_set_equality() {
        let mut set1 = MtgjsonSetObject::new();
        let mut set2 = MtgjsonSetObject::new();

        set1.code = Some("TST".to_string());
        set2.code = Some("TST".to_string());

        assert!(set1.__eq__(&set2));

        set2.code = Some("DIFF".to_string());
        assert!(!set1.__eq__(&set2));
    }

    #[test]
    fn test_set_hash() {
        let mut set = MtgjsonSetObject::new();
        set.code = Some("TST".to_string());

        let hash1 = set.__hash__();
        let hash2 = set.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_json_object_trait() {
        let set = MtgjsonSetObject::new();
        let keys_to_skip = set.build_keys_to_skip();

        // Should skip certain fields
        assert!(keys_to_skip.contains("search_uri"));
        assert!(keys_to_skip.contains("extra_tokens"));
    }

    // COMPREHENSIVE ADDITIONAL TESTS FOR FULL COVERAGE

    #[test]
    fn test_set_all_boolean_flags() {
        let mut set = MtgjsonSetObject::new();

        set.is_foreign_only = true;
        set.is_foil_only = true;
        set.is_non_foil_only = false;
        set.is_online_only = true;
        set.is_partial_preview = false;

        assert!(set.is_foreign_only);
        assert!(set.is_foil_only);
        assert!(!set.is_non_foil_only);
        assert!(set.is_online_only);
        assert!(!set.is_partial_preview);
    }

    #[test]
    fn test_set_all_optional_fields() {
        let mut set = MtgjsonSetObject::new();

        set.base_set_size = Some(249);
        set.booster = Some("standard_booster".to_string());
        set.cardsphere_set_id = Some(12345);
        set.code_v3 = Some("LEA".to_string());
        set.keyrune_code = Some("lea".to_string());
        set.mcm_id = Some(67890);
        set.mcm_id_extras = Some(67891);
        set.mcm_name = Some("Alpha".to_string());
        set.mtgo_code = Some("LEA".to_string());
        set.parent_code = Some("PARENT".to_string());
        set.tcgplayer_group_id = Some(99999);
        set.token_set_code = Some("TLEA".to_string());

        assert_eq!(set.base_set_size, Some(249));
        assert_eq!(set.booster, Some("standard_booster".to_string()));
        assert_eq!(set.cardsphere_set_id, Some(12345));
        assert_eq!(set.code_v3, Some("LEA".to_string()));
        assert_eq!(set.keyrune_code, Some("lea".to_string()));
        assert_eq!(set.mcm_id, Some(67890));
        assert_eq!(set.mcm_id_extras, Some(67891));
        assert_eq!(set.mcm_name, Some("Alpha".to_string()));
        assert_eq!(set.mtgo_code, Some("LEA".to_string()));
        assert_eq!(set.parent_code, Some("PARENT".to_string()));
        assert_eq!(set.tcgplayer_group_id, Some(99999));
        assert_eq!(set.token_set_code, Some("TLEA".to_string()));
    }

    #[test]
    fn test_set_languages() {
        let mut set = MtgjsonSetObject::new();

        set.languages = vec![
            "English".to_string(),
            "Japanese".to_string(),
            "French".to_string(),
        ];

        assert_eq!(set.languages.len(), 3);
        assert!(set.languages.contains(&"English".to_string()));
        assert!(set.languages.contains(&"Japanese".to_string()));
        assert!(set.languages.contains(&"French".to_string()));
    }

    #[test]
    fn test_set_extra_tokens() {
        let mut set = MtgjsonSetObject::new();

        set.extra_tokens = vec!["Token1".to_string(), "Token2".to_string()];

        assert_eq!(set.extra_tokens.len(), 2);
        assert!(set.extra_tokens.contains(&"Token1".to_string()));
        assert!(set.extra_tokens.contains(&"Token2".to_string()));
    }

    #[test]
    fn test_set_translations() {
        let mut set = MtgjsonSetObject::new();

        let translations = MtgjsonTranslations::new(None);
        set.translations = translations;

        // Test that translations field is properly set
        assert_eq!(set.translations.chinese_simplified, None);
        assert_eq!(set.translations.french, None);
        assert_eq!(set.translations.german, None);
    }

    #[test]
    fn test_set_clone_trait() {
        let mut set = MtgjsonSetObject::new();
        set.name = "Original Set".to_string();
        set.code = Some("ORIG".to_string());
        set.total_set_size = 100;

        let cloned_set = set.clone();

        assert_eq!(set.name, cloned_set.name);
        assert_eq!(set.code, cloned_set.code);
        assert_eq!(set.total_set_size, cloned_set.total_set_size);
    }

    #[test]
    fn test_set_edge_cases() {
        let mut set = MtgjsonSetObject::new();

        // Test empty string fields
        set.name = "".to_string();
        set.release_date = "".to_string();
        set.type_ = "".to_string();

        assert_eq!(set.name, "");
        assert_eq!(set.release_date, "");
        assert_eq!(set.type_, "");

        // Test special characters
        set.name = "Æther Revolt™".to_string();
        assert!(set.name.contains("Æ"));
        assert!(set.name.contains("™"));
    }

    #[test]
    fn test_set_large_collections() {
        let mut set = MtgjsonSetObject::new();

        // Add many cards
        for i in 0..1000 {
            let mut card = MtgjsonCardObject::new(false);
            card.name = format!("Card {}", i);
            card.uuid = format!("uuid-{}", i);
            set.add_card(card);
        }

        assert_eq!(set.cards.len(), 1000);
        assert_eq!(set.get_total_cards(), 1000);

        // Test finding specific cards in large collection
        let found = set.find_card_by_name("Card 500");
        assert!(found.is_some());
    }

    #[test]
    fn test_set_serialization_deserialization() {
        let mut set = MtgjsonSetObject::new();
        set.name = "Test Set".to_string();
        set.code = Some("TST".to_string());
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();
        set.total_set_size = 100;
        set.base_set_size = Some(90);
        set.is_foil_only = false;
        set.is_online_only = true;

        let json_result = set.to_json();
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();

        // Test that serialization contains expected fields
        assert!(json_str.contains("Test Set"));
        assert!(json_str.contains("TST"));
        assert!(json_str.contains("2023-01-01"));
        assert!(json_str.contains("expansion"));

        // Test deserialization
        let deserialized: Result<MtgjsonSetObject, _> = serde_json::from_str(&json_str);
        assert!(deserialized.is_ok());

        let deserialized_set = deserialized.unwrap();
        assert_eq!(deserialized_set.name, "Test Set");
        assert_eq!(deserialized_set.code, Some("TST".to_string()));
        assert_eq!(deserialized_set.release_date, "2023-01-01");
        assert_eq!(deserialized_set.type_, "expansion");
    }

    #[test]
    fn test_set_complex_integration_scenario() {
        let mut set = MtgjsonSetObject::new();

        // Set up a complex set scenario
        set.name = "Magic 2015 Core Set".to_string();
        set.code = Some("M15".to_string());
        set.code_v3 = Some("M15".to_string());
        set.release_date = "2014-07-18".to_string();
        set.type_ = "core".to_string();
        set.total_set_size = 284;
        set.base_set_size = Some(269);
        set.is_foil_only = false;
        set.is_non_foil_only = false;
        set.is_online_only = false;
        set.is_partial_preview = false;
        set.languages = vec![
            "English".to_string(),
            "Japanese".to_string(),
            "Chinese Simplified".to_string(),
        ];
        set.mcm_id = Some(1234);
        set.tcgplayer_group_id = Some(5678);

        // Add various cards
        let mut creature = MtgjsonCardObject::new(false);
        creature.name = "Grizzly Bears".to_string();
        creature.types = vec!["Creature".to_string()];
        creature.subtypes = vec!["Bear".to_string()];
        creature.rarity = "common".to_string();
        creature.language = "English".to_string();
        creature.finishes = vec!["nonfoil".to_string(), "foil".to_string()];

        let mut planeswalker = MtgjsonCardObject::new(false);
        planeswalker.name = "Jace, the Living Guildpact".to_string();
        planeswalker.types = vec!["Legendary".to_string(), "Planeswalker".to_string()];
        planeswalker.subtypes = vec!["Jace".to_string()];
        planeswalker.rarity = "mythic".to_string();
        planeswalker.language = "English".to_string();

        let mut token = MtgjsonCardObject::new(true);
        token.name = "Angel".to_string();
        token.types = vec!["Token".to_string(), "Creature".to_string()];
        token.subtypes = vec!["Angel".to_string()];

        set.add_card(creature);
        set.add_card(planeswalker);
        set.add_token(token);

        // Add a deck
        let mut deck = MtgjsonDeckObject::new("Intro Pack - Hit the Ground Running", None);
        deck.code = "M15".to_string();
        deck.type_ = "intro".to_string();
        set.add_deck(deck);

        // Add a sealed product
        let mut booster = MtgjsonSealedProductObject::new();
        set.add_sealed_product(booster);

        // Test all the complex interactions
        assert_eq!(set.name, "Magic 2015 Core Set");
        assert_eq!(set.code, Some("M15".to_string()));
        assert_eq!(set.cards.len(), 2);
        assert_eq!(set.tokens.len(), 1);
        assert_eq!(set.decks.len(), 1);
        assert_eq!(set.sealed_product.len(), 1);
        assert_eq!(set.get_total_cards(), 3);

        // Test rarity distribution
        let rarity_counts = set.get_cards_by_rarity();
        assert_eq!(rarity_counts.get("common"), Some(&1));
        assert_eq!(rarity_counts.get("mythic"), Some(&1));

        // Test languages
        let languages = set.get_unique_languages();
        assert!(languages.contains(&"English".to_string()));

        // Test foil detection
        assert!(set.has_foil_cards());
        assert!(set.has_non_foil_cards());

        // Test finding cards
        let grizzly_index = set.find_card_by_name("Grizzly Bears");
        assert_eq!(grizzly_index, Some(0));

        let jace_index = set.find_card_by_name("Jace, the Living Guildpact");
        assert_eq!(jace_index, Some(1));

        // Test validation
        let errors = set.validate();
        assert_eq!(errors.len(), 0);

        // Test JSON serialization of complex set
        let json_result = set.to_json();
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        assert!(json_str.contains("Magic 2015 Core Set"));
        assert!(json_str.contains("M15"));
        assert!(json_str.contains("core"));
        assert!(json_str.contains("Grizzly Bears"));
        assert!(json_str.contains("Jace, the Living Guildpact"));
    }

    #[test]
    fn test_set_empty_collections_handling() {
        let set = MtgjsonSetObject::new();

        // Test methods on empty collections
        assert_eq!(set.get_total_cards(), 0);
        assert_eq!(set.get_cards_by_rarity().len(), 0);
        assert_eq!(set.get_unique_languages().len(), 0);
        assert_eq!(set.find_card_by_name("Any Name"), None);
        assert_eq!(set.find_card_by_uuid("any-uuid"), None);
        assert_eq!(set.get_cards_of_rarity("any"), Vec::<usize>::new());
        assert!(!set.has_foil_cards());
        assert!(!set.has_non_foil_cards());
    }

    #[test]
    fn test_set_search_uri() {
        let mut set = MtgjsonSetObject::new();

        set.search_uri = "https://api.scryfall.com/cards/search?q=set:m15".to_string();

        assert_eq!(
            set.search_uri,
            "https://api.scryfall.com/cards/search?q=set:m15"
        );
    }

    #[test]
    fn test_set_partial_eq_trait() {
        let mut set1 = MtgjsonSetObject::new();
        let mut set2 = MtgjsonSetObject::new();

        set1.code = Some("TST".to_string());
        set1.name = "Test Set".to_string();

        set2.code = Some("TST".to_string());
        set2.name = "Test Set".to_string();

        assert_eq!(set1, set2);

        set2.name = "Different Set".to_string();
        assert_ne!(set1, set2);
    }

    #[test]
    fn test_set_with_massive_data() {
        let mut set = MtgjsonSetObject::new();

        // Stress test with large amounts of data
        for i in 0..10000 {
            let mut card = MtgjsonCardObject::new(false);
            card.name = format!("Test Card {}", i);
            card.uuid = format!("uuid-{:010}", i); // Zero-padded for sorting
            card.number = format!("{}", i + 1);
            card.rarity = match i % 4 {
                0 => "common",
                1 => "uncommon",
                2 => "rare",
                _ => "mythic",
            }
            .to_string();
            set.add_card(card);
        }

        set.sort_cards();

        assert_eq!(set.cards.len(), 10000);
        assert_eq!(set.get_total_cards(), 10000);

        // Test that sorting worked
        assert_eq!(set.cards[0].name, "Test Card 0");
        assert_eq!(set.cards[9999].name, "Test Card 9999");

        // Test rarity distribution
        let rarity_counts = set.get_cards_by_rarity();
        assert_eq!(rarity_counts.get("common").unwrap(), &2500);
        assert_eq!(rarity_counts.get("uncommon").unwrap(), &2500);
        assert_eq!(rarity_counts.get("rare").unwrap(), &2500);
        assert_eq!(rarity_counts.get("mythic").unwrap(), &2500);

        // Test finding specific cards
        let found = set.find_card_by_name("Test Card 5000");
        assert!(found.is_some());

        let uuid_found = set.find_card_by_uuid("uuid-0005000000");
        assert!(uuid_found.is_some());
    }
}
