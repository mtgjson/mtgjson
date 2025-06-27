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
        format!("{} ({})", 
                self.name, 
                self.code.as_deref().unwrap_or("???"))
    }

    /// Python repr representation
    pub fn __repr__(&self) -> String {
        format!("MtgjsonSetObject(code={:?}, name={:?})", self.code, self.name)
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
        self.cards.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    /// Sort tokens in the set
    pub fn sort_tokens(&mut self) {
        self.tokens.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
        self.cards.iter().position(|card| 
            card.name == name
        )
    }

    /// Find card by UUID
    pub fn find_card_by_uuid(&self, uuid: &str) -> Option<usize> {
        self.cards.iter().position(|card| 
            card.uuid == uuid
        )
    }

    /// Get cards of specific rarity
    pub fn get_cards_of_rarity(&self, rarity: &str) -> Vec<usize> {
        self.cards.iter().enumerate()
            .filter_map(|(i, card)| {
                if card.rarity == rarity {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if set contains any foil cards
    pub fn has_foil_cards(&self) -> bool {
        self.cards.iter().any(|card| card.finishes.contains(&"foil".to_string())) ||
        self.tokens.iter().any(|token| token.finishes.contains(&"foil".to_string()))
    }

    /// Check if set contains any non-foil cards
    pub fn has_non_foil_cards(&self) -> bool {
        self.cards.iter().any(|card| card.finishes.contains(&"nonfoil".to_string())) ||
        self.tokens.iter().any(|token| token.finishes.contains(&"nonfoil".to_string()))
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
        self.base_set_size = Some(self.cards.iter()
            .filter(|card| !card.is_token && 
                           card.rarity != "special" && 
                           !card.types.contains(&"Token".to_string()))
            .count() as i32);
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
        let allow_if_falsey = [
            "cards", "tokens", "is_foil_only", "is_online_only", 
            "base_set_size", "total_set_size"
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