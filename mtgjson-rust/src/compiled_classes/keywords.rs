use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// MTGJSON Keywords Object
/// Rust equivalent of MtgjsonKeywordsObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonKeywords")]
pub struct MtgjsonKeywords {
    #[pyo3(get, set)]
    pub ability_words: Vec<String>,
    
    #[pyo3(get, set)]
    pub keyword_actions: Vec<String>,
    
    #[pyo3(get, set)]
    pub keyword_abilities: Vec<String>,
}

#[pymethods]
impl MtgjsonKeywords {
    #[new]
    pub fn new() -> Self {
        Self {
            ability_words: Self::default_ability_words(),
            keyword_actions: Self::default_keyword_actions(),
            keyword_abilities: Self::default_keyword_abilities(),
        }
    }

    /// Create from custom lists
    #[staticmethod]
    pub fn from_lists(
        ability_words: Vec<String>,
        keyword_actions: Vec<String>,
        keyword_abilities: Vec<String>,
    ) -> Self {
        Self {
            ability_words,
            keyword_actions,
            keyword_abilities,
        }
    }

    /// Add an ability word
    pub fn add_ability_word(&mut self, ability_word: String) {
        if !self.ability_words.contains(&ability_word) {
            self.ability_words.push(ability_word);
            self.ability_words.sort();
        }
    }

    /// Add a keyword action
    pub fn add_keyword_action(&mut self, keyword_action: String) {
        if !self.keyword_actions.contains(&keyword_action) {
            self.keyword_actions.push(keyword_action);
            self.keyword_actions.sort();
        }
    }

    /// Add a keyword ability
    pub fn add_keyword_ability(&mut self, keyword_ability: String) {
        if !self.keyword_abilities.contains(&keyword_ability) {
            self.keyword_abilities.push(keyword_ability);
            self.keyword_abilities.sort();
        }
    }

    /// Check if a word is an ability word
    pub fn is_ability_word(&self, word: &str) -> bool {
        self.ability_words.iter().any(|w| w.eq_ignore_ascii_case(word))
    }

    /// Check if a word is a keyword action
    pub fn is_keyword_action(&self, word: &str) -> bool {
        self.keyword_actions.iter().any(|w| w.eq_ignore_ascii_case(word))
    }

    /// Check if a word is a keyword ability
    pub fn is_keyword_ability(&self, word: &str) -> bool {
        self.keyword_abilities.iter().any(|w| w.eq_ignore_ascii_case(word))
    }

    /// Get all keywords (combined)
    pub fn get_all_keywords(&self) -> Vec<String> {
        let mut all_keywords = Vec::new();
        all_keywords.extend(self.ability_words.clone());
        all_keywords.extend(self.keyword_actions.clone());
        all_keywords.extend(self.keyword_abilities.clone());
        all_keywords.sort();
        all_keywords.dedup();
        all_keywords
    }

    /// Search for keywords containing a substring
    pub fn search_keywords(&self, substring: &str) -> Vec<String> {
        let substring_lower = substring.to_lowercase();
        let mut results = Vec::new();
        
        for keyword in self.get_all_keywords() {
            if keyword.to_lowercase().contains(&substring_lower) {
                results.push(keyword);
            }
        }
        
        results
    }

    /// Get total count of all keywords
    pub fn total_count(&self) -> usize {
        self.ability_words.len() + self.keyword_actions.len() + self.keyword_abilities.len()
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Default ability words (placeholder - would normally come from Scryfall)
    #[staticmethod]
    fn default_ability_words() -> Vec<String> {
        vec![
            "Adamant".to_string(),
            "Addendum".to_string(),
            "Alliance".to_string(),
            "Battalion".to_string(),
            "Bloodrush".to_string(),
            "Channel".to_string(),
            "Chroma".to_string(),
            "Cohort".to_string(),
            "Constellation".to_string(),
            "Converge".to_string(),
            "Council's dilemma".to_string(),
            "Delirium".to_string(),
            "Domain".to_string(),
            "Eminence".to_string(),
            "Enrage".to_string(),
            "Fateful hour".to_string(),
            "Ferocious".to_string(),
            "Formidable".to_string(),
            "Grandeur".to_string(),
            "Hellbent".to_string(),
            "Heroic".to_string(),
            "Imprint".to_string(),
            "Inspired".to_string(),
            "Join forces".to_string(),
            "Kinship".to_string(),
            "Landfall".to_string(),
            "Lieutenant".to_string(),
            "Metalcraft".to_string(),
            "Morbid".to_string(),
            "Parley".to_string(),
            "Radiance".to_string(),
            "Raid".to_string(),
            "Rally".to_string(),
            "Revolt".to_string(),
            "Spell mastery".to_string(),
            "Strive".to_string(),
            "Sweep".to_string(),
            "Tempting offer".to_string(),
            "Threshold".to_string(),
            "Undergrowth".to_string(),
            "Will of the council".to_string(),
        ]
    }

    /// Default keyword actions (placeholder - would normally come from Scryfall)
    #[staticmethod]
    fn default_keyword_actions() -> Vec<String> {
        vec![
            "Abandon".to_string(),
            "Activate".to_string(),
            "Adapt".to_string(),
            "Attach".to_string(),
            "Cast".to_string(),
            "Counter".to_string(),
            "Create".to_string(),
            "Destroy".to_string(),
            "Discard".to_string(),
            "Exchange".to_string(),
            "Exile".to_string(),
            "Fight".to_string(),
            "Mill".to_string(),
            "Play".to_string(),
            "Regenerate".to_string(),
            "Reveal".to_string(),
            "Sacrifice".to_string(),
            "Scry".to_string(),
            "Search".to_string(),
            "Shuffle".to_string(),
            "Tap".to_string(),
            "Untap".to_string(),
        ]
    }

    /// Default keyword abilities (placeholder - would normally come from Scryfall)
    #[staticmethod]
    fn default_keyword_abilities() -> Vec<String> {
        vec![
            "Deathtouch".to_string(),
            "Defender".to_string(),
            "Double strike".to_string(),
            "First strike".to_string(),
            "Flying".to_string(),
            "Haste".to_string(),
            "Hexproof".to_string(),
            "Indestructible".to_string(),
            "Lifelink".to_string(),
            "Menace".to_string(),
            "Reach".to_string(),
            "Trample".to_string(),
            "Vigilance".to_string(),
            "Affinity".to_string(),
            "Amplify".to_string(),
            "Annihilator".to_string(),
            "Aura swap".to_string(),
            "Banding".to_string(),
            "Bestow".to_string(),
            "Bloodthirst".to_string(),
            "Bushido".to_string(),
            "Buyback".to_string(),
            "Cascade".to_string(),
            "Changeling".to_string(),
            "Convoke".to_string(),
            "Cycling".to_string(),
            "Dredge".to_string(),
            "Echo".to_string(),
            "Equip".to_string(),
            "Evoke".to_string(),
            "Exalted".to_string(),
            "Flashback".to_string(),
            "Horsemanship".to_string(),
            "Infect".to_string(),
            "Kicker".to_string(),
            "Landwalk".to_string(),
            "Madness".to_string(),
            "Morph".to_string(),
            "Ninjutsu".to_string(),
            "Persist".to_string(),
            "Phasing".to_string(),
            "Protection".to_string(),
            "Prowess".to_string(),
            "Rampage".to_string(),
            "Rebound".to_string(),
            "Shroud".to_string(),
            "Split second".to_string(),
            "Storm".to_string(),
            "Suspend".to_string(),
            "Undying".to_string(),
            "Wither".to_string(),
        ]
    }
}

impl Default for MtgjsonKeywords {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonKeywords {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords_creation() {
        let keywords = MtgjsonKeywords::new();
        assert!(!keywords.ability_words.is_empty());
        assert!(!keywords.keyword_actions.is_empty());
        assert!(!keywords.keyword_abilities.is_empty());
    }

    #[test]
    fn test_add_keywords() {
        let mut keywords = MtgjsonKeywords::new();
        
        let initial_ability_count = keywords.ability_words.len();
        keywords.add_ability_word("Test Ability".to_string());
        assert_eq!(keywords.ability_words.len(), initial_ability_count + 1);
        assert!(keywords.is_ability_word("Test Ability"));
    }

    #[test]
    fn test_search_keywords() {
        let keywords = MtgjsonKeywords::new();
        let flying_results = keywords.search_keywords("fly");
        assert!(flying_results.contains(&"Flying".to_string()));
    }

    #[test]
    fn test_keyword_checks() {
        let keywords = MtgjsonKeywords::new();
        assert!(keywords.is_keyword_ability("Flying"));
        assert!(keywords.is_keyword_action("Scry"));
        assert!(keywords.is_ability_word("Landfall"));
    }

    #[test]
    fn test_from_lists() {
        let abilities = vec!["Test Ability".to_string()];
        let actions = vec!["Test Action".to_string()];
        let ability_words = vec!["Test Word".to_string()];
        
        let keywords = MtgjsonKeywords::from_lists(ability_words, actions, abilities);
        assert_eq!(keywords.ability_words.len(), 1);
        assert_eq!(keywords.keyword_actions.len(), 1);
        assert_eq!(keywords.keyword_abilities.len(), 1);
    }
}