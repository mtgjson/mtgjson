//! MTGJSON Singular Card Object

use crate::base::{skip_if_empty_optional_string, skip_if_empty_string, skip_if_empty_vec, JsonObject};
use crate::foreign_data::MtgjsonForeignData;
use crate::game_formats::MtgjsonGameFormats;
use crate::identifiers::MtgjsonIdentifiers;
use crate::leadership_skills::MtgjsonLeadershipSkills;
use crate::legalities::MtgjsonLegalities;
use crate::prices::MtgjsonPrices;
use crate::purchase_urls::MtgjsonPurchaseUrls;
use crate::related_cards::MtgjsonRelatedCards;
use crate::rulings::MtgjsonRuling;
use crate::utils::MtgjsonUtils;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

//! MtgjsonCard
//!
//! This struct represents a single Magic: The Gathering card.
//! It is used to store all the data for a single card.
//! Note: All 

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonCard")]
pub struct MtgjsonCard {
    #[pyo3(get, set)]
    pub artist: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub artist_ids: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub ascii_name: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub attraction_lights: Option<Vec<String>>,
    
    #[pyo3(get, set)]
    pub availability: MtgjsonGameFormats,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub booster_types: Vec<String>,
    
    #[pyo3(get, set)]
    pub border_color: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub card_parts: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub color_identity: Vec<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub color_indicator: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub colors: Vec<String>,
    
    #[pyo3(get, set)]
    pub converted_mana_cost: f64,
    
    #[pyo3(get, set)]
    pub count: i32,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub defense: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub duel_deck: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub edhrec_rank: Option<i32>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub edhrec_saltiness: Option<f64>,
    
    #[pyo3(get, set)]
    pub face_converted_mana_cost: f64,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub face_flavor_name: Option<String>,
    
    #[pyo3(get, set)]
    pub face_mana_value: f64,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub face_name: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub finishes: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub first_printing: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub flavor_name: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub flavor_text: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub foreign_data: Vec<MtgjsonForeignData>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub frame_effects: Vec<String>,
    
    #[pyo3(get, set)]
    pub frame_version: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub hand: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub has_alternative_deck_limit: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub has_content_warning: Option<bool>,
    
    /// Deprecated - Remove in 5.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub has_foil: Option<bool>,
    
    /// Deprecated - Remove in 5.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub has_non_foil: Option<bool>,
    
    #[pyo3(get, set)]
    pub identifiers: MtgjsonIdentifiers,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_alternative: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_foil: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_full_art: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_funny: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_game_changer: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_online_only: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_oversized: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_promo: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_rebalanced: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_reprint: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_reserved: Option<bool>,
    
    /// Deprecated - Remove in 5.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_starter: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_story_spotlight: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_textless: Option<bool>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub is_timeshifted: Option<bool>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub keywords: Vec<String>,
    
    #[pyo3(get, set)]
    pub language: String,
    
    #[pyo3(get, set)]
    pub layout: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub leadership_skills: Option<MtgjsonLeadershipSkills>,
    
    #[pyo3(get, set)]
    pub legalities: MtgjsonLegalities,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub life: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub loyalty: Option<String>,
    
    #[pyo3(get, set)]
    pub mana_cost: String,
    
    #[pyo3(get, set)]
    pub mana_value: f64,
    
    #[pyo3(get, set)]
    pub name: String,
    
    #[pyo3(get, set)]
    pub number: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub orientation: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub original_printings: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub original_release_date: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub original_text: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub original_type: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub other_face_ids: Vec<String>,
    
    #[pyo3(get, set)]
    pub power: String,
    
    #[pyo3(get, set)]
    pub prices: MtgjsonPrices,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub printings: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub promo_types: Vec<String>,
    
    #[pyo3(get, set)]
    pub purchase_urls: MtgjsonPurchaseUrls,
    
    #[pyo3(get, set)]
    pub rarity: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub rebalanced_printings: Vec<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub related_cards: Option<MtgjsonRelatedCards>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub reverse_related: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub rulings: Option<Vec<MtgjsonRuling>>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub security_stamp: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub side: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub signature: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub source_products: Option<HashMap<String, Vec<String>>>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub subsets: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub subtypes: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub supertypes: Vec<String>,
    
    #[pyo3(get, set)]
    pub text: String,
    
    #[pyo3(get, set)]
    pub toughness: String,
    
    #[pyo3(get, set)]
    pub type_: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub types: Vec<String>,
    
    #[pyo3(get, set)]
    pub uuid: String,
    
    #[serde(skip_serializing_if = "skip_if_empty_vec")]
    #[pyo3(get, set)]
    pub variations: Vec<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub watermark: Option<String>,

    // Outside entities, not published
    #[serde(skip)]
    #[pyo3(get, set)]
    pub set_code: String,
    
    #[serde(skip)]
    #[pyo3(get, set)]
    pub is_token: bool,
    
    #[serde(skip)]
    #[pyo3(get, set)]
    pub raw_purchase_urls: HashMap<String, String>,
    
    // Internal fields
    #[serde(skip)]
    names: Option<Vec<String>>,
    
    #[serde(skip)]
    illustration_ids: Vec<String>,
    
    #[serde(skip)]
    watermark_resource: HashMap<String, Vec<serde_json::Value>>,
}

// PyO3 methods
#[pymethods]
impl MtgjsonCard {
    #[new]
    #[pyo3(signature = (is_token = false))]
    pub fn new(is_token: bool) -> Self {
        Self {
            artist: String::new(),
            artist_ids: None,
            ascii_name: None,
            attraction_lights: None,
            availability: MtgjsonGameFormats::new(),
            booster_types: Vec::new(),
            border_color: String::new(),
            card_parts: Vec::new(),
            color_identity: Vec::new(),
            color_indicator: None,
            colors: Vec::new(),
            converted_mana_cost: 0.0,
            count: 1,
            defense: None,
            duel_deck: None,
            edhrec_rank: None,
            edhrec_saltiness: None,
            face_converted_mana_cost: 0.0,
            face_flavor_name: None,
            face_mana_value: 0.0,
            face_name: None,
            finishes: Vec::new(),
            first_printing: None,
            flavor_name: None,
            flavor_text: None,
            foreign_data: Vec::new(),
            frame_effects: Vec::new(),
            frame_version: String::new(),
            hand: None,
            has_alternative_deck_limit: None,
            has_content_warning: None,
            has_foil: None,
            has_non_foil: None,
            identifiers: MtgjsonIdentifiers::new(),
            is_alternative: None,
            is_foil: None,
            is_full_art: None,
            is_funny: None,
            is_game_changer: None,
            is_online_only: None,
            is_oversized: None,
            is_promo: None,
            is_rebalanced: None,
            is_reprint: None,
            is_reserved: None,
            is_starter: None,
            is_story_spotlight: None,
            is_textless: None,
            is_timeshifted: None,
            keywords: Vec::new(),
            language: String::new(),
            layout: String::new(),
            leadership_skills: None,
            legalities: MtgjsonLegalities::new(),
            life: None,
            loyalty: None,
            mana_cost: String::new(),
            mana_value: 0.0,
            name: String::new(),
            number: String::new(),
            orientation: None,
            original_printings: Vec::new(),
            original_release_date: None,
            original_text: None,
            original_type: None,
            other_face_ids: Vec::new(),
            power: String::new(),
            prices: MtgjsonPrices::new("", "", "", "USD", None, None, None, None, None, None),
            printings: Vec::new(),
            promo_types: Vec::new(),
            purchase_urls: MtgjsonPurchaseUrls::new(),
            rarity: String::new(),
            rebalanced_printings: Vec::new(),
            related_cards: None,
            reverse_related: None,
            rulings: None,
            security_stamp: None,
            side: None,
            signature: None,
            source_products: None,
            subsets: None,
            subtypes: Vec::new(),
            supertypes: Vec::new(),
            text: String::new(),
            toughness: String::new(),
            type_: String::new(),
            types: Vec::new(),
            uuid: String::new(),
            variations: Vec::new(),
            watermark: None,
            set_code: String::new(),
            is_token,
            raw_purchase_urls: HashMap::new(),
            names: None,
            illustration_ids: Vec::new(),
            watermark_resource: HashMap::new(),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Set internal illustration IDs for this card
    pub fn set_illustration_ids(&mut self, illustration_ids: Vec<String>) {
        self.illustration_ids = illustration_ids;
    }

    /// Get the internal illustration IDs roster for this card
    pub fn get_illustration_ids(&self) -> Vec<String> {
        self.illustration_ids.clone()
    }

    /// Get internal names array for this card
    pub fn get_names(&self) -> Vec<String> {
        self.names.clone().unwrap_or_default()
    }

    /// Set internal names array for this card
    pub fn set_names(&mut self, names: Option<Vec<String>>) {
        self.names = names.map(|n| n.into_iter().map(|s| s.trim().to_string()).collect());
    }

    /// Append to internal names array for this card
    pub fn append_names(&mut self, name: String) {
        if let Some(ref mut names) = self.names {
            names.push(name);
        } else {
            self.set_names(Some(vec![name]));
        }
    }

    /// Set watermark with special processing
    pub fn set_watermark(&mut self, watermark: Option<String>) {
        // Watermarks sometimes aren't specific enough, so we
        // must manually update them. This only applies if the
        // watermark is "set" and then we will append the actual
        // set code to the watermark.
        let watermark = match watermark {
            Some(w) if !w.is_empty() => w,
            _ => return,
        };

        // TODO: Load watermark resource if needed
        // For now, just set the watermark directly
        if watermark == "set" {
            // Would need to load resource and match against card name
            // For now, just set it as-is
            self.watermark = Some(watermark);
        } else {
            self.watermark = Some(watermark);
        }
    }

    /// Get attributes of a card that don't change from printing to printing
    pub fn get_atomic_keys(&self) -> Vec<String> {
        vec![
            "ascii_name".to_string(),
            "color_identity".to_string(),
            "color_indicator".to_string(),
            "colors".to_string(),
            "converted_mana_cost".to_string(),
            "count".to_string(),
            "defense".to_string(),
            "edhrec_rank".to_string(),
            "edhrec_saltiness".to_string(),
            "face_converted_mana_cost".to_string(),
            "face_mana_value".to_string(),
            "face_name".to_string(),
            "foreign_data".to_string(),
            "hand".to_string(),
            "has_alternative_deck_limit".to_string(),
            "identifiers".to_string(),
            "is_funny".to_string(),
            "is_reserved".to_string(),
            "keywords".to_string(),
            "layout".to_string(),
            "leadership_skills".to_string(),
            "legalities".to_string(),
            "life".to_string(),
            "loyalty".to_string(),
            "mana_cost".to_string(),
            "mana_value".to_string(),
            "name".to_string(),
            "power".to_string(),
            "printings".to_string(),
            "purchase_urls".to_string(),
            "rulings".to_string(),
            "scryfall_oracle_id".to_string(),
            "side".to_string(),
            "subtypes".to_string(),
            "supertypes".to_string(),
            "text".to_string(),
            "toughness".to_string(),
            "type".to_string(),
            "types".to_string(),
        ]
    }

    /// Check if card is equal to another (by number and side)
    pub fn eq(&self, other: &MtgjsonCard) -> bool {
        self.number == other.number && 
        (self.side.as_deref().unwrap_or("") == other.side.as_deref().unwrap_or(""))
    }

    /// Compare cards for sorting
    pub fn compare(&self, other: &MtgjsonCard) -> PyResult<i32> {
        match self.partial_cmp(other) {
            Some(Ordering::Less) => Ok(-1),
            Some(Ordering::Equal) => Ok(0),
            Some(Ordering::Greater) => Ok(1),
            None => Ok(0),
        }
    }
}

impl Default for MtgjsonCard {
    fn default() -> Self {
        Self::new(false)
    }
}

impl PartialOrd for MtgjsonCard {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_side = self.side.as_deref().unwrap_or("");
        let other_side = other.side.as_deref().unwrap_or("");

        if self.number == other.number {
            return Some(self_side.cmp(other_side));
        }

        let (self_number_clean, self_len) = MtgjsonUtils::clean_card_number(&self.number);
        let (other_number_clean, other_len) = MtgjsonUtils::clean_card_number(&other.number);

        // Implement the comparison logic from Python
        if self.number.chars().all(|c| c.is_ascii_digit()) && 
           other.number.chars().all(|c| c.is_ascii_digit()) {
            if self_number_clean == other_number_clean {
                if self_len != other_len {
                    return Some(self_len.cmp(&other_len));
                }
                return Some(self_side.cmp(other_side));
            }
            return Some(self_number_clean.cmp(&other_number_clean));
        }

        if self.number.chars().all(|c| c.is_ascii_digit()) {
            if self_number_clean == other_number_clean {
                return Some(Ordering::Less);
            }
            return Some(self_number_clean.cmp(&other_number_clean));
        }

        if other.number.chars().all(|c| c.is_ascii_digit()) {
            if self_number_clean == other_number_clean {
                return Some(Ordering::Greater);
            }
            return Some(self_number_clean.cmp(&other_number_clean));
        }

        if self_number_clean == other_number_clean {
            if self_side.is_empty() && other_side.is_empty() {
                return Some(self.number.cmp(&other.number));
            }
            return Some(self_side.cmp(other_side));
        }

        if self_number_clean == other_number_clean {
            if self_len != other_len {
                return Some(self_len.cmp(&other_len));
            }
            return Some(self_side.cmp(other_side));
        }

        Some(self_number_clean.cmp(&other_number_clean))
    }
}

impl JsonObject for MtgjsonCard {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut excluded_keys = HashSet::new();

        if self.is_token {
            excluded_keys.extend([
                "rulings".to_string(),
                "rarity".to_string(),
                "prices".to_string(),
                "purchase_urls".to_string(),
                "printings".to_string(),
                "converted_mana_cost".to_string(),
                "mana_value".to_string(),
                "foreign_data".to_string(),
                "legalities".to_string(),
                "leadership_skills".to_string(),
            ]);
        } else {
            excluded_keys.insert("reverse_related".to_string());
        }

        excluded_keys.extend([
            "is_token".to_string(),
            "raw_purchase_urls".to_string(),
            "set_code".to_string(),
        ]);

        // Allow certain falsey values
        let allow_if_falsey = [
            "supertypes", "types", "subtypes", "has_foil", "has_non_foil",
            "color_identity", "colors", "converted_mana_cost", "mana_value",
            "face_converted_mana_cost", "face_mana_value", "foreign_data", "reverse_related"
        ];

        // Skip empty values that aren't in the allow list
        if self.artist.is_empty() && !allow_if_falsey.contains(&"artist") {
            excluded_keys.insert("artist".to_string());
        }

        for (key, value) in self.to_json().items() {
            if !value {
                if !allow_if_falsey.contains(&key) {
                    excluded_keys.insert(key);
                }
            }
        }

        excluded_keys
    }
}