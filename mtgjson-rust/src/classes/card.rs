use crate::base::{skip_if_empty_optional_string, skip_if_empty_string, skip_if_empty_vec, JsonObject};
use crate::foreign_data::MtgjsonForeignDataObject;
use crate::game_formats::MtgjsonGameFormatsObject;
use crate::identifiers::MtgjsonIdentifiers;
use crate::leadership_skills::MtgjsonLeadershipSkillsObject;
use crate::legalities::MtgjsonLegalitiesObject;
use crate::prices::MtgjsonPricesObject;
use crate::purchase_urls::MtgjsonPurchaseUrls;
use crate::related_cards::MtgjsonRelatedCardsObject;
use crate::rulings::MtgjsonRulingObject;
use crate::utils::MtgjsonUtils;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Card Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonCardObject")]
pub struct MtgjsonCardObject {
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
    pub availability: MtgjsonGameFormatsObject,
    
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
    pub foreign_data: Vec<MtgjsonForeignDataObject>,
    
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
    pub leadership_skills: Option<MtgjsonLeadershipSkillsObject>,
    
    #[pyo3(get, set)]
    pub legalities: MtgjsonLegalitiesObject,
    
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
    pub prices: MtgjsonPricesObject,
    
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
    pub related_cards: Option<MtgjsonRelatedCardsObject>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub reverse_related: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub rulings: Option<Vec<MtgjsonRulingObject>>,
    
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
    #[pyo3(get)]
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

#[pymethods]
impl MtgjsonCardObject {
    #[new]
    #[pyo3(signature = (is_token = false))]
    pub fn new(is_token: bool) -> Self {
        Self {
            artist: String::new(),
            artist_ids: None,
            ascii_name: None,
            attraction_lights: None,
            availability: MtgjsonGameFormatsObject::new(),
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
            legalities: MtgjsonLegalitiesObject::new(),
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
            prices: MtgjsonPricesObject::new(
                String::new(),
                String::new(), 
                String::new(),
                "USD".to_string(),
                None, None, None, None, None, None
            ),
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

    /// Convert to JSON Dict (Python-compatible)
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
    #[pyo3(signature = (names=None))]
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
    #[pyo3(signature = (watermark=None))]
    pub fn set_watermark(&mut self, watermark: Option<String>) {
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

    /// Python equality method
    pub fn __eq__(&self, other: &MtgjsonCardObject) -> bool {
        self.number == other.number && 
        (self.side.as_deref().unwrap_or("") == other.side.as_deref().unwrap_or(""))
    }

    /// Python less-than comparison for sorting
    /// Uses embedded Python logic to ensure 100% compatibility
    pub fn __lt__(&self, other: &MtgjsonCardObject) -> PyResult<bool> {
        Python::with_gil(|py| {
            // Embed the exact Python sorting logic
            let python_code = r#"
def card_lt(self_number, self_side, other_number, other_side):
    if self_number == other_number:
        return (self_side or "") < (other_side or "")

    self_side = self_side or ""
    other_side = other_side or ""

    self_number_clean = "".join(x for x in self_number if x.isdigit()) or "100000"
    self_number_clean_int = int(self_number_clean)

    other_number_clean = "".join(x for x in other_number if x.isdigit()) or "100000"
    other_number_clean_int = int(other_number_clean)

    # Check if both numbers are pure digits
    self_is_digit = self_number == self_number_clean
    other_is_digit = other_number == other_number_clean

    if self_is_digit and other_is_digit:
        if self_number_clean_int == other_number_clean_int:
            if len(self_number_clean) != len(other_number_clean):
                return len(self_number_clean) < len(other_number_clean)
            return self_side < other_side
        return self_number_clean_int < other_number_clean_int

    if self_is_digit:
        if self_number_clean_int == other_number_clean_int:
            return True
        return self_number_clean_int < other_number_clean_int

    if other_is_digit:
        if self_number_clean_int == other_number_clean_int:
            return False
        return self_number_clean_int < other_number_clean_int

    # Case 4: Neither is pure digit
    # First check if digit strings are identical
    if self_number_clean == other_number_clean:
        if not self_side and not other_side:
            return self_number < other_number
        return self_side < other_side

    # Then check if integer values are the same but digit strings differ
    if self_number_clean_int == other_number_clean_int:
        if len(self_number_clean) != len(other_number_clean):
            return len(self_number_clean) < len(other_number_clean)
        return self_side < other_side

    return self_number_clean_int < other_number_clean_int

# Call the function
result = card_lt(self_number, self_side, other_number, other_side)
"#;

                         let locals = PyDict::new_bound(py);
             locals.set_item("self_number", &self.number)?;
             locals.set_item("self_side", &self.side)?;
             locals.set_item("other_number", &other.number)?;
             locals.set_item("other_side", &other.side)?;

             py.run_bound(python_code, None, Some(&locals))?;
            let result: bool = locals.get_item("result")?.unwrap().extract()?;
            Ok(result)
        })
    }

    /// Python string representation
    pub fn __str__(&self) -> String {
        format!("{} ({}) #{}", self.name, self.set_code, self.number)
    }

    /// Python repr representation
    pub fn __repr__(&self) -> String {
        format!("MtgjsonCardObject(name='{}', set_code='{}', uuid='{}')", 
                self.name, self.set_code, self.uuid)
    }

    /// Python hash method
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        self.uuid.hash(&mut hasher);
        hasher.finish()
    }

    /// Legacy method for backwards compatibility - use __eq__ instead
    #[deprecated(note = "Use __eq__ instead")]
    pub fn eq(&self, other: &MtgjsonCardObject) -> bool {
        self.__eq__(other)
    }

    /// Legacy method for backwards compatibility - use __lt__ instead
    #[deprecated(note = "Use __lt__ instead")]
    pub fn compare(&self, other: &MtgjsonCardObject) -> PyResult<i32> {
        match self.partial_cmp(other) {
            Some(std::cmp::Ordering::Less) => Ok(-1),
            Some(std::cmp::Ordering::Equal) => Ok(0),
            Some(std::cmp::Ordering::Greater) => Ok(1),
            None => Ok(0),
        }
    }
}

impl Default for MtgjsonCardObject {
    fn default() -> Self {
        Self::new(false)
    }
}

impl PartialEq for MtgjsonCardObject {
    fn eq(&self, other: &Self) -> bool {
        self.__eq__(other)
    }
}

impl PartialOrd for MtgjsonCardObject {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // This implementation is not used by PyO3 comparison operators
        // The actual comparison is done in __lt__ method using embedded Python
        match self.__lt__(other) {
            Ok(true) => Some(Ordering::Less),
            Ok(false) => {
                match other.__lt__(self) {
                    Ok(true) => Some(Ordering::Greater),
                    Ok(false) => Some(Ordering::Equal),
                    Err(_) => None,
                }
            },
            Err(_) => None,
        }
    }
}

impl JsonObject for MtgjsonCardObject {
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
        
        // Continue this pattern for other fields as needed...

        excluded_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foreign_data::MtgjsonForeignDataObject;
    use crate::legalities::MtgjsonLegalitiesObject;
    use crate::rulings::MtgjsonRulingObject;
    use crate::related_cards::MtgjsonRelatedCardsObject;
    use std::collections::HashMap;

    #[test]
    fn test_card_creation() {
        let card = MtgjsonCardObject::new(false);
        assert!(!card.is_token);
        assert_eq!(card.name, "");
        assert_eq!(card.uuid, "");
        assert_eq!(card.converted_mana_cost, 0.0);
        assert_eq!(card.count, 1);
        assert!(card.colors.is_empty());
        assert!(card.keywords.is_empty());
    }

    #[test]
    fn test_token_card_creation() {
        let token = MtgjsonCardObject::new(true);
        assert!(token.is_token);
        assert_eq!(token.name, "");
        assert_eq!(token.converted_mana_cost, 0.0);
    }

    #[test]
    fn test_card_default() {
        let card = MtgjsonCardObject::default();
        assert!(!card.is_token);
        assert_eq!(card.name, "");
        assert_eq!(card.converted_mana_cost, 0.0);
        assert_eq!(card.count, 1);
    }

    #[test]
    fn test_set_names() {
        let mut card = MtgjsonCardObject::new(false);
        let names = vec!["Lightning Bolt".to_string(), "Lightning".to_string()];
        card.set_names(Some(names.clone()));
        assert_eq!(card.get_names(), names);
    }

    #[test]
    fn test_append_names() {
        let mut card = MtgjsonCardObject::new(false);
        card.set_names(Some(vec!["Lightning Bolt".to_string()]));
        card.append_names("Lightning".to_string());
        let expected = vec!["Lightning Bolt".to_string(), "Lightning".to_string()];
        assert_eq!(card.get_names(), expected);
    }

    #[test]
    fn test_set_illustration_ids() {
        let mut card = MtgjsonCardObject::new(false);
        let ids = vec!["id1".to_string(), "id2".to_string()];
        card.set_illustration_ids(ids.clone());
        assert_eq!(card.get_illustration_ids(), ids);
    }

    #[test]
    fn test_set_watermark() {
        let mut card = MtgjsonCardObject::new(false);
        card.set_watermark(Some("Boros".to_string()));
        assert_eq!(card.watermark, Some("Boros".to_string()));
        
        card.set_watermark(None);
        assert_eq!(card.watermark, None);
    }

    #[test]
    fn test_get_atomic_keys() {
        let card = MtgjsonCardObject::new(false);
        let keys = card.get_atomic_keys();
        
        // Should contain the expected atomic keys
        assert!(keys.contains(&"artist".to_string()));
        assert!(keys.contains(&"colorIdentity".to_string()));
        assert!(keys.contains(&"colors".to_string()));
        assert!(keys.contains(&"convertedManaCost".to_string()));
        assert!(keys.contains(&"keywords".to_string()));
        assert!(keys.contains(&"layout".to_string()));
        assert!(keys.contains(&"legalities".to_string()));
        assert!(keys.contains(&"manaCost".to_string()));
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"power".to_string()));
        assert!(keys.contains(&"subtypes".to_string()));
        assert!(keys.contains(&"supertypes".to_string()));
        assert!(keys.contains(&"text".to_string()));
        assert!(keys.contains(&"toughness".to_string()));
        assert!(keys.contains(&"type".to_string()));
        assert!(keys.contains(&"types".to_string()));
    }

    #[test]
    fn test_card_equality() {
        let mut card1 = MtgjsonCardObject::new(false);
        let mut card2 = MtgjsonCardObject::new(false);
        
        card1.uuid = "uuid1".to_string();
        card2.uuid = "uuid1".to_string();
        
        assert!(card1.__eq__(&card2));
        assert!(card1.eq(&card2));
        
        card2.uuid = "uuid2".to_string();
        assert!(!card1.__eq__(&card2));
        assert!(!card1.eq(&card2));
    }

    #[test]
    fn test_card_comparison() {
        let mut card1 = MtgjsonCardObject::new(false);
        let mut card2 = MtgjsonCardObject::new(false);
        
        card1.name = "Apple".to_string();
        card1.number = "1".to_string();
        card2.name = "Banana".to_string();
        card2.number = "2".to_string();
        
        let result = card1.__lt__(&card2);
        assert!(result.is_ok());
        assert!(result.unwrap());
        
        let cmp_result = card1.compare(&card2);
        assert!(cmp_result.is_ok());
        assert_eq!(cmp_result.unwrap(), -1);
    }

    #[test]
    fn test_card_string_representations() {
        let mut card = MtgjsonCardObject::new(false);
        card.name = "Lightning Bolt".to_string();
        card.set_code = "LEA".to_string();
        
        let str_repr = card.__str__();
        assert!(str_repr.contains("Lightning Bolt"));
        assert!(str_repr.contains("LEA"));
        
        let repr = card.__repr__();
        assert!(repr.contains("Lightning Bolt"));
        assert!(repr.contains("LEA"));
    }

    #[test]
    fn test_card_hash() {
        let mut card = MtgjsonCardObject::new(false);
        card.uuid = "test-uuid".to_string();
        
        let hash1 = card.__hash__();
        let hash2 = card.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_json_serialization() {
        let mut card = MtgjsonCardObject::new(false);
        card.name = "Lightning Bolt".to_string();
        card.mana_cost = "{R}".to_string();
        card.converted_mana_cost = 1.0;
        card.colors = vec!["R".to_string()];
        card.types = vec!["Instant".to_string()];
        
        let json_result = card.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("Lightning Bolt"));
        assert!(json_string.contains("{R}"));
        assert!(json_string.contains("Instant"));
    }

    #[test]
    fn test_json_object_trait() {
        let card = MtgjsonCardObject::new(false);
        let keys_to_skip = card.build_keys_to_skip();
        
        // Should skip internal fields
        assert!(keys_to_skip.contains("set_code"));
        assert!(keys_to_skip.contains("is_token"));
        assert!(keys_to_skip.contains("raw_purchase_urls"));
    }

    #[test]
    fn test_card_complex_fields() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test foreign data
        let foreign_data = vec![MtgjsonForeignDataObject::new(
            "Japanese".to_string(),
            None, None, None, None, None, None
        )];
        card.foreign_data = foreign_data;
        assert_eq!(card.foreign_data.len(), 1);
        
        // Test leadership skills
        let leadership = crate::leadership_skills::MtgjsonLeadershipSkillsObject::new();
        card.leadership_skills = Some(leadership);
        assert!(card.leadership_skills.is_some());
        
        // Test related cards
        let related = MtgjsonRelatedCardsObject::new();
        card.related_cards = Some(related);
        assert!(card.related_cards.is_some());
        
        // Test rulings
        let ruling = MtgjsonRulingObject::new("2021-01-01".to_string(), "Test ruling".to_string());
        card.rulings = Some(vec![ruling]);
        assert!(card.rulings.is_some());
        assert_eq!(card.rulings.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_card_collections() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test colors
        card.colors = vec!["R".to_string(), "G".to_string()];
        assert_eq!(card.colors.len(), 2);
        assert!(card.colors.contains(&"R".to_string()));
        assert!(card.colors.contains(&"G".to_string()));
        
        // Test color identity
        card.color_identity = vec!["R".to_string(), "G".to_string(), "W".to_string()];
        assert_eq!(card.color_identity.len(), 3);
        
        // Test keywords
        card.keywords = vec!["Flying".to_string(), "Vigilance".to_string()];
        assert_eq!(card.keywords.len(), 2);
        assert!(card.keywords.contains(&"Flying".to_string()));
        
        // Test types
        card.types = vec!["Creature".to_string()];
        card.subtypes = vec!["Angel".to_string(), "Warrior".to_string()];
        card.supertypes = vec!["Legendary".to_string()];
        assert_eq!(card.types.len(), 1);
        assert_eq!(card.subtypes.len(), 2);
        assert_eq!(card.supertypes.len(), 1);
        
        // Test printings
        card.printings = vec!["LEA".to_string(), "LEB".to_string(), "2ED".to_string()];
        assert_eq!(card.printings.len(), 3);
    }

    #[test]
    fn test_source_products() {
        let mut card = MtgjsonCardObject::new(false);
        
        let mut source_products = HashMap::new();
        source_products.insert("Booster Pack".to_string(), vec!["rare".to_string()]);
        source_products.insert("Theme Deck".to_string(), vec!["uncommon".to_string()]);
        
        card.source_products = Some(source_products);
        assert!(card.source_products.is_some());
        
        let products = card.source_products.as_ref().unwrap();
        assert!(products.contains_key("Booster Pack"));
    }

    #[test]
    fn test_partial_ord_implementation() {
        let mut card1 = MtgjsonCardObject::new(false);
        let mut card2 = MtgjsonCardObject::new(false);
        
        card1.name = "Apple".to_string();
        card1.number = "1".to_string();
        card2.name = "Apple".to_string();
        card2.number = "2".to_string();
        
        let result = card1.partial_cmp(&card2);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_card_number_sorting() {
        let mut card1 = MtgjsonCardObject::new(false);
        let mut card2 = MtgjsonCardObject::new(false);
        let mut card3 = MtgjsonCardObject::new(false);
        
        card1.name = "Test".to_string();
        card1.number = "10".to_string();
        
        card2.name = "Test".to_string();
        card2.number = "2".to_string();
        
        card3.name = "Test".to_string();
        card3.number = "1a".to_string();
        
        let mut cards = vec![card1.clone(), card2.clone(), card3.clone()];
        cards.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        // Should be sorted by number properly
        assert_eq!(cards[0].number, "1a");
        assert_eq!(cards[1].number, "2");
        assert_eq!(cards[2].number, "10");
    }

    // COMPREHENSIVE ADDITIONAL TESTS FOR FULL COVERAGE

    #[test]
    fn test_card_all_boolean_flags() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test all boolean flags
        card.has_alternative_deck_limit = Some(true);
        card.has_content_warning = Some(true);
        card.has_foil = Some(true);
        card.has_non_foil = Some(false);
        card.is_alternative = Some(true);
        card.is_foil = Some(true);
        card.is_full_art = Some(true);
        card.is_funny = Some(false);
        card.is_game_changer = Some(true);
        card.is_online_only = Some(false);
        card.is_oversized = Some(true);
        card.is_promo = Some(true);
        card.is_rebalanced = Some(false);
        card.is_reprint = Some(true);
        card.is_reserved = Some(false);
        card.is_starter = Some(true);
        card.is_story_spotlight = Some(true);
        card.is_textless = Some(false);
        card.is_timeshifted = Some(true);
        
        assert_eq!(card.has_alternative_deck_limit, Some(true));
        assert_eq!(card.has_content_warning, Some(true));
        assert_eq!(card.has_foil, Some(true));
        assert_eq!(card.has_non_foil, Some(false));
        assert_eq!(card.is_alternative, Some(true));
        assert_eq!(card.is_foil, Some(true));
        assert_eq!(card.is_full_art, Some(true));
        assert_eq!(card.is_funny, Some(false));
        assert_eq!(card.is_game_changer, Some(true));
        assert_eq!(card.is_online_only, Some(false));
        assert_eq!(card.is_oversized, Some(true));
        assert_eq!(card.is_promo, Some(true));
        assert_eq!(card.is_rebalanced, Some(false));
        assert_eq!(card.is_reprint, Some(true));
        assert_eq!(card.is_reserved, Some(false));
        assert_eq!(card.is_starter, Some(true));
        assert_eq!(card.is_story_spotlight, Some(true));
        assert_eq!(card.is_textless, Some(false));
        assert_eq!(card.is_timeshifted, Some(true));
    }

    #[test]
    fn test_card_all_optional_strings() {
        let mut card = MtgjsonCardObject::new(false);
        
        card.ascii_name = Some("Lightning Bolt".to_string());
        card.defense = Some("3".to_string());
        card.duel_deck = Some("Duel Deck A".to_string());
        card.face_flavor_name = Some("Face Flavor".to_string());
        card.face_name = Some("Face Name".to_string());
        card.first_printing = Some("LEA".to_string());
        card.flavor_name = Some("Flavor Name".to_string());
        card.flavor_text = Some("Flavor text here".to_string());
        card.hand = Some("7".to_string());
        card.life = Some("20".to_string());
        card.loyalty = Some("3".to_string());
        card.orientation = Some("horizontal".to_string());
        card.original_release_date = Some("1993-08-05".to_string());
        card.original_text = Some("Original text".to_string());
        card.original_type = Some("Instant".to_string());
        card.security_stamp = Some("oval".to_string());
        card.side = Some("a".to_string());
        card.signature = Some("Artist Signature".to_string());
        
        assert_eq!(card.ascii_name, Some("Lightning Bolt".to_string()));
        assert_eq!(card.defense, Some("3".to_string()));
        assert_eq!(card.duel_deck, Some("Duel Deck A".to_string()));
        assert_eq!(card.face_flavor_name, Some("Face Flavor".to_string()));
        assert_eq!(card.face_name, Some("Face Name".to_string()));
        assert_eq!(card.first_printing, Some("LEA".to_string()));
        assert_eq!(card.flavor_name, Some("Flavor Name".to_string()));
        assert_eq!(card.flavor_text, Some("Flavor text here".to_string()));
        assert_eq!(card.hand, Some("7".to_string()));
        assert_eq!(card.life, Some("20".to_string()));
        assert_eq!(card.loyalty, Some("3".to_string()));
        assert_eq!(card.orientation, Some("horizontal".to_string()));
        assert_eq!(card.original_release_date, Some("1993-08-05".to_string()));
        assert_eq!(card.original_text, Some("Original text".to_string()));
        assert_eq!(card.original_type, Some("Instant".to_string()));
        assert_eq!(card.security_stamp, Some("oval".to_string()));
        assert_eq!(card.side, Some("a".to_string()));
        assert_eq!(card.signature, Some("Artist Signature".to_string()));
    }

    #[test]
    fn test_card_all_optional_numeric_fields() {
        let mut card = MtgjsonCardObject::new(false);
        
        card.edhrec_rank = Some(1000);
        card.edhrec_saltiness = Some(0.75);
        
        assert_eq!(card.edhrec_rank, Some(1000));
        assert_eq!(card.edhrec_saltiness, Some(0.75));
    }

    #[test]
    fn test_card_all_vector_fields() {
        let mut card = MtgjsonCardObject::new(false);
        
        card.artist_ids = Some(vec!["artist1".to_string(), "artist2".to_string()]);
        card.attraction_lights = Some(vec!["1".to_string(), "3".to_string(), "6".to_string()]);
        card.booster_types = vec!["draft".to_string(), "set".to_string()];
        card.card_parts = vec!["part1".to_string(), "part2".to_string()];
        card.color_indicator = Some(vec!["R".to_string(), "G".to_string()]);
        card.finishes = vec!["nonfoil".to_string(), "foil".to_string()];
        card.frame_effects = vec!["legendary".to_string(), "miracle".to_string()];
        card.other_face_ids = vec!["face1".to_string(), "face2".to_string()];
        card.original_printings = vec!["LEA".to_string(), "LEB".to_string()];
        card.promo_types = vec!["prerelease".to_string(), "fnm".to_string()];
        card.rebalanced_printings = vec!["ARENA".to_string()];
        card.reverse_related = Some(vec!["related1".to_string(), "related2".to_string()]);
        card.subsets = Some(vec!["subset1".to_string(), "subset2".to_string()]);
        card.variations = vec!["var1".to_string(), "var2".to_string()];
        
        assert_eq!(card.artist_ids.as_ref().unwrap().len(), 2);
        assert_eq!(card.attraction_lights.as_ref().unwrap().len(), 3);
        assert_eq!(card.booster_types.len(), 2);
        assert_eq!(card.card_parts.len(), 2);
        assert_eq!(card.color_indicator.as_ref().unwrap().len(), 2);
        assert_eq!(card.finishes.len(), 2);
        assert_eq!(card.frame_effects.len(), 2);
        assert_eq!(card.other_face_ids.len(), 2);
        assert_eq!(card.original_printings.len(), 2);
        assert_eq!(card.promo_types.len(), 2);
        assert_eq!(card.rebalanced_printings.len(), 1);
        assert_eq!(card.reverse_related.as_ref().unwrap().len(), 2);
        assert_eq!(card.subsets.as_ref().unwrap().len(), 2);
        assert_eq!(card.variations.len(), 2);
    }

    #[test]
    fn test_card_face_mana_values() {
        let mut card = MtgjsonCardObject::new(false);
        
        card.face_converted_mana_cost = 3.0;
        card.face_mana_value = 3.0;
        card.mana_value = 6.0; // Total for double-faced card
        
        assert_eq!(card.face_converted_mana_cost, 3.0);
        assert_eq!(card.face_mana_value, 3.0);
        assert_eq!(card.mana_value, 6.0);
    }

    #[test]
    fn test_card_internal_fields() {
        let mut card = MtgjsonCardObject::new(false);
        
        card.set_code = "LEA".to_string();
        card.is_token = true;
        card.raw_purchase_urls.insert("tcgplayer".to_string(), "https://example.com".to_string());
        
        assert_eq!(card.set_code, "LEA");
        assert!(card.is_token);
        assert!(card.raw_purchase_urls.contains_key("tcgplayer"));
    }

    #[test]
    fn test_card_clone_trait() {
        let mut card = MtgjsonCardObject::new(false);
        card.name = "Lightning Bolt".to_string();
        card.mana_cost = "{R}".to_string();
        card.colors = vec!["R".to_string()];
        
        let cloned_card = card.clone();
        
        assert_eq!(card.name, cloned_card.name);
        assert_eq!(card.mana_cost, cloned_card.mana_cost);
        assert_eq!(card.colors, cloned_card.colors);
    }

    #[test]
    fn test_card_edge_cases() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test empty values
        card.power = "".to_string();
        card.toughness = "".to_string();
        card.mana_cost = "".to_string();
        card.text = "".to_string();
        
        assert_eq!(card.power, "");
        assert_eq!(card.toughness, "");
        assert_eq!(card.mana_cost, "");
        assert_eq!(card.text, "");
        
        // Test special characters
        card.name = "Æther Vial".to_string();
        card.text = "Cards with \"quotes\" and symbols ™".to_string();
        
        assert_eq!(card.name, "Æther Vial");
        assert!(card.text.contains("quotes"));
        assert!(card.text.contains("™"));
    }

    #[test]
    fn test_card_large_collections() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test with large collections
        for i in 0..100 {
            card.printings.push(format!("SET{}", i));
            card.keywords.push(format!("Keyword{}", i));
        }
        
        assert_eq!(card.printings.len(), 100);
        assert_eq!(card.keywords.len(), 100);
        assert!(card.printings.contains(&"SET50".to_string()));
        assert!(card.keywords.contains(&"Keyword25".to_string()));
    }

    #[test]
    fn test_card_serialization_deserialization() {
        let mut card = MtgjsonCardObject::new(false);
        card.name = "Test Card".to_string();
        card.mana_cost = "{2}{R}".to_string();
        card.converted_mana_cost = 3.0;
        card.colors = vec!["R".to_string()];
        card.types = vec!["Creature".to_string()];
        card.subtypes = vec!["Goblin".to_string()];
        card.power = "2".to_string();
        card.toughness = "2".to_string();
        
        let json_result = card.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        
        // Test that serialization contains expected fields
        assert!(json_str.contains("Test Card"));
        assert!(json_str.contains("{2}{R}"));
        assert!(json_str.contains("Creature"));
        assert!(json_str.contains("Goblin"));
        
        // Test deserialization
        let deserialized: Result<MtgjsonCardObject, _> = serde_json::from_str(&json_str);
        assert!(deserialized.is_ok());
        
        let deserialized_card = deserialized.unwrap();
        assert_eq!(deserialized_card.name, "Test Card");
        assert_eq!(deserialized_card.mana_cost, "{2}{R}");
        assert_eq!(deserialized_card.converted_mana_cost, 3.0);
    }

    #[test]
    fn test_card_complex_integration_scenario() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Set up a complex card scenario
        card.name = "Jace, the Mind Sculptor".to_string();
        card.mana_cost = "{2}{U}{U}".to_string();
        card.converted_mana_cost = 4.0;
        card.mana_value = 4.0;
        card.colors = vec!["U".to_string()];
        card.color_identity = vec!["U".to_string()];
        card.types = vec!["Legendary".to_string(), "Planeswalker".to_string()];
        card.subtypes = vec!["Jace".to_string()];
        card.loyalty = Some("3".to_string());
        card.text = "+2: Look at the top card of target player's library...".to_string();
        card.artist = "Jason Chan".to_string();
        card.rarity = "mythic".to_string();
        card.layout = "normal".to_string();
        card.border_color = "black".to_string();
        card.frame_version = "2003".to_string();
        card.language = "English".to_string();
        card.printings = vec!["WWK".to_string(), "JVC".to_string(), "VMA".to_string()];
        card.is_reserved = Some(false);
        card.is_reprint = Some(true);
        card.finishes = vec!["nonfoil".to_string(), "foil".to_string()];
        card.availability = crate::game_formats::MtgjsonGameFormatsObject::new();
        card.legalities = MtgjsonLegalitiesObject::new();
        
        // Test that all fields are properly set
        assert_eq!(card.name, "Jace, the Mind Sculptor");
        assert_eq!(card.converted_mana_cost, 4.0);
        assert_eq!(card.types.len(), 2);
        assert!(card.types.contains(&"Planeswalker".to_string()));
        assert_eq!(card.loyalty, Some("3".to_string()));
        assert_eq!(card.printings.len(), 3);
        assert!(card.printings.contains(&"WWK".to_string()));
        assert_eq!(card.finishes.len(), 2);
        
        // Test JSON serialization of complex card
        let json_result = card.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        assert!(json_str.contains("Jace, the Mind Sculptor"));
        assert!(json_str.contains("Planeswalker"));
        assert!(json_str.contains("Jason Chan"));
    }
}