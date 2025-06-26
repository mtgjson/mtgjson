use crate::base::{skip_if_empty_optional_string, JsonObject};
use crate::identifiers::MtgjsonIdentifiers;
use crate::purchase_urls::MtgjsonPurchaseUrls;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Sealed Product Category
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "SealedProductCategory")]
pub enum SealedProductCategory {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "booster_pack")]
    BoosterPack,
    #[serde(rename = "booster_box")]
    BoosterBox,
    #[serde(rename = "booster_case")]
    BoosterCase,
    #[serde(rename = "deck")]
    Deck,
    #[serde(rename = "multiple_decks")]
    MultiDeck,
    #[serde(rename = "deck_box")]
    DeckBox,
    #[serde(rename = "box_set")]
    BoxSet,
    #[serde(rename = "kit")]
    Kit,
    #[serde(rename = "bundle")]
    Bundle,
    #[serde(rename = "bundle_case")]
    BundleCase,
    #[serde(rename = "limited_aid_tool")]
    Limited,
    #[serde(rename = "limited_aid_case")]
    LimitedCase,
    #[serde(rename = "subset")]
    Subset,
    // Archived categories kept for back-compatibility
    #[serde(rename = "case")]
    Case,
    #[serde(rename = "commander_deck")]
    CommanderDeck,
    #[serde(rename = "land_station")]
    LandStation,
    #[serde(rename = "two_player_starter_set")]
    TwoPlayerStarterSet,
    #[serde(rename = "draft_set")]
    DraftSet,
    #[serde(rename = "prerelease_pack")]
    PrereleasePack,
    #[serde(rename = "prerelease_case")]
    PrereleaseCase,
}

#[pymethods]
impl SealedProductCategory {
    /// Convert to JSON string
    pub fn to_json(&self) -> Option<String> {
        match self {
            SealedProductCategory::Unknown => None,
            _ => Some(format!("{:?}", self).to_lowercase()),
        }
    }

    /// Create from string
    #[staticmethod]
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "booster_pack" => SealedProductCategory::BoosterPack,
            "booster_box" => SealedProductCategory::BoosterBox,
            "booster_case" => SealedProductCategory::BoosterCase,
            "deck" => SealedProductCategory::Deck,
            "multiple_decks" => SealedProductCategory::MultiDeck,
            "deck_box" => SealedProductCategory::DeckBox,
            "box_set" => SealedProductCategory::BoxSet,
            "kit" => SealedProductCategory::Kit,
            "bundle" => SealedProductCategory::Bundle,
            "bundle_case" => SealedProductCategory::BundleCase,
            "limited_aid_tool" => SealedProductCategory::Limited,
            "limited_aid_case" => SealedProductCategory::LimitedCase,
            "subset" => SealedProductCategory::Subset,
            "case" => SealedProductCategory::Case,
            "commander_deck" => SealedProductCategory::CommanderDeck,
            "land_station" => SealedProductCategory::LandStation,
            "two_player_starter_set" => SealedProductCategory::TwoPlayerStarterSet,
            "draft_set" => SealedProductCategory::DraftSet,
            "prerelease_pack" => SealedProductCategory::PrereleasePack,
            "prerelease_case" => SealedProductCategory::PrereleaseCase,
            _ => SealedProductCategory::Unknown,
        }
    }
}

impl Default for SealedProductCategory {
    fn default() -> Self {
        SealedProductCategory::Unknown
    }
}

/// MTGJSON Sealed Product Subtype
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "SealedProductSubtype")]
pub enum SealedProductSubtype {
    #[serde(rename = "unknown")]
    Unknown,
    // Booster types
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "draft")]
    Draft,
    #[serde(rename = "play")]
    Play,
    #[serde(rename = "set")]
    Set,
    #[serde(rename = "collector")]
    Collector,
    #[serde(rename = "jumpstart")]
    Jumpstart,
    #[serde(rename = "promotional")]
    Promotional,
    #[serde(rename = "theme")]
    Theme,
    #[serde(rename = "welcome")]
    Welcome,
    #[serde(rename = "topper")]
    Topper,
    #[serde(rename = "six-card")]
    Six,
    // Deck types
    #[serde(rename = "planeswalker")]
    Planeswalker,
    #[serde(rename = "challenge")]
    Challenge,
    #[serde(rename = "challenger")]
    Challenger,
    #[serde(rename = "event")]
    Event,
    #[serde(rename = "championship")]
    Championship,
    #[serde(rename = "intro")]
    Intro,
    #[serde(rename = "commander")]
    Commander,
    #[serde(rename = "brawl")]
    Brawl,
    #[serde(rename = "archenemy")]
    Archenemy,
    #[serde(rename = "planechase")]
    Planechase,
    // Multi-deck types
    #[serde(rename = "two_player_starter")]
    TwoPlayerStarter,
    #[serde(rename = "duel")]
    Duel,
    #[serde(rename = "clash")]
    Clash,
    #[serde(rename = "battle_pack")]
    Battle,
    #[serde(rename = "game_night")]
    GameNight,
    // Box Set types
    #[serde(rename = "from_the_vault")]
    FromTheVault,
    #[serde(rename = "spellbook")]
    Spellbook,
    #[serde(rename = "secret_lair")]
    SecretLair,
    #[serde(rename = "secret_lair_bundle")]
    SecretLairBundle,
    #[serde(rename = "commander_collection")]
    CommanderCollection,
    #[serde(rename = "collectors_edition")]
    CollectorsEdition,
    #[serde(rename = "convention_exclusive")]
    Convention,
    // Kit types
    #[serde(rename = "guild_kit")]
    GuildKit,
    #[serde(rename = "deck_builders_toolkit")]
    DeckBuildersToolkit,
    #[serde(rename = "land_station")]
    LandStation,
    // Bundle types
    #[serde(rename = "gift_bundle")]
    GiftBundle,
    #[serde(rename = "fat_pack")]
    FatPack,
    // Limited Play Aids
    #[serde(rename = "draft_set")]
    DraftSet,
    #[serde(rename = "sealed_set")]
    SealedSet,
    #[serde(rename = "tournament_deck")]
    Tournament,
    #[serde(rename = "starter_deck")]
    Starter,
    #[serde(rename = "prerelease_kit")]
    Prerelease,
    // Other
    #[serde(rename = "minimal_packaging")]
    Minimal,
    #[serde(rename = "premium")]
    Premium,
    #[serde(rename = "advanced")]
    Advanced,
    #[serde(rename = "other")]
    Other,
}

#[pymethods]
impl SealedProductSubtype {
    /// Convert to JSON string
    pub fn to_json(&self) -> Option<String> {
        match self {
            SealedProductSubtype::Unknown => None,
            _ => Some(format!("{:?}", self).to_lowercase()),
        }
    }

    /// Create from string
    #[staticmethod]
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "default" => SealedProductSubtype::Default,
            "draft" => SealedProductSubtype::Draft,
            "play" => SealedProductSubtype::Play,
            "set" => SealedProductSubtype::Set,
            "collector" => SealedProductSubtype::Collector,
            "jumpstart" => SealedProductSubtype::Jumpstart,
            "promotional" => SealedProductSubtype::Promotional,
            "theme" => SealedProductSubtype::Theme,
            "welcome" => SealedProductSubtype::Welcome,
            "topper" => SealedProductSubtype::Topper,
            "six-card" => SealedProductSubtype::Six,
            "planeswalker" => SealedProductSubtype::Planeswalker,
            "challenge" => SealedProductSubtype::Challenge,
            "challenger" => SealedProductSubtype::Challenger,
            "event" => SealedProductSubtype::Event,
            "championship" => SealedProductSubtype::Championship,
            "intro" => SealedProductSubtype::Intro,
            "commander" => SealedProductSubtype::Commander,
            "brawl" => SealedProductSubtype::Brawl,
            "archenemy" => SealedProductSubtype::Archenemy,
            "planechase" => SealedProductSubtype::Planechase,
            _ => SealedProductSubtype::Unknown,
        }
    }
}

impl Default for SealedProductSubtype {
    fn default() -> Self {
        SealedProductSubtype::Unknown
    }
}

/// MTGJSON Singular Sealed Product Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonSealedProduct")]
pub struct MtgjsonSealedProduct {
    #[pyo3(get, set)]
    pub name: String,
    
    #[pyo3(get, set)]
    pub uuid: String,
    
    #[pyo3(get, set)]
    pub identifiers: MtgjsonIdentifiers,
    
    #[pyo3(get, set)]
    pub purchase_urls: MtgjsonPurchaseUrls,
    
    #[serde(skip)]
    #[pyo3(get, set)]
    pub raw_purchase_urls: HashMap<String, String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub release_date: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub language: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub category: Option<SealedProductCategory>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub subtype: Option<SealedProductSubtype>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub contents: Option<HashMap<String, serde_json::Value>>,
    
    /// Number of packs in a booster box [DEPRECATED]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub product_size: Option<i32>,
    
    /// Number of cards in a booster pack or deck
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub card_count: Option<i32>,
}

#[pymethods]
impl MtgjsonSealedProduct {
    #[new]
    pub fn new() -> Self {
        Self {
            name: String::new(),
            uuid: String::new(),
            identifiers: MtgjsonIdentifiers::new(),
            purchase_urls: MtgjsonPurchaseUrls::new(),
            raw_purchase_urls: HashMap::new(),
            release_date: None,
            language: None,
            category: None,
            subtype: None,
            contents: None,
            product_size: None,
            card_count: None,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self) -> PyResult<HashMap<String, serde_json::Value>> {
        let mut result = HashMap::new();
        
        if !self.name.is_empty() {
            result.insert("name".to_string(), serde_json::Value::String(self.name.clone()));
        }
        if !self.uuid.is_empty() {
            result.insert("uuid".to_string(), serde_json::Value::String(self.uuid.clone()));
        }
        
        // Include identifiers
        if let Ok(identifiers_json) = serde_json::to_value(&self.identifiers) {
            result.insert("identifiers".to_string(), identifiers_json);
        }
        
        // Include purchase URLs
        if let Ok(urls_json) = serde_json::to_value(&self.purchase_urls) {
            result.insert("purchaseUrls".to_string(), urls_json);
        }
        
        if let Some(ref val) = self.release_date {
            if !val.is_empty() {
                result.insert("releaseDate".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.language {
            if !val.is_empty() {
                result.insert("language".to_string(), serde_json::Value::String(val.clone()));
            }
        }
        
        if let Some(ref val) = self.category {
            if let Some(category_str) = val.to_json() {
                result.insert("category".to_string(), serde_json::Value::String(category_str));
            }
        }
        
        if let Some(ref val) = self.subtype {
            if let Some(subtype_str) = val.to_json() {
                result.insert("subtype".to_string(), serde_json::Value::String(subtype_str));
            }
        }
        
        if let Some(ref val) = self.contents {
            result.insert("contents".to_string(), serde_json::to_value(val).unwrap());
        }
        
        if let Some(val) = self.product_size {
            result.insert("productSize".to_string(), serde_json::Value::Number(val.into()));
        }
        
        if let Some(val) = self.card_count {
            result.insert("cardCount".to_string(), serde_json::Value::Number(val.into()));
        }
        
        Ok(result)
    }

    /// Check if sealed product has meaningful content
    pub fn has_content(&self) -> bool {
        !self.name.is_empty() || !self.uuid.is_empty()
    }

    /// Generate UUID if not set
    pub fn generate_uuid(&mut self) {
        if self.uuid.is_empty() {
            self.uuid = uuid::Uuid::new_v4().to_string();
        }
    }
}

impl Default for MtgjsonSealedProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonSealedProduct {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("raw_purchase_urls".to_string());
        keys_to_skip
    }
}