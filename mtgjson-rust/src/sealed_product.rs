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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<SealedProductCategory>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<SealedProductSubtype>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifiers: Option<crate::identifiers::MtgjsonIdentifiers>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purchase_urls: Option<crate::purchase_urls::MtgjsonPurchaseUrls>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_purchase_urls: Option<crate::purchase_urls::MtgjsonPurchaseUrls>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>,
}

#[pymethods]
impl MtgjsonSealedProduct {
    #[new]
    pub fn new() -> Self {
        Self {
            category: None,
            subtype: None,
            // Initialize like Python: identifiers, purchase_urls, raw_purchase_urls are initialized
            identifiers: Some(crate::identifiers::MtgjsonIdentifiers::new()),
            name: None,
            purchase_urls: Some(crate::purchase_urls::MtgjsonPurchaseUrls::new()),
            raw_purchase_urls: Some(crate::purchase_urls::MtgjsonPurchaseUrls::new()),
            release_date: None,
            uuid: None,
            contents: None,
        }
    }

    /// Convert to JSON string
    pub fn to_json_string(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Check if sealed product has meaningful content
    pub fn has_content(&self) -> bool {
        self.name.as_ref().map_or(false, |n| !n.is_empty()) || 
        self.uuid.as_ref().map_or(false, |u| !u.is_empty())
    }

    /// Get a summary of the sealed product
    pub fn get_summary(&self) -> String {
        format!(
            "SealedProduct: {} ({})",
            self.name.as_ref().unwrap_or(&"Unknown".to_string()),
            self.uuid.as_ref().unwrap_or(&"No UUID".to_string())
        )
    }

    /// Generate UUID if not set
    pub fn generate_uuid(&mut self) {
        if self.uuid.is_none() || self.uuid.as_ref().unwrap().is_empty() {
            let new_uuid = uuid::Uuid::new_v4().to_string();
            self.uuid = Some(new_uuid);
        }
    }

    /// Convert to JSON like Python to_json() method
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })
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