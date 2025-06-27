use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
    #[serde(rename = "other")]
    Other,
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
    #[serde(rename = "booster")]
    Booster,
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
#[pyclass(name = "MtgjsonSealedProductObject")]
pub struct MtgjsonSealedProductObject {
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
    pub count: Option<i32>,

    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_code: Option<String>,

    // Change to JSON string for PyO3 compatibility
    #[pyo3(get, set)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>, // JSON string instead of HashMap<String, serde_json::Value>
}

#[pymethods]
impl MtgjsonSealedProductObject {
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
            count: None,
            set_code: None,
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

impl Default for MtgjsonSealedProductObject {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonSealedProductObject {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        let mut keys_to_skip = HashSet::new();
        keys_to_skip.insert("raw_purchase_urls".to_string());
        keys_to_skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sealed_product_creation() {
        let product = MtgjsonSealedProductObject::new();
        assert_eq!(product.name, None);
        assert_eq!(product.category, None);
        assert_eq!(product.subtype, None);
        assert_eq!(product.count, None);
        assert_eq!(product.uuid, None);
        assert_eq!(product.release_date, None);
        assert_eq!(product.set_code, None);
        assert_eq!(product.purchase_urls, None);
        assert_eq!(product.identifiers, None);
        assert_eq!(product.raw_purchase_urls, None);
    }

    #[test]
    fn test_sealed_product_category_enum() {
        // Test all category variants
        let booster_pack = SealedProductCategory::BoosterPack;
        let bundle = SealedProductCategory::Bundle;
        let deck = SealedProductCategory::Deck;
        let prerelease_pack = SealedProductCategory::PrereleasePack;
        let other = SealedProductCategory::Other;

        // Test serialization/deserialization
        let categories = vec![booster_pack, bundle, deck, prerelease_pack, other];
        for category in categories {
            let json = serde_json::to_string(&category).unwrap();
            let deserialized: SealedProductCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(category, deserialized);
        }
    }

    #[test]
    fn test_sealed_product_subtype_enum() {
        // Test all subtype variants
        let booster = SealedProductSubtype::Booster;
        let fat_pack = SealedProductSubtype::FatPack;
        let prerelease = SealedProductSubtype::Prerelease;
        let intro_pack = SealedProductSubtype::IntroPack;
        let other = SealedProductSubtype::Other;

        // Test serialization/deserialization
        let subtypes = vec![booster, fat_pack, prerelease, intro_pack, other];
        for subtype in subtypes {
            let json = serde_json::to_string(&subtype).unwrap();
            let deserialized: SealedProductSubtype = serde_json::from_str(&json).unwrap();
            assert_eq!(subtype, deserialized);
        }
    }

    #[test]
    fn test_sealed_product_complete_object() {
        let mut product = MtgjsonSealedProductObject::new();
        
        product.name = Some("Innistrad: Midnight Hunt Bundle".to_string());
        product.category = Some(SealedProductCategory::Bundle);
        product.subtype = Some(SealedProductSubtype::FatPack);
        product.count = Some(10); // 10 booster packs
        product.uuid = Some("12345678-1234-1234-1234-123456789abc".to_string());
        product.release_date = Some("2021-09-24".to_string());
        product.set_code = Some("MID".to_string());
        
        // Verify all fields
        assert_eq!(product.name, Some("Innistrad: Midnight Hunt Bundle".to_string()));
        assert_eq!(product.category, Some(SealedProductCategory::Bundle));
        assert_eq!(product.subtype, Some(SealedProductSubtype::FatPack));
        assert_eq!(product.count, Some(10));
        assert_eq!(product.uuid, Some("12345678-1234-1234-1234-123456789abc".to_string()));
        assert_eq!(product.release_date, Some("2021-09-24".to_string()));
        assert_eq!(product.set_code, Some("MID".to_string()));
    }

    #[test]
    fn test_sealed_product_booster_pack() {
        let mut product = MtgjsonSealedProductObject::new();
        
        product.name = Some("Throne of Eldraine Booster Pack".to_string());
        product.category = Some(SealedProductCategory::BoosterPack);
        product.subtype = Some(SealedProductSubtype::Booster);
        product.count = Some(15); // 15 cards
        product.set_code = Some("ELD".to_string());
        
        assert_eq!(product.category, Some(SealedProductCategory::BoosterPack));
        assert_eq!(product.subtype, Some(SealedProductSubtype::Booster));
        assert_eq!(product.count, Some(15));
    }

    #[test]
    fn test_sealed_product_prerelease_pack() {
        let mut product = MtgjsonSealedProductObject::new();
        
        product.name = Some("Zendikar Rising Prerelease Pack".to_string());
        product.category = Some(SealedProductCategory::PrereleasePack);
        product.subtype = Some(SealedProductSubtype::Prerelease);
        product.count = Some(6); // 6 booster packs
        product.set_code = Some("ZNR".to_string());
        
        assert_eq!(product.category, Some(SealedProductCategory::PrereleasePack));
        assert_eq!(product.subtype, Some(SealedProductSubtype::Prerelease));
        assert_eq!(product.count, Some(6));
    }

    #[test]
    fn test_sealed_product_deck() {
        let mut product = MtgjsonSealedProductObject::new();
        
        product.name = Some("Commander Legends Commander Deck".to_string());
        product.category = Some(SealedProductCategory::Deck);
        product.subtype = Some(SealedProductSubtype::Other);
        product.count = Some(100); // 100 cards
        product.set_code = Some("CMR").to_string();
        
        assert_eq!(product.category, Some(SealedProductCategory::Deck));
        assert_eq!(product.count, Some(100));
    }

    #[test]
    fn test_sealed_product_edge_cases() {
        let mut product = MtgjsonSealedProductObject::new();
        
        // Test very long product name
        let long_name = "Very Long Product Name ".repeat(50);
        product.name = Some(long_name.clone());
        assert_eq!(product.name, Some(long_name));
        
        // Test zero count
        product.count = Some(0);
        assert_eq!(product.count, Some(0));
        
        // Test maximum count
        product.count = Some(i32::MAX);
        assert_eq!(product.count, Some(i32::MAX));
        
        // Test empty strings
        product.set_code = Some("".to_string());
        assert_eq!(product.set_code, Some("".to_string()));
    }

    #[test]
    fn test_sealed_product_identifiers() {
        let mut product = MtgjsonSealedProductObject::new();
        
        // Create identifiers object
        let mut identifiers = crate::classes::MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.tcgplayer_id = Some("67890".to_string());
        
        product.identifiers = Some(identifiers.clone());
        
        assert!(product.identifiers.is_some());
        assert_eq!(product.identifiers.as_ref().unwrap().card_kingdom_id, Some("12345".to_string()));
        assert_eq!(product.identifiers.as_ref().unwrap().tcgplayer_id, Some("67890".to_string()));
    }

    #[test]
    fn test_sealed_product_purchase_urls() {
        let mut product = MtgjsonSealedProductObject::new();
        
        // Create purchase URLs object
        let mut purchase_urls = crate::classes::MtgjsonPurchaseUrls::new();
        purchase_urls.card_kingdom = Some("https://cardkingdom.com/product/123".to_string());
        purchase_urls.tcgplayer = Some("https://tcgplayer.com/product/456".to_string());
        
        product.purchase_urls = Some(purchase_urls.clone());
        
        assert!(product.purchase_urls.is_some());
        assert_eq!(product.purchase_urls.as_ref().unwrap().card_kingdom, Some("https://cardkingdom.com/product/123".to_string()));
        assert_eq!(product.purchase_urls.as_ref().unwrap().tcgplayer, Some("https://tcgplayer.com/product/456".to_string()));
    }

    #[test]
    fn test_sealed_product_raw_purchase_urls() {
        let mut product = MtgjsonSealedProductObject::new();
        
        let mut raw_urls = HashMap::new();
        raw_urls.insert("cardkingdom".to_string(), "https://cardkingdom.com/raw/123".to_string());
        raw_urls.insert("tcgplayer".to_string(), "https://tcgplayer.com/raw/456".to_string());
        
        product.raw_purchase_urls = Some(raw_urls.clone());
        
        assert!(product.raw_purchase_urls.is_some());
        assert_eq!(product.raw_purchase_urls.as_ref().unwrap().get("cardkingdom"), Some(&"https://cardkingdom.com/raw/123".to_string()));
        assert_eq!(product.raw_purchase_urls.as_ref().unwrap().get("tcgplayer"), Some(&"https://tcgplayer.com/raw/456".to_string()));
    }

    #[test]
    fn test_sealed_product_json_serialization() {
        let mut product = MtgjsonSealedProductObject::new();
        
        product.name = Some("Test Product".to_string());
        product.category = Some(SealedProductCategory::Bundle);
        product.subtype = Some(SealedProductSubtype::FatPack);
        product.count = Some(8);
        product.set_code = Some("TST".to_string());
        product.uuid = Some("test-uuid-123".to_string());
        
        let json_result = serde_json::to_string(&product);
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("Test Product"));
        assert!(json_string.contains("Bundle"));
        assert!(json_string.contains("FatPack"));
        assert!(json_string.contains("8"));
        assert!(json_string.contains("TST"));
        
        // Test deserialization
        let deserialized: MtgjsonSealedProductObject = serde_json::from_str(&json_string).unwrap();
        assert_eq!(deserialized.name, Some("Test Product".to_string()));
        assert_eq!(deserialized.category, Some(SealedProductCategory::Bundle));
        assert_eq!(deserialized.subtype, Some(SealedProductSubtype::FatPack));
        assert_eq!(deserialized.count, Some(8));
        assert_eq!(deserialized.set_code, Some("TST".to_string()));
    }

    #[test]
    fn test_sealed_product_default_trait() {
        let product = MtgjsonSealedProductObject::default();
        assert_eq!(product.name, None);
        assert_eq!(product.category, None);
        assert_eq!(product.subtype, None);
        assert_eq!(product.count, None);
        assert_eq!(product.uuid, None);
        assert_eq!(product.release_date, None);
        assert_eq!(product.set_code, None);
        assert_eq!(product.purchase_urls, None);
        assert_eq!(product.identifiers, None);
        assert_eq!(product.raw_purchase_urls, None);
    }

    #[test]
    fn test_sealed_product_clone() {
        let mut original = MtgjsonSealedProductObject::new();
        original.name = Some("Original Product".to_string());
        original.category = Some(SealedProductCategory::BoosterPack);
        original.count = Some(15);
        
        let cloned = original.clone();
        assert_eq!(cloned.name, Some("Original Product".to_string()));
        assert_eq!(cloned.category, Some(SealedProductCategory::BoosterPack));
        assert_eq!(cloned.count, Some(15));
        
        // Verify independence
        assert_eq!(original.name, cloned.name);
    }

    #[test]
    fn test_sealed_product_equality() {
        let mut product1 = MtgjsonSealedProductObject::new();
        let mut product2 = MtgjsonSealedProductObject::new();
        
        product1.name = Some("Same Product".to_string());
        product1.category = Some(SealedProductCategory::Bundle);
        
        product2.name = Some("Same Product".to_string());
        product2.category = Some(SealedProductCategory::Bundle);
        
        assert_eq!(product1, product2);
        
        product2.category = Some(SealedProductCategory::BoosterPack);
        assert_ne!(product1, product2);
    }

    #[test]
    fn test_sealed_product_category_display() {
        // Test that categories can be converted to strings appropriately
        let categories = vec![
            SealedProductCategory::BoosterPack,
            SealedProductCategory::Bundle,
            SealedProductCategory::Deck,
            SealedProductCategory::PrereleasePack,
            SealedProductCategory::Other,
        ];
        
        for category in categories {
            let json = serde_json::to_string(&category).unwrap();
            assert!(!json.is_empty());
            assert!(json.len() > 2); // More than just quotes
        }
    }

    #[test]
    fn test_sealed_product_subtype_display() {
        // Test that subtypes can be converted to strings appropriately
        let subtypes = vec![
            SealedProductSubtype::Booster,
            SealedProductSubtype::FatPack,
            SealedProductSubtype::Prerelease,
            SealedProductSubtype::IntroPack,
            SealedProductSubtype::Other,
        ];
        
        for subtype in subtypes {
            let json = serde_json::to_string(&subtype).unwrap();
            assert!(!json.is_empty());
            assert!(json.len() > 2); // More than just quotes
        }
    }

    #[test]
    fn test_sealed_product_partial_data() {
        // Test products with only some fields set
        let mut product = MtgjsonSealedProductObject::new();
        product.name = Some("Partial Product".to_string());
        product.category = Some(SealedProductCategory::Other);
        // Leave other fields as None
        
        assert_eq!(product.name, Some("Partial Product".to_string()));
        assert_eq!(product.category, Some(SealedProductCategory::Other));
        assert_eq!(product.subtype, None);
        assert_eq!(product.count, None);
        assert_eq!(product.uuid, None);
    }

    #[test]
    fn test_sealed_product_date_validation() {
        let mut product = MtgjsonSealedProductObject::new();
        
        // Test various date formats
        let dates = vec![
            "2021-09-24",
            "2020-01-01", 
            "2025-12-31",
            "1993-08-05", // Alpha release date
        ];
        
        for date in dates {
            product.release_date = Some(date.to_string());
            assert_eq!(product.release_date, Some(date.to_string()));
        }
    }

    #[test]
    fn test_sealed_product_uuid_validation() {
        let mut product = MtgjsonSealedProductObject::new();
        
        // Test various UUID formats
        let uuids = vec![
            "12345678-1234-1234-1234-123456789abc",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "00000000-0000-0000-0000-000000000000",
        ];
        
        for uuid in uuids {
            product.uuid = Some(uuid.to_string());
            assert_eq!(product.uuid, Some(uuid.to_string()));
        }
    }

    #[test]
    fn test_sealed_product_comprehensive_example() {
        // Create a comprehensive sealed product example
        let product = MtgjsonSealedProductObject {
            name: Some("Kamigawa: Neon Dynasty Collector Booster Box".to_string()),
            category: Some(SealedProductCategory::BoosterPack),
            subtype: Some(SealedProductSubtype::Booster),
            count: Some(12), // 12 collector boosters
            uuid: Some("kamigawa-neon-dynasty-collector-box-2022".to_string()),
            release_date: Some("2022-02-18".to_string()),
            set_code: Some("NEO".to_string()),
            purchase_urls: None, // Would be filled by provider integration
            identifiers: None,   // Would be filled by provider integration
            raw_purchase_urls: None, // Would be filled by provider integration
        };
        
        // Verify all fields are set correctly
        assert_eq!(product.name, Some("Kamigawa: Neon Dynasty Collector Booster Box".to_string()));
        assert_eq!(product.category, Some(SealedProductCategory::BoosterPack));
        assert_eq!(product.subtype, Some(SealedProductSubtype::Booster));
        assert_eq!(product.count, Some(12));
        assert_eq!(product.uuid, Some("kamigawa-neon-dynasty-collector-box-2022".to_string()));
        assert_eq!(product.release_date, Some("2022-02-18".to_string()));
        assert_eq!(product.set_code, Some("NEO".to_string()));
        
        // Test JSON serialization of complete object
        let json_result = serde_json::to_string(&product);
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("Kamigawa"));
        assert!(json_string.contains("Collector"));
        assert!(json_string.contains("BoosterPack"));
        assert!(json_string.contains("NEO"));
        assert!(json_string.contains("2022-02-18"));
    }
}