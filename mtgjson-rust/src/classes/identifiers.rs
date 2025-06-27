use crate::base::{skip_if_empty_optional_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Card.Identifiers Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonIdentifiers")]
pub struct MtgjsonIdentifiers {
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom_etched_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom_foil_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub cardsphere_foil_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub cardsphere_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mcm_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mcm_meta_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtg_arena_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgjson_foil_version_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgjson_non_foil_version_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgjson_v4_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgo_foil_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub mtgo_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub multiverse_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub scryfall_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub scryfall_illustration_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub scryfall_card_back_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub scryfall_oracle_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub tcgplayer_etched_product_id: Option<String>,
    
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub tcgplayer_product_id: Option<String>,
}

impl Default for MtgjsonIdentifiers {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl MtgjsonIdentifiers {
    #[new]
    pub fn new() -> Self {
        Self {
            card_kingdom_etched_id: None,
            card_kingdom_foil_id: None,
            card_kingdom_id: Some(String::new()),
            cardsphere_foil_id: None,
            cardsphere_id: None,
            mcm_id: None,
            mcm_meta_id: None,
            mtg_arena_id: None,
            mtgjson_foil_version_id: None,
            mtgjson_non_foil_version_id: None,
            mtgjson_v4_id: None,
            mtgo_foil_id: None,
            mtgo_id: None,
            multiverse_id: Some(String::new()),
            scryfall_id: None,
            scryfall_illustration_id: None,
            scryfall_card_back_id: None,
            scryfall_oracle_id: None,
            tcgplayer_etched_product_id: None,
            tcgplayer_product_id: Some(String::new()),
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Convert to dictionary for Python compatibility
    pub fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut result = HashMap::new();
        
        if let Some(ref val) = self.card_kingdom_etched_id {
            if !val.is_empty() {
                result.insert("cardKingdomEtchedId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.card_kingdom_foil_id {
            if !val.is_empty() {
                result.insert("cardKingdomFoilId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.card_kingdom_id {
            if !val.is_empty() {
                result.insert("cardKingdomId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.cardsphere_foil_id {
            if !val.is_empty() {
                result.insert("cardsphereFoilId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.cardsphere_id {
            if !val.is_empty() {
                result.insert("cardsphereId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mcm_id {
            if !val.is_empty() {
                result.insert("mcmId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mcm_meta_id {
            if !val.is_empty() {
                result.insert("mcmMetaId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtg_arena_id {
            if !val.is_empty() {
                result.insert("mtgArenaId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtgjson_foil_version_id {
            if !val.is_empty() {
                result.insert("mtgjsonFoilVersionId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtgjson_non_foil_version_id {
            if !val.is_empty() {
                result.insert("mtgjsonNonFoilVersionId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtgjson_v4_id {
            if !val.is_empty() {
                result.insert("mtgjsonV4Id".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtgo_foil_id {
            if !val.is_empty() {
                result.insert("mtgoFoilId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.mtgo_id {
            if !val.is_empty() {
                result.insert("mtgoId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.multiverse_id {
            if !val.is_empty() {
                result.insert("multiverseId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.scryfall_id {
            if !val.is_empty() {
                result.insert("scryfallId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.scryfall_illustration_id {
            if !val.is_empty() {
                result.insert("scryfallIllustrationId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.scryfall_card_back_id {
            if !val.is_empty() {
                result.insert("scryfallCardBackId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.scryfall_oracle_id {
            if !val.is_empty() {
                result.insert("scryfallOracleId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.tcgplayer_etched_product_id {
            if !val.is_empty() {
                result.insert("tcgplayerEtchedProductId".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.tcgplayer_product_id {
            if !val.is_empty() {
                result.insert("tcgplayerProductId".to_string(), val.clone());
            }
        }
        
        Ok(result)
    }
}

impl JsonObject for MtgjsonIdentifiers {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        HashSet::new() // All empty values are handled by serde skip_serializing_if
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifiers_creation() {
        let identifiers = MtgjsonIdentifiers::new();
        assert_eq!(identifiers.card_kingdom_etched_id, None);
        assert_eq!(identifiers.card_kingdom_foil_id, None);
        assert_eq!(identifiers.card_kingdom_id, Some(String::new()));
        assert_eq!(identifiers.cardsphere_foil_id, None);
        assert_eq!(identifiers.cardsphere_id, None);
        assert_eq!(identifiers.mcm_id, None);
        assert_eq!(identifiers.mcm_meta_id, None);
        assert_eq!(identifiers.mtg_arena_id, None);
        assert_eq!(identifiers.mtgjson_foil_version_id, None);
        assert_eq!(identifiers.mtgjson_non_foil_version_id, None);
        assert_eq!(identifiers.mtgjson_v4_id, None);
        assert_eq!(identifiers.mtgo_foil_id, None);
        assert_eq!(identifiers.mtgo_id, None);
        assert_eq!(identifiers.multiverse_id, Some(String::new()));
        assert_eq!(identifiers.scryfall_id, None);
        assert_eq!(identifiers.scryfall_illustration_id, None);
        assert_eq!(identifiers.scryfall_card_back_id, None);
        assert_eq!(identifiers.scryfall_oracle_id, None);
        assert_eq!(identifiers.tcgplayer_etched_product_id, None);
        assert_eq!(identifiers.tcgplayer_product_id, Some(String::new()));
    }

    #[test]
    fn test_identifiers_default() {
        let identifiers = MtgjsonIdentifiers::default();
        assert_eq!(identifiers.card_kingdom_etched_id, None);
        assert_eq!(identifiers.card_kingdom_foil_id, None);
        assert_eq!(identifiers.card_kingdom_id, Some(String::new()));
        assert_eq!(identifiers.cardsphere_foil_id, None);
        assert_eq!(identifiers.mcm_id, None);
    }

    #[test]
    fn test_identifiers_with_string_values() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.card_kingdom_foil_id = Some("12346".to_string());
        identifiers.card_kingdom_etched_id = Some("12347".to_string());
        identifiers.cardsphere_foil_id = Some("CH12345".to_string());
        identifiers.cardsphere_id = Some("CH67890".to_string());
        identifiers.mcm_id = Some("67890".to_string());
        identifiers.mcm_meta_id = Some("67891".to_string());
        identifiers.mtg_arena_id = Some("98765".to_string());
        identifiers.mtgjson_foil_version_id = Some("foil123".to_string());
        identifiers.mtgjson_non_foil_version_id = Some("nonfoil123".to_string());
        identifiers.mtgjson_v4_id = Some("v4-id-123".to_string());
        identifiers.mtgo_foil_id = Some("MTGO12345".to_string());
        identifiers.mtgo_id = Some("MTGO12346".to_string());
        identifiers.multiverse_id = Some("112233".to_string());
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.scryfall_oracle_id = Some("oracle123".to_string());
        identifiers.scryfall_illustration_id = Some("illus123".to_string());
        identifiers.scryfall_card_back_id = Some("back123".to_string());
        identifiers.tcgplayer_product_id = Some("55555".to_string());
        identifiers.tcgplayer_etched_product_id = Some("55556".to_string());
        
        assert_eq!(identifiers.card_kingdom_id, Some("12345".to_string()));
        assert_eq!(identifiers.card_kingdom_foil_id, Some("12346".to_string()));
        assert_eq!(identifiers.card_kingdom_etched_id, Some("12347".to_string()));
        assert_eq!(identifiers.cardsphere_foil_id, Some("CH12345".to_string()));
        assert_eq!(identifiers.cardsphere_id, Some("CH67890".to_string()));
        assert_eq!(identifiers.mcm_id, Some("67890".to_string()));
        assert_eq!(identifiers.mcm_meta_id, Some("67891".to_string()));
        assert_eq!(identifiers.mtg_arena_id, Some("98765".to_string()));
        assert_eq!(identifiers.mtgjson_foil_version_id, Some("foil123".to_string()));
        assert_eq!(identifiers.mtgjson_non_foil_version_id, Some("nonfoil123".to_string()));
        assert_eq!(identifiers.mtgjson_v4_id, Some("v4-id-123".to_string()));
        assert_eq!(identifiers.mtgo_foil_id, Some("MTGO12345".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("MTGO12346".to_string()));
        assert_eq!(identifiers.multiverse_id, Some("112233".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("abc123def".to_string()));
        assert_eq!(identifiers.scryfall_oracle_id, Some("oracle123".to_string()));
        assert_eq!(identifiers.scryfall_illustration_id, Some("illus123".to_string()));
        assert_eq!(identifiers.scryfall_card_back_id, Some("back123".to_string()));
        assert_eq!(identifiers.tcgplayer_product_id, Some("55555".to_string()));
        assert_eq!(identifiers.tcgplayer_etched_product_id, Some("55556".to_string()));
    }

    #[test]
    fn test_identifiers_to_dict() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.multiverse_id = Some("112233".to_string());
        identifiers.tcgplayer_product_id = Some("55555".to_string());
        
        let dict_result = identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        assert_eq!(dict.get("cardKingdomId"), Some(&"12345".to_string()));
        assert_eq!(dict.get("scryfallId"), Some(&"abc123def".to_string()));
        assert_eq!(dict.get("multiverseId"), Some(&"112233".to_string()));
        assert_eq!(dict.get("tcgplayerProductId"), Some(&"55555".to_string()));
    }

    #[test]
    fn test_identifiers_to_dict_with_empty_values() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("".to_string());
        identifiers.scryfall_id = Some("valid-uuid".to_string());
        
        let dict_result = identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        // Empty strings should be filtered out
        assert!(!dict.contains_key("cardKingdomId"));
        assert_eq!(dict.get("scryfallId"), Some(&"valid-uuid".to_string()));
    }

    #[test]
    fn test_identifiers_json_serialization() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.multiverse_id = Some("112233".to_string());
        
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("12345"));
        assert!(json_string.contains("abc123def"));
        assert!(json_string.contains("112233"));
    }

    #[test]
    fn test_identifiers_json_object_trait() {
        let identifiers = MtgjsonIdentifiers::new();
        let keys_to_skip = identifiers.build_keys_to_skip();
        
        // Should return empty set unless specifically implemented
        assert!(keys_to_skip.is_empty());
    }

    // COMPREHENSIVE ADDITIONAL TESTS FOR FULL COVERAGE

    #[test]
    fn test_identifiers_clone_trait() {
        let mut original = MtgjsonIdentifiers::new();
        original.card_kingdom_id = Some("12345".to_string());
        original.scryfall_id = Some("abc123".to_string());
        original.multiverse_id = Some("67890".to_string());
        
        let cloned = original.clone();
        
        assert_eq!(original.card_kingdom_id, cloned.card_kingdom_id);
        assert_eq!(original.scryfall_id, cloned.scryfall_id);
        assert_eq!(original.multiverse_id, cloned.multiverse_id);
    }

    #[test]
    fn test_identifiers_partial_eq_trait() {
        let mut identifiers1 = MtgjsonIdentifiers::new();
        let mut identifiers2 = MtgjsonIdentifiers::new();
        
        identifiers1.card_kingdom_id = Some("12345".to_string());
        identifiers1.scryfall_id = Some("abc123".to_string());
        
        identifiers2.card_kingdom_id = Some("12345".to_string());
        identifiers2.scryfall_id = Some("abc123".to_string());
        
        assert_eq!(identifiers1, identifiers2);
        
        identifiers2.card_kingdom_id = Some("54321".to_string());
        assert_ne!(identifiers1, identifiers2);
    }

    #[test]
    fn test_identifiers_debug_trait() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("abc123".to_string());
        
        let debug_str = format!("{:?}", identifiers);
        assert!(debug_str.contains("MtgjsonIdentifiers"));
        assert!(debug_str.contains("12345"));
        assert!(debug_str.contains("abc123"));
    }

    #[test]
    fn test_identifiers_all_none() {
        let identifiers = MtgjsonIdentifiers::new();
        
        // All fields should be None initially (except defaults)
        assert_eq!(identifiers.card_kingdom_etched_id, None);
        assert_eq!(identifiers.card_kingdom_foil_id, None);
        assert_eq!(identifiers.cardsphere_foil_id, None);
        assert_eq!(identifiers.cardsphere_id, None);
        assert_eq!(identifiers.mcm_id, None);
        assert_eq!(identifiers.mcm_meta_id, None);
        assert_eq!(identifiers.mtg_arena_id, None);
        assert_eq!(identifiers.mtgjson_foil_version_id, None);
        assert_eq!(identifiers.mtgjson_non_foil_version_id, None);
        assert_eq!(identifiers.mtgjson_v4_id, None);
        assert_eq!(identifiers.mtgo_foil_id, None);
        assert_eq!(identifiers.mtgo_id, None);
        assert_eq!(identifiers.scryfall_id, None);
        assert_eq!(identifiers.scryfall_oracle_id, None);
        assert_eq!(identifiers.scryfall_illustration_id, None);
        assert_eq!(identifiers.scryfall_card_back_id, None);
        assert_eq!(identifiers.tcgplayer_etched_product_id, None);
    }

    #[test]
    fn test_identifiers_uuid_formats() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test various UUID formats
        identifiers.scryfall_id = Some("12345678-1234-5678-9012-123456789012".to_string());
        identifiers.scryfall_oracle_id = Some("87654321-4321-8765-2109-876543210987".to_string());
        identifiers.scryfall_illustration_id = Some("abcdef12-3456-789a-bcde-f123456789ab".to_string());
        identifiers.scryfall_card_back_id = Some("fedcba98-7654-321f-edcb-a987654321fe".to_string());
        
        assert_eq!(identifiers.scryfall_id, Some("12345678-1234-5678-9012-123456789012".to_string()));
        assert_eq!(identifiers.scryfall_oracle_id, Some("87654321-4321-8765-2109-876543210987".to_string()));
        assert_eq!(identifiers.scryfall_illustration_id, Some("abcdef12-3456-789a-bcde-f123456789ab".to_string()));
        assert_eq!(identifiers.scryfall_card_back_id, Some("fedcba98-7654-321f-edcb-a987654321fe".to_string()));
    }

    #[test]
    fn test_identifiers_numeric_strings() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test numeric IDs as strings
        identifiers.card_kingdom_id = Some("1".to_string());
        identifiers.card_kingdom_foil_id = Some("999999".to_string());
        identifiers.card_kingdom_etched_id = Some("0".to_string());
        identifiers.mcm_id = Some("123456789".to_string());
        identifiers.mcm_meta_id = Some("987654321".to_string());
        identifiers.mtg_arena_id = Some("55555".to_string());
        identifiers.multiverse_id = Some("444444".to_string());
        identifiers.tcgplayer_product_id = Some("777777".to_string());
        identifiers.tcgplayer_etched_product_id = Some("888888".to_string());
        
        assert_eq!(identifiers.card_kingdom_id, Some("1".to_string()));
        assert_eq!(identifiers.card_kingdom_foil_id, Some("999999".to_string()));
        assert_eq!(identifiers.card_kingdom_etched_id, Some("0".to_string()));
        assert_eq!(identifiers.mcm_id, Some("123456789".to_string()));
        assert_eq!(identifiers.mcm_meta_id, Some("987654321".to_string()));
        assert_eq!(identifiers.mtg_arena_id, Some("55555".to_string()));
        assert_eq!(identifiers.multiverse_id, Some("444444".to_string()));
        assert_eq!(identifiers.tcgplayer_product_id, Some("777777".to_string()));
        assert_eq!(identifiers.tcgplayer_etched_product_id, Some("888888".to_string()));
    }

    #[test]
    fn test_identifiers_alphanumeric_strings() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        identifiers.cardsphere_foil_id = Some("CH123ABC".to_string());
        identifiers.cardsphere_id = Some("CS456DEF".to_string());
        identifiers.mtgjson_foil_version_id = Some("mtgjson-foil-v1.2.3".to_string());
        identifiers.mtgjson_non_foil_version_id = Some("mtgjson-nonfoil-v1.2.3".to_string());
        identifiers.mtgjson_v4_id = Some("mtgjson-v4-legacy-id".to_string());
        identifiers.mtgo_foil_id = Some("MTGO456DEF".to_string());
        identifiers.mtgo_id = Some("MTGO789GHI".to_string());
        
        assert_eq!(identifiers.cardsphere_foil_id, Some("CH123ABC".to_string()));
        assert_eq!(identifiers.cardsphere_id, Some("CS456DEF".to_string()));
        assert_eq!(identifiers.mtgjson_foil_version_id, Some("mtgjson-foil-v1.2.3".to_string()));
        assert_eq!(identifiers.mtgjson_non_foil_version_id, Some("mtgjson-nonfoil-v1.2.3".to_string()));
        assert_eq!(identifiers.mtgjson_v4_id, Some("mtgjson-v4-legacy-id".to_string()));
        assert_eq!(identifiers.mtgo_foil_id, Some("MTGO456DEF".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("MTGO789GHI".to_string()));
    }

    #[test]
    fn test_identifiers_edge_case_values() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test with edge case values
        identifiers.card_kingdom_id = Some("0".to_string());  // Zero ID
        identifiers.multiverse_id = Some("4294967295".to_string());  // Large numeric string
        identifiers.scryfall_id = Some("".to_string());  // Empty string
        identifiers.mtgo_id = Some("a".to_string());  // Single character
        identifiers.cardsphere_id = Some("very-long-identifier-that-exceeds-normal-length-expectations-for-testing".to_string());
        
        assert_eq!(identifiers.card_kingdom_id, Some("0".to_string()));
        assert_eq!(identifiers.multiverse_id, Some("4294967295".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("a".to_string()));
        assert_eq!(identifiers.cardsphere_id, Some("very-long-identifier-that-exceeds-normal-length-expectations-for-testing".to_string()));
    }

    #[test]
    fn test_identifiers_special_characters() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test with special characters that might appear in IDs
        identifiers.mtgjson_v4_id = Some("mtgjson-v4_legacy.id@domain.com".to_string());
        identifiers.scryfall_id = Some("12345678-abcd-efgh-ijkl-mnopqrstuvwx".to_string());
        identifiers.cardsphere_foil_id = Some("CH#123$456%789".to_string());
        
        assert_eq!(identifiers.mtgjson_v4_id, Some("mtgjson-v4_legacy.id@domain.com".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("12345678-abcd-efgh-ijkl-mnopqrstuvwx".to_string()));
        assert_eq!(identifiers.cardsphere_foil_id, Some("CH#123$456%789".to_string()));
    }

    #[test]
    fn test_identifiers_to_dict_comprehensive() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Set all possible identifiers
        identifiers.card_kingdom_etched_id = Some("cke1".to_string());
        identifiers.card_kingdom_foil_id = Some("ckf2".to_string());
        identifiers.card_kingdom_id = Some("ck3".to_string());
        identifiers.cardsphere_foil_id = Some("csf4".to_string());
        identifiers.cardsphere_id = Some("cs5".to_string());
        identifiers.mcm_id = Some("mcm6".to_string());
        identifiers.mcm_meta_id = Some("mcmm7".to_string());
        identifiers.mtg_arena_id = Some("arena8".to_string());
        identifiers.mtgjson_foil_version_id = Some("mjf9".to_string());
        identifiers.mtgjson_non_foil_version_id = Some("mjnf10".to_string());
        identifiers.mtgjson_v4_id = Some("mjv4_11".to_string());
        identifiers.mtgo_foil_id = Some("mtgof12".to_string());
        identifiers.mtgo_id = Some("mtgo13".to_string());
        identifiers.multiverse_id = Some("mv14".to_string());
        identifiers.scryfall_id = Some("sf15".to_string());
        identifiers.scryfall_illustration_id = Some("sfi16".to_string());
        identifiers.scryfall_card_back_id = Some("sfcb17".to_string());
        identifiers.scryfall_oracle_id = Some("sfo18".to_string());
        identifiers.tcgplayer_etched_product_id = Some("tcge19".to_string());
        identifiers.tcgplayer_product_id = Some("tcg20".to_string());
        
        let dict_result = identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        
        // Check all keys are properly converted to camelCase
        assert_eq!(dict.get("cardKingdomEtchedId"), Some(&"cke1".to_string()));
        assert_eq!(dict.get("cardKingdomFoilId"), Some(&"ckf2".to_string()));
        assert_eq!(dict.get("cardKingdomId"), Some(&"ck3".to_string()));
        assert_eq!(dict.get("cardsphereFoilId"), Some(&"csf4".to_string()));
        assert_eq!(dict.get("cardsphereId"), Some(&"cs5".to_string()));
        assert_eq!(dict.get("mcmId"), Some(&"mcm6".to_string()));
        assert_eq!(dict.get("mcmMetaId"), Some(&"mcmm7".to_string()));
        assert_eq!(dict.get("mtgArenaId"), Some(&"arena8".to_string()));
        assert_eq!(dict.get("mtgjsonFoilVersionId"), Some(&"mjf9".to_string()));
        assert_eq!(dict.get("mtgjsonNonFoilVersionId"), Some(&"mjnf10".to_string()));
        assert_eq!(dict.get("mtgjsonV4Id"), Some(&"mjv4_11".to_string()));
        assert_eq!(dict.get("mtgoFoilId"), Some(&"mtgof12".to_string()));
        assert_eq!(dict.get("mtgoId"), Some(&"mtgo13".to_string()));
        assert_eq!(dict.get("multiverseId"), Some(&"mv14".to_string()));
        assert_eq!(dict.get("scryfallId"), Some(&"sf15".to_string()));
        assert_eq!(dict.get("scryfallIllustrationId"), Some(&"sfi16".to_string()));
        assert_eq!(dict.get("scryfallCardBackId"), Some(&"sfcb17".to_string()));
        assert_eq!(dict.get("scryfallOracleId"), Some(&"sfo18".to_string()));
        assert_eq!(dict.get("tcgplayerEtchedProductId"), Some(&"tcge19".to_string()));
        assert_eq!(dict.get("tcgplayerProductId"), Some(&"tcg20".to_string()));
        
        // Should have all 20 fields
        assert_eq!(dict.len(), 20);
    }

    #[test]
    fn test_identifiers_serialization_deserialization() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.multiverse_id = Some("67890".to_string());
        identifiers.tcgplayer_product_id = Some("55555".to_string());
        identifiers.mtg_arena_id = Some("arena123".to_string());
        
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        
        // Test that serialization contains expected fields
        assert!(json_str.contains("12345"));
        assert!(json_str.contains("abc123def"));
        assert!(json_str.contains("67890"));
        assert!(json_str.contains("55555"));
        assert!(json_str.contains("arena123"));
        
        // Test deserialization
        let deserialized: Result<MtgjsonIdentifiers, _> = serde_json::from_str(&json_str);
        assert!(deserialized.is_ok());
        
        let deserialized_identifiers = deserialized.unwrap();
        assert_eq!(deserialized_identifiers.card_kingdom_id, Some("12345".to_string()));
        assert_eq!(deserialized_identifiers.scryfall_id, Some("abc123def".to_string()));
        assert_eq!(deserialized_identifiers.multiverse_id, Some("67890".to_string()));
        assert_eq!(deserialized_identifiers.tcgplayer_product_id, Some("55555".to_string()));
        assert_eq!(deserialized_identifiers.mtg_arena_id, Some("arena123".to_string()));
    }

    #[test]
    fn test_identifiers_complex_integration_scenario() {
        // Create a comprehensive identifiers object for a famous card like Black Lotus
        let mut black_lotus_identifiers = MtgjsonIdentifiers::new();
        
        // Set up realistic identifiers for Black Lotus (Alpha)
        black_lotus_identifiers.card_kingdom_id = Some("1001".to_string());
        black_lotus_identifiers.cardsphere_id = Some("CS-LOTUS-001".to_string());
        black_lotus_identifiers.mcm_id = Some("1".to_string()); // Often low for Alpha cards
        black_lotus_identifiers.mtg_arena_id = None; // Not on Arena
        black_lotus_identifiers.mtgo_id = Some("MTGO-1".to_string());
        black_lotus_identifiers.multiverse_id = Some("3".to_string()); // Actual Multiverse ID
        black_lotus_identifiers.scryfall_id = Some("bd8fa327-dd41-4737-8f19-2cf5eb1f7cdd".to_string());
        black_lotus_identifiers.scryfall_oracle_id = Some("272b85cf-3286-4b7e-8ccf-2c83a2bb9a48".to_string());
        black_lotus_identifiers.scryfall_illustration_id = Some("5f2b7b03-3c76-42f7-9ee4-00f4d5b4e0c8".to_string());
        black_lotus_identifiers.tcgplayer_product_id = Some("1".to_string());
        
        // Test all functionality on this complex scenario
        assert_eq!(black_lotus_identifiers.card_kingdom_id, Some("1001".to_string()));
        assert_eq!(black_lotus_identifiers.multiverse_id, Some("3".to_string()));
        assert_eq!(black_lotus_identifiers.mtg_arena_id, None);
        
        // Test to_dict conversion
        let dict_result = black_lotus_identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        assert_eq!(dict.get("cardKingdomId"), Some(&"1001".to_string()));
        assert_eq!(dict.get("multiverseId"), Some(&"3".to_string()));
        assert_eq!(dict.get("scryfallId"), Some(&"bd8fa327-dd41-4737-8f19-2cf5eb1f7cdd".to_string()));
        assert!(!dict.contains_key("mtgArenaId")); // Should not contain None values
        
        // Test JSON serialization
        let json_result = black_lotus_identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        assert!(json_str.contains("1001"));
        assert!(json_str.contains("bd8fa327-dd41-4737-8f19-2cf5eb1f7cdd"));
        assert!(json_str.contains("272b85cf-3286-4b7e-8ccf-2c83a2bb9a48"));
        
        // Test cloning
        let cloned = black_lotus_identifiers.clone();
        assert_eq!(black_lotus_identifiers, cloned);
    }

    #[test]
    fn test_identifiers_empty_dict_conversion() {
        let identifiers = MtgjsonIdentifiers::new();
        
        let dict_result = identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        
        // Should be empty since default values are empty strings which get filtered
        assert!(dict.is_empty());
    }

    #[test]
    fn test_identifiers_mixed_none_and_values() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Set only some identifiers to test partial data
        identifiers.scryfall_id = Some("scryfall-uuid".to_string());
        identifiers.multiverse_id = Some("12345".to_string());
        // Leave others as None or empty
        
        let dict_result = identifiers.to_dict();
        assert!(dict_result.is_ok());
        
        let dict = dict_result.unwrap();
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.get("scryfallId"), Some(&"scryfall-uuid".to_string()));
        assert_eq!(dict.get("multiverseId"), Some(&"12345".to_string()));
        
        // Should not contain keys for None or empty values
        assert!(!dict.contains_key("cardKingdomId"));
        assert!(!dict.contains_key("tcgplayerProductId"));
    }

    #[test]
    fn test_identifiers_unicode_and_special_chars() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test with unicode and special characters
        identifiers.mtgjson_v4_id = Some("マジック・ザ・ギャザリング".to_string()); // Magic in Japanese
        identifiers.scryfall_id = Some("café-münü-naïve-résumé".to_string());
        identifiers.cardsphere_id = Some("test@domain.com".to_string());
        identifiers.mtgo_id = Some("test_id-with.dots&symbols!".to_string());
        
        assert_eq!(identifiers.mtgjson_v4_id, Some("マジック・ザ・ギャザリング".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("café-münü-naïve-résumé".to_string()));
        assert_eq!(identifiers.cardsphere_id, Some("test@domain.com".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("test_id-with.dots&symbols!".to_string()));
        
        // Test serialization with unicode
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        assert!(json_str.contains("マジック"));
        assert!(json_str.contains("café"));
    }

    #[test]
    fn test_identifiers_all_fields_coverage() {
        // Ensure we test every single field in the struct
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Set every field to a unique value to test coverage
        identifiers.card_kingdom_etched_id = Some("cke1".to_string());
        identifiers.card_kingdom_foil_id = Some("ckf2".to_string());
        identifiers.card_kingdom_id = Some("ck3".to_string());
        identifiers.cardsphere_foil_id = Some("csf4".to_string());
        identifiers.cardsphere_id = Some("cs5".to_string());
        identifiers.mcm_id = Some("mcm6".to_string());
        identifiers.mcm_meta_id = Some("mcmm7".to_string());
        identifiers.mtg_arena_id = Some("arena8".to_string());
        identifiers.mtgjson_foil_version_id = Some("mjf9".to_string());
        identifiers.mtgjson_non_foil_version_id = Some("mjnf10".to_string());
        identifiers.mtgjson_v4_id = Some("mjv4_11".to_string());
        identifiers.mtgo_foil_id = Some("mtgof12".to_string());
        identifiers.mtgo_id = Some("mtgo13".to_string());
        identifiers.multiverse_id = Some("mv14".to_string());
        identifiers.scryfall_id = Some("sf15".to_string());
        identifiers.scryfall_illustration_id = Some("sfi16".to_string());
        identifiers.scryfall_card_back_id = Some("sfcb17".to_string());
        identifiers.scryfall_oracle_id = Some("sfo18".to_string());
        identifiers.tcgplayer_etched_product_id = Some("tcge19".to_string());
        identifiers.tcgplayer_product_id = Some("tcg20".to_string());
        
        // Verify all fields are set correctly
        assert_eq!(identifiers.card_kingdom_etched_id, Some("cke1".to_string()));
        assert_eq!(identifiers.card_kingdom_foil_id, Some("ckf2".to_string()));
        assert_eq!(identifiers.card_kingdom_id, Some("ck3".to_string()));
        assert_eq!(identifiers.cardsphere_foil_id, Some("csf4".to_string()));
        assert_eq!(identifiers.cardsphere_id, Some("cs5".to_string()));
        assert_eq!(identifiers.mcm_id, Some("mcm6".to_string()));
        assert_eq!(identifiers.mcm_meta_id, Some("mcmm7".to_string()));
        assert_eq!(identifiers.mtg_arena_id, Some("arena8".to_string()));
        assert_eq!(identifiers.mtgjson_foil_version_id, Some("mjf9".to_string()));
        assert_eq!(identifiers.mtgjson_non_foil_version_id, Some("mjnf10".to_string()));
        assert_eq!(identifiers.mtgjson_v4_id, Some("mjv4_11".to_string()));
        assert_eq!(identifiers.mtgo_foil_id, Some("mtgof12".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("mtgo13".to_string()));
        assert_eq!(identifiers.multiverse_id, Some("mv14".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("sf15".to_string()));
        assert_eq!(identifiers.scryfall_illustration_id, Some("sfi16".to_string()));
        assert_eq!(identifiers.scryfall_card_back_id, Some("sfcb17".to_string()));
        assert_eq!(identifiers.scryfall_oracle_id, Some("sfo18".to_string()));
        assert_eq!(identifiers.tcgplayer_etched_product_id, Some("tcge19".to_string()));
        assert_eq!(identifiers.tcgplayer_product_id, Some("tcg20".to_string()));
        
        // Test serialization with all fields
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        
        // Should contain all the unique values
        for i in 1..=20 {
            let search_str = format!("{}", i);
            assert!(json_str.contains(&search_str), "JSON should contain value {}", i);
        }
    }
}