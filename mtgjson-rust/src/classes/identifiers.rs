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
        assert_eq!(identifiers.card_kingdom_id, None);
        assert_eq!(identifiers.cardhoarder_foil_id, None);
        assert_eq!(identifiers.mcm_id, None);
        assert_eq!(identifiers.mcm_meta_id, None);
        assert_eq!(identifiers.mtg_arena_id, None);
        assert_eq!(identifiers.mtgo_foil_id, None);
        assert_eq!(identifiers.mtgo_id, None);
        assert_eq!(identifiers.multiverse_id, None);
        assert_eq!(identifiers.scryfall_id, None);
        assert_eq!(identifiers.scryfall_oracle_id, None);
        assert_eq!(identifiers.scryfall_illustration_id, None);
        assert_eq!(identifiers.tcgplayer_product_id, None);
        assert_eq!(identifiers.tcgplayer_etched_product_id, None);
    }

    #[test]
    fn test_identifiers_default() {
        let identifiers = MtgjsonIdentifiers::default();
        assert_eq!(identifiers.card_kingdom_etched_id, None);
        assert_eq!(identifiers.card_kingdom_foil_id, None);
        assert_eq!(identifiers.card_kingdom_id, None);
        assert_eq!(identifiers.cardhoarder_foil_id, None);
        assert_eq!(identifiers.mcm_id, None);
    }

    #[test]
    fn test_identifiers_with_values() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        identifiers.card_kingdom_id = Some(12345);
        identifiers.card_kingdom_foil_id = Some(12346);
        identifiers.card_kingdom_etched_id = Some(12347);
        identifiers.cardhoarder_foil_id = Some("CH12345".to_string());
        identifiers.mcm_id = Some(67890);
        identifiers.mcm_meta_id = Some(67891);
        identifiers.mtg_arena_id = Some(98765);
        identifiers.mtgo_foil_id = Some("MTGO12345".to_string());
        identifiers.mtgo_id = Some("MTGO12346".to_string());
        identifiers.multiverse_id = Some(112233);
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.scryfall_oracle_id = Some("oracle123".to_string());
        identifiers.scryfall_illustration_id = Some("illus123".to_string());
        identifiers.tcgplayer_product_id = Some(55555);
        identifiers.tcgplayer_etched_product_id = Some(55556);
        
        assert_eq!(identifiers.card_kingdom_id, Some(12345));
        assert_eq!(identifiers.card_kingdom_foil_id, Some(12346));
        assert_eq!(identifiers.card_kingdom_etched_id, Some(12347));
        assert_eq!(identifiers.cardhoarder_foil_id, Some("CH12345".to_string()));
        assert_eq!(identifiers.mcm_id, Some(67890));
        assert_eq!(identifiers.mcm_meta_id, Some(67891));
        assert_eq!(identifiers.mtg_arena_id, Some(98765));
        assert_eq!(identifiers.mtgo_foil_id, Some("MTGO12345".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("MTGO12346".to_string()));
        assert_eq!(identifiers.multiverse_id, Some(112233));
        assert_eq!(identifiers.scryfall_id, Some("abc123def".to_string()));
        assert_eq!(identifiers.scryfall_oracle_id, Some("oracle123".to_string()));
        assert_eq!(identifiers.scryfall_illustration_id, Some("illus123".to_string()));
        assert_eq!(identifiers.tcgplayer_product_id, Some(55555));
        assert_eq!(identifiers.tcgplayer_etched_product_id, Some(55556));
    }

    #[test]
    fn test_identifiers_json_serialization() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some(12345);
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.multiverse_id = Some(112233);
        
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("12345"));
        assert!(json_string.contains("abc123def"));
        assert!(json_string.contains("112233"));
    }

    #[test]
    fn test_identifiers_string_representations() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some(12345);
        identifiers.scryfall_id = Some("abc123def".to_string());
        
        let str_repr = identifiers.__str__();
        assert!(str_repr.contains("12345") || str_repr.contains("abc123def"));
        
        let repr = identifiers.__repr__();
        assert!(repr.contains("MtgjsonIdentifiers"));
    }

    #[test]
    fn test_identifiers_equality() {
        let mut identifiers1 = MtgjsonIdentifiers::new();
        let mut identifiers2 = MtgjsonIdentifiers::new();
        
        identifiers1.card_kingdom_id = Some(12345);
        identifiers1.scryfall_id = Some("abc123".to_string());
        
        identifiers2.card_kingdom_id = Some(12345);
        identifiers2.scryfall_id = Some("abc123".to_string());
        
        assert!(identifiers1.__eq__(&identifiers2));
        
        identifiers2.card_kingdom_id = Some(54321);
        assert!(!identifiers1.__eq__(&identifiers2));
    }

    #[test]
    fn test_identifiers_hash() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some(12345);
        identifiers.scryfall_id = Some("abc123".to_string());
        
        let hash1 = identifiers.__hash__();
        let hash2 = identifiers.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_identifiers_partial_data() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Set only some identifiers
        identifiers.scryfall_id = Some("abc123".to_string());
        identifiers.multiverse_id = Some(112233);
        
        // Others should remain None
        assert_eq!(identifiers.scryfall_id, Some("abc123".to_string()));
        assert_eq!(identifiers.multiverse_id, Some(112233));
        assert_eq!(identifiers.card_kingdom_id, None);
        assert_eq!(identifiers.tcgplayer_product_id, None);
        assert_eq!(identifiers.mtg_arena_id, None);
    }

    #[test]
    fn test_identifiers_all_none() {
        let identifiers = MtgjsonIdentifiers::new();
        
        // All fields should be None initially
        assert_eq!(identifiers.card_kingdom_id, None);
        assert_eq!(identifiers.card_kingdom_foil_id, None);
        assert_eq!(identifiers.card_kingdom_etched_id, None);
        assert_eq!(identifiers.cardhoarder_foil_id, None);
        assert_eq!(identifiers.mcm_id, None);
        assert_eq!(identifiers.mcm_meta_id, None);
        assert_eq!(identifiers.mtg_arena_id, None);
        assert_eq!(identifiers.mtgo_foil_id, None);
        assert_eq!(identifiers.mtgo_id, None);
        assert_eq!(identifiers.multiverse_id, None);
        assert_eq!(identifiers.scryfall_id, None);
        assert_eq!(identifiers.scryfall_oracle_id, None);
        assert_eq!(identifiers.scryfall_illustration_id, None);
        assert_eq!(identifiers.tcgplayer_product_id, None);
        assert_eq!(identifiers.tcgplayer_etched_product_id, None);
    }

    #[test]
    fn test_identifiers_string_ids() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        identifiers.cardhoarder_foil_id = Some("CH123ABC".to_string());
        identifiers.mtgo_foil_id = Some("MTGO456DEF".to_string());
        identifiers.mtgo_id = Some("MTGO789GHI".to_string());
        identifiers.scryfall_id = Some("abcd-efgh-ijkl-mnop".to_string());
        identifiers.scryfall_oracle_id = Some("oracle-uuid-1234".to_string());
        identifiers.scryfall_illustration_id = Some("illus-uuid-5678".to_string());
        
        assert_eq!(identifiers.cardhoarder_foil_id, Some("CH123ABC".to_string()));
        assert_eq!(identifiers.mtgo_foil_id, Some("MTGO456DEF".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("MTGO789GHI".to_string()));
        assert_eq!(identifiers.scryfall_id, Some("abcd-efgh-ijkl-mnop".to_string()));
        assert_eq!(identifiers.scryfall_oracle_id, Some("oracle-uuid-1234".to_string()));
        assert_eq!(identifiers.scryfall_illustration_id, Some("illus-uuid-5678".to_string()));
    }

    #[test]
    fn test_identifiers_numeric_ids() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        identifiers.card_kingdom_id = Some(1);
        identifiers.card_kingdom_foil_id = Some(999999);
        identifiers.card_kingdom_etched_id = Some(0);
        identifiers.mcm_id = Some(123456789);
        identifiers.mcm_meta_id = Some(987654321);
        identifiers.mtg_arena_id = Some(55555);
        identifiers.multiverse_id = Some(444444);
        identifiers.tcgplayer_product_id = Some(777777);
        identifiers.tcgplayer_etched_product_id = Some(888888);
        
        assert_eq!(identifiers.card_kingdom_id, Some(1));
        assert_eq!(identifiers.card_kingdom_foil_id, Some(999999));
        assert_eq!(identifiers.card_kingdom_etched_id, Some(0));
        assert_eq!(identifiers.mcm_id, Some(123456789));
        assert_eq!(identifiers.mcm_meta_id, Some(987654321));
        assert_eq!(identifiers.mtg_arena_id, Some(55555));
        assert_eq!(identifiers.multiverse_id, Some(444444));
        assert_eq!(identifiers.tcgplayer_product_id, Some(777777));
        assert_eq!(identifiers.tcgplayer_etched_product_id, Some(888888));
    }

    #[test]
    fn test_identifiers_clone() {
        let mut original = MtgjsonIdentifiers::new();
        original.card_kingdom_id = Some(12345);
        original.scryfall_id = Some("abc123".to_string());
        original.multiverse_id = Some(67890);
        
        let cloned = original.clone();
        
        assert_eq!(original.card_kingdom_id, cloned.card_kingdom_id);
        assert_eq!(original.scryfall_id, cloned.scryfall_id);
        assert_eq!(original.multiverse_id, cloned.multiverse_id);
    }

    #[test]
    fn test_identifiers_json_object_trait() {
        let identifiers = MtgjsonIdentifiers::new();
        let keys_to_skip = identifiers.build_keys_to_skip();
        
        // Should return empty set unless specifically implemented
        assert!(keys_to_skip.is_empty());
    }

    #[test]
    fn test_identifiers_edge_cases() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test with edge case values
        identifiers.card_kingdom_id = Some(0);  // Zero ID
        identifiers.multiverse_id = Some(u32::MAX);  // Maximum value
        identifiers.scryfall_id = Some("".to_string());  // Empty string
        identifiers.mtgo_id = Some("a".to_string());  // Single character
        
        assert_eq!(identifiers.card_kingdom_id, Some(0));
        assert_eq!(identifiers.multiverse_id, Some(u32::MAX));
        assert_eq!(identifiers.scryfall_id, Some("".to_string()));
        assert_eq!(identifiers.mtgo_id, Some("a".to_string()));
    }

    #[test]
    fn test_identifiers_serialization_with_none_values() {
        let identifiers = MtgjsonIdentifiers::new();
        
        let json_result = identifiers.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        // Should not contain null fields due to skip_serializing_if
        assert!(!json_string.contains("null"));
        // Should be an empty object or minimal object
        assert!(json_string.len() < 100);
    }
}