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