use crate::base::{skip_if_empty_optional_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Card.PurchaseURLs Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonPurchaseUrls")]
pub struct MtgjsonPurchaseUrls {
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom_etched: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub card_kingdom_foil: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub cardmarket: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub tcgplayer: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub tcgplayer_etched: Option<String>,
}

#[pymethods]
impl MtgjsonPurchaseUrls {
    #[new]
    pub fn new() -> Self {
        Self::default()
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

        if let Some(ref val) = self.card_kingdom {
            if !val.is_empty() {
                result.insert("cardKingdom".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.card_kingdom_etched {
            if !val.is_empty() {
                result.insert("cardKingdomEtched".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.card_kingdom_foil {
            if !val.is_empty() {
                result.insert("cardKingdomFoil".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.cardmarket {
            if !val.is_empty() {
                result.insert("cardmarket".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.tcgplayer {
            if !val.is_empty() {
                result.insert("tcgplayer".to_string(), val.clone());
            }
        }
        if let Some(ref val) = self.tcgplayer_etched {
            if !val.is_empty() {
                result.insert("tcgplayerEtched".to_string(), val.clone());
            }
        }

        Ok(result)
    }

    /// Check if any URLs are present
    pub fn has_urls(&self) -> bool {
        self.card_kingdom.is_some()
            || self.card_kingdom_etched.is_some()
            || self.card_kingdom_foil.is_some()
            || self.cardmarket.is_some()
            || self.tcgplayer.is_some()
            || self.tcgplayer_etched.is_some()
    }

    /// Get all available URLs as a list of tuples (provider, url)
    pub fn get_available_urls(&self) -> Vec<(String, String)> {
        let mut urls = Vec::new();

        if let Some(ref url) = self.card_kingdom {
            if !url.is_empty() {
                urls.push(("cardKingdom".to_string(), url.clone()));
            }
        }
        if let Some(ref url) = self.card_kingdom_etched {
            if !url.is_empty() {
                urls.push(("cardKingdomEtched".to_string(), url.clone()));
            }
        }
        if let Some(ref url) = self.card_kingdom_foil {
            if !url.is_empty() {
                urls.push(("cardKingdomFoil".to_string(), url.clone()));
            }
        }
        if let Some(ref url) = self.cardmarket {
            if !url.is_empty() {
                urls.push(("cardmarket".to_string(), url.clone()));
            }
        }
        if let Some(ref url) = self.tcgplayer {
            if !url.is_empty() {
                urls.push(("tcgplayer".to_string(), url.clone()));
            }
        }
        if let Some(ref url) = self.tcgplayer_etched {
            if !url.is_empty() {
                urls.push(("tcgplayerEtched".to_string(), url.clone()));
            }
        }

        urls
    }
}

impl JsonObject for MtgjsonPurchaseUrls {
    fn build_keys_to_skip(&self) -> HashSet<String> {
        HashSet::new() // All empty values are handled by serde skip_serializing_if
    }
}
