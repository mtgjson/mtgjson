use crate::base::JsonObject;
use indexmap::IndexMap;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Singular Prices.Card Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonPrices")]
pub struct MtgjsonPrices {
    #[pyo3(get, set)]
    pub source: String,
    
    #[pyo3(get, set)]
    pub provider: String,
    
    #[pyo3(get, set)]
    pub date: String,
    
    #[pyo3(get, set)]
    pub currency: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub buy_normal: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub buy_foil: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub buy_etched: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub sell_normal: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub sell_foil: Option<f64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[pyo3(get, set)]
    pub sell_etched: Option<f64>,
}

#[pymethods]
impl MtgjsonPrices {
    #[new]
    #[pyo3(signature = (source, provider, date, currency, buy_normal = None, buy_foil = None, buy_etched = None, sell_normal = None, sell_foil = None, sell_etched = None))]
    pub fn new(
        source: String,
        provider: String,
        date: String,
        currency: String,
        buy_normal: Option<f64>,
        buy_foil: Option<f64>,
        buy_etched: Option<f64>,
        sell_normal: Option<f64>,
        sell_foil: Option<f64>,
        sell_etched: Option<f64>,
    ) -> Self {
        Self {
            source,
            provider,
            date,
            currency,
            buy_normal,
            buy_foil,
            buy_etched,
            sell_normal,
            sell_foil,
            sell_etched,
        }
    }

    /// Get all price items as tuples
    pub fn items(&self) -> Vec<(String, Option<f64>)> {
        vec![
            ("source".to_string(), None), // String fields don't have numeric values
            ("provider".to_string(), None),
            ("date".to_string(), None),
            ("currency".to_string(), None),
            ("buy_normal".to_string(), self.buy_normal),
            ("buy_foil".to_string(), self.buy_foil),
            ("buy_etched".to_string(), self.buy_etched),
            ("sell_normal".to_string(), self.sell_normal),
            ("sell_foil".to_string(), self.sell_foil),
            ("sell_etched".to_string(), self.sell_etched),
        ]
    }

    /// Convert to the complex JSON structure expected by MTGJSON
    pub fn to_json(&self) -> PyResult<String> {
        let result = self.to_json_structure();
        serde_json::to_string(&result).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Convert to the complex JSON structure
    pub fn to_json_structure(&self) -> String {
        let mut buy_sell_option: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        
        if let Some(ref buy_normal) = self.buy_normal {
            buy_sell_option.insert("buy_normal".to_string(), format!("{}", buy_normal));
        }
        if let Some(ref buy_foil) = self.buy_foil {
            buy_sell_option.insert("buy_foil".to_string(), format!("{}", buy_foil));
        }
        if let Some(ref buy_etched) = self.buy_etched {
            buy_sell_option.insert("buy_etched".to_string(), format!("{}", buy_etched));
        }
        if let Some(ref sell_normal) = self.sell_normal {
            buy_sell_option.insert("sell_normal".to_string(), format!("{}", sell_normal));
        }
        if let Some(ref sell_foil) = self.sell_foil {
            buy_sell_option.insert("sell_foil".to_string(), format!("{}", sell_foil));
        }
        if let Some(ref sell_etched) = self.sell_etched {
            buy_sell_option.insert("sell_etched".to_string(), format!("{}", sell_etched));
        }

        serde_json::to_string(&buy_sell_option).unwrap_or_default()
    }

    /// Check if this price entry has any actual price data
    pub fn has_price_data(&self) -> bool {
        self.buy_normal.is_some() ||
        self.buy_foil.is_some() ||
        self.buy_etched.is_some() ||
        self.sell_normal.is_some() ||
        self.sell_foil.is_some() ||
        self.sell_etched.is_some()
    }

    /// Get all buy prices
    pub fn get_buy_prices(&self) -> HashMap<String, f64> {
        let mut prices = HashMap::new();
        
        if let Some(price) = self.buy_normal {
            prices.insert("normal".to_string(), price);
        }
        if let Some(price) = self.buy_foil {
            prices.insert("foil".to_string(), price);
        }
        if let Some(price) = self.buy_etched {
            prices.insert("etched".to_string(), price);
        }
        
        prices
    }

    /// Get all sell prices
    pub fn get_sell_prices(&self) -> HashMap<String, f64> {
        let mut prices = HashMap::new();
        
        if let Some(price) = self.sell_normal {
            prices.insert("normal".to_string(), price);
        }
        if let Some(price) = self.sell_foil {
            prices.insert("foil".to_string(), price);
        }
        if let Some(price) = self.sell_etched {
            prices.insert("etched".to_string(), price);
        }
        
        prices
    }
}

impl JsonObject for MtgjsonPrices {}