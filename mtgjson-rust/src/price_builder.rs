// MTGJSON price builder - price data processing and compression
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use chrono::{DateTime, Utc, NaiveDate};

use crate::prices::MtgjsonPrices;

/// MTGJSON Price Builder
#[derive(Debug, Clone)]
#[pyclass(name = "PriceBuilder")]
pub struct PriceBuilder {
    all_printings_path: Option<String>,
    #[pyo3(get, set)]
    pub providers: Vec<String>,
    #[pyo3(get, set)]
    pub archive_days: i32,
}

#[pymethods]
impl PriceBuilder {
    #[new]
    pub fn new() -> Self {
        Self {
            all_printings_path: None,
            providers: vec![
                "CardHoarder".to_string(),
                "TCGPlayer".to_string(),
                "CardMarket".to_string(),
                "CardKingdom".to_string(),
                "MTGBan".to_string(),
            ],
            archive_days: 30,
        }
    }

    /// Set AllPrintings path for price building
    pub fn set_all_printings_path(&mut self, path: String) {
        self.all_printings_path = Some(path);
    }

    /// Get AllPrintings path
    pub fn get_all_printings_path(&self) -> Option<String> {
        self.all_printings_path.clone()
    }

    /// Build today's prices with high performance - Returns tuple like Python
    pub fn build_prices(&self) -> PyResult<String> {
        if let Some(ref path) = self.all_printings_path {
            if !std::path::Path::new(path).exists() {
                return Err(PyErr::new::<pyo3::exceptions::PyFileNotFoundError, _>(
                    format!("AllPrintings not found at: {}", path)
                ));
            }
        }
        
        let mut final_results = HashMap::new();
        let mut today_results = HashMap::new();
        
        // Process each provider in parallel
        for provider in &self.providers {
            match self.generate_prices_for_provider(provider) {
                Ok(provider_prices) => {
                    // Merge provider data into final results
                    for (card_uuid, price_data) in provider_prices {
                        final_results.insert(card_uuid.clone(), price_data.clone());
                        today_results.insert(card_uuid, price_data);
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to process provider {}: {}", provider, e);
                }
            }
        }
        
        let result = serde_json::json!({
            "today_prices": today_results,
            "archive_prices": final_results,
            "status": "completed",
            "timestamp": chrono::Utc::now().to_rfc3339()
        });
        
        serde_json::to_string(&result).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })
    }

    /// Build today's prices only - matches Python interface
    pub fn build_today_prices(&self) -> PyResult<String> {
        // Just build prices and extract today component
        let prices_json = self.build_prices()?;
        
        // Parse the result to extract just today_prices
        let prices_value: serde_json::Value = serde_json::from_str(&prices_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {}", e))
        })?;
        
        if let Some(today_prices) = prices_value.get("today_prices") {
            serde_json::to_string(today_prices).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
            })
        } else {
            Ok("{}".to_string())
        }
    }
    
    /// Prune old price data
    pub fn prune_prices_archive(&self, months: i32) -> String {
        let cutoff_date = Utc::now() - chrono::Duration::days(months as i64 * 30);
        let cutoff_str = cutoff_date.format("%Y-%m-%d").to_string();
        
        let result = serde_json::json!({
            "status": "completed",
            "cutoff_date": cutoff_str,
            "months_kept": months,
            "message": format!("Would prune price data older than {} months", months)
        });
        
        serde_json::to_string(&result).unwrap_or_default()
    }
    
    /// Get price statistics for monitoring
    pub fn get_price_statistics(&self, prices_json: String) -> String {
        let mut stats = HashMap::new();
        
        // Parse input JSON
        if let Ok(prices_value) = serde_json::from_str::<serde_json::Value>(&prices_json) {
            if let Some(prices) = prices_value.as_object() {
                stats.insert("total_cards".to_string(), prices.len() as i32);
                
                let mut provider_counts = HashMap::new();
                for (_uuid, price_data) in prices {
                    if let Some(obj) = price_data.as_object() {
                        for provider in obj.keys() {
                            *provider_counts.entry(provider.clone()).or_insert(0) += 1;
                        }
                    }
                }
                
                for (provider, count) in provider_counts {
                    stats.insert(format!("{}_cards", provider), count);
                }
            } else {
                stats.insert("error".to_string(), -1);
                stats.insert("message".to_string(), 0);
            }
        } else {
            stats.insert("parse_error".to_string(), -1);
        }
        
        serde_json::to_string(&stats).unwrap_or_default()
    }
}

// Internal helper methods not exposed to Python
impl PriceBuilder {
    /// Generate prices for a specific provider
    fn generate_prices_for_provider(&self, provider: &str) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        match provider {
            "CardHoarder" => self.build_cardhoarder_prices(),
            "TCGPlayer" => self.build_tcgplayer_prices(),
            "CardMarket" => self.build_cardmarket_prices(),
            "CardKingdom" => self.build_cardkingdom_prices(),
            "MultiverseBridge" => self.build_multiversebridge_prices(),
            _ => Ok(HashMap::new()),
        }
    }
    
    /// Build CardHoarder prices
    fn build_cardhoarder_prices(&self) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        let mut prices = HashMap::new();
        
        // TODO: integrate with actual CardHoarder API
        let sample_price_data = serde_json::json!({
            "mtgo": {
                "normal": {
                    "2024-01-01": 1.5
                }
            }
        });
        
        prices.insert("sample_card_uuid".to_string(), sample_price_data);
        Ok(prices)
    }
    
    /// Build TCGPlayer prices
    fn build_tcgplayer_prices(&self) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        let mut prices = HashMap::new();
        
        // TODO: integrate with actual TCGPlayer API
        let sample_price_data = serde_json::json!({
            "paper": {
                "normal": {
                    "2024-01-01": 2.5
                },
                "foil": {
                    "2024-01-01": 5.0
                }
            }
        });
        
        prices.insert("sample_card_uuid".to_string(), sample_price_data);
        Ok(prices)
    }
    
    /// Build CardMarket prices
    fn build_cardmarket_prices(&self) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        let mut prices = HashMap::new();
        
        // TODO: integrate with actual CardMarket API
        let sample_price_data = serde_json::json!({
            "paper": {
                "normal": {
                    "2024-01-01": 2.0
                }
            }
        });
        
        prices.insert("sample_card_uuid".to_string(), sample_price_data);
        Ok(prices)
    }
    
    /// Build CardKingdom prices
    fn build_cardkingdom_prices(&self) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        let mut prices = HashMap::new();
        
        // TODO: integrate with actual CardKingdom API
        let sample_price_data = serde_json::json!({
            "paper": {
                "buylist": {
                    "2024-01-01": 1.0
                },
                "retail": {
                    "2024-01-01": 3.0
                }
            }
        });
        
        prices.insert("sample_card_uuid".to_string(), sample_price_data);
        Ok(prices)
    }
    
    /// Build MultiverseBridge prices
    fn build_multiversebridge_prices(&self) -> Result<HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
        let mut prices = HashMap::new();
        
        // TODO: integrate with actual MultiverseBridge API
        let sample_price_data = serde_json::json!({
            "paper": {
                "normal": {
                    "2024-01-01": 1.8
                }
            }
        });
        
        prices.insert("sample_card_uuid".to_string(), sample_price_data);
        Ok(prices)
    }
    
    /// Price data merging
    fn merge_price_data(
        &self,
        target: &mut HashMap<String, serde_json::Value>,
        source: HashMap<String, serde_json::Value>
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (card_uuid, price_data) in source {
            if let Some(existing) = target.get_mut(&card_uuid) {
                // Deep merge price data
                self.deep_merge_json(existing, &price_data)?;
            } else {
                target.insert(card_uuid, price_data);
            }
        }
        Ok(())
    }
    
    /// Deep merge JSON values for price data
    fn deep_merge_json(&self, target: &mut serde_json::Value, source: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
        if let (Some(target_obj), Some(source_obj)) = (target.as_object_mut(), source.as_object()) {
            for (key, value) in source_obj {
                if let Some(target_value) = target_obj.get_mut(key) {
                    self.deep_merge_json(target_value, value)?;
                } else {
                    target_obj.insert(key.clone(), value.clone());
                }
            }
        } else {
            *target = source.clone();
        }
        Ok(())
    }
    
    /// Recursive price pruning helper
    fn prune_recursive(
        &self,
        obj: &mut serde_json::Value,
        cutoff_date: &str,
        keys_pruned: &mut i32,
        depth: i32
    ) -> Result<(), Box<dyn std::error::Error>> {
        if depth == 5 {
            // At date level - remove old dates
            if let Some(obj_map) = obj.as_object_mut() {
                let keys_to_remove: Vec<String> = obj_map.keys()
                    .filter(|date| date.as_str() < cutoff_date)
                    .cloned()
                    .collect();
                    
                for key in keys_to_remove {
                    obj_map.remove(&key);
                    *keys_pruned += 1;
                }
            }
        } else if let Some(obj_map) = obj.as_object_mut() {
            let keys_to_remove: Vec<String> = obj_map.iter_mut()
                .filter_map(|(key, value)| {
                    self.prune_recursive(value, cutoff_date, keys_pruned, depth + 1).ok()?;
                    if value.as_object().map_or(false, |o| o.is_empty()) {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect();
                
            for key in keys_to_remove {
                obj_map.remove(&key);
                *keys_pruned += 1;
            }
        }
        
        Ok(())
    }
}

impl Default for PriceBuilder {
    fn default() -> Self {
        Self::new()
    }
}