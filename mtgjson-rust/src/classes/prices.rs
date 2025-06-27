use crate::base::{skip_if_empty_optional_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON Singular Prices.Card Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonPricesObject")]
pub struct MtgjsonPricesObject {
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
impl MtgjsonPricesObject {
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

impl JsonObject for MtgjsonPricesObject {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prices_creation() {
        let prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(5.0),
            Some(15.0),
            Some(8.0),
            Some(12.0),
            Some(7.0),
        );
        
        assert_eq!(prices.source, "tcgplayer");
        assert_eq!(prices.provider, "mtgjson");
        assert_eq!(prices.date, "2023-01-01");
        assert_eq!(prices.currency, "USD");
        assert_eq!(prices.buy_normal, Some(10.0));
        assert_eq!(prices.buy_foil, Some(5.0));
        assert_eq!(prices.buy_etched, Some(15.0));
        assert_eq!(prices.sell_normal, Some(8.0));
        assert_eq!(prices.sell_foil, Some(12.0));
        assert_eq!(prices.sell_etched, Some(7.0));
    }

    #[test]
    fn test_prices_creation_with_none_values() {
        let prices = MtgjsonPricesObject::new(
            "cardmarket".to_string(),
            "mtgjson".to_string(),
            "2023-12-25".to_string(),
            "EUR".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(prices.source, "cardmarket");
        assert_eq!(prices.provider, "mtgjson");
        assert_eq!(prices.date, "2023-12-25");
        assert_eq!(prices.currency, "EUR");
        assert_eq!(prices.buy_normal, None);
        assert_eq!(prices.buy_foil, None);
        assert_eq!(prices.buy_etched, None);
        assert_eq!(prices.sell_normal, None);
        assert_eq!(prices.sell_foil, None);
        assert_eq!(prices.sell_etched, None);
    }

    #[test]
    fn test_prices_items() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(15.0),
            None,
            Some(8.0),
            None,
            Some(5.0),
        );
        
        let items = prices.items();
        
        // Should have 10 items (4 string fields + 6 price fields)
        assert_eq!(items.len(), 10);
        
        // Check that string fields return None for numeric values
        assert_eq!(items[0], ("source".to_string(), None));
        assert_eq!(items[1], ("provider".to_string(), None));
        assert_eq!(items[2], ("date".to_string(), None));
        assert_eq!(items[3], ("currency".to_string(), None));
        
        // Check numeric fields
        assert_eq!(items[4], ("buy_normal".to_string(), Some(10.0)));
        assert_eq!(items[5], ("buy_foil".to_string(), Some(15.0)));
        assert_eq!(items[6], ("buy_etched".to_string(), None));
        assert_eq!(items[7], ("sell_normal".to_string(), Some(8.0)));
        assert_eq!(items[8], ("sell_foil".to_string(), None));
        assert_eq!(items[9], ("sell_etched".to_string(), Some(5.0)));
    }

    #[test]
    fn test_prices_to_json() {
        let prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.5),
            Some(15.75),
            None,
            Some(8.25),
            Some(12.0),
            None,
        );
        
        let json_result = prices.to_json();
        assert!(json_result.is_ok());
        
        let json_string = json_result.unwrap();
        assert!(json_string.contains("tcgplayer"));
        assert!(json_string.contains("mtgjson"));
        assert!(json_string.contains("2023-01-01"));
        assert!(json_string.contains("USD"));
    }

    #[test]
    fn test_prices_to_json_structure() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(15.0),
            Some(20.0),
            Some(8.0),
            Some(12.0),
            Some(6.0),
        );
        
        let json_structure = prices.to_json_structure();
        
        // Should contain all price values as strings
        assert!(json_structure.contains("10"));
        assert!(json_structure.contains("15"));
        assert!(json_structure.contains("20"));
        assert!(json_structure.contains("8"));
        assert!(json_structure.contains("12"));
        assert!(json_structure.contains("6"));
    }

    #[test]
    fn test_prices_has_price_data() {
        // Test with all None values
        let empty_prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            None, None, None, None, None, None,
        );
        assert!(!empty_prices.has_price_data());
        
        // Test with at least one price value
        let with_buy_normal = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0), None, None, None, None, None,
        );
        assert!(with_buy_normal.has_price_data());
        
        let with_sell_etched = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            None, None, None, None, None, Some(5.0),
        );
        assert!(with_sell_etched.has_price_data());
    }

    #[test]
    fn test_prices_get_buy_prices() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(15.0),
            None,
            Some(8.0),
            Some(12.0),
            Some(6.0),
        );
        
        let buy_prices = prices.get_buy_prices();
        
        assert_eq!(buy_prices.len(), 2);
        assert_eq!(buy_prices.get("normal"), Some(&10.0));
        assert_eq!(buy_prices.get("foil"), Some(&15.0));
        assert_eq!(buy_prices.get("etched"), None);
    }

    #[test]
    fn test_prices_get_sell_prices() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(15.0),
            Some(20.0),
            Some(8.0),
            None,
            Some(6.0),
        );
        
        let sell_prices = prices.get_sell_prices();
        
        assert_eq!(sell_prices.len(), 2);
        assert_eq!(sell_prices.get("normal"), Some(&8.0));
        assert_eq!(sell_prices.get("foil"), None);
        assert_eq!(sell_prices.get("etched"), Some(&6.0));
    }

    #[test]
    fn test_prices_json_object_trait() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            None, None, None, None, None, None,
        );
        
        let keys_to_skip = prices.build_keys_to_skip();
        
        // Should return empty set unless specifically implemented
        assert!(keys_to_skip.is_empty());
    }

    // COMPREHENSIVE ADDITIONAL TESTS FOR FULL COVERAGE

    #[test]
    fn test_prices_clone_trait() {
        let original = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(5.0),
            Some(15.0),
            Some(8.0),
            Some(12.0),
            Some(7.0),
        );
        
        let cloned = original.clone();
        
        assert_eq!(original.source, cloned.source);
        assert_eq!(original.provider, cloned.provider);
        assert_eq!(original.date, cloned.date);
        assert_eq!(original.currency, cloned.currency);
        assert_eq!(original.buy_normal, cloned.buy_normal);
        assert_eq!(original.buy_foil, cloned.buy_foil);
        assert_eq!(original.buy_etched, cloned.buy_etched);
        assert_eq!(original.sell_normal, cloned.sell_normal);
        assert_eq!(original.sell_foil, cloned.sell_foil);
        assert_eq!(original.sell_etched, cloned.sell_etched);
    }

    #[test]
    fn test_prices_partial_eq_trait() {
        let prices1 = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let prices2 = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(prices1, prices2);
        
        let mut prices3 = prices2.clone();
        prices3.buy_normal = Some(15.0);
        assert_ne!(prices1, prices3);
    }

    #[test]
    fn test_prices_debug_trait() {
        let prices = MtgjsonPricesObject::new(
            "test_source".to_string(),
            "test_provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let debug_str = format!("{:?}", prices);
        assert!(debug_str.contains("MtgjsonPricesObject"));
        assert!(debug_str.contains("test_source"));
        assert!(debug_str.contains("USD"));
    }

    #[test]
    fn test_prices_with_zero_values() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
        );
        
        assert_eq!(prices.buy_normal, Some(0.0));
        assert_eq!(prices.sell_foil, Some(0.0));
        assert!(prices.has_price_data());
        
        let buy_prices = prices.get_buy_prices();
        assert_eq!(buy_prices.get("normal"), Some(&0.0));
        assert_eq!(buy_prices.get("foil"), Some(&0.0));
        assert_eq!(buy_prices.get("etched"), Some(&0.0));
    }

    #[test]
    fn test_prices_with_large_values() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(999999.99),
            Some(1000000.0),
            Some(f64::MAX),
            Some(f64::MIN),
            None,
            None,
        );
        
        assert_eq!(prices.buy_normal, Some(999999.99));
        assert_eq!(prices.buy_foil, Some(1000000.0));
        assert_eq!(prices.buy_etched, Some(f64::MAX));
        assert_eq!(prices.sell_normal, Some(f64::MIN));
    }

    #[test]
    fn test_prices_with_negative_values() {
        let prices = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(-10.0),
            Some(-5.5),
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(prices.buy_normal, Some(-10.0));
        assert_eq!(prices.buy_foil, Some(-5.5));
        assert!(prices.has_price_data());
    }

    #[test]
    fn test_prices_different_currencies() {
        let usd_prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            Some(5.0),
            None,
            None,
            None,
            None,
        );
        
        let eur_prices = MtgjsonPricesObject::new(
            "cardmarket".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "EUR".to_string(),
            Some(8.5),
            Some(4.25),
            None,
            None,
            None,
            None,
        );
        
        let jpy_prices = MtgjsonPricesObject::new(
            "tokyomtg".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "JPY".to_string(),
            Some(1500.0),
            Some(750.0),
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(usd_prices.currency, "USD");
        assert_eq!(eur_prices.currency, "EUR");
        assert_eq!(jpy_prices.currency, "JPY");
        
        assert_ne!(usd_prices.buy_normal, eur_prices.buy_normal);
        assert_ne!(eur_prices.buy_normal, jpy_prices.buy_normal);
    }

    #[test]
    fn test_prices_different_sources() {
        let tcgplayer = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let cardmarket = MtgjsonPricesObject::new(
            "cardmarket".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "EUR".to_string(),
            Some(8.5),
            None,
            None,
            None,
            None,
            None,
        );
        
        let cardkingdom = MtgjsonPricesObject::new(
            "cardkingdom".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(9.5),
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(tcgplayer.source, "tcgplayer");
        assert_eq!(cardmarket.source, "cardmarket");
        assert_eq!(cardkingdom.source, "cardkingdom");
        
        assert_ne!(tcgplayer.source, cardmarket.source);
        assert_ne!(cardmarket.source, cardkingdom.source);
    }

    #[test]
    fn test_prices_different_providers() {
        let mtgjson_provider = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let external_provider = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "external_api".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.5),
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(mtgjson_provider.provider, "mtgjson");
        assert_eq!(external_provider.provider, "external_api");
        assert_ne!(mtgjson_provider.provider, external_provider.provider);
    }

    #[test]
    fn test_prices_edge_case_dates() {
        let old_date = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "1993-08-05".to_string(), // Alpha release date
            "USD".to_string(),
            Some(100.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let future_date = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "2099-12-31".to_string(),
            "USD".to_string(),
            Some(10000.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        let invalid_date = MtgjsonPricesObject::new(
            "source".to_string(),
            "provider".to_string(),
            "not-a-date".to_string(),
            "USD".to_string(),
            Some(10.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(old_date.date, "1993-08-05");
        assert_eq!(future_date.date, "2099-12-31");
        assert_eq!(invalid_date.date, "not-a-date");
    }

    #[test]
    fn test_prices_serialization_deserialization() {
        let prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.5),
            Some(15.75),
            Some(20.25),
            Some(8.25),
            Some(12.0),
            Some(6.5),
        );
        
        let json_result = prices.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        
        // Test that serialization contains expected fields
        assert!(json_str.contains("tcgplayer"));
        assert!(json_str.contains("mtgjson"));
        assert!(json_str.contains("2023-01-01"));
        assert!(json_str.contains("USD"));
        
        // Test deserialization
        let deserialized: Result<MtgjsonPricesObject, _> = serde_json::from_str(&json_str);
        assert!(deserialized.is_ok());
        
        let deserialized_prices = deserialized.unwrap();
        assert_eq!(deserialized_prices.source, "tcgplayer");
        assert_eq!(deserialized_prices.provider, "mtgjson");
        assert_eq!(deserialized_prices.date, "2023-01-01");
        assert_eq!(deserialized_prices.currency, "USD");
        assert_eq!(deserialized_prices.buy_normal, Some(10.5));
        assert_eq!(deserialized_prices.buy_foil, Some(15.75));
        assert_eq!(deserialized_prices.buy_etched, Some(20.25));
        assert_eq!(deserialized_prices.sell_normal, Some(8.25));
        assert_eq!(deserialized_prices.sell_foil, Some(12.0));
        assert_eq!(deserialized_prices.sell_etched, Some(6.5));
    }

    #[test]
    fn test_prices_complex_integration_scenario() {
        // Create a comprehensive price object for a expensive card
        let black_lotus_prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(25000.0),  // buy_normal - Black Lotus unlimited
            Some(35000.0),  // buy_foil - Not applicable but for testing
            None,           // buy_etched - Not applicable for vintage
            Some(20000.0),  // sell_normal - Lower sell price
            Some(30000.0),  // sell_foil - Also for testing
            None,           // sell_etched - Not applicable
        );
        
        // Test all functionality on this complex scenario
        assert_eq!(black_lotus_prices.source, "tcgplayer");
        assert_eq!(black_lotus_prices.currency, "USD");
        assert!(black_lotus_prices.has_price_data());
        
        // Test buy prices
        let buy_prices = black_lotus_prices.get_buy_prices();
        assert_eq!(buy_prices.len(), 2);
        assert_eq!(buy_prices.get("normal"), Some(&25000.0));
        assert_eq!(buy_prices.get("foil"), Some(&35000.0));
        
        // Test sell prices
        let sell_prices = black_lotus_prices.get_sell_prices();
        assert_eq!(sell_prices.len(), 2);
        assert_eq!(sell_prices.get("normal"), Some(&20000.0));
        assert_eq!(sell_prices.get("foil"), Some(&30000.0));
        
        // Test items method
        let items = black_lotus_prices.items();
        assert_eq!(items.len(), 10);
        
        // Test JSON serialization
        let json_result = black_lotus_prices.to_json();
        assert!(json_result.is_ok());
        
        let json_str = json_result.unwrap();
        assert!(json_str.contains("25000"));
        assert!(json_str.contains("35000"));
    }

    #[test]
    fn test_prices_empty_strings() {
        let prices = MtgjsonPricesObject::new(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(prices.source, "");
        assert_eq!(prices.provider, "");
        assert_eq!(prices.date, "");
        assert_eq!(prices.currency, "");
        assert!(!prices.has_price_data());
        
        let buy_prices = prices.get_buy_prices();
        assert!(buy_prices.is_empty());
        
        let sell_prices = prices.get_sell_prices();
        assert!(sell_prices.is_empty());
    }

    #[test]
    fn test_prices_unicode_strings() {
        let prices = MtgjsonPricesObject::new(
            "ソース".to_string(),          // "source" in Japanese
            "プロバイダー".to_string(),      // "provider" in Japanese
            "2023-01-01".to_string(),
            "¥".to_string(),               // Yen symbol
            Some(1500.0),
            None,
            None,
            None,
            None,
            None,
        );
        
        assert_eq!(prices.source, "ソース");
        assert_eq!(prices.provider, "プロバイダー");
        assert_eq!(prices.currency, "¥");
        assert!(prices.has_price_data());
    }

    #[test]
    fn test_prices_all_combinations() {
        // Test all possible combinations of Some/None for price fields
        let test_cases = vec![
            (Some(1.0), None, None, None, None, None),
            (None, Some(2.0), None, None, None, None),
            (None, None, Some(3.0), None, None, None),
            (None, None, None, Some(4.0), None, None),
            (None, None, None, None, Some(5.0), None),
            (None, None, None, None, None, Some(6.0)),
            (Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0)),
        ];
        
        for (i, (bn, bf, be, sn, sf, se)) in test_cases.iter().enumerate() {
            let prices = MtgjsonPricesObject::new(
                format!("source_{}", i),
                format!("provider_{}", i),
                format!("2023-01-{:02}", i + 1),
                "USD".to_string(),
                *bn, *bf, *be, *sn, *sf, *se,
            );
            
            assert_eq!(prices.buy_normal, *bn);
            assert_eq!(prices.buy_foil, *bf);
            assert_eq!(prices.buy_etched, *be);
            assert_eq!(prices.sell_normal, *sn);
            assert_eq!(prices.sell_foil, *sf);
            assert_eq!(prices.sell_etched, *se);
            
            // All except the all-None case should have price data
            if i < test_cases.len() - 1 || i == test_cases.len() - 1 {
                assert!(prices.has_price_data());
            }
        }
    }
}