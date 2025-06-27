use mtgjson_rust::classes::*;
use std::collections::HashMap;

mod comprehensive_prices_tests {
    use super::*;
    use mtgjson_rust::classes::prices::MtgjsonPricesObject;

    /// Test all MtgjsonPricesObject constructors and return types
    #[test]
    fn test_prices_constructors_return_types() {
        // Test constructor with all parameters as Some
        let prices_all_some = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-01-01".to_string(),
            "USD".to_string(),
            Some(10.5),
            Some(15.75),
            Some(20.0),
            Some(8.25),
            Some(12.5),
            Some(6.75),
        );
        
        assert_eq!(prices_all_some.source, "tcgplayer");
        assert_eq!(prices_all_some.provider, "mtgjson");
        assert_eq!(prices_all_some.date, "2023-01-01");
        assert_eq!(prices_all_some.currency, "USD");
        assert_eq!(prices_all_some.buy_normal, Some(10.5));
        assert_eq!(prices_all_some.buy_foil, Some(15.75));
        assert_eq!(prices_all_some.buy_etched, Some(20.0));
        assert_eq!(prices_all_some.sell_normal, Some(8.25));
        assert_eq!(prices_all_some.sell_foil, Some(12.5));
        assert_eq!(prices_all_some.sell_etched, Some(6.75));
        
        // Test constructor with all prices as None
        let prices_all_none = MtgjsonPricesObject::new(
            "cardmarket".to_string(),
            "mtgjson".to_string(),
            "2023-02-01".to_string(),
            "EUR".to_string(),
            None, None, None, None, None, None,
        );
        
        assert_eq!(prices_all_none.source, "cardmarket");
        assert_eq!(prices_all_none.provider, "mtgjson");
        assert_eq!(prices_all_none.date, "2023-02-01");
        assert_eq!(prices_all_none.currency, "EUR");
        assert_eq!(prices_all_none.buy_normal, None);
        assert_eq!(prices_all_none.buy_foil, None);
        assert_eq!(prices_all_none.buy_etched, None);
        assert_eq!(prices_all_none.sell_normal, None);
        assert_eq!(prices_all_none.sell_foil, None);
        assert_eq!(prices_all_none.sell_etched, None);
        
        // Verify return types
        let source: String = prices_all_some.source.clone();
        let provider: String = prices_all_some.provider.clone();
        let date: String = prices_all_some.date.clone();
        let currency: String = prices_all_some.currency.clone();
        let buy_normal: Option<f64> = prices_all_some.buy_normal;
        let buy_foil: Option<f64> = prices_all_some.buy_foil;
        let buy_etched: Option<f64> = prices_all_some.buy_etched;
        let sell_normal: Option<f64> = prices_all_some.sell_normal;
        let sell_foil: Option<f64> = prices_all_some.sell_foil;
        let sell_etched: Option<f64> = prices_all_some.sell_etched;
        
        assert_eq!(source, "tcgplayer");
        assert_eq!(provider, "mtgjson");
        assert_eq!(date, "2023-01-01");
        assert_eq!(currency, "USD");
        assert_eq!(buy_normal, Some(10.5));
        assert_eq!(buy_foil, Some(15.75));
        assert_eq!(buy_etched, Some(20.0));
        assert_eq!(sell_normal, Some(8.25));
        assert_eq!(sell_foil, Some(12.5));
        assert_eq!(sell_etched, Some(6.75));
    }

    /// Test all method return types
    #[test]
    fn test_prices_method_return_types() {
        let prices = MtgjsonPricesObject::new(
            "test_source".to_string(),
            "test_provider".to_string(),
            "2023-03-01".to_string(),
            "USD".to_string(),
            Some(100.0),
            Some(150.0),
            Some(200.0),
            Some(80.0),
            Some(120.0),
            Some(60.0),
        );
        
        // Test items method return type
        let items: Vec<(String, Option<f64>)> = prices.items();
        assert_eq!(items.len(), 10); // 4 string fields + 6 price fields
        
        // Check string field items (should return None for numeric values)
        assert_eq!(items[0], ("source".to_string(), None));
        assert_eq!(items[1], ("provider".to_string(), None));
        assert_eq!(items[2], ("date".to_string(), None));
        assert_eq!(items[3], ("currency".to_string(), None));
        
        // Check numeric field items
        assert_eq!(items[4], ("buy_normal".to_string(), Some(100.0)));
        assert_eq!(items[5], ("buy_foil".to_string(), Some(150.0)));
        assert_eq!(items[6], ("buy_etched".to_string(), Some(200.0)));
        assert_eq!(items[7], ("sell_normal".to_string(), Some(80.0)));
        assert_eq!(items[8], ("sell_foil".to_string(), Some(120.0)));
        assert_eq!(items[9], ("sell_etched".to_string(), Some(60.0)));
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = prices.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        
        // Test to_json_structure method return type
        let json_structure: String = prices.to_json_structure();
        assert!(!json_structure.is_empty());
        
        // Test has_price_data method return type
        let has_data: bool = prices.has_price_data();
        assert!(has_data);
        
        // Test get_buy_prices method return type
        let buy_prices: HashMap<String, f64> = prices.get_buy_prices();
        assert_eq!(buy_prices.len(), 3);
        assert_eq!(buy_prices.get("normal"), Some(&100.0));
        assert_eq!(buy_prices.get("foil"), Some(&150.0));
        assert_eq!(buy_prices.get("etched"), Some(&200.0));
        
        // Test get_sell_prices method return type
        let sell_prices: HashMap<String, f64> = prices.get_sell_prices();
        assert_eq!(sell_prices.len(), 3);
        assert_eq!(sell_prices.get("normal"), Some(&80.0));
        assert_eq!(sell_prices.get("foil"), Some(&120.0));
        assert_eq!(sell_prices.get("etched"), Some(&60.0));
    }

    /// Test edge cases and special values with return types
    #[test]
    fn test_prices_edge_cases_return_types() {
        // Test with zero values
        let zero_prices = MtgjsonPricesObject::new(
            "zero_source".to_string(),
            "zero_provider".to_string(),
            "2023-04-01".to_string(),
            "USD".to_string(),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
        );
        
        let zero_has_data: bool = zero_prices.has_price_data();
        assert!(zero_has_data); // Zero is still considered valid data
        
        let zero_buy_prices: HashMap<String, f64> = zero_prices.get_buy_prices();
        let zero_sell_prices: HashMap<String, f64> = zero_prices.get_sell_prices();
        assert_eq!(zero_buy_prices.len(), 3);
        assert_eq!(zero_sell_prices.len(), 3);
        assert_eq!(zero_buy_prices.get("normal"), Some(&0.0));
        assert_eq!(zero_sell_prices.get("normal"), Some(&0.0));
        
        // Test with very large values
        let large_prices = MtgjsonPricesObject::new(
            "large_source".to_string(),
            "large_provider".to_string(),
            "2023-05-01".to_string(),
            "USD".to_string(),
            Some(f64::MAX),
            Some(1000000.99),
            None,
            Some(f64::MIN),
            None,
            Some(999999.01),
        );
        
        let large_buy_prices: HashMap<String, f64> = large_prices.get_buy_prices();
        let large_sell_prices: HashMap<String, f64> = large_prices.get_sell_prices();
        assert_eq!(large_buy_prices.len(), 2); // Only normal and foil have values
        assert_eq!(large_sell_prices.len(), 2); // Only normal and etched have values
        assert_eq!(large_buy_prices.get("normal"), Some(&f64::MAX));
        assert_eq!(large_buy_prices.get("foil"), Some(&1000000.99));
        assert_eq!(large_sell_prices.get("normal"), Some(&f64::MIN));
        assert_eq!(large_sell_prices.get("etched"), Some(&999999.01));
        
        // Test with mixed None/Some values
        let mixed_prices = MtgjsonPricesObject::new(
            "mixed_source".to_string(),
            "mixed_provider".to_string(),
            "2023-06-01".to_string(),
            "EUR".to_string(),
            Some(10.0),
            None,
            Some(15.0),
            None,
            Some(8.0),
            None,
        );
        
        let mixed_has_data: bool = mixed_prices.has_price_data();
        assert!(mixed_has_data);
        
        let mixed_buy_prices: HashMap<String, f64> = mixed_prices.get_buy_prices();
        let mixed_sell_prices: HashMap<String, f64> = mixed_prices.get_sell_prices();
        assert_eq!(mixed_buy_prices.len(), 2); // normal and etched
        assert_eq!(mixed_sell_prices.len(), 1); // only foil
        assert_eq!(mixed_buy_prices.get("normal"), Some(&10.0));
        assert_eq!(mixed_buy_prices.get("etched"), Some(&15.0));
        assert_eq!(mixed_sell_prices.get("foil"), Some(&8.0));
        
        // Test completely empty prices
        let empty_prices = MtgjsonPricesObject::new(
            "empty_source".to_string(),
            "empty_provider".to_string(),
            "2023-07-01".to_string(),
            "GBP".to_string(),
            None, None, None, None, None, None,
        );
        
        let empty_has_data: bool = empty_prices.has_price_data();
        assert!(!empty_has_data);
        
        let empty_buy_prices: HashMap<String, f64> = empty_prices.get_buy_prices();
        let empty_sell_prices: HashMap<String, f64> = empty_prices.get_sell_prices();
        assert_eq!(empty_buy_prices.len(), 0);
        assert_eq!(empty_sell_prices.len(), 0);
    }

    /// Test string field variations and return types
    #[test]
    fn test_prices_string_field_variations_return_types() {
        // Test with empty strings
        let empty_string_prices = MtgjsonPricesObject::new(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            Some(10.0), None, None, None, None, None,
        );
        
        let empty_source: String = empty_string_prices.source.clone();
        let empty_provider: String = empty_string_prices.provider.clone();
        let empty_date: String = empty_string_prices.date.clone();
        let empty_currency: String = empty_string_prices.currency.clone();
        
        assert_eq!(empty_source, "");
        assert_eq!(empty_provider, "");
        assert_eq!(empty_date, "");
        assert_eq!(empty_currency, "");
        
        // Test with unicode strings
        let unicode_prices = MtgjsonPricesObject::new(
            "🔥源".to_string(),
            "🎯提供者".to_string(),
            "📅2023-01-01".to_string(),
            "💰USD".to_string(),
            Some(42.42), None, None, None, None, None,
        );
        
        let unicode_source: String = unicode_prices.source.clone();
        let unicode_provider: String = unicode_prices.provider.clone();
        let unicode_date: String = unicode_prices.date.clone();
        let unicode_currency: String = unicode_prices.currency.clone();
        
        assert_eq!(unicode_source, "🔥源");
        assert_eq!(unicode_provider, "🎯提供者");
        assert_eq!(unicode_date, "📅2023-01-01");
        assert_eq!(unicode_currency, "💰USD");
        
        // Test with very long strings
        let long_source = "a".repeat(10000);
        let long_provider = "b".repeat(5000);
        let long_date = "c".repeat(1000);
        let long_currency = "d".repeat(100);
        
        let long_string_prices = MtgjsonPricesObject::new(
            long_source.clone(),
            long_provider.clone(),
            long_date.clone(),
            long_currency.clone(),
            Some(1.0), None, None, None, None, None,
        );
        
        let returned_long_source: String = long_string_prices.source.clone();
        let returned_long_provider: String = long_string_prices.provider.clone();
        let returned_long_date: String = long_string_prices.date.clone();
        let returned_long_currency: String = long_string_prices.currency.clone();
        
        assert_eq!(returned_long_source.len(), 10000);
        assert_eq!(returned_long_provider.len(), 5000);
        assert_eq!(returned_long_date.len(), 1000);
        assert_eq!(returned_long_currency.len(), 100);
        assert_eq!(returned_long_source, long_source);
        assert_eq!(returned_long_provider, long_provider);
        assert_eq!(returned_long_date, long_date);
        assert_eq!(returned_long_currency, long_currency);
    }

    /// Test JSON serialization and structure methods with return types
    #[test]
    fn test_prices_json_methods_return_types() {
        let prices = MtgjsonPricesObject::new(
            "json_test_source".to_string(),
            "json_test_provider".to_string(),
            "2023-08-01".to_string(),
            "CAD".to_string(),
            Some(25.5),
            Some(30.75),
            Some(35.0),
            Some(20.25),
            Some(25.0),
            Some(18.5),
        );
        
        // Test to_json method
        let json_result: Result<String, pyo3::PyErr> = prices.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        assert!(json_string.contains("json_test_source"));
        assert!(json_string.contains("json_test_provider"));
        assert!(json_string.contains("2023-08-01"));
        assert!(json_string.contains("CAD"));
        
        // Test to_json_structure method
        let json_structure: String = prices.to_json_structure();
        assert!(!json_structure.is_empty());
        assert!(json_structure.contains("25.5"));
        assert!(json_structure.contains("30.75"));
        assert!(json_structure.contains("35"));
        assert!(json_structure.contains("20.25"));
        assert!(json_structure.contains("25"));
        assert!(json_structure.contains("18.5"));
        
        // Test with empty prices
        let empty_prices = MtgjsonPricesObject::new(
            "empty_json_source".to_string(),
            "empty_json_provider".to_string(),
            "2023-09-01".to_string(),
            "JPY".to_string(),
            None, None, None, None, None, None,
        );
        
        let empty_json_result: Result<String, pyo3::PyErr> = empty_prices.to_json();
        assert!(empty_json_result.is_ok());
        let empty_json_string: String = empty_json_result.unwrap();
        assert!(!empty_json_string.is_empty());
        
        let empty_json_structure: String = empty_prices.to_json_structure();
        assert_eq!(empty_json_structure, "{}"); // Should be empty object
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_prices_trait_implementations() {
        let prices1 = MtgjsonPricesObject::new(
            "trait_source".to_string(),
            "trait_provider".to_string(),
            "2023-10-01".to_string(),
            "AUD".to_string(),
            Some(50.0),
            Some(75.0),
            None,
            Some(40.0),
            Some(65.0),
            None,
        );
        
        let prices2 = MtgjsonPricesObject::new(
            "trait_source_2".to_string(),
            "trait_provider_2".to_string(),
            "2023-11-01".to_string(),
            "NZD".to_string(),
            Some(45.0),
            None,
            Some(80.0),
            None,
            Some(70.0),
            Some(35.0),
        );
        
        // Test Clone trait
        let cloned_prices1 = prices1.clone();
        assert_eq!(prices1.source, cloned_prices1.source);
        assert_eq!(prices1.provider, cloned_prices1.provider);
        assert_eq!(prices1.date, cloned_prices1.date);
        assert_eq!(prices1.currency, cloned_prices1.currency);
        assert_eq!(prices1.buy_normal, cloned_prices1.buy_normal);
        assert_eq!(prices1.buy_foil, cloned_prices1.buy_foil);
        assert_eq!(prices1.buy_etched, cloned_prices1.buy_etched);
        assert_eq!(prices1.sell_normal, cloned_prices1.sell_normal);
        assert_eq!(prices1.sell_foil, cloned_prices1.sell_foil);
        assert_eq!(prices1.sell_etched, cloned_prices1.sell_etched);
        
        // Test PartialEq trait
        assert_eq!(prices1, cloned_prices1);
        assert_ne!(prices1, prices2);
        
        // Test Debug trait
        let debug_output = format!("{:?}", prices1);
        assert!(debug_output.contains("MtgjsonPricesObject"));
        assert!(debug_output.contains("trait_source"));
        assert!(debug_output.contains("AUD"));
        
        // Test equality with different combinations
        let prices3 = MtgjsonPricesObject::new(
            "trait_source".to_string(),
            "trait_provider".to_string(),
            "2023-10-01".to_string(),
            "AUD".to_string(),
            Some(50.0),
            Some(75.0),
            None,
            Some(40.0),
            Some(65.0),
            None,
        );
        
        assert_eq!(prices1, prices3); // Same values should be equal
        
        let mut prices4 = prices3.clone();
        prices4.buy_normal = Some(51.0); // Different price
        assert_ne!(prices1, prices4);
    }

    /// Test complex integration scenarios
    #[test]
    fn test_prices_complex_integration_return_types() {
        // Create prices for different card finishes and analyze them
        let normal_prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-12-01".to_string(),
            "USD".to_string(),
            Some(5.0),   // buy normal
            None,        // buy foil
            None,        // buy etched
            Some(4.0),   // sell normal
            None,        // sell foil
            None,        // sell etched
        );
        
        let foil_prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-12-01".to_string(),
            "USD".to_string(),
            None,        // buy normal
            Some(15.0),  // buy foil
            None,        // buy etched
            None,        // sell normal
            Some(12.0),  // sell foil
            None,        // sell etched
        );
        
        let etched_prices = MtgjsonPricesObject::new(
            "tcgplayer".to_string(),
            "mtgjson".to_string(),
            "2023-12-01".to_string(),
            "USD".to_string(),
            None,        // buy normal
            None,        // buy foil
            Some(25.0),  // buy etched
            None,        // sell normal
            None,        // sell foil
            Some(20.0),  // sell etched
        );
        
        // Test individual price data
        let normal_has_data: bool = normal_prices.has_price_data();
        let foil_has_data: bool = foil_prices.has_price_data();
        let etched_has_data: bool = etched_prices.has_price_data();
        
        assert!(normal_has_data);
        assert!(foil_has_data);
        assert!(etched_has_data);
        
        // Test getting specific price types
        let normal_buy_prices: HashMap<String, f64> = normal_prices.get_buy_prices();
        let normal_sell_prices: HashMap<String, f64> = normal_prices.get_sell_prices();
        let foil_buy_prices: HashMap<String, f64> = foil_prices.get_buy_prices();
        let foil_sell_prices: HashMap<String, f64> = foil_prices.get_sell_prices();
        let etched_buy_prices: HashMap<String, f64> = etched_prices.get_buy_prices();
        let etched_sell_prices: HashMap<String, f64> = etched_prices.get_sell_prices();
        
        assert_eq!(normal_buy_prices.len(), 1);
        assert_eq!(normal_sell_prices.len(), 1);
        assert_eq!(foil_buy_prices.len(), 1);
        assert_eq!(foil_sell_prices.len(), 1);
        assert_eq!(etched_buy_prices.len(), 1);
        assert_eq!(etched_sell_prices.len(), 1);
        
        assert_eq!(normal_buy_prices.get("normal"), Some(&5.0));
        assert_eq!(normal_sell_prices.get("normal"), Some(&4.0));
        assert_eq!(foil_buy_prices.get("foil"), Some(&15.0));
        assert_eq!(foil_sell_prices.get("foil"), Some(&12.0));
        assert_eq!(etched_buy_prices.get("etched"), Some(&25.0));
        assert_eq!(etched_sell_prices.get("etched"), Some(&20.0));
        
        // Test JSON serialization of all three
        let normal_json: Result<String, pyo3::PyErr> = normal_prices.to_json();
        let foil_json: Result<String, pyo3::PyErr> = foil_prices.to_json();
        let etched_json: Result<String, pyo3::PyErr> = etched_prices.to_json();
        
        assert!(normal_json.is_ok());
        assert!(foil_json.is_ok());
        assert!(etched_json.is_ok());
        
        // Test items method for comprehensive data analysis
        let normal_items: Vec<(String, Option<f64>)> = normal_prices.items();
        let foil_items: Vec<(String, Option<f64>)> = foil_prices.items();
        let etched_items: Vec<(String, Option<f64>)> = etched_prices.items();
        
        assert_eq!(normal_items.len(), 10);
        assert_eq!(foil_items.len(), 10);
        assert_eq!(etched_items.len(), 10);
        
        // Verify specific item values
        assert_eq!(normal_items[4], ("buy_normal".to_string(), Some(5.0)));
        assert_eq!(normal_items[7], ("sell_normal".to_string(), Some(4.0)));
        assert_eq!(foil_items[5], ("buy_foil".to_string(), Some(15.0)));
        assert_eq!(foil_items[8], ("sell_foil".to_string(), Some(12.0)));
        assert_eq!(etched_items[6], ("buy_etched".to_string(), Some(25.0)));
        assert_eq!(etched_items[9], ("sell_etched".to_string(), Some(20.0)));
    }

    /// Test JSON object trait implementation
    #[test]
    fn test_prices_json_object_trait_return_types() {
        let prices = MtgjsonPricesObject::new(
            "json_trait_source".to_string(),
            "json_trait_provider".to_string(),
            "2023-12-31".to_string(),
            "CHF".to_string(),
            Some(100.0), Some(150.0), Some(200.0),
            Some(90.0), Some(140.0), Some(180.0),
        );
        
        // Test JsonObject trait methods
        let keys_to_skip = prices.build_keys_to_skip();
        let keys_to_skip_type: std::collections::HashSet<String> = keys_to_skip;
        assert!(keys_to_skip_type.is_empty()); // Prices object doesn't skip any keys by default
    }
}