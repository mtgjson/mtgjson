use mtgjson_rust::classes::*;
use std::collections::HashMap;

mod comprehensive_deck_tests {
    use super::*;
    use mtgjson_rust::classes::deck::{MtgjsonDeckObject, MtgjsonDeckHeaderObject};

    /// Test all MtgjsonDeckObject constructors and return types
    #[test]
    fn test_deck_constructors_return_types() {
        // Test constructor with no sealed products
        let deck_no_sealed = MtgjsonDeckObject::new("Test Deck", None);
        assert_eq!(deck_no_sealed.name, "Test Deck");
        assert_eq!(deck_no_sealed.sealed_product_uuids, None);
        assert!(deck_no_sealed.main_board.is_empty());
        assert!(deck_no_sealed.side_board.is_empty());
        assert!(deck_no_sealed.commander.is_empty());
        assert!(deck_no_sealed.display_commander.is_empty());
        assert!(deck_no_sealed.planes.is_empty());
        assert!(deck_no_sealed.schemes.is_empty());
        assert!(deck_no_sealed.code.is_empty());
        assert!(deck_no_sealed.release_date.is_empty());
        assert!(deck_no_sealed.type_.is_empty());
        assert!(deck_no_sealed.file_name.is_empty());
        
        // Test constructor with sealed products
        let sealed_uuids = vec!["uuid1".to_string(), "uuid2".to_string()];
        let deck_with_sealed = MtgjsonDeckObject::new("Sealed Deck", Some(sealed_uuids.clone()));
        assert_eq!(deck_with_sealed.name, "Sealed Deck");
        assert_eq!(deck_with_sealed.sealed_product_uuids, Some(sealed_uuids));
        
        // Verify return types
        let name: String = deck_no_sealed.name.clone();
        let sealed_products: Option<Vec<String>> = deck_no_sealed.sealed_product_uuids.clone();
        let main_board: Vec<String> = deck_no_sealed.main_board.clone();
        let side_board: Vec<String> = deck_no_sealed.side_board.clone();
        let commander: Vec<String> = deck_no_sealed.commander.clone();
        let display_commander: Vec<String> = deck_no_sealed.display_commander.clone();
        let planes: Vec<String> = deck_no_sealed.planes.clone();
        let schemes: Vec<String> = deck_no_sealed.schemes.clone();
        let code: String = deck_no_sealed.code.clone();
        let release_date: String = deck_no_sealed.release_date.clone();
        let type_: String = deck_no_sealed.type_.clone();
        let file_name: String = deck_no_sealed.file_name.clone();
        
        assert_eq!(name, "Test Deck");
        assert!(sealed_products.is_none());
        assert!(main_board.is_empty());
        assert!(side_board.is_empty());
        assert!(commander.is_empty());
        assert!(display_commander.is_empty());
        assert!(planes.is_empty());
        assert!(schemes.is_empty());
        assert!(code.is_empty());
        assert!(release_date.is_empty());
        assert!(type_.is_empty());
        assert!(file_name.is_empty());
    }

    /// Test all deck field setters and getters with return types
    #[test]
    fn test_deck_field_return_types() {
        let mut deck = MtgjsonDeckObject::new("Field Test Deck", None);
        
        // Test all string fields
        deck.code = "FTD".to_string();
        deck.name = "Field Test Deck Updated".to_string();
        deck.release_date = "2023-01-01".to_string();
        deck.type_ = "constructed".to_string();
        deck.file_name = "field_test_deck.json".to_string();
        
        // Test all vector fields
        deck.main_board = vec![
            r#"{"name": "Lightning Bolt", "count": 4}"#.to_string(),
            r#"{"name": "Mountain", "count": 20}"#.to_string()
        ];
        deck.side_board = vec![
            r#"{"name": "Pyroblast", "count": 3}"#.to_string(),
            r#"{"name": "Red Elemental Blast", "count": 1}"#.to_string()
        ];
        deck.commander = vec![
            r#"{"name": "Krenko, Mob Boss", "count": 1}"#.to_string()
        ];
        deck.display_commander = vec![
            r#"{"name": "Krenko, Mob Boss", "count": 1}"#.to_string()
        ];
        deck.planes = vec![
            r#"{"name": "Tazeem", "count": 1}"#.to_string()
        ];
        deck.schemes = vec![
            r#"{"name": "All Shall Smolder in My Wake", "count": 1}"#.to_string()
        ];
        
        // Test optional field
        deck.sealed_product_uuids = Some(vec!["sealed1".to_string(), "sealed2".to_string()]);
        
        // Verify return types
        let code: String = deck.code.clone();
        let name: String = deck.name.clone();
        let release_date: String = deck.release_date.clone();
        let type_: String = deck.type_.clone();
        let file_name: String = deck.file_name.clone();
        let main_board: Vec<String> = deck.main_board.clone();
        let side_board: Vec<String> = deck.side_board.clone();
        let commander: Vec<String> = deck.commander.clone();
        let display_commander: Vec<String> = deck.display_commander.clone();
        let planes: Vec<String> = deck.planes.clone();
        let schemes: Vec<String> = deck.schemes.clone();
        let sealed_product_uuids: Option<Vec<String>> = deck.sealed_product_uuids.clone();
        
        assert_eq!(code, "FTD");
        assert_eq!(name, "Field Test Deck Updated");
        assert_eq!(release_date, "2023-01-01");
        assert_eq!(type_, "constructed");
        assert_eq!(file_name, "field_test_deck.json");
        assert_eq!(main_board.len(), 2);
        assert_eq!(side_board.len(), 2);
        assert_eq!(commander.len(), 1);
        assert_eq!(display_commander.len(), 1);
        assert_eq!(planes.len(), 1);
        assert_eq!(schemes.len(), 1);
        assert!(sealed_product_uuids.is_some());
        assert_eq!(sealed_product_uuids.unwrap().len(), 2);
    }

    /// Test all deck method return types
    #[test]
    fn test_deck_method_return_types() {
        let mut deck = MtgjsonDeckObject::new("Method Test Deck", None);
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = deck.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        
        // Test set_sanitized_name method return type (void)
        deck.code = "MTD".to_string();
        deck.set_sanitized_name("Unsafe/Name\\With:Chars");
        
        // Test add methods return types (void)
        deck.add_main_board_card(r#"{"name": "Lightning Bolt", "count": 4}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Pyroblast", "count": 2}"#.to_string());
        deck.add_commander_card(r#"{"name": "Krenko, Mob Boss", "count": 1}"#.to_string());
        
        // Test getter methods return types
        let total_cards: usize = deck.get_total_cards();
        assert_eq!(total_cards, 3);
        
        let main_board_count: usize = deck.get_main_board_count();
        assert_eq!(main_board_count, 1);
        
        let side_board_count: usize = deck.get_side_board_count();
        assert_eq!(side_board_count, 1);
        
        let has_cards: bool = deck.has_cards();
        assert!(has_cards);
        
        // Test clear method return type (void)
        deck.clear_all_cards();
        let has_cards_after_clear: bool = deck.has_cards();
        assert!(!has_cards_after_clear);
        
        let total_cards_after_clear: usize = deck.get_total_cards();
        assert_eq!(total_cards_after_clear, 0);
        
        // Test sealed product methods return types (void)
        let sealed_products = vec![MtgjsonSealedProductObject::new()];
        deck.add_sealed_product_uuids(sealed_products);
        
        let header = MtgjsonDeckHeaderObject::new(&deck);
        let sealed_products_for_api = vec![MtgjsonSealedProductObject::new()];
        deck.populate_deck_from_api(header, sealed_products_for_api);
    }

    /// Test MtgjsonDeckHeaderObject constructors and return types
    #[test]
    fn test_deck_header_constructors_return_types() {
        // Create a deck first
        let mut deck = MtgjsonDeckObject::new("Header Test Deck", None);
        deck.code = "HTD".to_string();
        deck.release_date = "2023-01-01".to_string();
        deck.type_ = "constructed".to_string();
        deck.file_name = "header_test.json".to_string();
        
        // Test header constructor from deck
        let header = MtgjsonDeckHeaderObject::new(&deck);
        assert_eq!(header.code, "HTD");
        assert_eq!(header.name, "Header Test Deck");
        assert_eq!(header.release_date, "2023-01-01");
        assert_eq!(header.type_, "constructed");
        assert_eq!(header.file_name, "header_test.json");
        
        // Test static constructor
        let static_header = MtgjsonDeckHeaderObject::from_deck_data(
            "SHD".to_string(),
            "Static Header Deck".to_string(),
            "2023-02-01".to_string(),
            "limited".to_string(),
            "static_header.json".to_string(),
        );
        
        // Verify return types
        let code: String = header.code.clone();
        let name: String = header.name.clone();
        let release_date: String = header.release_date.clone();
        let type_: String = header.type_.clone();
        let file_name: String = header.file_name.clone();
        
        assert_eq!(code, "HTD");
        assert_eq!(name, "Header Test Deck");
        assert_eq!(release_date, "2023-01-01");
        assert_eq!(type_, "constructed");
        assert_eq!(file_name, "header_test.json");
        
        // Verify static constructor return types
        let static_code: String = static_header.code.clone();
        let static_name: String = static_header.name.clone();
        let static_release_date: String = static_header.release_date.clone();
        let static_type: String = static_header.type_.clone();
        let static_file_name: String = static_header.file_name.clone();
        
        assert_eq!(static_code, "SHD");
        assert_eq!(static_name, "Static Header Deck");
        assert_eq!(static_release_date, "2023-02-01");
        assert_eq!(static_type, "limited");
        assert_eq!(static_file_name, "static_header.json");
    }

    /// Test deck header method return types
    #[test]
    fn test_deck_header_method_return_types() {
        let header = MtgjsonDeckHeaderObject::from_deck_data(
            "MHD".to_string(),
            "Method Header Deck".to_string(),
            "2023-03-01".to_string(),
            "commander".to_string(),
            "method_header.json".to_string(),
        );
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = header.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        
        // Test get_display_info method return type
        let display_info: HashMap<String, String> = header.get_display_info();
        assert_eq!(display_info.len(), 4);
        assert_eq!(display_info.get("code"), Some(&"MHD".to_string()));
        assert_eq!(display_info.get("name"), Some(&"Method Header Deck".to_string()));
        assert_eq!(display_info.get("releaseDate"), Some(&"2023-03-01".to_string()));
        assert_eq!(display_info.get("type"), Some(&"commander".to_string()));
    }

    /// Test edge cases and error conditions with return types
    #[test]
    fn test_deck_edge_cases_return_types() {
        // Test empty deck
        let empty_deck = MtgjsonDeckObject::new("", None);
        assert_eq!(empty_deck.name, "");
        assert!(empty_deck.sealed_product_uuids.is_none());
        
        let empty_total_cards: usize = empty_deck.get_total_cards();
        let empty_has_cards: bool = empty_deck.has_cards();
        assert_eq!(empty_total_cards, 0);
        assert!(!empty_has_cards);
        
        // Test deck with massive amounts of cards
        let mut massive_deck = MtgjsonDeckObject::new("Massive Deck", None);
        
        // Add many cards
        for i in 0..1000 {
            massive_deck.add_main_board_card(format!(r#"{{"name": "Card {}", "count": 1}}"#, i));
        }
        for i in 0..100 {
            massive_deck.add_side_board_card(format!(r#"{{"name": "Side {}", "count": 1}}"#, i));
        }
        for i in 0..10 {
            massive_deck.add_commander_card(format!(r#"{{"name": "Commander {}", "count": 1}}"#, i));
        }
        
        let massive_total_cards: usize = massive_deck.get_total_cards();
        let massive_main_board_count: usize = massive_deck.get_main_board_count();
        let massive_side_board_count: usize = massive_deck.get_side_board_count();
        let massive_has_cards: bool = massive_deck.has_cards();
        
        assert_eq!(massive_total_cards, 1110);
        assert_eq!(massive_main_board_count, 1000);
        assert_eq!(massive_side_board_count, 100);
        assert!(massive_has_cards);
        
        // Test deck with None sealed products vs Some empty vec
        let mut deck_none_sealed = MtgjsonDeckObject::new("None Sealed", None);
        let mut deck_empty_sealed = MtgjsonDeckObject::new("Empty Sealed", Some(Vec::new()));
        
        let none_sealed: Option<Vec<String>> = deck_none_sealed.sealed_product_uuids.clone();
        let empty_sealed: Option<Vec<String>> = deck_empty_sealed.sealed_product_uuids.clone();
        
        assert!(none_sealed.is_none());
        assert!(empty_sealed.is_some());
        assert_eq!(empty_sealed.unwrap().len(), 0);
        
        // Test extreme string values
        let unicode_deck = MtgjsonDeckObject::new("🔥⚡️🌟", None);
        let unicode_name: String = unicode_deck.name.clone();
        assert_eq!(unicode_name, "🔥⚡️🌟");
        
        // Test extremely long strings
        let long_name = "A".repeat(10000);
        let long_deck = MtgjsonDeckObject::new(&long_name, None);
        let long_deck_name: String = long_deck.name.clone();
        assert_eq!(long_deck_name.len(), 10000);
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_deck_trait_implementations() {
        let deck1 = MtgjsonDeckObject::new("Deck 1", Some(vec!["uuid1".to_string()]));
        let deck2 = MtgjsonDeckObject::new("Deck 2", None);
        
        // Test Clone trait
        let cloned_deck1 = deck1.clone();
        assert_eq!(deck1.name, cloned_deck1.name);
        assert_eq!(deck1.sealed_product_uuids, cloned_deck1.sealed_product_uuids);
        assert_eq!(deck1.main_board, cloned_deck1.main_board);
        assert_eq!(deck1.side_board, cloned_deck1.side_board);
        
        // Test PartialEq trait
        assert_eq!(deck1, cloned_deck1);
        assert_ne!(deck1, deck2);
        
        // Test Default trait
        let default_deck = MtgjsonDeckObject::default();
        assert_eq!(default_deck.name, "");
        assert!(default_deck.sealed_product_uuids.is_none());
        assert!(default_deck.main_board.is_empty());
        
        // Test Debug trait
        let debug_output = format!("{:?}", deck1);
        assert!(debug_output.contains("MtgjsonDeckObject"));
        
        // Test deck header traits
        let header1 = MtgjsonDeckHeaderObject::new(&deck1);
        let header2 = MtgjsonDeckHeaderObject::new(&deck2);
        
        let cloned_header1 = header1.clone();
        assert_eq!(header1, cloned_header1);
        assert_ne!(header1, header2);
        
        let header_debug = format!("{:?}", header1);
        assert!(header_debug.contains("MtgjsonDeckHeaderObject"));
    }

    /// Test comprehensive JSON operations
    #[test]
    fn test_deck_json_operations_return_types() {
        let mut deck = MtgjsonDeckObject::new("JSON Test Deck", Some(vec!["json_uuid".to_string()]));
        deck.code = "JTD".to_string();
        deck.release_date = "2023-04-01".to_string();
        deck.type_ = "standard".to_string();
        
        // Add some cards
        deck.add_main_board_card(r#"{"name": "Lightning Bolt", "count": 4, "uuid": "bolt_uuid"}"#.to_string());
        deck.add_side_board_card(r#"{"name": "Pyroblast", "count": 3, "uuid": "pyro_uuid"}"#.to_string());
        deck.add_commander_card(r#"{"name": "Krenko, Mob Boss", "count": 1, "uuid": "krenko_uuid"}"#.to_string());
        
        // Test JSON serialization
        let json_result: Result<String, pyo3::PyErr> = deck.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        assert!(json_string.contains("JSON Test Deck"));
        assert!(json_string.contains("JTD"));
        assert!(json_string.contains("Lightning Bolt"));
        
        // Test header JSON serialization
        let header = MtgjsonDeckHeaderObject::new(&deck);
        let header_json_result: Result<String, pyo3::PyErr> = header.to_json();
        assert!(header_json_result.is_ok());
        let header_json_string: String = header_json_result.unwrap();
        assert!(!header_json_string.is_empty());
        assert!(header_json_string.contains("JSON Test Deck"));
        assert!(header_json_string.contains("JTD"));
        
        // Test JSON object trait methods
        let deck_keys_to_skip = deck.build_keys_to_skip();
        let deck_keys_to_skip_type: std::collections::HashSet<String> = deck_keys_to_skip;
        assert!(!deck_keys_to_skip_type.is_empty()); // Should skip file_name
        
        let header_keys_to_skip = header.build_keys_to_skip();
        let header_keys_to_skip_type: std::collections::HashSet<String> = header_keys_to_skip;
        assert!(header_keys_to_skip_type.is_empty()); // Header has no special keys to skip
    }

    /// Test complex integration scenarios
    #[test]
    fn test_deck_complex_integration_return_types() {
        // Create a complex deck scenario
        let mut deck = MtgjsonDeckObject::new("Commander Deck", None);
        deck.code = "CDR".to_string();
        deck.release_date = "2023-05-01".to_string();
        deck.type_ = "commander".to_string();
        
        // Add a full 100-card commander deck
        deck.add_commander_card(r#"{"name": "Ghalta, Primal Hunger", "count": 1}"#.to_string());
        
        for i in 1..=99 {
            if i <= 35 {
                deck.add_main_board_card(format!(r#"{{"name": "Forest", "count": 1, "number": "{}"}}"#, i));
            } else {
                deck.add_main_board_card(format!(r#"{{"name": "Creature {}", "count": 1, "number": "{}"}}"#, i, i));
            }
        }
        
        // Add a sideboard (unusual for commander but testing flexibility)
        for i in 1..=15 {
            deck.add_side_board_card(format!(r#"{{"name": "Sideboard Card {}", "count": 1}}"#, i));
        }
        
        // Test all metrics
        let total_cards: usize = deck.get_total_cards();
        let main_board_count: usize = deck.get_main_board_count();
        let side_board_count: usize = deck.get_side_board_count();
        let has_cards: bool = deck.has_cards();
        
        assert_eq!(total_cards, 115); // 1 commander + 99 main + 15 side
        assert_eq!(main_board_count, 99);
        assert_eq!(side_board_count, 15);
        assert!(has_cards);
        
        // Test header creation from complex deck
        let header = MtgjsonDeckHeaderObject::new(&deck);
        let display_info: HashMap<String, String> = header.get_display_info();
        
        assert_eq!(display_info.get("code"), Some(&"CDR".to_string()));
        assert_eq!(display_info.get("name"), Some(&"Commander Deck".to_string()));
        assert_eq!(display_info.get("type"), Some(&"commander".to_string()));
        
        // Test serialization of complex deck
        let json_result: Result<String, pyo3::PyErr> = deck.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(json_string.contains("Ghalta, Primal Hunger"));
        assert!(json_string.contains("Forest"));
        assert!(json_string.contains("Sideboard Card"));
        
        // Test clearing and re-verification
        deck.clear_all_cards();
        let cleared_total: usize = deck.get_total_cards();
        let cleared_has_cards: bool = deck.has_cards();
        
        assert_eq!(cleared_total, 0);
        assert!(!cleared_has_cards);
    }
}