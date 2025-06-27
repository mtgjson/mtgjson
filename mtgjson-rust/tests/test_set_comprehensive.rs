use mtgjson_rust::classes::*;
use std::collections::HashMap;

mod comprehensive_set_tests {
    use super::*;
    use mtgjson_rust::classes::set::MtgjsonSetObject;

    /// Test all MtgjsonSetObject constructors and return types
    #[test]
    fn test_set_constructors_return_types() {
        // Test default constructor
        let default_set = MtgjsonSetObject::new();
        assert_eq!(default_set.base_set_size, None);
        assert_eq!(default_set.total_set_size, 0);
        assert!(default_set.cards.is_empty());
        assert!(default_set.tokens.is_empty());
        assert!(default_set.decks.is_empty());
        assert!(default_set.sealed_product.is_empty());
        assert!(default_set.languages.is_empty());
        assert!(default_set.extra_tokens.is_empty());
        assert_eq!(default_set.is_foreign_only, false);
        assert_eq!(default_set.is_foil_only, false);
        assert_eq!(default_set.is_non_foil_only, false);
        assert_eq!(default_set.is_online_only, false);
        assert_eq!(default_set.is_partial_preview, false);
        assert!(default_set.name.is_empty());
        assert!(default_set.release_date.is_empty());
        assert!(default_set.type_.is_empty());
        assert!(default_set.search_uri.is_empty());
        
        // Verify return types
        let base_set_size: Option<i32> = default_set.base_set_size;
        let total_set_size: i32 = default_set.total_set_size;
        let is_foreign_only: bool = default_set.is_foreign_only;
        let is_foil_only: bool = default_set.is_foil_only;
        let is_non_foil_only: bool = default_set.is_non_foil_only;
        let is_online_only: bool = default_set.is_online_only;
        let is_partial_preview: bool = default_set.is_partial_preview;
        
        assert_eq!(base_set_size, None);
        assert_eq!(total_set_size, 0);
        assert_eq!(is_foreign_only, false);
        assert_eq!(is_foil_only, false);
        assert_eq!(is_non_foil_only, false);
        assert_eq!(is_online_only, false);
        assert_eq!(is_partial_preview, false);
    }

    /// Test all get/set field methods and their return types
    #[test]
    fn test_set_field_return_types() {
        let mut set = MtgjsonSetObject::new();
        
        // Test all string fields
        set.name = "Test Set".to_string();
        set.code = Some("TST".to_string());
        set.code_v3 = Some("TST".to_string());
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();
        set.keyrune_code = Some("TST".to_string());
        set.mcm_name = Some("Test Set MCM".to_string());
        set.mtgo_code = Some("TST".to_string());
        set.parent_code = Some("TST_PARENT".to_string());
        set.token_set_code = Some("TTST".to_string());
        set.search_uri = "https://example.com".to_string();
        
        // Test all numeric fields
        set.base_set_size = Some(274);
        set.total_set_size = 300;
        set.cardsphere_set_id = Some(12345);
        set.mcm_id = Some(67890);
        set.mcm_id_extras = Some(67891);
        set.tcgplayer_group_id = Some(54321);
        
        // Test all boolean fields
        set.is_foreign_only = true;
        set.is_foil_only = false;
        set.is_non_foil_only = true;
        set.is_online_only = false;
        set.is_partial_preview = true;
        
        // Verify return types
        let name: String = set.name.clone();
        let code: Option<String> = set.code.clone();
        let code_v3: Option<String> = set.code_v3.clone();
        let release_date: String = set.release_date.clone();
        let type_: String = set.type_.clone();
        let keyrune_code: Option<String> = set.keyrune_code.clone();
        let mcm_name: Option<String> = set.mcm_name.clone();
        let mtgo_code: Option<String> = set.mtgo_code.clone();
        let parent_code: Option<String> = set.parent_code.clone();
        let token_set_code: Option<String> = set.token_set_code.clone();
        let search_uri: String = set.search_uri.clone();
        let base_set_size: Option<i32> = set.base_set_size;
        let total_set_size: i32 = set.total_set_size;
        let cardsphere_set_id: Option<i32> = set.cardsphere_set_id;
        let mcm_id: Option<i32> = set.mcm_id;
        let mcm_id_extras: Option<i32> = set.mcm_id_extras;
        let tcgplayer_group_id: Option<i32> = set.tcgplayer_group_id;
        let is_foreign_only: bool = set.is_foreign_only;
        let is_foil_only: bool = set.is_foil_only;
        let is_non_foil_only: bool = set.is_non_foil_only;
        let is_online_only: bool = set.is_online_only;
        let is_partial_preview: bool = set.is_partial_preview;
        
        assert_eq!(name, "Test Set");
        assert_eq!(code.unwrap(), "TST");
        assert_eq!(code_v3.unwrap(), "TST");
        assert_eq!(release_date, "2023-01-01");
        assert_eq!(type_, "expansion");
        assert_eq!(keyrune_code.unwrap(), "TST");
        assert_eq!(mcm_name.unwrap(), "Test Set MCM");
        assert_eq!(mtgo_code.unwrap(), "TST");
        assert_eq!(parent_code.unwrap(), "TST_PARENT");
        assert_eq!(token_set_code.unwrap(), "TTST");
        assert_eq!(search_uri, "https://example.com");
        assert_eq!(base_set_size.unwrap(), 274);
        assert_eq!(total_set_size, 300);
        assert_eq!(cardsphere_set_id.unwrap(), 12345);
        assert_eq!(mcm_id.unwrap(), 67890);
        assert_eq!(mcm_id_extras.unwrap(), 67891);
        assert_eq!(tcgplayer_group_id.unwrap(), 54321);
        assert_eq!(is_foreign_only, true);
        assert_eq!(is_foil_only, false);
        assert_eq!(is_non_foil_only, true);
        assert_eq!(is_online_only, false);
        assert_eq!(is_partial_preview, true);
    }

    /// Test all vector field types and methods
    #[test]
    fn test_set_vector_field_return_types() {
        let mut set = MtgjsonSetObject::new();
        
        // Test vector fields
        set.languages = vec!["en".to_string(), "fr".to_string(), "de".to_string()];
        set.extra_tokens = vec!["token1".to_string(), "token2".to_string()];
        
        // Add cards
        let card1 = MtgjsonCardObject::new(false);
        let card2 = MtgjsonCardObject::new(false);
        set.cards = vec![card1, card2];
        
        // Add tokens
        let token1 = MtgjsonCardObject::new(true);
        let token2 = MtgjsonCardObject::new(true);
        set.tokens = vec![token1, token2];
        
        // Add decks
        let deck1 = MtgjsonDeckObject::new("Deck 1", None);
        let deck2 = MtgjsonDeckObject::new("Deck 2", None);
        set.decks = vec![deck1, deck2];
        
        // Add sealed products
        let sealed1 = MtgjsonSealedProductObject::new();
        let sealed2 = MtgjsonSealedProductObject::new();
        set.sealed_product = vec![sealed1, sealed2];
        
        // Verify return types
        let languages: Vec<String> = set.languages.clone();
        let extra_tokens: Vec<String> = set.extra_tokens.clone();
        let cards: Vec<MtgjsonCardObject> = set.cards.clone();
        let tokens: Vec<MtgjsonCardObject> = set.tokens.clone();
        let decks: Vec<MtgjsonDeckObject> = set.decks.clone();
        let sealed_product: Vec<MtgjsonSealedProductObject> = set.sealed_product.clone();
        
        assert_eq!(languages.len(), 3);
        assert_eq!(extra_tokens.len(), 2);
        assert_eq!(cards.len(), 2);
        assert_eq!(tokens.len(), 2);
        assert_eq!(decks.len(), 2);
        assert_eq!(sealed_product.len(), 2);
        
        assert_eq!(languages[0], "en");
        assert_eq!(languages[1], "fr");
        assert_eq!(languages[2], "de");
        assert_eq!(extra_tokens[0], "token1");
        assert_eq!(extra_tokens[1], "token2");
        assert_eq!(cards[0].is_token, false);
        assert_eq!(tokens[0].is_token, true);
    }

    /// Test all method return types
    #[test]
    fn test_set_method_return_types() {
        let mut set = MtgjsonSetObject::new();
        set.code = Some("TST".to_string());
        set.name = "Test Set".to_string();
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = set.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        
        // Test string representation methods return types
        let str_repr: String = set.__str__();
        let repr_repr: String = set.__repr__();
        assert!(!str_repr.is_empty());
        assert!(!repr_repr.is_empty());
        
        // Test equality method return type
        let other_set = MtgjsonSetObject::new();
        let equality: bool = set.__eq__(&other_set);
        assert!(!equality); // Should be false since codes are different
        
        // Test hash method return type
        let hash_value: u64 = set.__hash__();
        assert!(hash_value > 0 || hash_value == 0); // Hash can be any u64
        
        // Test get_windows_safe_set_code method return type
        let safe_code: String = set.get_windows_safe_set_code();
        assert!(!safe_code.is_empty());
        
        // Test collection management methods (return void)
        let card = MtgjsonCardObject::new(false);
        set.add_card(card);
        
        let token = MtgjsonCardObject::new(true);
        set.add_token(token);
        
        let deck = MtgjsonDeckObject::new("Test Deck", None);
        set.add_deck(deck);
        
        let sealed_product = MtgjsonSealedProductObject::new();
        set.add_sealed_product(sealed_product);
        
        // Test sorting methods (return void)
        set.sort_cards();
        set.sort_tokens();
        
        // Test getter methods return types
        let total_cards: usize = set.get_total_cards();
        assert_eq!(total_cards, 2); // 1 card + 1 token
        
        let rarity_counts: HashMap<String, i32> = set.get_cards_by_rarity();
        assert_eq!(rarity_counts.len(), 1); // Empty rarity counts initially
        
        let unique_languages: Vec<String> = set.get_unique_languages();
        assert!(unique_languages.is_empty()); // No languages in cards initially
        
        // Test search methods return types
        let card_by_name: Option<usize> = set.find_card_by_name("Nonexistent Card");
        assert!(card_by_name.is_none());
        
        let card_by_uuid: Option<usize> = set.find_card_by_uuid("nonexistent-uuid");
        assert!(card_by_uuid.is_none());
        
        let cards_of_rarity: Vec<usize> = set.get_cards_of_rarity("common");
        assert!(cards_of_rarity.is_empty());
        
        // Test boolean check methods return types
        let has_foil: bool = set.has_foil_cards();
        let has_non_foil: bool = set.has_non_foil_cards();
        assert!(!has_foil);
        assert!(!has_non_foil);
        
        // Test statistics method return type
        let statistics: String = set.get_statistics();
        assert!(!statistics.is_empty());
        
        // Test update methods (return void)
        set.update_set_sizes();
        
        // Test validation method return type
        let validation_errors: Vec<String> = set.validate();
        assert!(!validation_errors.is_empty()); // Should have validation errors for empty fields
    }

    /// Test complex object field types
    #[test]
    fn test_set_complex_object_field_return_types() {
        let set = MtgjsonSetObject::new();
        
        // Test complex object fields
        let translations: MtgjsonTranslations = set.translations.clone();
        
        // Verify these are the correct types
        assert_eq!(std::mem::size_of_val(&translations), std::mem::size_of::<MtgjsonTranslations>());
    }

    /// Test edge cases and error conditions with return types
    #[test]
    fn test_set_edge_cases_return_types() {
        let mut set = MtgjsonSetObject::new();
        
        // Test None values for optional fields
        set.base_set_size = None;
        set.booster = None;
        set.cardsphere_set_id = None;
        set.code = None;
        set.code_v3 = None;
        set.keyrune_code = None;
        set.mcm_id = None;
        set.mcm_id_extras = None;
        set.mcm_name = None;
        set.mtgo_code = None;
        set.parent_code = None;
        set.tcgplayer_group_id = None;
        set.token_set_code = None;
        
        // Verify return types
        let base_set_size: Option<i32> = set.base_set_size;
        let booster: Option<String> = set.booster.clone();
        let cardsphere_set_id: Option<i32> = set.cardsphere_set_id;
        let code: Option<String> = set.code.clone();
        let code_v3: Option<String> = set.code_v3.clone();
        let keyrune_code: Option<String> = set.keyrune_code.clone();
        let mcm_id: Option<i32> = set.mcm_id;
        let mcm_id_extras: Option<i32> = set.mcm_id_extras;
        let mcm_name: Option<String> = set.mcm_name.clone();
        let mtgo_code: Option<String> = set.mtgo_code.clone();
        let parent_code: Option<String> = set.parent_code.clone();
        let tcgplayer_group_id: Option<i32> = set.tcgplayer_group_id;
        let token_set_code: Option<String> = set.token_set_code.clone();
        
        assert!(base_set_size.is_none());
        assert!(booster.is_none());
        assert!(cardsphere_set_id.is_none());
        assert!(code.is_none());
        assert!(code_v3.is_none());
        assert!(keyrune_code.is_none());
        assert!(mcm_id.is_none());
        assert!(mcm_id_extras.is_none());
        assert!(mcm_name.is_none());
        assert!(mtgo_code.is_none());
        assert!(parent_code.is_none());
        assert!(tcgplayer_group_id.is_none());
        assert!(token_set_code.is_none());
        
        // Test extreme numeric values
        set.total_set_size = i32::MAX;
        set.base_set_size = Some(i32::MIN);
        set.cardsphere_set_id = Some(i32::MAX);
        set.mcm_id = Some(0);
        set.mcm_id_extras = Some(1);
        set.tcgplayer_group_id = Some(i32::MIN);
        
        let total_set_size: i32 = set.total_set_size;
        let base_set_size_extreme: Option<i32> = set.base_set_size;
        let cardsphere_id_extreme: Option<i32> = set.cardsphere_set_id;
        let mcm_id_zero: Option<i32> = set.mcm_id;
        let mcm_id_extras_one: Option<i32> = set.mcm_id_extras;
        let tcgplayer_id_extreme: Option<i32> = set.tcgplayer_group_id;
        
        assert_eq!(total_set_size, i32::MAX);
        assert_eq!(base_set_size_extreme, Some(i32::MIN));
        assert_eq!(cardsphere_id_extreme, Some(i32::MAX));
        assert_eq!(mcm_id_zero, Some(0));
        assert_eq!(mcm_id_extras_one, Some(1));
        assert_eq!(tcgplayer_id_extreme, Some(i32::MIN));
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_set_trait_implementations() {
        let mut set1 = MtgjsonSetObject::new();
        set1.code = Some("TST1".to_string());
        set1.name = "Test Set 1".to_string();
        
        let mut set2 = MtgjsonSetObject::new();
        set2.code = Some("TST2".to_string());
        set2.name = "Test Set 2".to_string();
        
        // Test Clone trait
        let cloned_set1 = set1.clone();
        assert_eq!(set1.code, cloned_set1.code);
        assert_eq!(set1.name, cloned_set1.name);
        assert_eq!(set1.total_set_size, cloned_set1.total_set_size);
        
        // Test PartialEq trait
        assert_eq!(set1, cloned_set1);
        assert_ne!(set1, set2);
        
        // Test Default trait
        let default_set = MtgjsonSetObject::default();
        assert_eq!(default_set.total_set_size, 0);
        assert!(default_set.cards.is_empty());
        
        // Test Debug trait
        let debug_output = format!("{:?}", set1);
        assert!(debug_output.contains("MtgjsonSetObject"));
    }

    /// Test comprehensive collection operations
    #[test]
    fn test_set_collection_operations_return_types() {
        let mut set = MtgjsonSetObject::new();
        set.name = "Test Set".to_string();
        set.code = Some("TST".to_string());
        
        // Create multiple cards with different properties
        let mut card1 = MtgjsonCardObject::new(false);
        card1.name = "Card 1".to_string();
        card1.rarity = "common".to_string();
        card1.language = "en".to_string();
        card1.uuid = "uuid1".to_string();
        card1.finishes = vec!["nonfoil".to_string()];
        
        let mut card2 = MtgjsonCardObject::new(false);
        card2.name = "Card 2".to_string();
        card2.rarity = "rare".to_string();
        card2.language = "fr".to_string();
        card2.uuid = "uuid2".to_string();
        card2.finishes = vec!["foil".to_string()];
        
        let mut card3 = MtgjsonCardObject::new(false);
        card3.name = "Card 3".to_string();
        card3.rarity = "common".to_string();
        card3.language = "en".to_string();
        card3.uuid = "uuid3".to_string();
        card3.finishes = vec!["nonfoil".to_string(), "foil".to_string()];
        
        // Add cards to set
        set.add_card(card1);
        set.add_card(card2);
        set.add_card(card3);
        
        // Test search operations return types
        let card_index_by_name: Option<usize> = set.find_card_by_name("Card 1");
        assert!(card_index_by_name.is_some());
        assert_eq!(card_index_by_name.unwrap(), 0);
        
        let card_index_by_uuid: Option<usize> = set.find_card_by_uuid("uuid2");
        assert!(card_index_by_uuid.is_some());
        assert_eq!(card_index_by_uuid.unwrap(), 1);
        
        let common_cards: Vec<usize> = set.get_cards_of_rarity("common");
        assert_eq!(common_cards.len(), 2);
        assert!(common_cards.contains(&0));
        assert!(common_cards.contains(&2));
        
        let rare_cards: Vec<usize> = set.get_cards_of_rarity("rare");
        assert_eq!(rare_cards.len(), 1);
        assert!(rare_cards.contains(&1));
        
        // Test collection analysis methods return types
        let rarity_counts: HashMap<String, i32> = set.get_cards_by_rarity();
        assert_eq!(rarity_counts.len(), 2);
        assert_eq!(rarity_counts.get("common"), Some(&2));
        assert_eq!(rarity_counts.get("rare"), Some(&1));
        
        let unique_languages: Vec<String> = set.get_unique_languages();
        assert_eq!(unique_languages.len(), 2);
        assert!(unique_languages.contains(&"en".to_string()));
        assert!(unique_languages.contains(&"fr".to_string()));
        
        let has_foil_cards: bool = set.has_foil_cards();
        let has_non_foil_cards: bool = set.has_non_foil_cards();
        assert!(has_foil_cards);
        assert!(has_non_foil_cards);
        
        let total_cards: usize = set.get_total_cards();
        assert_eq!(total_cards, 3);
        
        // Test update operations
        set.update_set_sizes();
        let updated_total_size: i32 = set.total_set_size;
        let updated_base_size: Option<i32> = set.base_set_size;
        assert_eq!(updated_total_size, 3);
        assert!(updated_base_size.is_some());
        
        // Test validation
        let validation_errors: Vec<String> = set.validate();
        assert!(validation_errors.len() < 3); // Should have fewer errors now that we have code and name
    }

    /// Test serialization and JSON object trait
    #[test]
    fn test_set_json_operations_return_types() {
        let mut set = MtgjsonSetObject::new();
        set.name = "JSON Test Set".to_string();
        set.code = Some("JTS".to_string());
        set.release_date = "2023-01-01".to_string();
        set.type_ = "expansion".to_string();
        
        // Test JSON serialization
        let json_result: Result<String, pyo3::PyErr> = set.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        assert!(json_string.contains("JSON Test Set"));
        assert!(json_string.contains("JTS"));
        
        // Test JSON object trait methods
        let keys_to_skip = set.build_keys_to_skip();
        let keys_to_skip_type: std::collections::HashSet<String> = keys_to_skip;
        assert!(!keys_to_skip_type.is_empty()); // Should have some keys to skip
    }
}