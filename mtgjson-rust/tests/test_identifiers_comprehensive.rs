use mtgjson_rust::classes::*;
use std::collections::HashMap;

mod comprehensive_identifiers_tests {
    use super::*;
    use mtgjson_rust::classes::identifiers::MtgjsonIdentifiers;

    /// Test all MtgjsonIdentifiers constructors and return types
    #[test]
    fn test_identifiers_constructors_return_types() {
        // Test default constructor
        let default_identifiers = MtgjsonIdentifiers::new();
        assert_eq!(default_identifiers.card_kingdom_etched_id, None);
        assert_eq!(default_identifiers.card_kingdom_foil_id, None);
        assert_eq!(default_identifiers.card_kingdom_id, Some(String::new()));
        assert_eq!(default_identifiers.cardsphere_foil_id, None);
        assert_eq!(default_identifiers.cardsphere_id, None);
        assert_eq!(default_identifiers.mcm_id, None);
        assert_eq!(default_identifiers.mcm_meta_id, None);
        assert_eq!(default_identifiers.mtg_arena_id, None);
        assert_eq!(default_identifiers.mtgjson_foil_version_id, None);
        assert_eq!(default_identifiers.mtgjson_non_foil_version_id, None);
        assert_eq!(default_identifiers.mtgjson_v4_id, None);
        assert_eq!(default_identifiers.mtgo_foil_id, None);
        assert_eq!(default_identifiers.mtgo_id, None);
        assert_eq!(default_identifiers.multiverse_id, Some(String::new()));
        assert_eq!(default_identifiers.scryfall_id, None);
        assert_eq!(default_identifiers.scryfall_illustration_id, None);
        assert_eq!(default_identifiers.scryfall_card_back_id, None);
        assert_eq!(default_identifiers.scryfall_oracle_id, None);
        assert_eq!(default_identifiers.tcgplayer_etched_product_id, None);
        assert_eq!(default_identifiers.tcgplayer_product_id, Some(String::new()));
        
        // Test Default trait constructor
        let trait_default_identifiers = MtgjsonIdentifiers::default();
        assert_eq!(trait_default_identifiers.card_kingdom_id, Some(String::new()));
        assert_eq!(trait_default_identifiers.multiverse_id, Some(String::new()));
        assert_eq!(trait_default_identifiers.tcgplayer_product_id, Some(String::new()));
        
        // Verify return types
        let card_kingdom_etched_id: Option<String> = default_identifiers.card_kingdom_etched_id.clone();
        let card_kingdom_foil_id: Option<String> = default_identifiers.card_kingdom_foil_id.clone();
        let card_kingdom_id: Option<String> = default_identifiers.card_kingdom_id.clone();
        let cardsphere_foil_id: Option<String> = default_identifiers.cardsphere_foil_id.clone();
        let cardsphere_id: Option<String> = default_identifiers.cardsphere_id.clone();
        let mcm_id: Option<String> = default_identifiers.mcm_id.clone();
        let mcm_meta_id: Option<String> = default_identifiers.mcm_meta_id.clone();
        let mtg_arena_id: Option<String> = default_identifiers.mtg_arena_id.clone();
        let mtgjson_foil_version_id: Option<String> = default_identifiers.mtgjson_foil_version_id.clone();
        let mtgjson_non_foil_version_id: Option<String> = default_identifiers.mtgjson_non_foil_version_id.clone();
        let mtgjson_v4_id: Option<String> = default_identifiers.mtgjson_v4_id.clone();
        let mtgo_foil_id: Option<String> = default_identifiers.mtgo_foil_id.clone();
        let mtgo_id: Option<String> = default_identifiers.mtgo_id.clone();
        let multiverse_id: Option<String> = default_identifiers.multiverse_id.clone();
        let scryfall_id: Option<String> = default_identifiers.scryfall_id.clone();
        let scryfall_illustration_id: Option<String> = default_identifiers.scryfall_illustration_id.clone();
        let scryfall_card_back_id: Option<String> = default_identifiers.scryfall_card_back_id.clone();
        let scryfall_oracle_id: Option<String> = default_identifiers.scryfall_oracle_id.clone();
        let tcgplayer_etched_product_id: Option<String> = default_identifiers.tcgplayer_etched_product_id.clone();
        let tcgplayer_product_id: Option<String> = default_identifiers.tcgplayer_product_id.clone();
        
        assert!(card_kingdom_etched_id.is_none());
        assert!(card_kingdom_foil_id.is_none());
        assert!(card_kingdom_id.is_some());
        assert!(cardsphere_foil_id.is_none());
        assert!(cardsphere_id.is_none());
        assert!(mcm_id.is_none());
        assert!(mcm_meta_id.is_none());
        assert!(mtg_arena_id.is_none());
        assert!(mtgjson_foil_version_id.is_none());
        assert!(mtgjson_non_foil_version_id.is_none());
        assert!(mtgjson_v4_id.is_none());
        assert!(mtgo_foil_id.is_none());
        assert!(mtgo_id.is_none());
        assert!(multiverse_id.is_some());
        assert!(scryfall_id.is_none());
        assert!(scryfall_illustration_id.is_none());
        assert!(scryfall_card_back_id.is_none());
        assert!(scryfall_oracle_id.is_none());
        assert!(tcgplayer_etched_product_id.is_none());
        assert!(tcgplayer_product_id.is_some());
    }

    /// Test all identifier field assignments and return types
    #[test]
    fn test_identifiers_field_assignment_return_types() {
        let mut identifiers = MtgjsonIdentifiers::new();
        
        // Test Card Kingdom IDs
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.card_kingdom_foil_id = Some("12346".to_string());
        identifiers.card_kingdom_etched_id = Some("12347".to_string());
        
        // Test Cardsphere IDs
        identifiers.cardsphere_id = Some("CS67890".to_string());
        identifiers.cardsphere_foil_id = Some("CS67891".to_string());
        
        // Test MCM IDs
        identifiers.mcm_id = Some("MCM123".to_string());
        identifiers.mcm_meta_id = Some("MCMM456".to_string());
        
        // Test MTG Arena ID
        identifiers.mtg_arena_id = Some("ARENA789".to_string());
        
        // Test MTGJSON IDs
        identifiers.mtgjson_foil_version_id = Some("mtgjson_foil_123".to_string());
        identifiers.mtgjson_non_foil_version_id = Some("mtgjson_nonfoil_123".to_string());
        identifiers.mtgjson_v4_id = Some("mtgjson_v4_123".to_string());
        
        // Test MTGO IDs
        identifiers.mtgo_id = Some("MTGO321".to_string());
        identifiers.mtgo_foil_id = Some("MTGO322".to_string());
        
        // Test Multiverse ID
        identifiers.multiverse_id = Some("111222".to_string());
        
        // Test Scryfall IDs (UUIDs)
        identifiers.scryfall_id = Some("12345678-1234-5678-9012-123456789012".to_string());
        identifiers.scryfall_oracle_id = Some("87654321-4321-8765-2109-876543210987".to_string());
        identifiers.scryfall_illustration_id = Some("abcdef12-3456-789a-bcde-f123456789ab".to_string());
        identifiers.scryfall_card_back_id = Some("fedcba98-7654-321f-edcb-a987654321fe".to_string());
        
        // Test TCGPlayer IDs
        identifiers.tcgplayer_product_id = Some("TCG555".to_string());
        identifiers.tcgplayer_etched_product_id = Some("TCG556".to_string());
        
        // Verify return types
        let ck_id: Option<String> = identifiers.card_kingdom_id.clone();
        let ck_foil_id: Option<String> = identifiers.card_kingdom_foil_id.clone();
        let ck_etched_id: Option<String> = identifiers.card_kingdom_etched_id.clone();
        let cs_id: Option<String> = identifiers.cardsphere_id.clone();
        let cs_foil_id: Option<String> = identifiers.cardsphere_foil_id.clone();
        let mcm_id: Option<String> = identifiers.mcm_id.clone();
        let mcm_meta_id: Option<String> = identifiers.mcm_meta_id.clone();
        let arena_id: Option<String> = identifiers.mtg_arena_id.clone();
        let mtgjson_foil_id: Option<String> = identifiers.mtgjson_foil_version_id.clone();
        let mtgjson_nonfoil_id: Option<String> = identifiers.mtgjson_non_foil_version_id.clone();
        let mtgjson_v4_id: Option<String> = identifiers.mtgjson_v4_id.clone();
        let mtgo_id: Option<String> = identifiers.mtgo_id.clone();
        let mtgo_foil_id: Option<String> = identifiers.mtgo_foil_id.clone();
        let multiverse_id: Option<String> = identifiers.multiverse_id.clone();
        let scryfall_id: Option<String> = identifiers.scryfall_id.clone();
        let scryfall_oracle_id: Option<String> = identifiers.scryfall_oracle_id.clone();
        let scryfall_illustration_id: Option<String> = identifiers.scryfall_illustration_id.clone();
        let scryfall_card_back_id: Option<String> = identifiers.scryfall_card_back_id.clone();
        let tcgplayer_id: Option<String> = identifiers.tcgplayer_product_id.clone();
        let tcgplayer_etched_id: Option<String> = identifiers.tcgplayer_etched_product_id.clone();
        
        assert_eq!(ck_id.unwrap(), "12345");
        assert_eq!(ck_foil_id.unwrap(), "12346");
        assert_eq!(ck_etched_id.unwrap(), "12347");
        assert_eq!(cs_id.unwrap(), "CS67890");
        assert_eq!(cs_foil_id.unwrap(), "CS67891");
        assert_eq!(mcm_id.unwrap(), "MCM123");
        assert_eq!(mcm_meta_id.unwrap(), "MCMM456");
        assert_eq!(arena_id.unwrap(), "ARENA789");
        assert_eq!(mtgjson_foil_id.unwrap(), "mtgjson_foil_123");
        assert_eq!(mtgjson_nonfoil_id.unwrap(), "mtgjson_nonfoil_123");
        assert_eq!(mtgjson_v4_id.unwrap(), "mtgjson_v4_123");
        assert_eq!(mtgo_id.unwrap(), "MTGO321");
        assert_eq!(mtgo_foil_id.unwrap(), "MTGO322");
        assert_eq!(multiverse_id.unwrap(), "111222");
        assert_eq!(scryfall_id.unwrap(), "12345678-1234-5678-9012-123456789012");
        assert_eq!(scryfall_oracle_id.unwrap(), "87654321-4321-8765-2109-876543210987");
        assert_eq!(scryfall_illustration_id.unwrap(), "abcdef12-3456-789a-bcde-f123456789ab");
        assert_eq!(scryfall_card_back_id.unwrap(), "fedcba98-7654-321f-edcb-a987654321fe");
        assert_eq!(tcgplayer_id.unwrap(), "TCG555");
        assert_eq!(tcgplayer_etched_id.unwrap(), "TCG556");
    }

    /// Test all method return types
    #[test]
    fn test_identifiers_method_return_types() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("abc123def".to_string());
        identifiers.multiverse_id = Some("999888".to_string());
        identifiers.tcgplayer_product_id = Some("TCG777".to_string());
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = identifiers.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        assert!(json_string.contains("12345"));
        assert!(json_string.contains("abc123def"));
        assert!(json_string.contains("999888"));
        assert!(json_string.contains("TCG777"));
        
        // Test to_dict method return type
        let dict_result: Result<HashMap<String, String>, pyo3::PyErr> = identifiers.to_dict();
        assert!(dict_result.is_ok());
        let dict: HashMap<String, String> = dict_result.unwrap();
        assert!(!dict.is_empty());
        assert_eq!(dict.get("cardKingdomId"), Some(&"12345".to_string()));
        assert_eq!(dict.get("scryfallId"), Some(&"abc123def".to_string()));
        assert_eq!(dict.get("multiverseId"), Some(&"999888".to_string()));
        assert_eq!(dict.get("tcgplayerProductId"), Some(&"TCG777".to_string()));
    }

    /// Test edge cases and special values with return types
    #[test]
    fn test_identifiers_edge_cases_return_types() {
        // Test with empty strings
        let mut empty_identifiers = MtgjsonIdentifiers::new();
        empty_identifiers.card_kingdom_id = Some("".to_string());
        empty_identifiers.scryfall_id = Some("valid-uuid".to_string());
        empty_identifiers.multiverse_id = Some("".to_string());
        empty_identifiers.tcgplayer_product_id = Some("".to_string());
        
        let empty_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = empty_identifiers.to_dict();
        assert!(empty_dict_result.is_ok());
        let empty_dict: HashMap<String, String> = empty_dict_result.unwrap();
        
        // Empty strings should be filtered out
        assert!(!empty_dict.contains_key("cardKingdomId"));
        assert!(!empty_dict.contains_key("multiverseId"));
        assert!(!empty_dict.contains_key("tcgplayerProductId"));
        assert_eq!(empty_dict.get("scryfallId"), Some(&"valid-uuid".to_string()));
        
        // Test with None values
        let none_identifiers = MtgjsonIdentifiers::new();
        let none_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = none_identifiers.to_dict();
        assert!(none_dict_result.is_ok());
        let none_dict: HashMap<String, String> = none_dict_result.unwrap();
        
        // Most fields are None by default, so only defaults should be present
        let none_dict_size: usize = none_dict.len();
        assert!(none_dict_size <= 3); // Only default fields that aren't empty
        
        // Test with numeric string formats
        let mut numeric_identifiers = MtgjsonIdentifiers::new();
        numeric_identifiers.card_kingdom_id = Some("0".to_string());
        numeric_identifiers.card_kingdom_foil_id = Some("1".to_string());
        numeric_identifiers.card_kingdom_etched_id = Some("999999999".to_string());
        numeric_identifiers.mcm_id = Some("123456789012345".to_string());
        numeric_identifiers.mtg_arena_id = Some("00000".to_string());
        numeric_identifiers.multiverse_id = Some("1234567890".to_string());
        numeric_identifiers.tcgplayer_product_id = Some("9876543210".to_string());
        numeric_identifiers.tcgplayer_etched_product_id = Some("1111111111".to_string());
        
        let numeric_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = numeric_identifiers.to_dict();
        assert!(numeric_dict_result.is_ok());
        let numeric_dict: HashMap<String, String> = numeric_dict_result.unwrap();
        
        assert_eq!(numeric_dict.get("cardKingdomId"), Some(&"0".to_string()));
        assert_eq!(numeric_dict.get("cardKingdomFoilId"), Some(&"1".to_string()));
        assert_eq!(numeric_dict.get("cardKingdomEtchedId"), Some(&"999999999".to_string()));
        assert_eq!(numeric_dict.get("mcmId"), Some(&"123456789012345".to_string()));
        assert_eq!(numeric_dict.get("mtgArenaId"), Some(&"00000".to_string()));
        assert_eq!(numeric_dict.get("multiverseId"), Some(&"1234567890".to_string()));
        assert_eq!(numeric_dict.get("tcgplayerProductId"), Some(&"9876543210".to_string()));
        assert_eq!(numeric_dict.get("tcgplayerEtchedProductId"), Some(&"1111111111".to_string()));
        
        // Test with UUID formats
        let mut uuid_identifiers = MtgjsonIdentifiers::new();
        uuid_identifiers.scryfall_id = Some("12345678-1234-1234-1234-123456789012".to_string());
        uuid_identifiers.scryfall_oracle_id = Some("87654321-4321-4321-4321-876543210987".to_string());
        uuid_identifiers.scryfall_illustration_id = Some("abcdef01-2345-6789-abcd-ef0123456789".to_string());
        uuid_identifiers.scryfall_card_back_id = Some("fedcba98-7654-3210-fedc-ba9876543210".to_string());
        
        let uuid_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = uuid_identifiers.to_dict();
        assert!(uuid_dict_result.is_ok());
        let uuid_dict: HashMap<String, String> = uuid_dict_result.unwrap();
        
        assert_eq!(uuid_dict.get("scryfallId"), Some(&"12345678-1234-1234-1234-123456789012".to_string()));
        assert_eq!(uuid_dict.get("scryfallOracleId"), Some(&"87654321-4321-4321-4321-876543210987".to_string()));
        assert_eq!(uuid_dict.get("scryfallIllustrationId"), Some(&"abcdef01-2345-6789-abcd-ef0123456789".to_string()));
        assert_eq!(uuid_dict.get("scryfallCardBackId"), Some(&"fedcba98-7654-3210-fedc-ba9876543210".to_string()));
    }

    /// Test special characters and unicode in identifiers
    #[test]
    fn test_identifiers_special_characters_return_types() {
        let mut special_identifiers = MtgjsonIdentifiers::new();
        
        // Test with special characters (these shouldn't normally occur but testing robustness)
        special_identifiers.card_kingdom_id = Some("CK-123/456".to_string());
        special_identifiers.mcm_id = Some("MCM_789".to_string());
        special_identifiers.mtg_arena_id = Some("ARENA:555".to_string());
        special_identifiers.mtgjson_v4_id = Some("mtgjson-v4@123".to_string());
        
        // Test with unicode characters
        special_identifiers.mtgjson_foil_version_id = Some("🔥foil_123".to_string());
        special_identifiers.mtgjson_non_foil_version_id = Some("⚡nonfoil_456".to_string());
        
        let special_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = special_identifiers.to_dict();
        assert!(special_dict_result.is_ok());
        let special_dict: HashMap<String, String> = special_dict_result.unwrap();
        
        assert_eq!(special_dict.get("cardKingdomId"), Some(&"CK-123/456".to_string()));
        assert_eq!(special_dict.get("mcmId"), Some(&"MCM_789".to_string()));
        assert_eq!(special_dict.get("mtgArenaId"), Some(&"ARENA:555".to_string()));
        assert_eq!(special_dict.get("mtgjsonV4Id"), Some(&"mtgjson-v4@123".to_string()));
        assert_eq!(special_dict.get("mtgjsonFoilVersionId"), Some(&"🔥foil_123".to_string()));
        assert_eq!(special_dict.get("mtgjsonNonFoilVersionId"), Some(&"⚡nonfoil_456".to_string()));
        
        // Test JSON serialization with special characters
        let special_json_result: Result<String, pyo3::PyErr> = special_identifiers.to_json();
        assert!(special_json_result.is_ok());
        let special_json_string: String = special_json_result.unwrap();
        assert!(special_json_string.contains("CK-123/456"));
        assert!(special_json_string.contains("MCM_789"));
        assert!(special_json_string.contains("ARENA:555"));
        assert!(special_json_string.contains("🔥foil_123"));
        assert!(special_json_string.contains("⚡nonfoil_456"));
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_identifiers_trait_implementations() {
        let mut identifiers1 = MtgjsonIdentifiers::new();
        identifiers1.card_kingdom_id = Some("12345".to_string());
        identifiers1.scryfall_id = Some("abc123def".to_string());
        identifiers1.multiverse_id = Some("999888".to_string());
        
        let mut identifiers2 = MtgjsonIdentifiers::new();
        identifiers2.card_kingdom_id = Some("54321".to_string());
        identifiers2.scryfall_id = Some("fed321cba".to_string());
        identifiers2.multiverse_id = Some("888999".to_string());
        
        // Test Clone trait
        let cloned_identifiers1 = identifiers1.clone();
        assert_eq!(identifiers1.card_kingdom_id, cloned_identifiers1.card_kingdom_id);
        assert_eq!(identifiers1.scryfall_id, cloned_identifiers1.scryfall_id);
        assert_eq!(identifiers1.multiverse_id, cloned_identifiers1.multiverse_id);
        assert_eq!(identifiers1.card_kingdom_foil_id, cloned_identifiers1.card_kingdom_foil_id);
        assert_eq!(identifiers1.cardsphere_id, cloned_identifiers1.cardsphere_id);
        assert_eq!(identifiers1.mcm_id, cloned_identifiers1.mcm_id);
        assert_eq!(identifiers1.mtg_arena_id, cloned_identifiers1.mtg_arena_id);
        assert_eq!(identifiers1.mtgjson_foil_version_id, cloned_identifiers1.mtgjson_foil_version_id);
        assert_eq!(identifiers1.tcgplayer_product_id, cloned_identifiers1.tcgplayer_product_id);
        
        // Test PartialEq trait
        assert_eq!(identifiers1, cloned_identifiers1);
        assert_ne!(identifiers1, identifiers2);
        
        // Test Default trait
        let default_identifiers = MtgjsonIdentifiers::default();
        assert_eq!(default_identifiers.card_kingdom_id, Some(String::new()));
        assert_eq!(default_identifiers.multiverse_id, Some(String::new()));
        assert_eq!(default_identifiers.tcgplayer_product_id, Some(String::new()));
        
        // Test Debug trait
        let debug_output = format!("{:?}", identifiers1);
        assert!(debug_output.contains("MtgjsonIdentifiers"));
        assert!(debug_output.contains("12345"));
        assert!(debug_output.contains("abc123def"));
        
        // Test equality with different combinations
        let mut identifiers3 = identifiers1.clone();
        identifiers3.card_kingdom_id = Some("99999".to_string());
        assert_ne!(identifiers1, identifiers3);
        
        let mut identifiers4 = identifiers1.clone();
        identifiers4.scryfall_id = Some("different-uuid".to_string());
        assert_ne!(identifiers1, identifiers4);
    }

    /// Test JSON object trait implementation
    #[test]
    fn test_identifiers_json_object_trait_return_types() {
        let mut identifiers = MtgjsonIdentifiers::new();
        identifiers.card_kingdom_id = Some("12345".to_string());
        identifiers.scryfall_id = Some("uuid-here".to_string());
        
        // Test JsonObject trait methods
        let keys_to_skip = identifiers.build_keys_to_skip();
        let keys_to_skip_type: std::collections::HashSet<String> = keys_to_skip;
        assert!(keys_to_skip_type.is_empty()); // Identifiers doesn't skip keys by default
    }

    /// Test complete coverage of all identifier combinations
    #[test]
    fn test_identifiers_all_fields_comprehensive_return_types() {
        let mut comprehensive_identifiers = MtgjsonIdentifiers::new();
        
        // Set all possible identifier fields to test comprehensive coverage
        comprehensive_identifiers.card_kingdom_etched_id = Some("CKE1".to_string());
        comprehensive_identifiers.card_kingdom_foil_id = Some("CKF2".to_string());
        comprehensive_identifiers.card_kingdom_id = Some("CK3".to_string());
        comprehensive_identifiers.cardsphere_foil_id = Some("CSF4".to_string());
        comprehensive_identifiers.cardsphere_id = Some("CS5".to_string());
        comprehensive_identifiers.mcm_id = Some("MCM6".to_string());
        comprehensive_identifiers.mcm_meta_id = Some("MCMM7".to_string());
        comprehensive_identifiers.mtg_arena_id = Some("ARENA8".to_string());
        comprehensive_identifiers.mtgjson_foil_version_id = Some("MJSONF9".to_string());
        comprehensive_identifiers.mtgjson_non_foil_version_id = Some("MJSONNF10".to_string());
        comprehensive_identifiers.mtgjson_v4_id = Some("MJSONV411".to_string());
        comprehensive_identifiers.mtgo_foil_id = Some("MTGOF12".to_string());
        comprehensive_identifiers.mtgo_id = Some("MTGO13".to_string());
        comprehensive_identifiers.multiverse_id = Some("MV14".to_string());
        comprehensive_identifiers.scryfall_id = Some("SF15".to_string());
        comprehensive_identifiers.scryfall_illustration_id = Some("SFI16".to_string());
        comprehensive_identifiers.scryfall_card_back_id = Some("SFCB17".to_string());
        comprehensive_identifiers.scryfall_oracle_id = Some("SFO18".to_string());
        comprehensive_identifiers.tcgplayer_etched_product_id = Some("TCGE19".to_string());
        comprehensive_identifiers.tcgplayer_product_id = Some("TCG20".to_string());
        
        // Test JSON serialization with all fields
        let comprehensive_json_result: Result<String, pyo3::PyErr> = comprehensive_identifiers.to_json();
        assert!(comprehensive_json_result.is_ok());
        let comprehensive_json_string: String = comprehensive_json_result.unwrap();
        
        // Verify all fields are present in JSON
        assert!(comprehensive_json_string.contains("CKE1"));
        assert!(comprehensive_json_string.contains("CKF2"));
        assert!(comprehensive_json_string.contains("CK3"));
        assert!(comprehensive_json_string.contains("CSF4"));
        assert!(comprehensive_json_string.contains("CS5"));
        assert!(comprehensive_json_string.contains("MCM6"));
        assert!(comprehensive_json_string.contains("MCMM7"));
        assert!(comprehensive_json_string.contains("ARENA8"));
        assert!(comprehensive_json_string.contains("MJSONF9"));
        assert!(comprehensive_json_string.contains("MJSONNF10"));
        assert!(comprehensive_json_string.contains("MJSONV411"));
        assert!(comprehensive_json_string.contains("MTGOF12"));
        assert!(comprehensive_json_string.contains("MTGO13"));
        assert!(comprehensive_json_string.contains("MV14"));
        assert!(comprehensive_json_string.contains("SF15"));
        assert!(comprehensive_json_string.contains("SFI16"));
        assert!(comprehensive_json_string.contains("SFCB17"));
        assert!(comprehensive_json_string.contains("SFO18"));
        assert!(comprehensive_json_string.contains("TCGE19"));
        assert!(comprehensive_json_string.contains("TCG20"));
        
        // Test to_dict with all fields
        let comprehensive_dict_result: Result<HashMap<String, String>, pyo3::PyErr> = comprehensive_identifiers.to_dict();
        assert!(comprehensive_dict_result.is_ok());
        let comprehensive_dict: HashMap<String, String> = comprehensive_dict_result.unwrap();
        
        // Verify all camelCase keys are present
        assert_eq!(comprehensive_dict.get("cardKingdomEtchedId"), Some(&"CKE1".to_string()));
        assert_eq!(comprehensive_dict.get("cardKingdomFoilId"), Some(&"CKF2".to_string()));
        assert_eq!(comprehensive_dict.get("cardKingdomId"), Some(&"CK3".to_string()));
        assert_eq!(comprehensive_dict.get("cardsphereFoilId"), Some(&"CSF4".to_string()));
        assert_eq!(comprehensive_dict.get("cardsphereId"), Some(&"CS5".to_string()));
        assert_eq!(comprehensive_dict.get("mcmId"), Some(&"MCM6".to_string()));
        assert_eq!(comprehensive_dict.get("mcmMetaId"), Some(&"MCMM7".to_string()));
        assert_eq!(comprehensive_dict.get("mtgArenaId"), Some(&"ARENA8".to_string()));
        assert_eq!(comprehensive_dict.get("mtgjsonFoilVersionId"), Some(&"MJSONF9".to_string()));
        assert_eq!(comprehensive_dict.get("mtgjsonNonFoilVersionId"), Some(&"MJSONNF10".to_string()));
        assert_eq!(comprehensive_dict.get("mtgjsonV4Id"), Some(&"MJSONV411".to_string()));
        assert_eq!(comprehensive_dict.get("mtgoFoilId"), Some(&"MTGOF12".to_string()));
        assert_eq!(comprehensive_dict.get("mtgoId"), Some(&"MTGO13".to_string()));
        assert_eq!(comprehensive_dict.get("multiverseId"), Some(&"MV14".to_string()));
        assert_eq!(comprehensive_dict.get("scryfallId"), Some(&"SF15".to_string()));
        assert_eq!(comprehensive_dict.get("scryfallIllustrationId"), Some(&"SFI16".to_string()));
        assert_eq!(comprehensive_dict.get("scryfallCardBackId"), Some(&"SFCB17".to_string()));
        assert_eq!(comprehensive_dict.get("scryfallOracleId"), Some(&"SFO18".to_string()));
        assert_eq!(comprehensive_dict.get("tcgplayerEtchedProductId"), Some(&"TCGE19".to_string()));
        assert_eq!(comprehensive_dict.get("tcgplayerProductId"), Some(&"TCG20".to_string()));
        
        // Verify return type sizes
        let dict_size: usize = comprehensive_dict.len();
        assert_eq!(dict_size, 20); // All 20 identifier fields should be present
    }

    /// Test complex integration scenarios
    #[test]
    fn test_identifiers_complex_integration_return_types() {
        // Create multiple identifier objects representing different cards
        let mut card1_identifiers = MtgjsonIdentifiers::new();
        card1_identifiers.scryfall_id = Some("card1-scryfall-uuid".to_string());
        card1_identifiers.multiverse_id = Some("111111".to_string());
        card1_identifiers.card_kingdom_id = Some("1001".to_string());
        card1_identifiers.tcgplayer_product_id = Some("2001".to_string());
        
        let mut card2_identifiers = MtgjsonIdentifiers::new();
        card2_identifiers.scryfall_id = Some("card2-scryfall-uuid".to_string());
        card2_identifiers.multiverse_id = Some("222222".to_string());
        card2_identifiers.card_kingdom_foil_id = Some("1002".to_string());
        card2_identifiers.tcgplayer_etched_product_id = Some("2002".to_string());
        
        let mut card3_identifiers = MtgjsonIdentifiers::new();
        card3_identifiers.scryfall_oracle_id = Some("oracle-uuid-shared".to_string());
        card3_identifiers.mtg_arena_id = Some("3001".to_string());
        card3_identifiers.mcm_id = Some("4001".to_string());
        
        // Test JSON conversion for all three
        let card1_json: Result<String, pyo3::PyErr> = card1_identifiers.to_json();
        let card2_json: Result<String, pyo3::PyErr> = card2_identifiers.to_json();
        let card3_json: Result<String, pyo3::PyErr> = card3_identifiers.to_json();
        
        assert!(card1_json.is_ok());
        assert!(card2_json.is_ok());
        assert!(card3_json.is_ok());
        
        // Test dict conversion for all three
        let card1_dict: Result<HashMap<String, String>, pyo3::PyErr> = card1_identifiers.to_dict();
        let card2_dict: Result<HashMap<String, String>, pyo3::PyErr> = card2_identifiers.to_dict();
        let card3_dict: Result<HashMap<String, String>, pyo3::PyErr> = card3_identifiers.to_dict();
        
        assert!(card1_dict.is_ok());
        assert!(card2_dict.is_ok());
        assert!(card3_dict.is_ok());
        
        let card1_dict_unwrapped: HashMap<String, String> = card1_dict.unwrap();
        let card2_dict_unwrapped: HashMap<String, String> = card2_dict.unwrap();
        let card3_dict_unwrapped: HashMap<String, String> = card3_dict.unwrap();
        
        // Verify specific identifier mappings
        assert_eq!(card1_dict_unwrapped.get("scryfallId"), Some(&"card1-scryfall-uuid".to_string()));
        assert_eq!(card1_dict_unwrapped.get("multiverseId"), Some(&"111111".to_string()));
        assert_eq!(card2_dict_unwrapped.get("scryfallId"), Some(&"card2-scryfall-uuid".to_string()));
        assert_eq!(card2_dict_unwrapped.get("cardKingdomFoilId"), Some(&"1002".to_string()));
        assert_eq!(card3_dict_unwrapped.get("scryfallOracleId"), Some(&"oracle-uuid-shared".to_string()));
        assert_eq!(card3_dict_unwrapped.get("mtgArenaId"), Some(&"3001".to_string()));
        
        // Test cloning and equality
        let card1_clone = card1_identifiers.clone();
        assert_eq!(card1_identifiers, card1_clone);
        assert_ne!(card1_identifiers, card2_identifiers);
        assert_ne!(card2_identifiers, card3_identifiers);
        
        // Test modification doesn't affect clones
        let mut card1_modified = card1_identifiers.clone();
        card1_modified.card_kingdom_id = Some("modified".to_string());
        assert_ne!(card1_identifiers, card1_modified);
        assert_eq!(card1_identifiers, card1_clone); // Original clone unchanged
    }
}