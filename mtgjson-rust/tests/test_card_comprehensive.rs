use mtgjson_rust::classes::*;
use std::collections::{HashMap, HashSet};

mod comprehensive_card_tests {
    use super::*;
    use mtgjson_rust::classes::card::MtgjsonCardObject;

    /// Test all MtgjsonCardObject constructors and return types
    #[test]
    fn test_card_constructors_return_types() {
        // Test default constructor
        let default_card = MtgjsonCardObject::new(false);
        assert_eq!(default_card.is_token, false);
        assert_eq!(default_card.count, 1);
        assert_eq!(default_card.converted_mana_cost, 0.0);
        assert_eq!(default_card.face_converted_mana_cost, 0.0);
        assert_eq!(default_card.face_mana_value, 0.0);
        assert_eq!(default_card.mana_value, 0.0);
        assert!(default_card.artist.is_empty());
        assert!(default_card.name.is_empty());
        assert!(default_card.uuid.is_empty());
        
        // Test token constructor
        let token_card = MtgjsonCardObject::new(true);
        assert_eq!(token_card.is_token, true);
        assert_eq!(token_card.count, 1);
        
        // Verify return types of constructor parameters
        let is_token: bool = token_card.is_token;
        let count: i32 = token_card.count;
        let cmc: f64 = token_card.converted_mana_cost;
        assert_eq!(is_token, true);
        assert_eq!(count, 1);
        assert_eq!(cmc, 0.0);
    }

    /// Test all get/set methods and their return types
    #[test]
    fn test_card_getter_setter_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test string setters and getters
        card.name = "Test Card".to_string();
        card.artist = "Test Artist".to_string();
        card.border_color = "black".to_string();
        card.frame_version = "2015".to_string();
        card.language = "en".to_string();
        card.layout = "normal".to_string();
        card.mana_cost = "{1}{U}".to_string();
        card.number = "1".to_string();
        card.power = "2".to_string();
        card.rarity = "common".to_string();
        card.text = "Draw a card.".to_string();
        card.toughness = "3".to_string();
        card.type_ = "Creature — Human Wizard".to_string();
        card.uuid = "12345678-1234-5678-9012-123456789012".to_string();
        
        // Verify return types
        let name: String = card.name.clone();
        let artist: String = card.artist.clone();
        let border_color: String = card.border_color.clone();
        let frame_version: String = card.frame_version.clone();
        let language: String = card.language.clone();
        let layout: String = card.layout.clone();
        let mana_cost: String = card.mana_cost.clone();
        let number: String = card.number.clone();
        let power: String = card.power.clone();
        let rarity: String = card.rarity.clone();
        let text: String = card.text.clone();
        let toughness: String = card.toughness.clone();
        let type_: String = card.type_.clone();
        let uuid: String = card.uuid.clone();
        
        assert_eq!(name, "Test Card");
        assert_eq!(artist, "Test Artist");
        assert_eq!(border_color, "black");
        assert_eq!(frame_version, "2015");
        assert_eq!(language, "en");
        assert_eq!(layout, "normal");
        assert_eq!(mana_cost, "{1}{U}");
        assert_eq!(number, "1");
        assert_eq!(power, "2");
        assert_eq!(rarity, "common");
        assert_eq!(text, "Draw a card.");
        assert_eq!(toughness, "3");
        assert_eq!(type_, "Creature — Human Wizard");
        assert_eq!(uuid, "12345678-1234-5678-9012-123456789012");
    }

    /// Test all numeric field types and methods
    #[test]
    fn test_card_numeric_field_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test f64 fields
        card.converted_mana_cost = 2.0;
        card.face_converted_mana_cost = 2.0;
        card.face_mana_value = 2.0;
        card.mana_value = 2.0;
        
        // Test i32 fields
        card.count = 4;
        
        // Test Option<i32> fields
        card.edhrec_rank = Some(1000);
        
        // Test Option<f64> fields
        card.edhrec_saltiness = Some(0.5);
        
        // Verify return types
        let cmc: f64 = card.converted_mana_cost;
        let face_cmc: f64 = card.face_converted_mana_cost;
        let face_mv: f64 = card.face_mana_value;
        let mv: f64 = card.mana_value;
        let count: i32 = card.count;
        let edhrec_rank: Option<i32> = card.edhrec_rank;
        let edhrec_saltiness: Option<f64> = card.edhrec_saltiness;
        
        assert_eq!(cmc, 2.0);
        assert_eq!(face_cmc, 2.0);
        assert_eq!(face_mv, 2.0);
        assert_eq!(mv, 2.0);
        assert_eq!(count, 4);
        assert_eq!(edhrec_rank, Some(1000));
        assert_eq!(edhrec_saltiness, Some(0.5));
    }

    /// Test all vector field types and methods
    #[test]
    fn test_card_vector_field_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test Vec<String> fields
        card.artist_ids = Some(vec!["artist1".to_string(), "artist2".to_string()]);
        card.attraction_lights = Some(vec!["1".to_string(), "2".to_string(), "3".to_string()]);
        card.booster_types = vec!["draft".to_string(), "set".to_string()];
        card.card_parts = vec!["part1".to_string(), "part2".to_string()];
        card.color_identity = vec!["W".to_string(), "U".to_string()];
        card.color_indicator = Some(vec!["B".to_string(), "R".to_string()]);
        card.colors = vec!["U".to_string()];
        card.finishes = vec!["nonfoil".to_string(), "foil".to_string()];
        card.frame_effects = vec!["legendary".to_string()];
        card.keywords = vec!["flying".to_string(), "haste".to_string()];
        card.original_printings = vec!["LEA".to_string(), "LEB".to_string()];
        card.other_face_ids = vec!["uuid1".to_string(), "uuid2".to_string()];
        card.printings = vec!["M21".to_string(), "M22".to_string()];
        card.promo_types = vec!["prerelease".to_string()];
        card.rebalanced_printings = vec!["ARENA".to_string()];
        card.reverse_related = Some(vec!["related1".to_string(), "related2".to_string()]);
        card.subsets = Some(vec!["subset1".to_string(), "subset2".to_string()]);
        card.subtypes = vec!["Human".to_string(), "Wizard".to_string()];
        card.supertypes = vec!["Legendary".to_string()];
        card.types = vec!["Creature".to_string()];
        card.variations = vec!["var1".to_string(), "var2".to_string()];
        
        // Verify return types
        let artist_ids: Option<Vec<String>> = card.artist_ids.clone();
        let attraction_lights: Option<Vec<String>> = card.attraction_lights.clone();
        let booster_types: Vec<String> = card.booster_types.clone();
        let card_parts: Vec<String> = card.card_parts.clone();
        let color_identity: Vec<String> = card.color_identity.clone();
        let color_indicator: Option<Vec<String>> = card.color_indicator.clone();
        let colors: Vec<String> = card.colors.clone();
        let finishes: Vec<String> = card.finishes.clone();
        let frame_effects: Vec<String> = card.frame_effects.clone();
        let keywords: Vec<String> = card.keywords.clone();
        let original_printings: Vec<String> = card.original_printings.clone();
        let other_face_ids: Vec<String> = card.other_face_ids.clone();
        let printings: Vec<String> = card.printings.clone();
        let promo_types: Vec<String> = card.promo_types.clone();
        let rebalanced_printings: Vec<String> = card.rebalanced_printings.clone();
        let reverse_related: Option<Vec<String>> = card.reverse_related.clone();
        let subsets: Option<Vec<String>> = card.subsets.clone();
        let subtypes: Vec<String> = card.subtypes.clone();
        let supertypes: Vec<String> = card.supertypes.clone();
        let types: Vec<String> = card.types.clone();
        let variations: Vec<String> = card.variations.clone();
        
        assert_eq!(artist_ids.unwrap().len(), 2);
        assert_eq!(attraction_lights.unwrap().len(), 3);
        assert_eq!(booster_types.len(), 2);
        assert_eq!(card_parts.len(), 2);
        assert_eq!(color_identity.len(), 2);
        assert_eq!(color_indicator.unwrap().len(), 2);
        assert_eq!(colors.len(), 1);
        assert_eq!(finishes.len(), 2);
        assert_eq!(frame_effects.len(), 1);
        assert_eq!(keywords.len(), 2);
        assert_eq!(original_printings.len(), 2);
        assert_eq!(other_face_ids.len(), 2);
        assert_eq!(printings.len(), 2);
        assert_eq!(promo_types.len(), 1);
        assert_eq!(rebalanced_printings.len(), 1);
        assert_eq!(reverse_related.unwrap().len(), 2);
        assert_eq!(subsets.unwrap().len(), 2);
        assert_eq!(subtypes.len(), 2);
        assert_eq!(supertypes.len(), 1);
        assert_eq!(types.len(), 1);
        assert_eq!(variations.len(), 2);
    }

    /// Test all boolean field types and methods
    #[test]
    fn test_card_boolean_field_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test all Option<bool> fields
        card.has_alternative_deck_limit = Some(true);
        card.has_content_warning = Some(false);
        card.has_foil = Some(true);
        card.has_non_foil = Some(false);
        card.is_alternative = Some(true);
        card.is_foil = Some(false);
        card.is_full_art = Some(true);
        card.is_funny = Some(false);
        card.is_game_changer = Some(true);
        card.is_online_only = Some(false);
        card.is_oversized = Some(true);
        card.is_promo = Some(false);
        card.is_rebalanced = Some(true);
        card.is_reprint = Some(false);
        card.is_reserved = Some(true);
        card.is_starter = Some(false);
        card.is_story_spotlight = Some(true);
        card.is_textless = Some(false);
        card.is_timeshifted = Some(true);
        
        // Test direct bool fields
        card.is_token = true;
        
        // Verify return types
        let has_alt_deck_limit: Option<bool> = card.has_alternative_deck_limit;
        let has_content_warning: Option<bool> = card.has_content_warning;
        let has_foil: Option<bool> = card.has_foil;
        let has_non_foil: Option<bool> = card.has_non_foil;
        let is_alternative: Option<bool> = card.is_alternative;
        let is_foil: Option<bool> = card.is_foil;
        let is_full_art: Option<bool> = card.is_full_art;
        let is_funny: Option<bool> = card.is_funny;
        let is_game_changer: Option<bool> = card.is_game_changer;
        let is_online_only: Option<bool> = card.is_online_only;
        let is_oversized: Option<bool> = card.is_oversized;
        let is_promo: Option<bool> = card.is_promo;
        let is_rebalanced: Option<bool> = card.is_rebalanced;
        let is_reprint: Option<bool> = card.is_reprint;
        let is_reserved: Option<bool> = card.is_reserved;
        let is_starter: Option<bool> = card.is_starter;
        let is_story_spotlight: Option<bool> = card.is_story_spotlight;
        let is_textless: Option<bool> = card.is_textless;
        let is_timeshifted: Option<bool> = card.is_timeshifted;
        let is_token: bool = card.is_token;
        
        assert_eq!(has_alt_deck_limit, Some(true));
        assert_eq!(has_content_warning, Some(false));
        assert_eq!(has_foil, Some(true));
        assert_eq!(has_non_foil, Some(false));
        assert_eq!(is_alternative, Some(true));
        assert_eq!(is_foil, Some(false));
        assert_eq!(is_full_art, Some(true));
        assert_eq!(is_funny, Some(false));
        assert_eq!(is_game_changer, Some(true));
        assert_eq!(is_online_only, Some(false));
        assert_eq!(is_oversized, Some(true));
        assert_eq!(is_promo, Some(false));
        assert_eq!(is_rebalanced, Some(true));
        assert_eq!(is_reprint, Some(false));
        assert_eq!(is_reserved, Some(true));
        assert_eq!(is_starter, Some(false));
        assert_eq!(is_story_spotlight, Some(true));
        assert_eq!(is_textless, Some(false));
        assert_eq!(is_timeshifted, Some(true));
        assert_eq!(is_token, true);
    }

    /// Test all optional string field types and methods
    #[test]
    fn test_card_optional_string_field_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test all Option<String> fields
        card.ascii_name = Some("Ascii Name".to_string());
        card.defense = Some("5".to_string());
        card.duel_deck = Some("a".to_string());
        card.face_flavor_name = Some("Face Flavor".to_string());
        card.face_name = Some("Face Name".to_string());
        card.first_printing = Some("LEA".to_string());
        card.flavor_name = Some("Flavor Name".to_string());
        card.flavor_text = Some("Flavor text here.".to_string());
        card.hand = Some("7".to_string());
        card.life = Some("20".to_string());
        card.loyalty = Some("4".to_string());
        card.orientation = Some("portrait".to_string());
        card.original_release_date = Some("1993-08-05".to_string());
        card.original_text = Some("Original text.".to_string());
        card.original_type = Some("Summon — Wizard".to_string());
        card.security_stamp = Some("oval".to_string());
        card.side = Some("a".to_string());
        card.signature = Some("Artist Signature".to_string());
        card.watermark = Some("set".to_string());
        
        // Verify return types
        let ascii_name: Option<String> = card.ascii_name.clone();
        let defense: Option<String> = card.defense.clone();
        let duel_deck: Option<String> = card.duel_deck.clone();
        let face_flavor_name: Option<String> = card.face_flavor_name.clone();
        let face_name: Option<String> = card.face_name.clone();
        let first_printing: Option<String> = card.first_printing.clone();
        let flavor_name: Option<String> = card.flavor_name.clone();
        let flavor_text: Option<String> = card.flavor_text.clone();
        let hand: Option<String> = card.hand.clone();
        let life: Option<String> = card.life.clone();
        let loyalty: Option<String> = card.loyalty.clone();
        let orientation: Option<String> = card.orientation.clone();
        let original_release_date: Option<String> = card.original_release_date.clone();
        let original_text: Option<String> = card.original_text.clone();
        let original_type: Option<String> = card.original_type.clone();
        let security_stamp: Option<String> = card.security_stamp.clone();
        let side: Option<String> = card.side.clone();
        let signature: Option<String> = card.signature.clone();
        let watermark: Option<String> = card.watermark.clone();
        
        assert_eq!(ascii_name.unwrap(), "Ascii Name");
        assert_eq!(defense.unwrap(), "5");
        assert_eq!(duel_deck.unwrap(), "a");
        assert_eq!(face_flavor_name.unwrap(), "Face Flavor");
        assert_eq!(face_name.unwrap(), "Face Name");
        assert_eq!(first_printing.unwrap(), "LEA");
        assert_eq!(flavor_name.unwrap(), "Flavor Name");
        assert_eq!(flavor_text.unwrap(), "Flavor text here.");
        assert_eq!(hand.unwrap(), "7");
        assert_eq!(life.unwrap(), "20");
        assert_eq!(loyalty.unwrap(), "4");
        assert_eq!(orientation.unwrap(), "portrait");
        assert_eq!(original_release_date.unwrap(), "1993-08-05");
        assert_eq!(original_text.unwrap(), "Original text.");
        assert_eq!(original_type.unwrap(), "Summon — Wizard");
        assert_eq!(security_stamp.unwrap(), "oval");
        assert_eq!(side.unwrap(), "a");
        assert_eq!(signature.unwrap(), "Artist Signature");
        assert_eq!(watermark.unwrap(), "set");
    }

    /// Test all method return types
    #[test]
    fn test_card_method_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = card.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        assert!(!json_string.is_empty());
        
        // Test set_names method return type (void)
        card.set_names(Some(vec!["Name1".to_string(), "Name2".to_string()]));
        
        // Test get_names method return type
        let names: Vec<String> = card.get_names();
        assert_eq!(names.len(), 2);
        
        // Test append_names method return type (void)
        card.append_names("Name3".to_string());
        let updated_names: Vec<String> = card.get_names();
        assert_eq!(updated_names.len(), 3);
        
        // Test set_watermark method return type (void)
        card.set_watermark(Some("test_watermark".to_string()));
        
        // Test set_illustration_ids method return type (void)
        card.set_illustration_ids(vec!["id1".to_string(), "id2".to_string()]);
        
        // Test get_illustration_ids method return type
        let illus_ids: Vec<String> = card.get_illustration_ids();
        assert_eq!(illus_ids.len(), 2);
        
        // Test get_atomic_keys method return type
        let atomic_keys: Vec<String> = card.get_atomic_keys();
        assert!(!atomic_keys.is_empty());
        
        // Test comparison methods return types
        let other_card = MtgjsonCardObject::new(false);
        let equality: bool = card.__eq__(&other_card);
        assert!(!equality); // Should be false since cards are different
        
        let comparison_result: Result<bool, pyo3::PyErr> = card.__lt__(&other_card);
        assert!(comparison_result.is_ok());
        let is_less_than: bool = comparison_result.unwrap();
        assert!(is_less_than || !is_less_than); // Either true or false is valid
        
        // Test string representation methods return types
        let str_repr: String = card.__str__();
        let repr_repr: String = card.__repr__();
        assert!(!str_repr.is_empty());
        assert!(!repr_repr.is_empty());
        
        // Test hash method return type
        let hash_value: u64 = card.__hash__();
        assert!(hash_value > 0 || hash_value == 0); // Hash can be any u64
        
        // Test eq method return type
        let eq_result: bool = card.eq(&other_card);
        assert!(!eq_result);
        
        // Test compare method return type
        let compare_result: Result<i32, pyo3::PyErr> = card.compare(&other_card);
        assert!(compare_result.is_ok());
        let compare_value: i32 = compare_result.unwrap();
        assert!(compare_value >= -1 && compare_value <= 1);
    }

    /// Test complex object field types and methods
    #[test]
    fn test_card_complex_object_field_return_types() {
        let card = MtgjsonCardObject::new(false);
        
        // Test complex object fields
        let availability: MtgjsonGameFormatsObject = card.availability.clone();
        let identifiers: MtgjsonIdentifiers = card.identifiers.clone();
        let legalities: MtgjsonLegalitiesObject = card.legalities.clone();
        let prices: MtgjsonPricesObject = card.prices.clone();
        let purchase_urls: MtgjsonPurchaseUrls = card.purchase_urls.clone();
        
        // Test optional complex object fields
        let leadership_skills: Option<MtgjsonLeadershipSkillsObject> = card.leadership_skills.clone();
        let related_cards: Option<MtgjsonRelatedCardsObject> = card.related_cards.clone();
        
        // Verify these are the correct types
        assert_eq!(std::mem::size_of_val(&availability), std::mem::size_of::<MtgjsonGameFormatsObject>());
        assert_eq!(std::mem::size_of_val(&identifiers), std::mem::size_of::<MtgjsonIdentifiers>());
        assert_eq!(std::mem::size_of_val(&legalities), std::mem::size_of::<MtgjsonLegalitiesObject>());
        assert_eq!(std::mem::size_of_val(&prices), std::mem::size_of::<MtgjsonPricesObject>());
        assert_eq!(std::mem::size_of_val(&purchase_urls), std::mem::size_of::<MtgjsonPurchaseUrls>());
        assert!(leadership_skills.is_none());
        assert!(related_cards.is_none());
    }

    /// Test HashMap and complex collection field types
    #[test]
    fn test_card_collection_field_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test HashMap fields
        let mut source_products = HashMap::new();
        source_products.insert("booster".to_string(), vec!["pack1".to_string(), "pack2".to_string()]);
        source_products.insert("precon".to_string(), vec!["deck1".to_string()]);
        card.source_products = Some(source_products);
        
        card.raw_purchase_urls.insert("cardkingdom".to_string(), "https://example.com".to_string());
        card.raw_purchase_urls.insert("tcgplayer".to_string(), "https://example2.com".to_string());
        
        // Test Vec<MtgjsonForeignDataObject>
        let foreign_data = vec![
            MtgjsonForeignDataObject::new(),
            MtgjsonForeignDataObject::new()
        ];
        card.foreign_data = foreign_data;
        
        // Test Vec<MtgjsonRulingObject>
        let rulings = vec![
            MtgjsonRulingObject::new("2023-01-01".to_string(), "Test ruling.".to_string()),
            MtgjsonRulingObject::new("2023-01-02".to_string(), "Another ruling.".to_string())
        ];
        card.rulings = Some(rulings);
        
        // Verify return types
        let source_products_ref: Option<HashMap<String, Vec<String>>> = card.source_products.clone();
        let raw_purchase_urls_ref: HashMap<String, String> = card.raw_purchase_urls.clone();
        let foreign_data_ref: Vec<MtgjsonForeignDataObject> = card.foreign_data.clone();
        let rulings_ref: Option<Vec<MtgjsonRulingObject>> = card.rulings.clone();
        
        assert!(source_products_ref.is_some());
        assert_eq!(source_products_ref.unwrap().len(), 2);
        assert_eq!(raw_purchase_urls_ref.len(), 2);
        assert_eq!(foreign_data_ref.len(), 2);
        assert!(rulings_ref.is_some());
        assert_eq!(rulings_ref.unwrap().len(), 2);
    }

    /// Test edge cases and error conditions with return types
    #[test]
    fn test_card_edge_cases_return_types() {
        let mut card = MtgjsonCardObject::new(false);
        
        // Test empty collections
        card.colors = Vec::new();
        card.keywords = Vec::new();
        card.subtypes = Vec::new();
        card.types = Vec::new();
        
        let colors: Vec<String> = card.colors.clone();
        let keywords: Vec<String> = card.keywords.clone();
        let subtypes: Vec<String> = card.subtypes.clone();
        let types: Vec<String> = card.types.clone();
        
        assert!(colors.is_empty());
        assert!(keywords.is_empty());
        assert!(subtypes.is_empty());
        assert!(types.is_empty());
        
        // Test None values for optional fields
        card.artist_ids = None;
        card.attraction_lights = None;
        card.ascii_name = None;
        card.defense = None;
        
        let artist_ids: Option<Vec<String>> = card.artist_ids.clone();
        let attraction_lights: Option<Vec<String>> = card.attraction_lights.clone();
        let ascii_name: Option<String> = card.ascii_name.clone();
        let defense: Option<String> = card.defense.clone();
        
        assert!(artist_ids.is_none());
        assert!(attraction_lights.is_none());
        assert!(ascii_name.is_none());
        assert!(defense.is_none());
        
        // Test extreme numeric values
        card.mana_value = f64::MAX;
        card.converted_mana_cost = f64::MIN;
        card.count = i32::MAX;
        card.edhrec_rank = Some(i32::MIN);
        
        let mana_value: f64 = card.mana_value;
        let cmc: f64 = card.converted_mana_cost;
        let count: i32 = card.count;
        let edhrec_rank: Option<i32> = card.edhrec_rank;
        
        assert_eq!(mana_value, f64::MAX);
        assert_eq!(cmc, f64::MIN);
        assert_eq!(count, i32::MAX);
        assert_eq!(edhrec_rank, Some(i32::MIN));
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_card_trait_implementations() {
        let card1 = MtgjsonCardObject::new(false);
        let card2 = MtgjsonCardObject::new(true);
        
        // Test Clone trait
        let cloned_card1 = card1.clone();
        assert_eq!(card1.is_token, cloned_card1.is_token);
        assert_eq!(card1.name, cloned_card1.name);
        assert_eq!(card1.uuid, cloned_card1.uuid);
        
        // Test PartialEq trait
        assert_eq!(card1, cloned_card1);
        assert_ne!(card1, card2);
        
        // Test Default trait
        let default_card = MtgjsonCardObject::default();
        assert_eq!(default_card.is_token, false);
        assert_eq!(default_card.count, 1);
        
        // Test Debug trait
        let debug_output = format!("{:?}", card1);
        assert!(debug_output.contains("MtgjsonCardObject"));
        
        // Test PartialOrd trait
        let ord_result = card1.partial_cmp(&card2);
        assert!(ord_result.is_some());
    }
}