use mtgjson_rust::builders::*;
use std::collections::HashMap;

mod comprehensive_output_generator_tests {
    use super::*;
    use mtgjson_rust::builders::output_generator::OutputGenerator;

    /// Test all OutputGenerator constructors and return types
    #[test]
    fn test_output_generator_constructors_return_types() {
        // Test constructor with no parameters
        let default_generator = OutputGenerator::new(None, None);
        assert_eq!(default_generator.output_path, "./output");
        assert_eq!(default_generator.pretty_print, true);
        assert_eq!(default_generator.output_version, "5.0.0");
        assert!(default_generator.output_date.is_empty());
        assert!(default_generator.output_files.is_empty());
        assert_eq!(default_generator.compression_enabled, true);
        
        // Test constructor with custom parameters
        let custom_generator = OutputGenerator::new(
            Some("/custom/path".to_string()),
            Some(false),
        );
        assert_eq!(custom_generator.output_path, "/custom/path");
        assert_eq!(custom_generator.pretty_print, false);
        assert_eq!(custom_generator.output_version, "5.0.0");
        assert!(custom_generator.output_date.is_empty());
        assert!(custom_generator.output_files.is_empty());
        assert_eq!(custom_generator.compression_enabled, true);
        
        // Verify return types
        let output_path: String = default_generator.output_path.clone();
        let pretty_print: bool = default_generator.pretty_print;
        let output_version: String = default_generator.output_version.clone();
        let output_date: String = default_generator.output_date.clone();
        let output_files: Vec<String> = default_generator.output_files.clone();
        let compression_enabled: bool = default_generator.compression_enabled;
        
        assert_eq!(output_path, "./output");
        assert_eq!(pretty_print, true);
        assert_eq!(output_version, "5.0.0");
        assert!(output_date.is_empty());
        assert!(output_files.is_empty());
        assert_eq!(compression_enabled, true);
    }

    /// Test all setter methods and their return types
    #[test]
    fn test_output_generator_setter_return_types() {
        let mut generator = OutputGenerator::new(None, None);
        
        // Test set_output_version method (void return)
        generator.set_output_version("6.0.0".to_string());
        assert_eq!(generator.output_version, "6.0.0");
        
        // Test set_output_date method (void return)
        generator.set_output_date("2023-01-01".to_string());
        assert_eq!(generator.output_date, "2023-01-01");
        
        // Test enable_compression method (void return)
        generator.enable_compression(false);
        assert_eq!(generator.compression_enabled, false);
        
        generator.enable_compression(true);
        assert_eq!(generator.compression_enabled, true);
        
        // Test add_output_file method (void return)
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());
        assert_eq!(generator.output_files.len(), 2);
        assert!(generator.output_files.contains(&"AllCards.json".to_string()));
        assert!(generator.output_files.contains(&"AllSets.json".to_string()));
        
        // Test remove_output_file method (void return)
        generator.remove_output_file("AllCards.json".to_string());
        assert_eq!(generator.output_files.len(), 1);
        assert!(!generator.output_files.contains(&"AllCards.json".to_string()));
        assert!(generator.output_files.contains(&"AllSets.json".to_string()));
        
        // Test clear_output_files method (void return)
        generator.clear_output_files();
        assert_eq!(generator.output_files.len(), 0);
        assert!(generator.output_files.is_empty());
        
        // Verify return types of setters through property access
        let version_after_set: String = generator.output_version.clone();
        let date_after_set: String = generator.output_date.clone();
        let compression_after_set: bool = generator.compression_enabled;
        let files_after_clear: Vec<String> = generator.output_files.clone();
        
        assert_eq!(version_after_set, "6.0.0");
        assert_eq!(date_after_set, "2023-01-01");
        assert_eq!(compression_after_set, true);
        assert!(files_after_clear.is_empty());
    }

    /// Test all getter methods and their return types
    #[test]
    fn test_output_generator_getter_return_types() {
        let mut generator = OutputGenerator::new(
            Some("/test/path".to_string()),
            Some(false),
        );
        generator.set_output_version("7.0.0".to_string());
        generator.set_output_date("2023-12-31".to_string());
        generator.enable_compression(false);
        generator.add_output_file("TestFile.json".to_string());
        
        // Test all getter methods and their return types
        let output_path: String = generator.get_output_path();
        let pretty_print: bool = generator.get_pretty_print();
        let output_version: String = generator.get_output_version();
        let output_date: String = generator.get_output_date();
        let output_files: Vec<String> = generator.get_output_files();
        let compression_enabled: bool = generator.get_compression_enabled();
        
        assert_eq!(output_path, "/test/path");
        assert_eq!(pretty_print, false);
        assert_eq!(output_version, "7.0.0");
        assert_eq!(output_date, "2023-12-31");
        assert_eq!(output_files.len(), 1);
        assert_eq!(output_files[0], "TestFile.json");
        assert_eq!(compression_enabled, false);
    }

    /// Test all property setter methods and their return types
    #[test]
    fn test_output_generator_property_setter_return_types() {
        let mut generator = OutputGenerator::new(None, None);
        
        // Test set_output_path method (void return)
        generator.set_output_path("/new/path".to_string());
        let new_path: String = generator.get_output_path();
        assert_eq!(new_path, "/new/path");
        
        // Test set_pretty_print method (void return)
        generator.set_pretty_print(false);
        let new_pretty_print: bool = generator.get_pretty_print();
        assert_eq!(new_pretty_print, false);
        
        generator.set_pretty_print(true);
        let pretty_print_again: bool = generator.get_pretty_print();
        assert_eq!(pretty_print_again, true);
    }

    /// Test all generation methods and their return types
    #[test]
    fn test_output_generator_generation_method_return_types() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("8.0.0".to_string());
        generator.set_output_date("2023-06-15".to_string());
        
        // Add some files to generate
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());
        generator.add_output_file("AtomicCards.json".to_string());
        generator.add_output_file("DeckList.json".to_string());
        generator.add_output_file("SetList.json".to_string());
        generator.add_output_file("Keywords.json".to_string());
        generator.add_output_file("CardTypes.json".to_string());
        generator.add_output_file("UnknownFile.json".to_string());
        
        // Test generate_output_files method return type
        let output_result: Result<HashMap<String, String>, pyo3::PyErr> = generator.generate_output_files();
        assert!(output_result.is_ok());
        let output_map: HashMap<String, String> = output_result.unwrap();
        
        // Verify all files are present in the output
        assert_eq!(output_map.len(), 8);
        assert!(output_map.contains_key("AllCards.json"));
        assert!(output_map.contains_key("AllSets.json"));
        assert!(output_map.contains_key("AtomicCards.json"));
        assert!(output_map.contains_key("DeckList.json"));
        assert!(output_map.contains_key("SetList.json"));
        assert!(output_map.contains_key("Keywords.json"));
        assert!(output_map.contains_key("CardTypes.json"));
        assert!(output_map.contains_key("UnknownFile.json"));
        
        // Test individual generation methods return types
        let all_cards_result: Result<String, pyo3::PyErr> = generator.generate_all_cards();
        let all_sets_result: Result<String, pyo3::PyErr> = generator.generate_all_sets();
        let atomic_cards_result: Result<String, pyo3::PyErr> = generator.generate_atomic_cards();
        let deck_list_result: Result<String, pyo3::PyErr> = generator.generate_deck_list();
        let set_list_result: Result<String, pyo3::PyErr> = generator.generate_set_list();
        let keywords_result: Result<String, pyo3::PyErr> = generator.generate_keywords();
        let card_types_result: Result<String, pyo3::PyErr> = generator.generate_card_types();
        
        assert!(all_cards_result.is_ok());
        assert!(all_sets_result.is_ok());
        assert!(atomic_cards_result.is_ok());
        assert!(deck_list_result.is_ok());
        assert!(set_list_result.is_ok());
        assert!(keywords_result.is_ok());
        assert!(card_types_result.is_ok());
        
        let all_cards_json: String = all_cards_result.unwrap();
        let all_sets_json: String = all_sets_result.unwrap();
        let atomic_cards_json: String = atomic_cards_result.unwrap();
        let deck_list_json: String = deck_list_result.unwrap();
        let set_list_json: String = set_list_result.unwrap();
        let keywords_json: String = keywords_result.unwrap();
        let card_types_json: String = card_types_result.unwrap();
        
        // Verify all JSON strings are non-empty and contain expected structure
        assert!(!all_cards_json.is_empty());
        assert!(!all_sets_json.is_empty());
        assert!(!atomic_cards_json.is_empty());
        assert!(!deck_list_json.is_empty());
        assert!(!set_list_json.is_empty());
        assert!(!keywords_json.is_empty());
        assert!(!card_types_json.is_empty());
        
        assert!(all_cards_json.contains("meta"));
        assert!(all_sets_json.contains("data"));
        assert!(atomic_cards_json.contains("8.0.0"));
        assert!(deck_list_json.contains("2023-06-15"));
    }

    /// Test meta generation method return type
    #[test]
    fn test_output_generator_meta_generation_return_type() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("9.0.0".to_string());
        generator.set_output_date("2023-09-30".to_string());
        
        // Test generate_meta_object method return type
        let meta_string: String = generator.generate_meta_object();
        assert!(!meta_string.is_empty());
        assert!(meta_string.contains("9.0.0"));
        assert!(meta_string.contains("2023-09-30"));
        assert!(meta_string.contains("version"));
        assert!(meta_string.contains("date"));
    }

    /// Test compression method return types
    #[test]
    fn test_output_generator_compression_method_return_types() {
        let generator = OutputGenerator::new(None, None);
        
        // Test compress_output method return type
        let compress_result: Result<String, pyo3::PyErr> = generator.compress_output("test_file.json".to_string());
        assert!(compress_result.is_ok());
        let compressed_filename: String = compress_result.unwrap();
        assert_eq!(compressed_filename, "test_file.json.gz");
        
        // Test with different file extensions
        let compress_result2: Result<String, pyo3::PyErr> = generator.compress_output("another_file.txt".to_string());
        assert!(compress_result2.is_ok());
        let compressed_filename2: String = compress_result2.unwrap();
        assert_eq!(compressed_filename2, "another_file.txt.gz");
    }

    /// Test string representation methods return types
    #[test]
    fn test_output_generator_string_representation_return_types() {
        let mut generator = OutputGenerator::new(
            Some("/custom/output".to_string()),
            Some(true),
        );
        generator.set_output_version("10.0.0".to_string());
        
        // Test __str__ method return type
        let str_repr: String = generator.__str__();
        assert!(!str_repr.is_empty());
        assert!(str_repr.contains("/custom/output"));
        assert!(str_repr.contains("10.0.0"));
        
        // Test __repr__ method return type
        let repr_repr: String = generator.__repr__();
        assert!(!repr_repr.is_empty());
        assert!(repr_repr.contains("OutputGenerator"));
        assert!(repr_repr.contains("/custom/output"));
        assert!(repr_repr.contains("true"));
        assert!(repr_repr.contains("10.0.0"));
    }

    /// Test comparison methods return types
    #[test]
    fn test_output_generator_comparison_return_types() {
        let mut generator1 = OutputGenerator::new(
            Some("/path1".to_string()),
            Some(true),
        );
        generator1.set_output_version("1.0.0".to_string());
        generator1.set_output_date("2023-01-01".to_string());
        
        let mut generator2 = OutputGenerator::new(
            Some("/path2".to_string()),
            Some(false),
        );
        generator2.set_output_version("2.0.0".to_string());
        generator2.set_output_date("2023-02-01".to_string());
        
        let mut generator3 = OutputGenerator::new(
            Some("/path1".to_string()),
            Some(true),
        );
        generator3.set_output_version("1.0.0".to_string());
        generator3.set_output_date("2023-01-01".to_string());
        
        // Test __eq__ method return type
        let eq_result_false: bool = generator1.__eq__(&generator2);
        let eq_result_true: bool = generator1.__eq__(&generator3);
        assert!(!eq_result_false);
        assert!(eq_result_true);
        
        // Test __hash__ method return type
        let hash1: u64 = generator1.__hash__();
        let hash2: u64 = generator2.__hash__();
        let hash3: u64 = generator3.__hash__();
        
        assert_eq!(hash1, hash3); // Same values should have same hash
        assert_ne!(hash1, hash2); // Different values should have different hash (usually)
    }

    /// Test JSON serialization method return types
    #[test]
    fn test_output_generator_json_serialization_return_types() {
        let mut generator = OutputGenerator::new(
            Some("/json/test".to_string()),
            Some(false),
        );
        generator.set_output_version("11.0.0".to_string());
        generator.set_output_date("2023-11-11".to_string());
        generator.enable_compression(false);
        generator.add_output_file("JsonTest.json".to_string());
        
        // Test to_json method return type
        let json_result: Result<String, pyo3::PyErr> = generator.to_json();
        assert!(json_result.is_ok());
        let json_string: String = json_result.unwrap();
        
        assert!(!json_string.is_empty());
        assert!(json_string.contains("/json/test"));
        assert!(json_string.contains("false"));
        assert!(json_string.contains("11.0.0"));
        assert!(json_string.contains("2023-11-11"));
        assert!(json_string.contains("JsonTest.json"));
    }

    /// Test edge cases and error conditions return types
    #[test]
    fn test_output_generator_edge_cases_return_types() {
        // Test with empty paths and values
        let empty_generator = OutputGenerator::new(
            Some("".to_string()),
            Some(true),
        );
        
        let empty_path: String = empty_generator.get_output_path();
        let empty_version: String = empty_generator.get_output_version();
        let empty_date: String = empty_generator.get_output_date();
        let empty_files: Vec<String> = empty_generator.get_output_files();
        
        assert_eq!(empty_path, "");
        assert_eq!(empty_version, "5.0.0");
        assert!(empty_date.is_empty());
        assert!(empty_files.is_empty());
        
        // Test with very long strings
        let long_path = "a".repeat(10000);
        let long_version = "b".repeat(1000);
        let long_date = "c".repeat(500);
        
        let mut long_generator = OutputGenerator::new(
            Some(long_path.clone()),
            Some(false),
        );
        long_generator.set_output_version(long_version.clone());
        long_generator.set_output_date(long_date.clone());
        
        let returned_long_path: String = long_generator.get_output_path();
        let returned_long_version: String = long_generator.get_output_version();
        let returned_long_date: String = long_generator.get_output_date();
        
        assert_eq!(returned_long_path.len(), 10000);
        assert_eq!(returned_long_version.len(), 1000);
        assert_eq!(returned_long_date.len(), 500);
        assert_eq!(returned_long_path, long_path);
        assert_eq!(returned_long_version, long_version);
        assert_eq!(returned_long_date, long_date);
        
        // Test with unicode strings
        let unicode_path = "🔥/📁/✨".to_string();
        let unicode_version = "🚀1.0.0".to_string();
        let unicode_date = "📅2023-01-01".to_string();
        
        let mut unicode_generator = OutputGenerator::new(
            Some(unicode_path.clone()),
            Some(true),
        );
        unicode_generator.set_output_version(unicode_version.clone());
        unicode_generator.set_output_date(unicode_date.clone());
        
        let returned_unicode_path: String = unicode_generator.get_output_path();
        let returned_unicode_version: String = unicode_generator.get_output_version();
        let returned_unicode_date: String = unicode_generator.get_output_date();
        
        assert_eq!(returned_unicode_path, unicode_path);
        assert_eq!(returned_unicode_version, unicode_version);
        assert_eq!(returned_unicode_date, unicode_date);
    }

    /// Test comprehensive trait implementations
    #[test]
    fn test_output_generator_trait_implementations() {
        let mut generator1 = OutputGenerator::new(
            Some("/trait/test".to_string()),
            Some(true),
        );
        generator1.set_output_version("trait_v1.0.0".to_string());
        generator1.set_output_date("2023-trait-01".to_string());
        generator1.add_output_file("TraitFile.json".to_string());
        
        let generator2 = OutputGenerator::new(
            Some("/different/path".to_string()),
            Some(false),
        );
        
        // Test Clone trait
        let cloned_generator1 = generator1.clone();
        assert_eq!(generator1.output_path, cloned_generator1.output_path);
        assert_eq!(generator1.pretty_print, cloned_generator1.pretty_print);
        assert_eq!(generator1.output_version, cloned_generator1.output_version);
        assert_eq!(generator1.output_date, cloned_generator1.output_date);
        assert_eq!(generator1.output_files, cloned_generator1.output_files);
        assert_eq!(generator1.compression_enabled, cloned_generator1.compression_enabled);
        
        // Test Debug trait
        let debug_output = format!("{:?}", generator1);
        assert!(debug_output.contains("OutputGenerator"));
        assert!(debug_output.contains("/trait/test"));
        assert!(debug_output.contains("trait_v1.0.0"));
        assert!(debug_output.contains("TraitFile.json"));
        
        // Test equality
        assert_eq!(generator1.__eq__(&cloned_generator1), true);
        assert_eq!(generator1.__eq__(&generator2), false);
    }

    /// Test complex integration scenarios
    #[test]
    fn test_output_generator_complex_integration_return_types() {
        // Create a comprehensive generator scenario
        let mut comprehensive_generator = OutputGenerator::new(
            Some("/comprehensive/test/output".to_string()),
            Some(true),
        );
        comprehensive_generator.set_output_version("comprehensive_v12.5.7".to_string());
        comprehensive_generator.set_output_date("2023-comprehensive-test".to_string());
        comprehensive_generator.enable_compression(true);
        
        // Add all possible output files
        let all_files = vec![
            "AllCards.json",
            "AllSets.json", 
            "AtomicCards.json",
            "DeckList.json",
            "SetList.json",
            "Keywords.json",
            "CardTypes.json",
            "CustomFile1.json",
            "CustomFile2.json",
            "SpecialOutput.json"
        ];
        
        for file in &all_files {
            comprehensive_generator.add_output_file(file.to_string());
        }
        
        // Test comprehensive generation
        let comprehensive_output: Result<HashMap<String, String>, pyo3::PyErr> = comprehensive_generator.generate_output_files();
        assert!(comprehensive_output.is_ok());
        let comprehensive_map: HashMap<String, String> = comprehensive_output.unwrap();
        
        assert_eq!(comprehensive_map.len(), all_files.len());
        
        for file in &all_files {
            assert!(comprehensive_map.contains_key(*file));
            let content: &String = comprehensive_map.get(*file).unwrap();
            assert!(!content.is_empty());
            assert!(content.contains("meta") || content.contains("data"));
        }
        
        // Test comprehensive JSON serialization
        let comprehensive_json: Result<String, pyo3::PyErr> = comprehensive_generator.to_json();
        assert!(comprehensive_json.is_ok());
        let comprehensive_json_string: String = comprehensive_json.unwrap();
        
        assert!(comprehensive_json_string.contains("/comprehensive/test/output"));
        assert!(comprehensive_json_string.contains("comprehensive_v12.5.7"));
        assert!(comprehensive_json_string.contains("2023-comprehensive-test"));
        assert!(comprehensive_json_string.contains("true"));
        
        for file in &all_files {
            assert!(comprehensive_json_string.contains(file));
        }
        
        // Test comprehensive string representations
        let comprehensive_str: String = comprehensive_generator.__str__();
        let comprehensive_repr: String = comprehensive_generator.__repr__();
        
        assert!(comprehensive_str.contains("/comprehensive/test/output"));
        assert!(comprehensive_str.contains("comprehensive_v12.5.7"));
        assert!(comprehensive_repr.contains("OutputGenerator"));
        assert!(comprehensive_repr.contains("true"));
        
        // Test comprehensive hash and equality
        let comprehensive_hash: u64 = comprehensive_generator.__hash__();
        let comprehensive_clone = comprehensive_generator.clone();
        let comprehensive_clone_hash: u64 = comprehensive_clone.__hash__();
        let comprehensive_equality: bool = comprehensive_generator.__eq__(&comprehensive_clone);
        
        assert_eq!(comprehensive_hash, comprehensive_clone_hash);
        assert!(comprehensive_equality);
        
        // Test file manipulation operations
        comprehensive_generator.remove_output_file("CustomFile1.json".to_string());
        let files_after_removal: Vec<String> = comprehensive_generator.get_output_files();
        assert_eq!(files_after_removal.len(), all_files.len() - 1);
        assert!(!files_after_removal.contains(&"CustomFile1.json".to_string()));
        
        comprehensive_generator.clear_output_files();
        let files_after_clear: Vec<String> = comprehensive_generator.get_output_files();
        assert!(files_after_clear.is_empty());
        
        // Test generation with no files
        let empty_output: Result<HashMap<String, String>, pyo3::PyErr> = comprehensive_generator.generate_output_files();
        assert!(empty_output.is_ok());
        let empty_map: HashMap<String, String> = empty_output.unwrap();
        assert!(empty_map.is_empty());
    }

    /// Test Default trait implementation
    #[test]
    fn test_output_generator_default_trait_return_types() {
        let default_generator = OutputGenerator::default();
        
        // Verify default values match constructor defaults
        let default_path: String = default_generator.get_output_path();
        let default_pretty_print: bool = default_generator.get_pretty_print();
        let default_version: String = default_generator.get_output_version();
        let default_date: String = default_generator.get_output_date();
        let default_files: Vec<String> = default_generator.get_output_files();
        let default_compression: bool = default_generator.get_compression_enabled();
        
        assert_eq!(default_path, "./output");
        assert_eq!(default_pretty_print, true);
        assert_eq!(default_version, "5.0.0");
        assert!(default_date.is_empty());
        assert!(default_files.is_empty());
        assert_eq!(default_compression, true);
    }
}