// MTGJSON output generator - High performance file writing and JSON processing
use pyo3::prelude::*;

use serde_json;
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::classes::meta::MtgjsonMetaObject;
use crate::compiled_classes::*;

#[pyclass(name = "OutputGenerator")]
#[derive(Debug, Clone)]
pub struct OutputGenerator {
    pub output_path: String,
    pub pretty_print: bool,
    pub output_version: String,
    pub output_date: String,
    pub output_files: Vec<String>,
    pub compression_enabled: bool,
}

#[pymethods]
impl OutputGenerator {
    #[new]
    #[pyo3(signature = (output_path=None, pretty_print=None))]
    pub fn new(output_path: Option<String>, pretty_print: Option<bool>) -> Self {
        Self {
            output_path: output_path.unwrap_or_else(|| "./output".to_string()),
            pretty_print: pretty_print.unwrap_or(true),
            output_version: "5.0.0".to_string(),
            output_date: String::new(),
            output_files: Vec::new(),
            compression_enabled: true,
        }
    }

    /// Set the output version
    pub fn set_output_version(&mut self, version: String) {
        self.output_version = version;
    }

    /// Set the output date
    pub fn set_output_date(&mut self, date: String) {
        self.output_date = date;
    }

    /// Enable or disable compression
    pub fn enable_compression(&mut self, enabled: bool) {
        self.compression_enabled = enabled;
    }

    /// Add an output file to the list
    pub fn add_output_file(&mut self, filename: String) {
        if !self.output_files.contains(&filename) {
            self.output_files.push(filename);
        }
    }

    /// Remove an output file from the list
    pub fn remove_output_file(&mut self, filename: String) {
        self.output_files.retain(|f| f != &filename);
    }

    /// Clear all output files
    pub fn clear_output_files(&mut self) {
        self.output_files.clear();
    }

    /// Generate output files
    pub fn generate_output_files(&self) -> PyResult<HashMap<String, String>> {
        let mut output_map = HashMap::new();

        for filename in &self.output_files {
            let content = match filename.as_str() {
                "AllCards.json" => self.generate_all_cards()?,
                "AllSets.json" => self.generate_all_sets()?,
                "AtomicCards.json" => self.generate_atomic_cards()?,
                "DeckList.json" => self.generate_deck_list()?,
                "SetList.json" => self.generate_set_list()?,
                "Keywords.json" => self.generate_keywords()?,
                "CardTypes.json" => self.generate_card_types()?,
                _ => r#"{"meta": {}, "data": {}}"#.to_string(),
            };
            output_map.insert(filename.clone(), content);
        }

        Ok(output_map)
    }

    /// Generate meta object
    pub fn generate_meta_object(&self) -> String {
        let meta = serde_json::json!({
            "version": self.output_version,
            "date": self.output_date
        });
        serde_json::to_string(&meta).unwrap_or_default()
    }

    /// Getter and setter methods for Python compatibility
    #[getter]
    pub fn get_output_path(&self) -> String {
        self.output_path.clone()
    }

    #[setter]
    pub fn set_output_path(&mut self, path: String) {
        self.output_path = path;
    }

    #[getter]
    pub fn get_pretty_print(&self) -> bool {
        self.pretty_print
    }

    #[setter]
    pub fn set_pretty_print(&mut self, pretty: bool) {
        self.pretty_print = pretty;
    }

    #[getter]
    pub fn get_output_version(&self) -> String {
        self.output_version.clone()
    }

    #[getter]
    pub fn get_output_date(&self) -> String {
        self.output_date.clone()
    }

    #[getter]
    pub fn get_output_files(&self) -> Vec<String> {
        self.output_files.clone()
    }

    #[getter]
    pub fn get_compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    /// Generate all cards JSON
    pub fn generate_all_cards(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate all sets JSON
    pub fn generate_all_sets(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate atomic cards JSON
    pub fn generate_atomic_cards(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate deck list JSON
    pub fn generate_deck_list(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate set list JSON
    pub fn generate_set_list(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate keywords JSON
    pub fn generate_keywords(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// Generate card types JSON
    pub fn generate_card_types(&self) -> PyResult<String> {
        let meta_str = self.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or_default();
        let output = serde_json::json!({
            "meta": meta,
            "data": {}
        });
        Ok(serde_json::to_string(&output).unwrap())
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!(
            "OutputGenerator(path='{}', version='{}')",
            self.output_path, self.output_version
        )
    }

    /// Repr representation
    pub fn __repr__(&self) -> String {
        format!(
            "OutputGenerator(output_path='{}', pretty_print={}, version='{}')",
            self.output_path, self.pretty_print, self.output_version
        )
    }

    /// Equality comparison
    pub fn __eq__(&self, other: &OutputGenerator) -> bool {
        self.output_path == other.output_path
            && self.output_version == other.output_version
            && self.output_date == other.output_date
    }

    /// Hash method
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.output_path.hash(&mut hasher);
        self.output_version.hash(&mut hasher);
        hasher.finish()
    }

    /// JSON serialization
    pub fn to_json(&self) -> PyResult<String> {
        let obj = serde_json::json!({
            "output_path": self.output_path,
            "pretty_print": self.pretty_print,
            "output_version": self.output_version,
            "output_date": self.output_date,
            "output_files": self.output_files,
            "compression_enabled": self.compression_enabled
        });
        Ok(serde_json::to_string(&obj).unwrap())
    }

    /// Compress output file (placeholder)
    pub fn compress_output(&self, filename: String) -> PyResult<String> {
        Ok(format!("{}.gz", filename))
    }

    /// Generate all compiled output files with high performance
    #[pyo3(signature = (pretty_print=None))]
    pub fn generate_compiled_output_files(&self, pretty_print: Option<bool>) -> PyResult<()> {
        // Create output directory
        let output_dir = Path::new(&self.output_path);
        fs::create_dir_all(output_dir).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to create output dir: {}",
                e
            ))
        })?;

        // Set pretty print from parameter or use instance default
        let use_pretty_print = pretty_print.unwrap_or(self.pretty_print);

        // Generate all major outputs in parallel-friendly order
        self.build_all_printings_files(use_pretty_print)?;
        self.generate_compiled_prices_output(use_pretty_print)?;
        self.build_compiled_list(use_pretty_print)?;
        self.build_keywords(use_pretty_print)?;
        self.build_card_types(use_pretty_print)?;
        self.build_meta(use_pretty_print)?;
        self.build_set_list(use_pretty_print)?;
        self.build_atomic_cards(use_pretty_print)?;
        self.build_deck_list(use_pretty_print)?;
        self.build_enum_values(use_pretty_print)?;

        Ok(())
    }

    /// Build AllPrintings and related format files
    pub fn build_all_printings_files(&self, pretty_print: bool) -> PyResult<()> {
        // Generate AllPrintings
        let all_printings = MtgjsonAllPrintings::new();
        let all_printings_json = serde_json::to_string(&all_printings).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("AllPrintings", all_printings_json, pretty_print)?;

        self.build_format_specific_files(&all_printings, pretty_print)?;

        // Generate AllIdentifiers
        let all_identifiers = MtgjsonAllIdentifiers::new();
        let all_identifiers_json = serde_json::to_string(&all_identifiers).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("AllIdentifiers", all_identifiers_json, pretty_print)?;

        Ok(())
    }

    /// Build format-specific AllPrintings files
    pub fn build_format_specific_files(
        &self,
        all_printings: &MtgjsonAllPrintings,
        pretty_print: bool,
    ) -> PyResult<()> {
        let format_map = self.construct_format_map()?;

        for (format_name, _set_codes) in format_map {
            let format_data = self.filter_all_printings_by_format(all_printings, &format_name)?;
            let filename = format!("AllPrintings{}", format_name);
            let format_data_json = serde_json::to_string(&format_data).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Serialization error: {}",
                    e
                ))
            })?;
            self.create_compiled_output(&filename, format_data_json, pretty_print)?;
        }

        Ok(())
    }

    /// Build atomic cards files
    pub fn build_atomic_cards(&self, pretty_print: bool) -> PyResult<()> {
        let atomic_cards = MtgjsonAtomicCards::new(None);
        let atomic_cards_json = serde_json::to_string(&atomic_cards).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("AtomicCards", atomic_cards_json, pretty_print)?;

        self.build_atomic_specific_files(pretty_print)?;

        Ok(())
    }

    /// Build format-specific atomic cards
    pub fn build_atomic_specific_files(&self, pretty_print: bool) -> PyResult<()> {
        let card_format_map = self.construct_atomic_cards_format_map()?;

        for (format_name, _cards) in card_format_map {
            let atomic_cards = MtgjsonAtomicCards::new(None);
            let filename = format!("{}Cards", format_name);
            let atomic_cards_json = serde_json::to_string(&atomic_cards).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Serialization error: {}",
                    e
                ))
            })?;
            self.create_compiled_output(&filename, atomic_cards_json, pretty_print)?;
        }

        Ok(())
    }

    /// Generate compiled prices output
    pub fn generate_compiled_prices_output(&self, pretty_print: bool) -> PyResult<()> {
        let prices_data = HashMap::<String, serde_json::Value>::new();
        let prices_data_json = serde_json::to_string(&prices_data).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;

        self.create_compiled_output("AllPrices", prices_data_json.clone(), pretty_print)?;
        self.create_compiled_output("AllPricesToday", prices_data_json, pretty_print)?;

        Ok(())
    }

    /// Build other compiled outputs
    pub fn build_compiled_list(&self, pretty_print: bool) -> PyResult<()> {
        let compiled_list = MtgjsonCompiledList::new();
        let compiled_list_json = serde_json::to_string(&compiled_list).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("CompiledList", compiled_list_json, pretty_print)
    }

    pub fn build_keywords(&self, pretty_print: bool) -> PyResult<()> {
        let keywords = MtgjsonKeywords::new();
        let keywords_json = serde_json::to_string(&keywords).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("Keywords", keywords_json, pretty_print)
    }

    pub fn build_card_types(&self, pretty_print: bool) -> PyResult<()> {
        let card_types = MtgjsonCardTypesObject::new();
        let card_types_json = serde_json::to_string(&card_types).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("CardTypes", card_types_json, pretty_print)
    }

    pub fn build_meta(&self, pretty_print: bool) -> PyResult<()> {
        let meta = MtgjsonMetaObject::with_current_date(None);
        let meta_json = serde_json::to_string(&meta).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("Meta", meta_json, pretty_print)
    }

    pub fn build_set_list(&self, pretty_print: bool) -> PyResult<()> {
        let set_list = MtgjsonSetObjectList::new();
        let set_list_json = serde_json::to_string(&set_list).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("SetList", set_list_json, pretty_print)
    }

    pub fn build_deck_list(&self, pretty_print: bool) -> PyResult<()> {
        let deck_list = MtgjsonDeckObjectList::new(Vec::new());
        let deck_list_json = serde_json::to_string(&deck_list).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("DeckList", deck_list_json, pretty_print)
    }

    pub fn build_enum_values(&self, pretty_print: bool) -> PyResult<()> {
        let enum_values = MtgjsonEnumValues::new();
        let enum_values_json = serde_json::to_string(&enum_values).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })?;
        self.create_compiled_output("EnumValues", enum_values_json, pretty_print)
    }

    /// Create compiled output file - Fixed to match Python signature
    pub fn create_compiled_output(
        &self,
        filename: &str,
        data_json: String,
        pretty_print: bool,
    ) -> PyResult<()> {
        let output_path = Path::new(&self.output_path).join(format!("{}.json", filename));

        let file = fs::File::create(&output_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create file: {}", e))
        })?;
        let mut writer = BufWriter::new(file);

        // Parse the JSON string back to a Value for structure creation
        let data_value: serde_json::Value = serde_json::from_str(&data_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON data: {}", e))
        })?;

        let meta = MtgjsonMetaObject::with_current_date(None);
        let output_structure = serde_json::json!({
            "meta": meta,
            "data": data_value
        });

        if pretty_print {
            serde_json::to_writer_pretty(&mut writer, &output_structure)
        } else {
            serde_json::to_writer(&mut writer, &output_structure)
        }
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "JSON serialization error: {}",
                e
            ))
        })?;

        writer.flush().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to flush file: {}", e))
        })?;

        Ok(())
    }

    /// Construct format map
    pub fn construct_format_map(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let mut format_map = HashMap::new();

        let formats = vec![
            "standard", "pioneer", "modern", "legacy", "vintage", "pauper",
        ];

        for format in formats {
            format_map.insert(format.to_string(), Vec::new());
        }

        // TODO: Load actual AllPrintings data and filter by legalities
        // This would be integrated with real data processing

        Ok(format_map)
    }

    /// Construct atomic cards format map
    pub fn construct_atomic_cards_format_map(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let mut format_map = HashMap::new();

        let formats = vec![
            "Standard", "Pioneer", "Modern", "Legacy", "Vintage", "Pauper",
        ];

        for format in formats {
            format_map.insert(format.to_string(), Vec::new());
        }

        Ok(format_map)
    }

    /// Filter AllPrintings data by format
    pub fn filter_all_printings_by_format(
        &self,
        all_printings: &MtgjsonAllPrintings,
        format_name: &str,
    ) -> PyResult<MtgjsonAllPrintings> {
        Ok(MtgjsonAllPrintings::new())
    }

    /// Generate file hashes for integrity checking
    pub fn generate_output_file_hashes(&self) -> PyResult<()> {
        let output_dir = Path::new(&self.output_path);

        for entry in fs::read_dir(output_dir).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e))
            })?;

            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                let hash = self.calculate_file_hash(entry.path().display().to_string())?;
                let hash_filename = format!("{}.sha256", entry.path().display());
                fs::write(&hash_filename, hash).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                        "Failed to write hash: {}",
                        e
                    ))
                })?;
            }
        }

        Ok(())
    }

    /// Calculate SHA256 hash of a file
    pub fn calculate_file_hash(&self, path: String) -> PyResult<String> {
        // TODO: Simplified hash calculation - would use a proper crypto library
        let path_obj = Path::new(&path);
        let contents = fs::read(path_obj).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file: {}", e))
        })?;

        // TODO: Use sha2 crate for actual SHA256
        Ok(format!("{:x}", contents.len()))
    }
}

impl Default for OutputGenerator {
    fn default() -> Self {
        Self::new(None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_output_generator_creation() {
        let generator = OutputGenerator::new(None, None);
        assert_eq!(generator.output_version, "5.0.0");
        assert_eq!(generator.output_date, "");
        assert!(generator.output_files.is_empty());
        assert!(generator.compression_enabled);
    }

    #[test]
    fn test_output_generator_default() {
        let generator = OutputGenerator::default();
        assert_eq!(generator.output_version, "5.0.0");
        assert_eq!(generator.output_date, "");
        assert!(generator.output_files.is_empty());
        assert!(generator.compression_enabled);
    }

    #[test]
    fn test_set_output_version() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("5.1.0".to_string());
        assert_eq!(generator.output_version, "5.1.0");
    }

    #[test]
    fn test_set_output_date() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_date("2023-01-01".to_string());
        assert_eq!(generator.output_date, "2023-01-01");
    }

    #[test]
    fn test_enable_compression() {
        let mut generator = OutputGenerator::new(None, None);
        generator.enable_compression(false);
        assert!(!generator.compression_enabled);

        generator.enable_compression(true);
        assert!(generator.compression_enabled);
    }

    #[test]
    fn test_add_output_file() {
        let mut generator = OutputGenerator::new(None, None);
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());

        assert_eq!(generator.output_files.len(), 2);
        assert!(generator
            .output_files
            .contains(&"AllCards.json".to_string()));
        assert!(generator.output_files.contains(&"AllSets.json".to_string()));
    }

    #[test]
    fn test_remove_output_file() {
        let mut generator = OutputGenerator::new(None, None);
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());

        generator.remove_output_file("AllCards.json".to_string());

        assert_eq!(generator.output_files.len(), 1);
        assert!(!generator
            .output_files
            .contains(&"AllCards.json".to_string()));
        assert!(generator.output_files.contains(&"AllSets.json".to_string()));
    }

    #[test]
    fn test_clear_output_files() {
        let mut generator = OutputGenerator::new(None, None);
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());

        generator.clear_output_files();

        assert!(generator.output_files.is_empty());
    }

    #[test]
    fn test_generate_output_files() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_date("2023-01-01".to_string());
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllSets.json".to_string());

        let result = generator.generate_output_files();
        assert!(result.is_ok());

        let output_map = result.unwrap();
        assert_eq!(output_map.len(), 2);
        assert!(output_map.contains_key("AllCards.json"));
        assert!(output_map.contains_key("AllSets.json"));
    }

    #[test]
    fn test_generate_meta_object() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("5.1.0".to_string());
        generator.set_output_date("2023-01-01".to_string());

        let meta_str = generator.generate_meta_object();
        let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap();
        assert_eq!(meta["version"], "5.1.0");
        assert_eq!(meta["date"], "2023-01-01");
    }

    #[test]
    fn test_generate_all_cards() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_date("2023-01-01".to_string());
        let result = generator.generate_all_cards();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_all_sets() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_date("2023-01-01".to_string());
        let result = generator.generate_all_sets();
        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_atomic_cards() {
        let generator = OutputGenerator::new(None, None);
        let result = generator.generate_atomic_cards();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_deck_list() {
        let generator = OutputGenerator::new(None, None);
        let result = generator.generate_deck_list();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_set_list() {
        let generator = OutputGenerator::new(None, None);
        let result = generator.generate_set_list();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_keywords() {
        let generator = OutputGenerator::new(None, None);
        let result = generator.generate_keywords();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_generate_card_types() {
        let generator = OutputGenerator::new(None, None);
        let result = generator.generate_card_types();

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.contains("\"meta\""));
        assert!(output.contains("\"data\""));
    }

    #[test]
    fn test_compress_output() {
        let generator = OutputGenerator::new(None, None);
        let test_data = "This is a test string for compression".to_string();

        let result = generator.compress_output(test_data.clone());
        assert!(result.is_ok());

        let compressed = result.unwrap();
        // Compressed data should be different from original
        assert_ne!(compressed.len(), test_data.len());
    }

    #[test]
    fn test_compress_output_disabled() {
        let mut generator = OutputGenerator::new(None, None);
        generator.enable_compression(false);

        let test_data = "This is a test string".to_string();
        let result = generator.compress_output(test_data.clone());

        assert!(result.is_ok());
        let output = result.unwrap();
        // When compression is disabled, output should be same as input
        assert_eq!(output, test_data);
    }

    #[test]
    fn test_json_serialization() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("5.1.0".to_string());
        generator.set_output_date("2023-01-01".to_string());
        generator.add_output_file("test.json".to_string());

        let json_result = generator.to_json();
        assert!(json_result.is_ok());

        let json_string = json_result.unwrap();
        assert!(json_string.contains("5.1.0"));
        assert!(json_string.contains("2023-01-01"));
        assert!(json_string.contains("test.json"));
    }

    #[test]
    fn test_string_representations() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("5.1.0".to_string());
        generator.set_output_date("2023-01-01".to_string());

        let str_repr = generator.__str__();
        assert!(str_repr.contains("5.1.0"));
        assert!(str_repr.contains("2023-01-01"));

        let repr = generator.__repr__();
        assert!(repr.contains("5.1.0"));
        assert!(repr.contains("2023-01-01"));
    }

    #[test]
    fn test_equality() {
        let mut generator1 = OutputGenerator::new(None, None);
        let mut generator2 = OutputGenerator::new(None, None);

        generator1.set_output_version("5.1.0".to_string());
        generator2.set_output_version("5.1.0".to_string());

        assert!(generator1.__eq__(&generator2));

        generator2.set_output_version("5.2.0".to_string());
        assert!(!generator1.__eq__(&generator2));
    }

    #[test]
    fn test_hash() {
        let mut generator = OutputGenerator::new(None, None);
        generator.set_output_version("5.1.0".to_string());

        let hash1 = generator.__hash__();
        let hash2 = generator.__hash__();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_output_file_duplicates() {
        let mut generator = OutputGenerator::new(None, None);
        generator.add_output_file("AllCards.json".to_string());
        generator.add_output_file("AllCards.json".to_string()); // Duplicate

        // Should not add duplicates
        assert_eq!(generator.output_files.len(), 1);
    }

    #[test]
    fn test_remove_nonexistent_file() {
        let mut generator = OutputGenerator::new(None, None);
        generator.add_output_file("AllCards.json".to_string());

        generator.remove_output_file("NonExistent.json".to_string());

        // Should still have the original file
        assert_eq!(generator.output_files.len(), 1);
        assert!(generator
            .output_files
            .contains(&"AllCards.json".to_string()));
    }

    #[test]
    fn test_large_output_generation() {
        let mut generator = OutputGenerator::new(None, None);

        // Add many output files
        for i in 0..100 {
            generator.add_output_file(format!("file_{}.json", i));
        }

        assert_eq!(generator.output_files.len(), 100);

        let result = generator.generate_output_files();
        assert!(result.is_ok());

        let output_map = result.unwrap();
        assert_eq!(output_map.len(), 100);
    }

    #[test]
    fn test_error_handling() {
        let generator = OutputGenerator::new(None, None);

        // Test error handling in various generation methods
        // These should not panic even with empty/invalid data
        let _ = generator.generate_all_cards();
        let _ = generator.generate_all_sets();
        let _ = generator.generate_atomic_cards();
        let _ = generator.generate_deck_list();
        let _ = generator.generate_set_list();
        let _ = generator.generate_keywords();
        let _ = generator.generate_card_types();
    }

    #[test]
    fn test_clone() {
        let mut original = OutputGenerator::new(None, None);
        original.set_output_version("5.1.0".to_string());
        original.set_output_date("2023-01-01".to_string());
        original.add_output_file("test.json".to_string());
        original.enable_compression(false);

        let cloned = original.clone();

        assert_eq!(original.output_version, cloned.output_version);
        assert_eq!(original.output_date, cloned.output_date);
        assert_eq!(original.output_files, cloned.output_files);
        assert_eq!(original.compression_enabled, cloned.compression_enabled);
    }
}
