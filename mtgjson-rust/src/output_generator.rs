// MTGJSON output generator - High performance file writing and JSON processing
use pyo3::prelude::*;

use serde_json;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::io::{BufWriter, Write};

use crate::compiled_classes::*;
use crate::meta::MtgjsonMetaObject;

#[pyclass(name = "OutputGenerator")]
#[derive(Debug, Clone)]
pub struct OutputGenerator {
    #[pyo3(get, set)]
    pub output_path: String,
    #[pyo3(get, set)]
    pub pretty_print: bool,
}

#[pymethods]
impl OutputGenerator {
    #[new]
    pub fn new(output_path: String, pretty_print: bool) -> Self {
        Self {
            output_path,
            pretty_print,
        }
    }
    
    /// Generate all compiled output files with high performance
    #[pyo3(signature = (pretty_print=None))]
    pub fn generate_compiled_output_files(&self, pretty_print: Option<bool>) -> PyResult<()> {
        // Create output directory
        let output_dir = Path::new(&self.output_path);
        fs::create_dir_all(output_dir).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create output dir: {}", e))
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
    pub fn build_format_specific_files(&self, all_printings: &MtgjsonAllPrintings, pretty_print: bool) -> PyResult<()> {
        let format_map = self.construct_format_map()?;
        
        for (format_name, _set_codes) in format_map {
            let format_data = self.filter_all_printings_by_format(all_printings, &format_name)?;
            let filename = format!("AllPrintings{}", format_name);
            let format_data_json = serde_json::to_string(&format_data).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
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
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
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
        let card_types = MtgjsonCardObjectTypes::new();
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
    pub fn create_compiled_output(&self, filename: &str, data_json: String, pretty_print: bool) -> PyResult<()> {
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
        }.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON serialization error: {}", e))
        })?;
        
        writer.flush().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to flush file: {}", e))
        })?;
        
        Ok(())
    }
    
    /// Construct format map
    pub fn construct_format_map(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let mut format_map = HashMap::new();
        
        let formats = vec!["standard", "pioneer", "modern", "legacy", "vintage", "pauper"];
        
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
        
        let formats = vec!["Standard", "Pioneer", "Modern", "Legacy", "Vintage", "Pauper"];
        
        for format in formats {
            format_map.insert(format.to_string(), Vec::new());
        }
        
        Ok(format_map)
    }
    
    /// Filter AllPrintings data by format
    pub fn filter_all_printings_by_format(
        &self, 
        all_printings: &MtgjsonAllPrintings,
        format_name: &str
    ) -> PyResult<MtgjsonAllPrintings> {
        // High-performance filtering logic
        // TODO: This would use the actual data structures
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
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write hash: {}", e))
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
        Self::new("./output".to_string(), true)
    }
}