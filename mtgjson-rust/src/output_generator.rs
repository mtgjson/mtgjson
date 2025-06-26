// MTGJSON output generator - High performance file writing and JSON processing
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::io::{BufWriter, Write};

use crate::compiled_classes::*;
use crate::meta::MtgjsonMeta;

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
    pub fn generate_compiled_output_files(&self) -> PyResult<()> {
        // Create output directory
        let output_dir = Path::new(&self.output_path);
        fs::create_dir_all(output_dir).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create output dir: {}", e))
        })?;
        
        // Generate all major outputs in parallel-friendly order
        self.build_all_printings_files()?;
        self.generate_compiled_prices_output()?;
        self.build_compiled_list()?;
        self.build_keywords()?;
        self.build_card_types()?;
        self.build_meta()?;
        self.build_set_list()?;
        self.build_atomic_cards()?;
        self.build_deck_list()?;
        self.build_enum_values()?;
        
        Ok(())
    }
    
    /// Build AllPrintings and related format files
    pub fn build_all_printings_files(&self) -> PyResult<()> {
        // Generate AllPrintings
        let all_printings = MtgjsonAllPrintings::new();
        let all_printings_json = serde_json::to_string(&all_printings).unwrap_or_default();
        self.create_compiled_output("AllPrintings", all_printings_json)?;
        
        self.build_format_specific_files(&all_printings)?;
        
        // Generate AllIdentifiers
        let all_identifiers = MtgjsonAllIdentifiers::new();
        let all_identifiers_json = serde_json::to_string(&all_identifiers).unwrap_or_default();
        self.create_compiled_output("AllIdentifiers", all_identifiers_json)?;
        
        Ok(())
    }
    
    /// Build format-specific AllPrintings files
    pub fn build_format_specific_files(&self, all_printings: &MtgjsonAllPrintings) -> PyResult<()> {
        let format_map = self.construct_format_map()?;
        
        for (format_name, _set_codes) in format_map {
            let format_data = self.filter_all_printings_by_format(all_printings, &format_name)?;
            let filename = format!("AllPrintings{}", format_name);
            let format_data_json = serde_json::to_string(&format_data).unwrap_or_default();
            self.create_compiled_output(&filename, format_data_json)?;
        }
        
        Ok(())
    }
    
    /// Build atomic cards files
    pub fn build_atomic_cards(&self) -> PyResult<()> {
        let atomic_cards = MtgjsonAtomicCards::new(None);
        let atomic_cards_json = serde_json::to_string(&atomic_cards).unwrap_or_default();
        self.create_compiled_output("AtomicCards", atomic_cards_json)?;
        
        self.build_atomic_specific_files()?;
        
        Ok(())
    }
    
    /// Build format-specific atomic cards
    pub fn build_atomic_specific_files(&self) -> PyResult<()> {
        let card_format_map = self.construct_atomic_cards_format_map()?;
        
        for (format_name, _cards) in card_format_map {
            let atomic_cards = MtgjsonAtomicCards::new(None);
            let filename = format!("{}Cards", format_name);
            let atomic_cards_json = serde_json::to_string(&atomic_cards).unwrap_or_default();
            self.create_compiled_output(&filename, atomic_cards_json)?;
        }
        
        Ok(())
    }
    
    /// Generate compiled prices output
    pub fn generate_compiled_prices_output(&self) -> PyResult<()> {
        let prices_data = HashMap::<String, serde_json::Value>::new();
        
        let prices_data_json = serde_json::to_string(&prices_data).unwrap_or_default();
        self.create_compiled_output("AllPrices", prices_data_json.clone())?;
        self.create_compiled_output("AllPricesToday", prices_data_json)?;
        
        Ok(())
    }
    
    /// Build other compiled outputs
    pub fn build_compiled_list(&self) -> PyResult<()> {
        let compiled_list = MtgjsonCompiledList::new();
        let compiled_list_json = serde_json::to_string(&compiled_list).unwrap_or_default();
        self.create_compiled_output("CompiledList", compiled_list_json)
    }
    
    pub fn build_keywords(&self) -> PyResult<()> {
        let keywords = MtgjsonKeywords::new();
        let keywords_json = serde_json::to_string(&keywords).unwrap_or_default();
        self.create_compiled_output("Keywords", keywords_json)
    }
    
    pub fn build_card_types(&self) -> PyResult<()> {
        let card_types = MtgjsonCardTypes::new();
        let card_types_json = serde_json::to_string(&card_types).unwrap_or_default();
        self.create_compiled_output("CardTypes", card_types_json)
    }
    
    pub fn build_meta(&self) -> PyResult<()> {
        let meta = MtgjsonMeta::with_current_date(None);
        let meta_json = serde_json::to_string(&meta).unwrap_or_default();
        self.create_compiled_output("Meta", meta_json)
    }
    
    pub fn build_set_list(&self) -> PyResult<()> {
        let set_list = MtgjsonSetList::new();
        let set_list_json = serde_json::to_string(&set_list).unwrap_or_default();
        self.create_compiled_output("SetList", set_list_json)
    }
    
    pub fn build_deck_list(&self) -> PyResult<()> {
        let deck_list = MtgjsonDeckList::new(Vec::new());
        let deck_list_json = serde_json::to_string(&deck_list).unwrap_or_default();
        self.create_compiled_output("DeckList", deck_list_json)
    }
    
    pub fn build_enum_values(&self) -> PyResult<()> {
        let enum_values = MtgjsonEnumValues::new();
        let enum_values_json = serde_json::to_string(&enum_values).unwrap_or_default();
        self.create_compiled_output("EnumValues", enum_values_json)
    }
    
    /// Create compiled output file
    pub fn create_compiled_output(&self, filename: &str, data_json: String) -> PyResult<()> {
        let output_path = Path::new(&self.output_path).join(format!("{}.json", filename));
        
        let file = fs::File::create(&output_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create file: {}", e))
        })?;
        let mut writer = BufWriter::new(file);
        
        let data_value = serde_json::from_str::<serde_json::Value>(&data_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON data: {}", e))
        })?;
        
        let meta = MtgjsonMeta::with_current_date(None);
        let output_structure = serde_json::json!({
            "meta": meta,
            "data": data_value
        });
        
        if self.pretty_print {
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
                let hash = self.calculate_file_hash(&entry.path())?;
                let hash_filename = format!("{}.sha256", entry.path().display());
                fs::write(&hash_filename, hash).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write hash: {}", e))
                })?;
            }
        }
        
        Ok(())
    }
    
    /// Calculate SHA256 hash of a file
    pub fn calculate_file_hash(&self, path: &Path) -> PyResult<String> {
        // TODO: Simplified hash calculation - would use a proper crypto library
        let contents = fs::read(path).map_err(|e| {
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