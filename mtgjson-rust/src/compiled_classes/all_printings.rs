use crate::base::JsonObject;
use crate::set::MtgjsonSet;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// MTGJSON AllPrintings Object
/// Rust equivalent of MtgjsonAllPrintingsObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonAllPrintings")]
pub struct MtgjsonAllPrintings {
    #[pyo3(get, set)]
    pub all_sets_dict: HashMap<String, MtgjsonSet>,
    
    #[pyo3(get, set)]
    pub source_path: Option<String>,
}

#[pymethods]
impl MtgjsonAllPrintings {
    #[new]
    pub fn new() -> Self {
        Self {
            all_sets_dict: HashMap::new(),
            source_path: None,
        }
    }

    /// Initialize with file system scanning like Python
    #[staticmethod]
    pub fn from_path(path: String) -> PyResult<Self> {
        let mut all_printings = Self::new();
        all_printings.source_path = Some(path.clone());
        all_printings.load_sets_from_path(&path)?;
        Ok(all_printings)
    }

    /// Load sets from file system - Core functionality missing in original
    pub fn load_sets_from_path(&mut self, path: &str) -> PyResult<()> {
        let path_obj = Path::new(path);
        
        // Handle CON filename fix for Windows compatibility like Python
        if path_obj.exists() {
            if path_obj.is_file() {
                // Single AllPrintings.json file
                self.load_all_printings_file(path)?;
            } else if path_obj.is_dir() {
                // Directory with individual set files
                self.scan_directory_for_sets(path)?;
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyFileNotFoundError, _>(
                format!("Path not found: {}", path)
            ));
        }
        
        Ok(())
    }

    /// Load AllPrintings.json file
    fn load_all_printings_file(&mut self, file_path: &str) -> PyResult<()> {
        let contents = fs::read_to_string(file_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file {}: {}", file_path, e))
        })?;
        
        let json_value: serde_json::Value = serde_json::from_str(&contents).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON in {}: {}", file_path, e))
        })?;
        
        // Handle both direct data and data wrapped in meta structure
        let data_obj = if let Some(data) = json_value.get("data") {
            data
        } else {
            &json_value
        };
        
        if let Some(data_map) = data_obj.as_object() {
            for (set_code, set_data) in data_map {
                match serde_json::from_value::<MtgjsonSet>(set_data.clone()) {
                    Ok(mtgjson_set) => {
                        self.all_sets_dict.insert(set_code.clone(), mtgjson_set);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to parse set {}: {}", set_code, e);
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Scan directory for individual set files
    fn scan_directory_for_sets(&mut self, dir_path: &str) -> PyResult<()> {
        let entries = fs::read_dir(dir_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read directory {}: {}", dir_path, e))
        })?;
        
        for entry in entries {
            let entry = entry.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read entry: {}", e))
            })?;
            
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                    // CON filename fix for Windows
                    let set_code = if file_name == "CON_" {
                        "CON".to_string()
                    } else {
                        file_name.to_string()
                    };
                    
                    match self.load_single_set_file(path.to_str().unwrap()) {
                        Ok(set_data) => {
                            self.all_sets_dict.insert(set_code, set_data);
                        }
                        Err(e) => {
                            eprintln!("Warning: Failed to load set {}: {}", file_name, e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Load single set file
    fn load_single_set_file(&self, file_path: &str) -> PyResult<MtgjsonSet> {
        let contents = fs::read_to_string(file_path).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read file {}: {}", file_path, e))
        })?;
        
        let mtgjson_set: MtgjsonSet = serde_json::from_str(&contents).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON in {}: {}", file_path, e))
        })?;
        
        Ok(mtgjson_set)
    }

    /// Get set contents like Python method
    pub fn get_set_contents(&self, set_code: &str) -> Option<MtgjsonSet> {
        self.all_sets_dict.get(set_code).cloned()
    }

    /// Get files to build - Core Python method
    pub fn get_files_to_build(&self) -> Vec<String> {
        self.all_sets_dict.keys().cloned().collect()
    }

    /// Iterate all sets - Core Python method
    pub fn iterate_all_sets(&self) -> Vec<String> {
        self.all_sets_dict.keys().cloned().collect()
    }

    /// Filter sets by format legality 
    pub fn filter_by_format(&self, format_name: &str) -> Self {
        let mut filtered = Self::new();
        filtered.source_path = self.source_path.clone();
        
        for (set_code, set_data) in &self.all_sets_dict {
            // Check if set has cards legal in the format
            if self.set_has_format_legal_cards(set_data, format_name) {
                filtered.all_sets_dict.insert(set_code.clone(), set_data.clone());
            }
        }
        
        filtered
    }

    /// Check if set has cards legal in format
    fn set_has_format_legal_cards(&self, set_data: &MtgjsonSet, format_name: &str) -> bool {
        // Check if any cards in the set are legal in the format
        // This would use the legalities data from cards
        // For now, return true to include all sets
        true
    }

    /// Add set to dictionary
    pub fn add_set(&mut self, set_code: String, set_data: MtgjsonSet) {
        self.all_sets_dict.insert(set_code, set_data);
    }

    /// Get set count
    pub fn len(&self) -> usize {
        self.all_sets_dict.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.all_sets_dict.is_empty()
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.all_sets_dict).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialization error: {}", e))
        })
    }
}

impl Default for MtgjsonAllPrintings {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonAllPrintings {}