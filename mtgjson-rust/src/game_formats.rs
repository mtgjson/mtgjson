use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// MTGJSON Singular Card.GameFormats Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonGameFormatsObject")]
pub struct MtgjsonGameFormatsObject {
    #[pyo3(get, set)]
    pub paper: bool,
    
    #[pyo3(get, set)]
    pub mtgo: bool,
    
    #[pyo3(get, set)]
    pub arena: bool,
    
    #[pyo3(get, set)]
    pub shandalar: bool,
    
    #[pyo3(get, set)]
    pub dreamcast: bool,
}

#[pymethods]
impl MtgjsonGameFormatsObject {
    #[new]
    pub fn new() -> Self {
        Self {
            paper: false,
            mtgo: false,
            arena: false,
            shandalar: false,
            dreamcast: false,
        }
    }

    /// Convert to JSON - returns list of available formats
    pub fn to_json(&self) -> PyResult<Vec<String>> {
        let mut formats = Vec::new();
        
        if self.paper {
            formats.push("paper".to_string());
        }
        if self.mtgo {
            formats.push("mtgo".to_string());
        }
        if self.arena {
            formats.push("arena".to_string());
        }
        if self.shandalar {
            formats.push("shandalar".to_string());
        }
        if self.dreamcast {
            formats.push("dreamcast".to_string());
        }
        
        Ok(formats)
    }

    /// Get available formats as a list
    pub fn get_available_formats(&self) -> Vec<String> {
        self.to_json().unwrap_or_default()
    }
}

impl JsonObject for MtgjsonGameFormatsObject {}

impl From<&[&str]> for MtgjsonGameFormatsObject {
    fn from(formats: &[&str]) -> Self {
        let mut game_formats = Self::new();
        
        for format in formats {
            match format.to_lowercase().as_str() {
                "paper" => game_formats.paper = true,
                "mtgo" => game_formats.mtgo = true,
                "arena" => game_formats.arena = true,
                "shandalar" => game_formats.shandalar = true,
                "dreamcast" => game_formats.dreamcast = true,
                _ => {}
            }
        }
        
        game_formats
    }
}