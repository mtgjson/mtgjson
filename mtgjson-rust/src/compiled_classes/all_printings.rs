use crate::base::JsonObject;
use crate::set::MtgjsonSet;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON AllPrintings Object
/// Rust equivalent of MtgjsonAllPrintingsObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonAllPrintings")]
pub struct MtgjsonAllPrintings {
    #[pyo3(get, set)]
    pub all_sets_dict: HashMap<String, MtgjsonSet>,
}

#[pymethods]
impl MtgjsonAllPrintings {
    #[new]
    pub fn new() -> Self {
        Self {
            all_sets_dict: HashMap::new(),
        }
    }

    // TODO: Implement full AllPrintings functionality
    pub fn add_set(&mut self, set_code: String, set_data: MtgjsonSet) {
        self.all_sets_dict.insert(set_code, set_data);
    }
}

impl Default for MtgjsonAllPrintings {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonAllPrintings {}