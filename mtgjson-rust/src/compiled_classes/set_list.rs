use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON SetList Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonSetList")]
pub struct MtgjsonSetList {
    #[pyo3(get, set)]
    pub set_list: Vec<HashMap<String, String>>,
}

#[pymethods]
impl MtgjsonSetList {
    #[new]
    pub fn new() -> Self {
        Self {
            set_list: Vec::new(),
        }
    }
}

impl Default for MtgjsonSetList {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonSetList {}