use crate::classes::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON SetList Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonSetObjectList")]
pub struct MtgjsonSetObjectList {
    #[pyo3(get, set)]
    pub set_list: Vec<HashMap<String, String>>,
}

#[pymethods]
impl MtgjsonSetObjectList {
    #[new]
    pub fn new() -> Self {
        Self {
            set_list: Vec::new(),
        }
    }
}

impl Default for MtgjsonSetObjectList {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonSetObjectList {}
