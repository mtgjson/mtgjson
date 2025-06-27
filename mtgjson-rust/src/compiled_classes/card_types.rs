use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON CardTypes Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonCardTypesObject")]
pub struct MtgjsonCardTypesObject {
    #[pyo3(get, set)]
    pub types: HashMap<String, HashMap<String, Vec<String>>>,
}

#[pymethods]
impl MtgjsonCardTypesObject {
    #[new]
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }
}

impl Default for MtgjsonCardTypesObject {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonCardTypesObject {}