use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON AtomicCards Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonAtomicCards")]
pub struct MtgjsonAtomicCards {
    #[pyo3(get, set)]
    pub atomic_cards_dict: HashMap<String, Vec<crate::PyJsonValue>>,
}

#[pymethods]
impl MtgjsonAtomicCards {
    #[new]
    pub fn new() -> Self {
        Self {
            atomic_cards_dict: HashMap::new(),
        }
    }
}

impl Default for MtgjsonAtomicCards {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonAtomicCards {}