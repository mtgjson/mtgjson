use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON AtomicCards Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonAtomicCards")]
pub struct MtgjsonAtomicCards {
    #[pyo3(get, set)]
    pub atomic_cards_dict: HashMap<String, Vec<String>>,
}

#[pymethods]
impl MtgjsonAtomicCards {
    #[new]
    pub fn new(cards_data: Option<HashMap<String, Vec<String>>>) -> Self {
        Self {
            atomic_cards_dict: cards_data.unwrap_or_default(),
        }
    }
}

impl Default for MtgjsonAtomicCards {
    fn default() -> Self {
        Self::new(None)
    }
}

impl JsonObject for MtgjsonAtomicCards {}