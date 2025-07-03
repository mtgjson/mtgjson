use crate::classes::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON EnumValues Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonEnumValues")]
pub struct MtgjsonEnumValues {
    #[pyo3(get, set)]
    pub attr_value_dict: HashMap<String, String>,
}

#[pymethods]
impl MtgjsonEnumValues {
    #[new]
    pub fn new() -> Self {
        Self {
            attr_value_dict: HashMap::new(),
        }
    }
}

impl Default for MtgjsonEnumValues {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonEnumValues {}
