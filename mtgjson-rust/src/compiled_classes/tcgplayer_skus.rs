use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON TcgplayerSkus Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonTcgplayerSkus")]
pub struct MtgjsonTcgplayerSkus {
    #[pyo3(get, set)]
    pub enhanced_tcgplayer_skus: HashMap<String, Vec<HashMap<String, crate::PyJsonValue>>>,
}

#[pymethods]
impl MtgjsonTcgplayerSkus {
    #[new]
    pub fn new() -> Self {
        Self {
            enhanced_tcgplayer_skus: HashMap::new(),
        }
    }
}

impl Default for MtgjsonTcgplayerSkus {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonTcgplayerSkus {}