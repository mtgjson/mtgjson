use crate::classes::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// MTGJSON TcgplayerSkus Object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonTcgplayerSkus")]
pub struct MtgjsonTcgplayerSkus {
    #[pyo3(get, set)]
    pub tcgplayer_skus: HashMap<String, Vec<HashMap<String, String>>>,
}

#[pymethods]
impl MtgjsonTcgplayerSkus {
    #[new]
    #[pyo3(signature = (_all_printings_path=None))]
    pub fn new(_all_printings_path: Option<std::path::PathBuf>) -> Self {
        Self {
            tcgplayer_skus: HashMap::new(),
        }
    }
}

impl Default for MtgjsonTcgplayerSkus {
    fn default() -> Self {
        Self::new(None)
    }
}

impl JsonObject for MtgjsonTcgplayerSkus {}
