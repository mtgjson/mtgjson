use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use crate::classes::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderResult};

#[pyclass(name = "GathererProvider")]
pub struct GathererProvider {
    base: BaseProvider,
    multiverse_id_to_data: HashMap<String, Vec<HashMap<String, String>>>,
}

impl GathererProvider {
    const GATHERER_ID_MAPPING_URL: &'static str = "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/gatherer_mapping.json?raw=True";
}

#[pymethods]
impl GathererProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = Self::build_http_header_static()?;
        let base = BaseProvider::new("gatherer".to_string(), headers);
        
        let mut provider = GathererProvider {
            base,
            multiverse_id_to_data: HashMap::new(),
        };
        
        // Download the gatherer mapping data on initialization
        provider.initialize_data()?;
        
        Ok(provider)
    }

    /// Build HTTP header with GitHub token
    pub fn _build_http_header(&self) -> PyResult<HashMap<String, String>> {
        Self::build_http_header_static()
    }

    /// Download content from GitHub
    #[pyo3(signature = (url, params=None))]
    pub fn download(&mut self, url: String, params: Option<HashMap<String, String>>) -> PyResult<PyObject> {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Runtime creation error: {}", e)));
            }
        };
        
        let result = rt.block_on(async {
            match self.base.download_json(&url, params).await {
                Ok(json) => json,
                Err(e) => {
                    println!("Error downloading GitHub Gatherer: {}", e);
                    Value::Object(serde_json::Map::new())
                }
            }
        });
        
        // Convert serde_json::Value to PyObject
        Python::with_gil(|py| {
            let result_str = result.to_string();
            Ok(result_str.to_object(py))
        })
    }

    /// Get card(s) matching a given multiverseId
    pub fn get_cards(&self, multiverse_id: String) -> PyResult<Vec<HashMap<String, String>>> {
        if let Some(cards) = self.multiverse_id_to_data.get(&multiverse_id) {
            Ok(cards.clone())
        } else {
            Ok(Vec::new())
        }
    }

    /// Get the multiverse ID to data mapping
    #[getter]
    pub fn get_multiverse_id_to_data(&self) -> PyResult<HashMap<String, Vec<HashMap<String, String>>>> {
        Ok(self.multiverse_id_to_data.clone())
    }

    /// Static method to build HTTP header
    #[staticmethod]
    fn build_http_header_static() -> PyResult<HashMap<String, String>> {
        let mut headers = HashMap::new();
        
        // Try to read GitHub token from environment variable
        if let Ok(github_token) = std::env::var("MTGJSON_GITHUB_API_TOKEN") {
            if !github_token.is_empty() {
                headers.insert("Authorization".to_string(), format!("Bearer {}", github_token));
            }
        }
        
        Ok(headers)
    }

    /// Initialize the gatherer mapping data
    fn initialize_data(&mut self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Runtime error: {}", e)))?;
        
        let data = rt.block_on(async {
            self.base.download_json(Self::GATHERER_ID_MAPPING_URL, None).await
        });
        
        match data {
            Ok(data_value) => {
                if let Some(data_obj) = data_value.as_object() {
                    for (multiverse_id, card_data) in data_obj {
                        if let Some(cards_array) = card_data.as_array() {
                            let mut cards = Vec::new();
                            for card in cards_array {
                                if let Some(card_obj) = card.as_object() {
                                    let mut card_map = HashMap::new();
                                    for (key, value) in card_obj {
                                        if let Some(value_str) = value.as_str() {
                                            card_map.insert(key.clone(), value_str.to_string());
                                        }
                                    }
                                    cards.push(card_map);
                                }
                            }
                            self.multiverse_id_to_data.insert(multiverse_id.clone(), cards);
                        }
                    }
                }
                Ok(())
            },
            Err(e) => {
                println!("Failed to download gatherer mapping: {}", e);
                Ok(()) // Don't fail initialization, just log the error
            }
        }
    }

    /// Read Gatherer configuration
    fn read_gatherer_config(&self) -> HashMap<String, String> {
        let mut config = HashMap::new();
        
        // Try to read from environment variables first
        if let Ok(base_url) = std::env::var("GATHERER_BASE_URL") {
            config.insert("base_url".to_string(), base_url);
        } else {
            // Default Gatherer URL
            config.insert("base_url".to_string(), "https://gatherer.wizards.com".to_string());
        }
        
        if let Ok(timeout) = std::env::var("GATHERER_TIMEOUT") {
            config.insert("timeout".to_string(), timeout);
        } else {
            config.insert("timeout".to_string(), "30".to_string());
        }
        
        // Gatherer doesn't require authentication, but we can configure other options
        if let Ok(user_agent) = std::env::var("GATHERER_USER_AGENT") {
            config.insert("user_agent".to_string(), user_agent);
        }
        
        config
    }
}

#[async_trait]
impl AbstractProvider for GathererProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "GathererProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        Self::build_http_header_static().unwrap_or_default()
    }
    
    async fn download(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Value> {
        self.base.download_json(url, params).await
    }
    
    async fn download_raw(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<String> {
        self.base.download_text(url, params).await
    }
    
    fn log_download(&self, response: &Response) {
        println!("Downloaded {} (Status: {})", response.url(), response.status());
    }
    
    fn generic_generate_today_price_dict(
        &self,
        _third_party_to_mtgjson: &HashMap<String, HashSet<String>>,
        _price_data_rows: &[Value],
        _card_platform_id_key: &str,
        _default_prices_object: &MtgjsonPricesObject,
        _foil_key: &str,
        _retail_key: Option<&str>,
        _retail_quantity_key: Option<&str>,
        _buy_key: Option<&str>,
        _buy_quantity_key: Option<&str>,
        _etched_key: Option<&str>,
        _etched_value: Option<&str>,
    ) -> HashMap<String, MtgjsonPricesObject> {
        // Gatherer doesn't provide price data
        HashMap::new()
    }
}