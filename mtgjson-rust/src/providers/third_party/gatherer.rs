use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::HashMap;
use crate::prices::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

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
    pub fn download(&mut self, url: String, params: Option<HashMap<String, String>>) -> PyResult<Value> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            match self.base.get(&url, params).await {
                Ok(response) => {
                    if response.status().is_success() {
                        let json: Value = response.json().await.map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON parse error: {}", e))
                        })?;
                        Ok(json)
                    } else {
                        println!("Error downloading GitHub Gatherer: {} --- {}", response.status(), response.status());
                        Ok(Value::Object(serde_json::Map::new()))
                    }
                },
                Err(e) => {
                    println!("Error downloading GitHub Gatherer: {}", e);
                    Ok(Value::Object(serde_json::Map::new()))
                }
            }
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
        // For now, return empty headers - this would need actual config implementation
        let mut headers = HashMap::new();
        // TODO: Implement actual config reading
        // let github_token = MtgjsonConfig().get("GitHub", "api_token");
        // if !github_token.is_empty() {
        //     headers.insert("Authorization".to_string(), format!("Bearer {}", github_token));
        // }
        
        Ok(headers)
    }

    /// Initialize the gatherer mapping data
    fn initialize_data(&mut self) -> PyResult<()> {
        let data = self.download(Self::GATHERER_ID_MAPPING_URL.to_string(), None)?;
        
        // Parse the JSON data into our internal format
        if let Some(data_obj) = data.as_object() {
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
    async fn download_async(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Response> {
        self.base.get(url, params).await
    }

    async fn generate_today_price_dict(&self, _all_printings_path: &str) -> ProviderResult<HashMap<String, MtgjsonPricesObject>> {
        // Gatherer doesn't provide price data
        Ok(HashMap::new())
    }
}