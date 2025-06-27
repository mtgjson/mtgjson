use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc};
use crate::classes::prices::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

#[pyclass(name = "WhatsInStandardProvider")]
pub struct WhatsInStandardProvider {
    base: BaseProvider,
    set_codes: HashSet<String>,
    standard_legal_sets: HashSet<String>,
}

impl WhatsInStandardProvider {
    const API_ENDPOINT: &'static str = "https://whatsinstandard.com/api/v6/standard.json";
}

#[pymethods]
impl WhatsInStandardProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = HashMap::new();
        let base = BaseProvider::new("standard".to_string(), headers);
        
        let mut provider = Self {
            base,
            set_codes: HashSet::new(),
            standard_legal_sets: HashSet::new(),
        };
        
        // Initialize set codes
        let runtime = tokio::runtime::Runtime::new()?;
        provider.set_codes = runtime.block_on(async {
            provider.standard_legal_set_codes_async().await
        }).unwrap_or_default();
        
        Ok(provider)
    }
    
    /// Get all set codes from sets that are currently legal in Standard
    pub fn standard_legal_set_codes(&self) -> PyResult<HashSet<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.standard_legal_set_codes_async().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Standard sets error: {}", e)))
    }
}

impl WhatsInStandardProvider {
    async fn standard_legal_set_codes_async(&self) -> ProviderResult<HashSet<String>> {
        if !self.standard_legal_sets.is_empty() {
            return Ok(self.standard_legal_sets.clone());
        }
        
        let api_response = self.download(Self::API_ENDPOINT, None).await?;
        let empty_vec = vec![];
        let sets = api_response.get("sets")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty_vec);
        
        let now = Utc::now();
        let mut standard_set_codes = HashSet::new();
        
        for set_object in sets {
            let code = set_object.get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_uppercase();
            
            let enter_date = set_object.get("enterDate")
                .and_then(|v| v.get("exact"))
                .and_then(|v| v.as_str())
                .unwrap_or("9999");
            
            let exit_date = set_object.get("exitDate")
                .and_then(|v| v.get("exact"))
                .and_then(|v| v.as_str())
                .unwrap_or("9999");
            
            // Parse dates
            let enter_parsed = if enter_date == "9999" {
                DateTime::<Utc>::MAX_UTC
            } else {
                DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", enter_date))
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or(DateTime::<Utc>::MIN_UTC)
            };
            
            let exit_parsed = if exit_date == "9999" {
                DateTime::<Utc>::MAX_UTC
            } else {
                DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", exit_date))
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or(DateTime::<Utc>::MAX_UTC)
            };
            
            if enter_parsed <= now && now <= exit_parsed {
                standard_set_codes.insert(code);
            }
        }
        
        Ok(standard_set_codes)
    }
}

#[async_trait]
impl AbstractProvider for WhatsInStandardProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "WhatsInStandardProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    
    async fn download(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Value> {
        let response = self.base.get_request(url, params).await?;
        
        if !response.status().is_success() {
            eprintln!("WhatsInStandard Download Error ({}): {}", 
                     response.status(), 
                     response.text().await.unwrap_or_default());
            
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            return self.download(url, None).await;
        }
        
        response.json().await.map_err(|e| {
            ProviderError::ParseError(format!("JSON parse error: {}", e))
        })
    }
    
    async fn download_raw(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<String> {
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
        HashMap::new()
    }
}