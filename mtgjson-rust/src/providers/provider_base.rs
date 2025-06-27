use async_trait::async_trait;
use reqwest::{Client, Response};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc};
use crate::prices::MtgjsonPricesObject;
use super::{ProviderError, ProviderResult};

/// Abstract provider trait that all providers must implement
#[async_trait]
pub trait AbstractProvider: Send + Sync {
    /// Get the class ID for the provider
    pub fn get_class_id(&self) -> &str;

    /// Get the class name for the provider
    pub fn get_class_name(&self) -> &str;
    
    /// Build HTTP headers for authentication
    pub fn build_http_header(&self) -> HashMap<String, String>;
    
    /// Download content from a URL with optional parameters
    pub async fn download(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Value>;
    
    /// Download raw content (for non-JSON responses)
    pub async fn download_raw(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<String>;
    
    /// Log download information
    pub fn log_download(&self, response: &Response);
    
    /// Get today's date in YYYY-MM-DD format
    pub fn today_date(&self) -> String {
        Utc::now().format("%Y-%m-%d").to_string()
    }
    
    /// Generic method to generate today's price dictionary
    pub fn generic_generate_today_price_dict(
        &self,
        third_party_to_mtgjson: &HashMap<String, HashSet<String>>,
        price_data_rows: &[Value],
        card_platform_id_key: &str,
        default_prices_object: &MtgjsonPricesObject,
        foil_key: &str,
        retail_key: Option<&str>,
        retail_quantity_key: Option<&str>,
        buy_key: Option<&str>,
        buy_quantity_key: Option<&str>,
        etched_key: Option<&str>,
        etched_value: Option<&str>,
    ) -> HashMap<String, MtgjsonPricesObject>;
}

/// Base provider struct that implements common functionality
#[pyclass(name = "BaseProvider")]
pub struct BaseProvider {
    pub class_id: String,
    pub client: Client,
    pub headers: HashMap<String, String>,
}

impl BaseProvider {
    /// Create a new base provider
    pub fn new(class_id: String, headers: HashMap<String, String>) -> Self {
        let mut client_builder = Client::builder();
        
        // Add default headers
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (key, value) in &headers {
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                default_headers.insert(name, val);
            }
        }
        
        let client = client_builder
            .default_headers(default_headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
            
        Self {
            class_id,
            client,
            headers,
        }
    }
    
    /// Make an HTTP GET request (alias for get_request)
    pub async fn get(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Response> {
        self.get_request(url, params).await
    }
    
    /// Make an HTTP GET request
    pub async fn get_request(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Response> {
        let mut request = self.client.get(url);
        
        if let Some(p) = params {
            request = request.query(&p);
        }
        
        request.send().await.map_err(|e| {
            ProviderError::NetworkError(format!("Request failed: {}", e))
        })
    }
    
    /// Download JSON content
    pub async fn download_json(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Value> {
        let response = self.get_request(url, params).await?;
        
        if !response.status().is_success() {
            return Err(ProviderError::NetworkError(format!(
                "HTTP error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        
        response.json().await.map_err(|e| {
            ProviderError::ParseError(format!("JSON parse error: {}", e))
        })
    }
    
    /// Download text content
    pub async fn download_text(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<String> {
        let response = self.get_request(url, params).await?;
        
        if !response.status().is_success() {
            return Err(ProviderError::NetworkError(format!(
                "HTTP error {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        
        response.text().await.map_err(|e| {
            ProviderError::NetworkError(format!("Text download error: {}", e))
        })
    }
}

/// Rate limiter for API calls
#[pyclass(name = "RateLimiter")]
pub struct RateLimiter {
    last_call: tokio::sync::Mutex<DateTime<Utc>>,
    min_interval: chrono::Duration,
}

impl RateLimiter {
    pub fn new(calls_per_second: f64) -> Self {
        let min_interval = chrono::Duration::milliseconds((1000.0 / calls_per_second) as i64);
        Self {
            last_call: tokio::sync::Mutex::new(DateTime::UNIX_EPOCH),
            min_interval,
        }
    }
    
    pub async fn wait_if_needed(&self) {
        let now = Utc::now();
        let mut last_call = self.last_call.lock().await;
        
        let elapsed = now - *last_call;
        if elapsed < self.min_interval {
            let wait_time = self.min_interval - elapsed;
            drop(last_call);
            tokio::time::sleep(wait_time.to_std().unwrap_or(std::time::Duration::from_millis(100))).await;
        }
        
        *self.last_call.lock().await = Utc::now();
    }
}