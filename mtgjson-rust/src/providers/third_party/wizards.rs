use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use regex::Regex;
use chrono::{DateTime, Utc};
use crate::prices::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

#[pyclass(name = "WizardsProvider")]
pub struct WizardsProvider {
    base: BaseProvider,
    magic_rules_url: String,
    magic_rules: String,
    one_week_ago: i64,
}

impl WizardsProvider {
    const TRANSLATION_URL: &'static str = "https://magic.wizards.com/{}/products/card-set-archive";
    const INITIAL_MAGIC_RULES_URL: &'static str = "https://magic.wizards.com/en/rules";
}

#[pymethods]
impl WizardsProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = HashMap::new();
        let base = BaseProvider::new("wizards".to_string(), headers);
        let one_week_ago = (Utc::now().timestamp() - 7 * 86400) as i64;
        
        Ok(Self {
            base,
            magic_rules_url: Self::INITIAL_MAGIC_RULES_URL.to_string(),
            magic_rules: String::new(),
            one_week_ago,
        })
    }
    
    /// Download the comprehensive rules from Wizards site
    pub fn get_magic_rules(&mut self) -> PyResult<String> {
        if !self.magic_rules.is_empty() {
            return Ok(self.magic_rules.clone());
        }
        
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.get_magic_rules_async().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Magic rules error: {}", e)))
    }
}

impl WizardsProvider {
    async fn get_magic_rules_async(&mut self) -> ProviderResult<String> {
        if !self.magic_rules.is_empty() {
            return Ok(self.magic_rules.clone());
        }
        
        // Download the rules page to find the actual rules text file
        let response_text = self.download_raw(&self.magic_rules_url, None).await?;
        
        // Get the comp rules from the website (as it changes often)
        // Also split up the regex find so we only have the URL
        let re = Regex::new(r#"href=".*\.txt""#).map_err(|e| {
            ProviderError::ParseError(format!("Regex error: {}", e))
        })?;
        
        if let Some(capture) = re.find(&response_text) {
            let href_match = capture.as_str();
            // Extract URL from href="..." (remove first 6 chars 'href="' and last char '"')
            if href_match.len() > 7 {
                self.magic_rules_url = href_match[6..href_match.len()-1].to_string();
            }
        }
        
        // Download the actual rules text file
        let rules_response = self.download_raw(&self.magic_rules_url, None).await?;
        
        // Clean up the response text
        let cleaned_rules = rules_response
            .replace("â€™", "'") // Replace weird character encoding
            .lines()
            .collect::<Vec<_>>()
            .join("\n");
        
        self.magic_rules = cleaned_rules.clone();
        Ok(cleaned_rules)
    }
}

#[async_trait]
impl AbstractProvider for WizardsProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "WizardsProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    
    async fn download(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Value> {
        // This provider primarily returns HTML/text, not JSON
        let text = self.download_raw(url, params).await?;
        Ok(Value::String(text))
    }
    
    async fn download_raw(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<String> {
        let response = self.base.get_request(url, params).await?;
        self.log_download(&response);
        
        response.text().await.map_err(|e| {
            ProviderError::NetworkError(format!("Text download error: {}", e))
        })
    }
    
    fn log_download(&self, response: &Response) {
        println!("Downloaded {} (Status: {})", response.url(), response.status());
    }
    
    fn generic_generate_today_price_dict(
        &self,
        _third_party_to_mtgjson: &HashMap<String, HashSet<String>>,
        _price_data_rows: &[Value],
        _card_platform_id_key: &str,
        _default_prices_object: &MtgjsonPrices,
        _foil_key: &str,
        _retail_key: Option<&str>,
        _retail_quantity_key: Option<&str>,
        _buy_key: Option<&str>,
        _buy_quantity_key: Option<&str>,
        _etched_key: Option<&str>,
        _etched_value: Option<&str>,
    ) -> HashMap<String, MtgjsonPrices> {
        HashMap::new()
    }
}