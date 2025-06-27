use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use crate::classes::{MtgjsonPricesObject, MtgjsonSealedProductObject};
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

#[pyclass(name = "TCGPlayerProvider")]
pub struct TCGPlayerProvider {
    base: BaseProvider,
    api_version: String,
}

#[pymethods]
impl TCGPlayerProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = HashMap::new();
        let base = BaseProvider::new("tcg".to_string(), headers);
        let api_version = "v1.39.0".to_string();
        
        Ok(Self { base, api_version })
    }
    
    /// Generate today's price dictionary
    pub fn generate_today_price_dict(&self, all_printings_path: &str) -> PyResult<HashMap<String, MtgjsonPricesObject>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.generate_today_price_dict_async(all_printings_path).await
        }).map_err(|e: ProviderError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Price dict error: {}", e)))
    }
    
    /// Update sealed URLs for sealed products
    #[staticmethod]
    pub fn update_sealed_urls(mut sealed_products: Vec<MtgjsonSealedProductObject>) -> Vec<MtgjsonSealedProductObject> {
        let product_url = "https://partner.tcgplayer.com/c/4948039/1780961/21018?subId1=api&u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F{}%3Fpage%3D1";
        
        for sealed_product in &mut sealed_products {
            if let Some(ref identifiers) = sealed_product.identifiers {
                if let Some(tcgplayer_id) = &identifiers.tcgplayer_product_id {
                    let url = product_url.replace("{}", tcgplayer_id);
                    let mut purchase_urls = crate::classes::MtgjsonPurchaseUrls::new();
                    purchase_urls.tcgplayer = Some(url);
                    sealed_product.raw_purchase_urls = Some(purchase_urls);
                }
            }
        }
        sealed_products
    }
    
    /// Get TCGPlayer magic set IDs
    pub fn get_tcgplayer_magic_set_ids(&self) -> PyResult<Vec<(String, String)>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.get_tcgplayer_magic_set_ids_async().await
        }).map_err(|e: ProviderError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Set IDs error: {}", e)))
    }
    
    /// Get TCGPlayer SKU data
    pub fn get_tcgplayer_sku_data(&self, group_id_and_name: (String, String)) -> PyResult<Vec<Value>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.get_tcgplayer_sku_data_async(group_id_and_name).await
        }).map_err(|e: ProviderError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("SKU data error: {}", e)))
    }
}

impl TCGPlayerProvider {
    async fn generate_today_price_dict_async(&self, _all_printings_path: &str) -> ProviderResult<HashMap<String, MtgjsonPricesObject>> {
        // Placeholder implementation
        Ok(HashMap::new())
    }
    
    async fn get_tcgplayer_magic_set_ids_async(&self) -> ProviderResult<Vec<(String, String)>> {
        let mut magic_set_ids = Vec::new();
        let mut api_offset = 0;
        
        loop {
            let url = format!("https://api.tcgplayer.com/{}/catalog/categories/1/groups", self.api_version);
            let params = Some([("offset".to_string(), api_offset.to_string())].iter().cloned().collect());
            
            let response = self.download(&url, params).await?;
            let empty_vec = vec![];
            let results = response.get("results")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty_vec);
            
            if results.is_empty() {
                break;
            }
            
            for magic_set in results {
                if let (Some(group_id), Some(name)) = (
                    magic_set.get("groupId").and_then(|v| v.as_str()),
                    magic_set.get("name").and_then(|v| v.as_str())
                ) {
                    magic_set_ids.push((group_id.to_string(), name.to_string()));
                }
            }
            
            api_offset += results.len();
        }
        
        Ok(magic_set_ids)
    }
    
    async fn get_tcgplayer_sku_data_async(&self, group_id_and_name: (String, String)) -> ProviderResult<Vec<Value>> {
        let mut magic_set_product_data = Vec::new();
        let mut api_offset = 0;
        
        loop {
            let url = "https://api.tcgplayer.com/catalog/products";
            let params = Some([
                ("offset".to_string(), api_offset.to_string()),
                ("limit".to_string(), "100".to_string()),
                ("categoryId".to_string(), "1".to_string()),
                ("includeSkus".to_string(), "true".to_string()),
                ("groupId".to_string(), group_id_and_name.0.clone()),
            ].iter().cloned().collect());
            
            let response = self.download(url, params).await?;
            let empty_vec2 = vec![];
            let results = response.get("results")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty_vec2);
            
            if results.is_empty() {
                break;
            }
            
            magic_set_product_data.extend(results.clone());
            api_offset += results.len();
        }
        
        Ok(magic_set_product_data)
    }
}

#[async_trait]
impl AbstractProvider for TCGPlayerProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "TCGPlayerProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        self.base.headers.clone()
    }
    
    async fn download(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Value> {
        let url_with_version = url.replace("[API_VERSION]", &self.api_version);
        self.base.download_json(&url_with_version, params).await
    }
    
    async fn download_raw(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<String> {
        let url_with_version = url.replace("[API_VERSION]", &self.api_version);
        self.base.download_text(&url_with_version, params).await
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