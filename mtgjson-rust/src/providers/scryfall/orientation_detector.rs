use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use scraper::{Html, Selector};
use crate::prices::MtgjsonPricesObject;
use super::{super::{AbstractProvider, BaseProvider, ProviderError, ProviderResult}, sf_utils};

#[pyclass(name = "ScryfallProviderOrientationDetector")]
pub struct ScryfallProviderOrientationDetector {
    base: BaseProvider,
}

impl ScryfallProviderOrientationDetector {
    const MAIN_PAGE_URL: &'static str = "https://scryfall.com/sets/{}";
}

#[pymethods]
impl ScryfallProviderOrientationDetector {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = sf_utils::build_http_header();
        let base = BaseProvider::new("sf_orient".to_string(), headers);
        
        Ok(Self { base })
    }
    
    /// Get UUID to orientation mapping
    pub fn get_uuid_to_orientation_map(&self, set_code: &str) -> PyResult<HashMap<String, String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.get_uuid_to_orientation_map_async(set_code).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Orientation map error: {}", e)))
    }
}

impl ScryfallProviderOrientationDetector {
    async fn get_uuid_to_orientation_map_async(&self, set_code: &str) -> ProviderResult<HashMap<String, String>> {
        let url = Self::MAIN_PAGE_URL.replace("{}", set_code);
        let response_text = self.download_raw(&url, None).await?;
        
        let document = Html::parse_document(&response_text);
        
        // Parse orientation headers
        let header_selector = Selector::parse("span.card-grid-header-content").unwrap();
        let grid_selector = Selector::parse("div.card-grid-inner").unwrap();
        
        let orientation_headers: Vec<_> = document.select(&header_selector).collect();
        let card_grids: Vec<_> = document.select(&grid_selector).collect();
        
        let mut return_map = HashMap::new();
        
        for (orientation_header, card_grid) in orientation_headers.iter().zip(card_grids.iter()) {
            let orientation = self.parse_orientation(orientation_header);
            let card_uuids = self.parse_card_entries(card_grid);
            
            for card_uuid in card_uuids {
                return_map.insert(card_uuid, orientation.clone());
            }
        }
        
        Ok(return_map)
    }
    
    fn parse_orientation(&self, orientation_header: &scraper::ElementRef) -> String {
        if let Some(link) = orientation_header.select(&Selector::parse("a").unwrap()).next() {
            if let Some(id) = link.value().attr("id") {
                return id.to_string();
            }
        }
        "unknown".to_string()
    }
    
    fn parse_card_entries(&self, card_grid: &scraper::ElementRef) -> Vec<String> {
        let item_selector = Selector::parse("div.card-grid-item").unwrap();
        card_grid.select(&item_selector)
            .filter_map(|item| item.value().attr("data-card-id"))
            .map(|id| id.to_string())
            .collect()
    }
}

#[async_trait]
impl AbstractProvider for ScryfallProviderOrientationDetector {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "ScryfallProviderOrientationDetector"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        sf_utils::build_http_header()
    }
    
    async fn download(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Value> {
        // This provider returns HTML, not JSON
        let text = self.download_raw(url, params).await?;
        Ok(Value::String(text))
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