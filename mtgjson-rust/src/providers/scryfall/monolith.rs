use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyList;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tokio::time::{sleep, Duration};
use crate::prices::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, RateLimiter, ProviderError, ProviderResult};
use super::sf_utils;

#[pyclass(name = "ScryfallProvider")]
pub struct ScryfallProvider {
    base: BaseProvider,
    rate_limiter: RateLimiter,
    cards_without_limits: HashSet<String>,
}

impl ScryfallProvider {
    const ALL_SETS_URL: &'static str = "https://api.scryfall.com/sets/";
    const CARDS_URL: &'static str = "https://api.scryfall.com/cards/";
    const CARDS_URL_ALL_DETAIL_BY_SET_CODE: &'static str = 
        "https://api.scryfall.com/cards/search?include_extras=true&include_variations=true&order=set&q=e%3A{}&unique=prints";
    const CARDS_WITHOUT_LIMITS_URL: &'static str = 
        "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)%20or%20(o:deck%20o:have%20o:up%20o:to%20o:cards%20o:named)";
    const CARDS_IN_BASE_SET_URL: &'static str = 
        "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20is:booster%20unique:prints";
    const CARDS_IN_SET: &'static str = 
        "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20unique:prints";
    const TYPE_CATALOG: &'static str = "https://api.scryfall.com/catalog/{0}";
    const CARDS_WITH_ALCHEMY_SPELLBOOK_URL: &'static str = 
        "https://api.scryfall.com/cards/search?q=is:alchemy%20and%20oracle:/conjure|draft|%27s%20spellbook/&include_extras=true";
    const SPELLBOOK_SEARCH_URL: &'static str = 
        "https://api.scryfall.com/cards/search?q=spellbook:%22{}%22&include_extras=true";
}

#[pymethods]
impl ScryfallProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = sf_utils::build_http_header();
        let base = BaseProvider::new("sf".to_string(), headers);
        let rate_limiter = RateLimiter::new(15.0); // 15 calls per second
        
        Ok(Self {
            base,
            rate_limiter,
            cards_without_limits: HashSet::new(),
        })
    }
    
    /// Download all pages from a paginated Scryfall API endpoint
    #[pyo3(signature = (starting_url, params=None))]
    pub fn download_all_pages<'py>(
        &self,
        py: Python<'py>,
        starting_url: &str,
        params: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let cards = runtime.block_on(async {
            self.download_all_pages_async(starting_url, params).await
        })?;
        
        let py_list = PyList::new_bound(py, cards.iter().map(|v| v.to_string()));
        Ok(py_list)
    }
    
    /// Download cards for a specific set
    pub fn download_cards<'py>(
        &self,
        py: Python<'py>,
        set_code: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let url = Self::CARDS_URL_ALL_DETAIL_BY_SET_CODE.replace("{}", set_code);
        
        // Get the raw cards as JSON Values
        let runtime = tokio::runtime::Runtime::new()?;
        let mut cards = runtime.block_on(async {
            self.download_all_pages_async(&url, None).await
        })?;
        
        // Sort by card name and collector number
        cards.sort_by(|a, b| {
            // First sort by card name
            let name_a = a.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let name_b = b.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            
            match name_a.cmp(&name_b) {
                std::cmp::Ordering::Equal => {
                    // If names are equal, sort by collector number
                    let num_a = a.get("collector_number")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let num_b = b.get("collector_number")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    // Try to parse as numbers for proper numeric sorting
                    match (num_a.parse::<u32>(), num_b.parse::<u32>()) {
                        (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
                        _ => num_a.cmp(num_b) // Fall back to string comparison
                    }
                }
                other => other
            }
        });
        
        // Convert sorted JSON objects to Python strings for PyList
        let py_list = PyList::new_bound(py, cards.iter().map(|v| v.to_string()));
        Ok(py_list)
    }
    
    /// Generate cards without limits
    pub fn generate_cards_without_limits(&mut self) -> PyResult<Vec<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let cards = runtime.block_on(async {
            self.get_card_names(Self::CARDS_WITHOUT_LIMITS_URL).await
        })?;
        
        self.cards_without_limits = cards.iter().cloned().collect();
        Ok(cards)
    }
    
    /// Get alchemy cards with spellbooks
    pub fn get_alchemy_cards_with_spellbooks(&self) -> PyResult<Vec<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        Ok(runtime.block_on(async {
            self.get_card_names(Self::CARDS_WITH_ALCHEMY_SPELLBOOK_URL).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Alchemy cards error: {}", e)))?)
    }
    
    /// Get card names in spellbook
    pub fn get_card_names_in_spellbook(&self, card_name: &str) -> PyResult<Vec<String>> {
        let url = Self::SPELLBOOK_SEARCH_URL.replace("{}", card_name);
        let runtime = tokio::runtime::Runtime::new()?;
        Ok(runtime.block_on(async {
            self.get_card_names(&url).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Spellbook cards error: {}", e)))?)
    }
    
    /// Get catalog entry
    pub fn get_catalog_entry(&self, catalog_key: &str) -> PyResult<Vec<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let url = Self::TYPE_CATALOG.replace("{0}", catalog_key);
            match self.download(&url, None).await {
                Ok(data) => {
                    if data.get("object").and_then(|v| v.as_str()) == Some("error") {
                        return Err(ProviderError::NetworkError(format!("Unable to build {}. Not found", catalog_key)));
                    }
                    
                    let empty_vec = vec![];
                    let catalog_data = data.get("data")
                        .and_then(|v| v.as_array())
                        .unwrap_or(&empty_vec);
                    
                    Ok(catalog_data.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.to_string())
                        .collect())
                }
                Err(e) => Err(e),
            }
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Catalog error: {}", e)))
    }
    
    /// Get all Scryfall sets
    pub fn get_all_scryfall_sets(&self) -> PyResult<Vec<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            match self.download(Self::ALL_SETS_URL, None).await {
                Ok(data) => {
                    if data.get("object").and_then(|v| v.as_str()) == Some("error") {
                        return Err(ProviderError::NetworkError("Downloading Scryfall data failed".to_string()));
                    }
                    
                    let empty_vec = vec![];
                    let sets_data = data.get("data")
                        .and_then(|v| v.as_array())
                        .unwrap_or(&empty_vec);
                    
                    let mut set_codes: Vec<String> = sets_data.iter()
                        .filter_map(|set_obj| {
                            set_obj.get("code")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_uppercase())
                        })
                        .collect();
                    
                    // Remove Scryfall token sets (but leave extra sets)
                    let set_codes_clone = set_codes.clone();
                    set_codes.retain(|set_code| {
                        !(set_code.starts_with('T') && 
                          set_codes_clone.contains(&set_code[1..].to_string()))
                    });
                    
                    set_codes.sort();
                    Ok(set_codes)
                }
                Err(e) => Err(e),
            }
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Sets error: {}", e)))
    }
    
    /// Get sets already built (static method)
    #[staticmethod]
    pub fn get_sets_already_built() -> PyResult<Vec<String>> {
        // Placeholder implementation - would need to check filesystem
        Ok(vec![])
    }
    
    /// Get sets to build based on arguments
    pub fn get_sets_to_build(&self, all_sets: bool, sets: Vec<String>, skip_sets: Vec<String>, resume_build: bool) -> PyResult<Vec<String>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let result = runtime.block_on(async {
            let mut final_skip_sets = skip_sets;
            
            if resume_build {
                // Add already built sets to skip list
                let already_built = Self::get_sets_already_built().unwrap_or_default();
                final_skip_sets.extend(already_built);
            }
            
            if !all_sets {
                // Return specific sets minus skipped ones
                let set_set: HashSet<String> = sets.into_iter().collect();
                let skip_set: HashSet<String> = final_skip_sets.into_iter().collect();
                let mut result: Vec<String> = set_set.difference(&skip_set).cloned().collect();
                result.sort();
                return Ok(result);
            }
            
            // Get all Scryfall sets
            let all_scryfall_sets = match self.get_all_scryfall_sets() {
                Ok(sets) => sets,
                Err(_) => return Ok(vec![]),
            };
            
            // Remove token sets and skipped sets
            let non_token_sets: HashSet<String> = all_scryfall_sets.into_iter()
                .filter(|s| !(s.starts_with('T') && all_scryfall_sets.contains(&s[1..].to_string())))
                .collect();
            
            let skip_set: HashSet<String> = final_skip_sets.into_iter().collect();
            let mut result: Vec<String> = non_token_sets.difference(&skip_set).cloned().collect();
            result.sort();
            Ok(result)
        });
        
        result.map_err(|e: Box<dyn std::error::Error>| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Sets to build error: {}", e)))
    }
}

impl ScryfallProvider {
    /// Download all pages from a paginated endpoint
    pub async fn download_all_pages_async(
        &self,
        starting_url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Vec<Value>> {
        let mut all_cards = Vec::new();
        let mut page_downloaded = 1;
        let mut current_url = format!("{}&page={}", starting_url, page_downloaded);
        
        loop {
            self.rate_limiter.wait_if_needed().await;
            
            let response = self.download(&current_url, params.clone()).await?;
            
            if response.get("object").and_then(|v| v.as_str()) == Some("error") {
                let code = response.get("code").and_then(|v| v.as_str()).unwrap_or("");
                if code != "not_found" {
                    eprintln!("Unable to download {}: {:?}", current_url, response);
                }
                break;
            }
            
            let empty_vec = vec![];
            let data_response = response.get("data")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty_vec);
            
            all_cards.extend(data_response.clone());
            
            // Go to the next page, if it exists
            if !response.get("has_more").and_then(|v| v.as_bool()).unwrap_or(false) {
                break;
            }
            
            page_downloaded += 1;
            current_url = format!("{}&page={}", starting_url, page_downloaded);
        }
        
        Ok(all_cards)
    }
    
    /// Get card names from a URL search
    async fn get_card_names(&self, url: &str) -> ProviderResult<Vec<String>> {
        let data = self.download(url, None).await?;
        let empty_vec = vec![];
        let cards = data.get("data")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty_vec);
        
        let names: HashSet<String> = cards.iter()
            .filter_map(|card| {
                card.get("name").and_then(|v| v.as_str()).map(|s| s.to_string())
            })
            .collect();
        
        Ok(names.into_iter().collect())
    }
}

#[async_trait]
impl AbstractProvider for ScryfallProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "ScryfallProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        self.base.headers.clone()
    }
    
    async fn download(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<Value> {
        self.rate_limiter.wait_if_needed().await;
        
        // Retry logic for chunked encoding errors
        for retry in 0..3 {
            match self.base.download_json(url, params.clone()).await {
                Ok(result) => return Ok(result),
                Err(ProviderError::NetworkError(msg)) if msg.contains("chunked") && retry < 2 => {
                    sleep(Duration::from_secs(3 - retry as u64)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        Err(ProviderError::NetworkError("Max retries exceeded".to_string()))
    }
    
    async fn download_raw(
        &self,
        url: &str,
        params: Option<HashMap<String, String>>,
    ) -> ProviderResult<String> {
        self.rate_limiter.wait_if_needed().await;
        self.base.download_text(url, params).await
    }
    
    fn log_download(&self, response: &Response) {
        println!("Downloaded {} (Status: {})", response.url(), response.status());
    }
    
    fn generic_generate_today_price_dict(
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
    ) -> HashMap<String, MtgjsonPricesObject> {
        let mut today_dict: HashMap<String, MtgjsonPricesObject> = HashMap::new();
        
        for data_row in price_data_rows {
            let third_party_id = data_row.get(card_platform_id_key)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            
            if let Some(mtgjson_uuids) = third_party_to_mtgjson.get(&third_party_id) {
                for mtgjson_uuid in mtgjson_uuids {
                    let is_foil = data_row.get(foil_key)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_lowercase() == "true")
                        .unwrap_or(false);
                    
                    let is_etched = etched_key.and_then(|key| {
                        data_row.get(key).and_then(|v| {
                            etched_value.map(|val| v.as_str().unwrap_or("").contains(val))
                        })
                    }).unwrap_or(false);
                    
                    let mut prices = today_dict.entry(mtgjson_uuid.clone())
                        .or_insert_with(|| default_prices_object.clone());
                    
                    // Handle retail prices
                    if let Some(retail_key) = retail_key {
                        let should_skip_retail = retail_quantity_key
                            .and_then(|qty_key| data_row.get(qty_key))
                            .and_then(|v| v.as_f64())
                            .map(|qty| qty == 0.0)
                            .unwrap_or(false);
                        
                        if !should_skip_retail {
                            if let Some(price) = data_row.get(retail_key).and_then(|v| v.as_f64()) {
                                match (is_etched, is_foil) {
                                    (true, _) => prices.sell_etched = Some(price),
                                    (false, true) => prices.sell_foil = Some(price),
                                    (false, false) => prices.sell_normal = Some(price),
                                }
                            }
                        }
                    }
                    
                    // Handle buy prices
                    if let Some(buy_key) = buy_key {
                        let should_skip_buy = buy_quantity_key
                            .and_then(|qty_key| data_row.get(qty_key))
                            .and_then(|v| v.as_f64())
                            .map(|qty| qty == 0.0)
                            .unwrap_or(false);
                        
                        if !should_skip_buy {
                            if let Some(price) = data_row.get(buy_key).and_then(|v| v.as_f64()) {
                                match (is_etched, is_foil) {
                                    (true, _) => prices.buy_etched = Some(price),
                                    (false, true) => prices.buy_foil = Some(price),
                                    (false, false) => prices.buy_normal = Some(price),
                                }
                            }
                        }
                    }
                }
            }
        }
        
        today_dict
    }
}