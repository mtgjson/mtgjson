use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PySet, PyDict};
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc, NaiveDate};
use std::sync::{Arc, RwLock};
use once_cell::sync::Lazy;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;
use std::time::{Duration, Instant};

use crate::classes::prices::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

#[derive(Debug, Clone)]
struct StandardSetsCache {
    sets: FxHashSet<String>,
    last_updated: Instant,
    ttl: Duration,
}

impl StandardSetsCache {
    fn new(ttl_hours: u64) -> Self {
        Self {
            sets: FxHashSet::default(),
            last_updated: Instant::now() - Duration::from_secs(3600 * 24),
            ttl: Duration::from_secs(3600 * ttl_hours),
        }
    }
    
    fn is_valid(&self) -> bool {
        self.last_updated.elapsed() < self.ttl
    }
    
    fn update(&mut self, sets: FxHashSet<String>) {
        self.sets = sets;
        self.last_updated = Instant::now();
    }
    
    fn get(&self) -> Option<&FxHashSet<String>> {
        if self.is_valid() {
            Some(&self.sets)
        } else {
            None
        }
    }
}

static STANDARD_SETS_CACHE: Lazy<Arc<RwLock<StandardSetsCache>>> = 
    Lazy::new(|| Arc::new(RwLock::new(StandardSetsCache::new(6))));

#[pyclass(name = "WhatsInStandardProvider")]
pub struct WhatsInStandardProvider {
    base: BaseProvider,
    #[pyo3(get)]
    set_codes: PyObject,
}

impl WhatsInStandardProvider {
    const API_ENDPOINT: &'static str = "https://whatsinstandard.com/api/v6/standard.json";
    const RETRY_DELAY_MS: u64 = 5000;
    const MAX_RETRIES: u32 = 3;
    const DATE_FORMAT: &'static str = "%Y-%m-%d";
    
    #[inline(always)]
    fn parse_date_optimized(date_str: &str) -> Option<DateTime<Utc>> {
        if date_str == "9999" {
            return Some(DateTime::<Utc>::from_timestamp(253402300799, 0)?);
        }
        
        if date_str.len() == 10 && date_str.chars().nth(4) == Some('-') && date_str.chars().nth(7) == Some('-') {
            if let Ok(naive_date) = NaiveDate::parse_from_str(date_str, Self::DATE_FORMAT) {
                return naive_date.and_hms_opt(0, 0, 0)?.and_utc().into();
            }
        }
        
        DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", date_str))
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    }
    
    #[inline]
    fn is_set_currently_standard(
        enter_date_str: &str, 
        exit_date_str: &str, 
        now: &DateTime<Utc>
    ) -> bool {
        if exit_date_str == "9999" && enter_date_str != "9999" {
            if let Some(enter_date) = Self::parse_date_optimized(enter_date_str) {
                return enter_date <= *now;
            }
        }
        
        let enter_date = Self::parse_date_optimized(enter_date_str)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let exit_date = Self::parse_date_optimized(exit_date_str)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(253402300799, 0).unwrap());
        
        enter_date <= *now && *now <= exit_date
    }
}

#[pymethods]
impl WhatsInStandardProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = HashMap::new();
        let base = BaseProvider::new("standard".to_string(), headers);
        
        let provider = Self {
            base,
            set_codes: Python::with_gil(|py| {
                PySet::empty_bound(py)
                    .map(|s| s.to_object(py))
                    .unwrap_or_else(|_| py.None())
            }),
        };
        
        Ok(provider)
    }
    
    pub fn standard_legal_set_codes(&self, py: Python) -> PyResult<PyObject> {
        {
            let cache = STANDARD_SETS_CACHE.read().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Cache lock error: {}", e))
            })?;
            
            if let Some(cached_sets) = cache.get() {
                let py_set = PySet::empty_bound(py)?;
                for set_code in cached_sets.iter() {
                    py_set.add(set_code)?;
                }
                return Ok(py_set.to_object(py));
            }
        }
        
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create async runtime: {}", e)
            ))?;
        
        let sets = rt.block_on(async {
            self.standard_legal_set_codes_async().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Standard sets fetch error: {}", e)
        ))?;
        
        {
            let mut cache = STANDARD_SETS_CACHE.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Cache write error: {}", e))
            })?;
            cache.update(sets.clone());
        }
        
        let py_set = PySet::empty_bound(py)?;
        for set_code in sets.iter() {
            py_set.add(set_code)?;
        }
        
        Ok(py_set.to_object(py))
    }
    
    pub fn refresh_cache(&self, py: Python) -> PyResult<PyObject> {
        {
            let mut cache = STANDARD_SETS_CACHE.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Cache lock error: {}", e))
            })?;
            // Force cache invalidation
            cache.last_updated = Instant::now() - Duration::from_secs(3600 * 24);
        }
        
        // Fetch fresh data
        self.standard_legal_set_codes(py)
    }
    
    pub fn is_set_standard_legal(&self, py: Python, set_code: &str) -> PyResult<bool> {
        let standard_sets = self.standard_legal_set_codes(py)?;
        let py_set = standard_sets.downcast_bound::<PySet>(py)?;
        Ok(py_set.contains(set_code.to_uppercase())?)
    }
    
    pub fn get_cache_stats(&self, py: Python) -> PyResult<PyObject> {
        let cache = STANDARD_SETS_CACHE.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Cache lock error: {}", e))
        })?;
        
        let stats_dict = PyDict::new_bound(py);
        stats_dict.set_item("set_count", cache.sets.len())?;
        stats_dict.set_item("is_valid", cache.is_valid())?;
        stats_dict.set_item("age_seconds", cache.last_updated.elapsed().as_secs())?;
        stats_dict.set_item("ttl_seconds", cache.ttl.as_secs())?;
        
        Ok(stats_dict.to_object(py))
    }
}

impl WhatsInStandardProvider {
    async fn standard_legal_set_codes_async(&self) -> ProviderResult<FxHashSet<String>> {
        let mut last_error = None;
        
        for attempt in 0..Self::MAX_RETRIES {
            match self.fetch_standard_sets_attempt().await {
                Ok(sets) => return Ok(sets),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < Self::MAX_RETRIES - 1 {
                        let delay = Duration::from_millis(Self::RETRY_DELAY_MS * (2_u64.pow(attempt)));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| 
            ProviderError::NetworkError("Failed to fetch standard sets after retries".to_string())
        ))
    }
    
    /// Single attempt to fetch standard sets with optimized parsing
    async fn fetch_standard_sets_attempt(&self) -> ProviderResult<FxHashSet<String>> {
        let api_response = self.download(Self::API_ENDPOINT, None).await?;
        
        let sets_array = api_response.get("sets")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ProviderError::ParseError("Missing 'sets' array in API response".to_string()))?;
        
        let now = Utc::now();
        let mut standard_set_codes = FxHashSet::default();
        standard_set_codes.reserve(sets_array.len());
        
        // Process sets with optimized iteration and parsing
        for set_object in sets_array.iter() {
            // Fast field extraction with error tolerance
            let code = set_object.get("code")
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase())
                .filter(|s| !s.is_empty());
            
            let Some(set_code) = code else { continue; };
            
            let enter_date = set_object
                .get("enterDate")
                .and_then(|v| v.get("exact"))
                .and_then(|v| v.as_str())
                .unwrap_or("9999");
            
            let exit_date = set_object
                .get("exitDate")
                .and_then(|v| v.get("exact"))
                .and_then(|v| v.as_str())
                .unwrap_or("9999");
            
            // Ultra-fast standard legality check
            if Self::is_set_currently_standard(enter_date, exit_date, &now) {
                standard_set_codes.insert(set_code);
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
        let mut headers = HashMap::with_capacity(3);
        headers.insert("User-Agent".to_string(), 
                      "MTGJSON/5.0 (https://mtgjson.com)".to_string());
        headers.insert("Accept".to_string(), 
                      "application/json".to_string());
        headers.insert("Accept-Encoding".to_string(), 
                      "gzip, deflate".to_string());
        headers
    }
    
    /// Optimized download with intelligent retry and error handling
    async fn download(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<Value> {
        let headers = self.build_http_header();
        
        let mut retry_count = 0;
        loop {
            let response = self.base.get_request(url, params.clone()).await?;
            
            if response.status().is_success() {
                return response.json().await.map_err(|e| {
                    ProviderError::ParseError(format!("JSON parse error: {}", e))
                });
            }
            
            // Handle rate limiting and server errors intelligently
            let status_code = response.status().as_u16();
            let should_retry = match status_code {
                429 | 502 | 503 | 504 => true, // Rate limit, bad gateway, service unavailable, timeout
                500..=599 => retry_count < 2,   // Server errors - limited retries
                _ => false,                     // Client errors - don't retry
            };
            
            if !should_retry || retry_count >= Self::MAX_RETRIES {
                let error_text = response.text().await.unwrap_or_default();
                return Err(ProviderError::NetworkError(format!(
                    "HTTP {} error after {} retries: {}", 
                    status_code, retry_count, error_text
                )));
            }
            
            retry_count += 1;
            let delay = Duration::from_millis(Self::RETRY_DELAY_MS * (2_u64.pow(retry_count - 1)));
            tokio::time::sleep(delay).await;
        }
    }
    
    /// Fast raw text download with minimal overhead
    async fn download_raw(&self, url: &str, params: Option<HashMap<String, String>>) -> ProviderResult<String> {
        self.base.download_text(url, params).await
    }
    
    /// Optimized logging with minimal performance impact
    fn log_download(&self, response: &Response) {
        if cfg!(debug_assertions) {
            println!("WhatsInStandard: {} ({})", response.url(), response.status());
        }
    }
    
    /// Placeholder implementation for price-related functionality
    /// (WhatsInStandardProvider focuses on legality, not pricing)
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
        HashMap::new() // Not applicable for standard legality provider
    }
}

impl Default for WhatsInStandardProvider {
    fn default() -> Self {
        Self::new().expect("Failed to create WhatsInStandardProvider")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    
    #[test]
    fn test_date_parsing_optimization() {
        // Test fast path
        let date1 = WhatsInStandardProvider::parse_date_optimized("2023-10-15");
        assert!(date1.is_some());
        
        // Test special case
        let date2 = WhatsInStandardProvider::parse_date_optimized("9999");
        assert!(date2.is_some());
        
        // Test invalid date
        let date3 = WhatsInStandardProvider::parse_date_optimized("invalid");
        assert!(date3.is_none());
    }
    
    #[test]
    fn test_standard_legality_check() {
        let now = Utc::now();
        
        // Test currently standard set
        assert!(WhatsInStandardProvider::is_set_currently_standard(
            "2023-01-01", "9999", &now
        ));
        
        // Test expired set
        assert!(!WhatsInStandardProvider::is_set_currently_standard(
            "2020-01-01", "2022-01-01", &now
        ));
        
        // Test future set
        assert!(!WhatsInStandardProvider::is_set_currently_standard(
            "2030-01-01", "9999", &now
        ));
    }
    
    #[tokio::test]
    async fn test_cache_functionality() {
        let mut cache = StandardSetsCache::new(1);
        
        // Initially invalid
        assert!(cache.get().is_none());
        
        // Update cache
        let mut test_sets = FxHashSet::default();
        test_sets.insert("DOM".to_string());
        cache.update(test_sets);
        
        // Now valid
        assert!(cache.get().is_some());
        assert!(cache.get().unwrap().contains("DOM"));
    }
    
    #[test]
    fn test_python_api_compatibility() {
        Python::with_gil(|py| {
            let provider = WhatsInStandardProvider::new().unwrap();
            
            // Test basic functionality
            let result = provider.standard_legal_set_codes(py);
            assert!(result.is_ok());
            
            // Test cache stats
            let stats = provider.get_cache_stats(py);
            assert!(stats.is_ok());
            
            // Test specific set check
            let is_legal = provider.is_set_standard_legal(py, "DOM");
            assert!(is_legal.is_ok());
        });
    }
    
    #[bench]
    fn bench_date_parsing(b: &mut test::Bencher) {
        b.iter(|| {
            WhatsInStandardProvider::parse_date_optimized("2023-10-15")
        });
    }
    
    #[bench] 
    fn bench_standard_check(b: &mut test::Bencher) {
        let now = Utc::now();
        b.iter(|| {
            WhatsInStandardProvider::is_set_currently_standard(
                "2023-01-01", "9999", &now
            )
        });
    }
}