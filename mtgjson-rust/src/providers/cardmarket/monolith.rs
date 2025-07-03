use crate::classes::MtgjsonPricesObject;
use crate::providers::{AbstractProvider, BaseProvider, ProviderResult};
use async_trait::async_trait;
use log::{error, info, warn};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::{PyObject, PyResult};
use reqwest::{Client, Response};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;

/// CardMarket API provider for MTG price data
#[pyclass(name = "CardMarketProvider")]
pub struct CardMarketProvider {
    base: BaseProvider,
    set_map: HashMap<String, HashMap<String, Value>>,
    price_guide_url: String,
    connection_available: bool,
    client: Client,
    today_date: String,
}

#[pymethods]
impl CardMarketProvider {
    #[new]
    #[pyo3(signature = (headers=None, init_map=None))]
    pub fn new(headers: Option<HashMap<String, String>>, init_map: Option<bool>) -> PyResult<Self> {
        let headers: HashMap<String, String> = headers.unwrap_or_default();
        let base: BaseProvider = BaseProvider::new("mkm".to_string(), headers);
        let init_map: bool = init_map.unwrap_or(true);

        // Check for CardMarket configuration
        let has_cardmarket_config = Self::check_cardmarket_config();

        if !has_cardmarket_config {
            warn!("CardMarket config section not established. Skipping requests");
            return Ok(CardMarketProvider {
                base,
                set_map: HashMap::new(),
                price_guide_url: String::new(),
                connection_available: false,
                client: Client::new(),
                today_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            });
        }

        // Read configuration and set environment variables
        let price_guide_url =
            Self::get_config_value("CardMarket", "prices_api_url").unwrap_or_default();

        Self::setup_environment_variables();

        // Validate required environment variables
        if !Self::validate_credentials() {
            warn!("CardMarket keys values missing. Skipping requests");
            return Ok(CardMarketProvider {
                base,
                set_map: HashMap::new(),
                price_guide_url,
                connection_available: false,
                client: Client::new(),
                today_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            });
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create HTTP client: {}",
                    e
                ))
            })?;

        let mut provider = CardMarketProvider {
            base,
            set_map: HashMap::new(),
            price_guide_url,
            connection_available: true,
            client,
            today_date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
        };

        if init_map {
            provider.init_set_map()?;
        }

        Ok(provider)
    }

    /// Download from CardMarket JSON APIs
    pub fn download(
        &self,
        py: Python,
        url: String,
        params: Option<HashMap<String, String>>,
    ) -> PyResult<PyObject> {
        if !self.connection_available {
            let empty_dict = pyo3::types::PyDict::new_bound(py);
            return Ok(empty_dict.into());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let mut request_builder = self.client.get(&url);

            if let Some(params) = params {
                request_builder = request_builder.query(&params);
            }

            match request_builder.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<Value>().await {
                            Ok(json) => json,
                            Err(e) => {
                                error!("JSON parse error for {}: {}", url, e);
                                Value::Object(Map::new())
                            }
                        }
                    } else {
                        error!(
                            "Error downloading CardMarket Data: {} --- {}",
                            response.status(),
                            url
                        );
                        Value::Object(Map::new())
                    }
                }
                Err(e) => {
                    error!("Error downloading CardMarket Data from {}: {}", url, e);
                    Value::Object(Map::new())
                }
            }
        });

        // Convert serde_json::Value to PyObject
        let json_str = serde_json::to_string(&result).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "JSON serialization error: {}",
                e
            ))
        })?;
        let json_module = pyo3::types::PyModule::import_bound(py, "json")?;
        let py_obj = json_module.call_method1("loads", (json_str,))?;
        Ok(py_obj.into())
        
    }

    /// Generate a single-day price structure from Card Market
    pub fn generate_today_price_dict(
        &self,
        all_printings_path: String,
    ) -> PyResult<HashMap<String, MtgjsonPricesObject>> {
        if !self.connection_available {
            return Ok(HashMap::new());
        }

        // TODO: Implement generate_entity_mapping equivalent
        // For now, using placeholder mappings
        let mtgjson_finish_map: HashMap<String, Vec<String>> = HashMap::new();
        let mtgjson_id_map: HashMap<String, Vec<String>> = HashMap::new();

        info!("Building CardMarket retail data");

        let price_data = self.get_card_market_data()?;

        let mut today_dict = HashMap::new();

        for (product_id, price_entities) in price_data {
            let avg_sell_price = price_entities.get("trend").and_then(|v| *v);
            let avg_foil_price = price_entities.get("trend-foil").and_then(|v| *v);

            if let Some(mtgjson_uuids) = mtgjson_id_map.get(&product_id) {
                for mtgjson_uuid in mtgjson_uuids {
                    if !today_dict.contains_key(mtgjson_uuid) {
                        if avg_sell_price.is_none() && avg_foil_price.is_none() {
                            continue;
                        }

                        today_dict.insert(
                            mtgjson_uuid.clone(),
                            MtgjsonPricesObject {
                                currency: "EUR".to_string(),
                                date: self.today_date.clone(),
                                provider: "cardmarket".to_string(),
                                source: "paper".to_string(),
                                sell_normal: None,
                                sell_foil: None,
                                sell_etched: None,
                                buy_normal: None,
                                buy_foil: None,
                                buy_etched: None,
                            },
                        );
                    }

                    if let Some(entry) = today_dict.get_mut(mtgjson_uuid) {
                        if let Some(price) = avg_sell_price {
                            entry.sell_normal = Some(price);
                        }

                        if let Some(foil_price) = avg_foil_price {
                            if mtgjson_finish_map
                                .get(&product_id)
                                .map_or(false, |finishes| finishes.contains(&"etched".to_string()))
                            {
                                entry.sell_etched = Some(foil_price);
                            } else {
                                entry.sell_foil = Some(foil_price);
                            }
                        }
                    }
                }
            }
        }

        Ok(today_dict)
    }

    /// Get MKM Set ID from pre-generated map
    pub fn get_set_id(&self, set_name: String) -> PyResult<Option<i32>> {
        if self.set_map.is_empty() {
            return Ok(None);
        }

        if let Some(set_data) = self.set_map.get(&set_name.to_lowercase()) {
            if let Some(mcm_id) = set_data.get("mcmId") {
                if let Some(id) = mcm_id.as_i64() {
                    return Ok(Some(id as i32));
                }
            }
        }
        Ok(None)
    }

    /// Get "Extras" MKM Set ID from pre-generated map
    pub fn get_extras_set_id(&self, set_name: String) -> PyResult<Option<i32>> {
        if self.set_map.is_empty() {
            return Ok(None);
        }

        let extras_set_name = format!("{}: extras", set_name.to_lowercase());
        if let Some(set_data) = self.set_map.get(&extras_set_name) {
            if let Some(mcm_id) = set_data.get("mcmId") {
                if let Some(id) = mcm_id.as_i64() {
                    return Ok(Some(id as i32));
                }
            }
        }
        Ok(None)
    }

    /// Get MKM Set Name from pre-generated map
    pub fn get_set_name(&self, set_name: String) -> PyResult<Option<String>> {
        if self.set_map.is_empty() {
            return Ok(None);
        }

        if let Some(set_data) = self.set_map.get(&set_name.to_lowercase()) {
            if let Some(mcm_name) = set_data.get("mcmName") {
                if let Some(name) = mcm_name.as_str() {
                    return Ok(Some(name.to_string()));
                }
            }
        }
        Ok(None)
    }

    /// Build HTTP header (not used, returns empty dict)
    #[pyo3(signature = ())]
    pub fn _build_http_header(&self) -> PyResult<HashMap<String, String>> {
        Ok(HashMap::new())
    }

    /// Get MKM cards for a set with retry logic
    pub fn get_mkm_cards(&self, mcm_id: Option<i32>) -> PyResult<PyObject> {
        if !self.connection_available {
            return Python::with_gil(|py| {
                let empty_dict = pyo3::types::PyDict::new_bound(py);
                Ok(empty_dict.into())
            });
        } else if mcm_id.is_none() {
            return Python::with_gil(|py| {
                let empty_dict = pyo3::types::PyDict::new_bound(py);
                Ok(empty_dict.into())
            });
        }
        let mcm_id = mcm_id.unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            // Retry logic - try up to 5 times with delays
            for attempt in 0..5 {
                match self.fetch_mkm_cards(mcm_id).await {
                    Ok(cards) => return cards,
                    Err(e) => {
                        warn!(
                            "MKM connection error for set {}, attempt {}: {}",
                            mcm_id,
                            attempt + 1,
                            e
                        );
                        if attempt < 4 {
                            sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
            }

            error!(
                "MKM had a critical failure after 5 attempts. Skipping set {}",
                mcm_id
            );
            HashMap::new()
        });

        // Convert HashMap<String, Vec<Value>> to PyObject
        Python::with_gil(|py| {
            let py_dict = pyo3::types::PyDict::new_bound(py);
            for (key, values) in result {
                let py_list = PyList::empty_bound(py);
                for value in values {
                    let json_str = serde_json::to_string(&value).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "JSON serialization error: {}",
                            e
                        ))
                    })?;
                    let json_module = pyo3::types::PyModule::import_bound(py, "json")?;
                    let py_obj = json_module.call_method1("loads", (json_str,))?;
                    py_list.append(py_obj)?;
                }
                py_dict.set_item(key, py_list)?;
            }
            Ok(py_dict.into())
        })
    }

    /// Check if MKM config section exists
    pub fn has_mkm_config(&self) -> bool {
        // In a real implementation, this would check MtgjsonConfig
        // For now, check environment variables as a fallback
        std::env::var("MKM_ACCESS_TOKEN").is_ok()
            && std::env::var("MKM_ACCESS_TOKEN_SECRET").is_ok()
            && std::env::var("MKM_APP_TOKEN").is_ok()
            && std::env::var("MKM_APP_SECRET").is_ok()
    }

    /// Read MKM configuration from config file or environment
    pub fn read_mkm_config(&self) -> HashMap<String, String> {
        let mut config = HashMap::new();

        // Try to read from environment variables first
        if let Ok(access_token) = std::env::var("MKM_ACCESS_TOKEN") {
            config.insert("access_token".to_string(), access_token);
        }
        if let Ok(access_token_secret) = std::env::var("MKM_ACCESS_TOKEN_SECRET") {
            config.insert("access_token_secret".to_string(), access_token_secret);
        }
        if let Ok(app_token) = std::env::var("MKM_APP_TOKEN") {
            config.insert("app_token".to_string(), app_token);
        }
        if let Ok(app_secret) = std::env::var("MKM_APP_SECRET") {
            config.insert("app_secret".to_string(), app_secret);
        }

        // In a real implementation, would also read from MtgjsonConfig file
        // if environment variables are not available

        config
    }

    /// Get MKM expansion data from API
    pub fn get_mkm_expansion_data(&self) -> PyResult<PyObject> {
        if !self.has_mkm_config() {
            eprintln!("Warning: MKM configuration not found");
            return Python::with_gil(|py| {
                let empty_list = PyList::empty_bound(py);
                Ok(empty_list.into())
            });
        }

        // In a real implementation, this would make authenticated API calls to CardMarket
        // using OAuth 1.0a authentication with the configured tokens
        let expansions_url = "https://api.cardmarket.com/ws/v2.0/expansions";

        // For now, return empty array but log the attempt
        println!("Would call MKM API: {}", expansions_url);
        Python::with_gil(|py| {
            let empty_list = PyList::empty_bound(py);
            Ok(empty_list.into())
        })
    }

    /// Get MKM expansion singles for a specific expansion
    pub fn get_mkm_expansion_singles(&self, expansion_id: i32) -> PyResult<PyObject> {
        if !self.has_mkm_config() {
            eprintln!("Warning: MKM configuration not found");
            return Python::with_gil(|py| {
                let empty_list = PyList::empty_bound(py);
                Ok(empty_list.into())
            });
        }

        // In a real implementation, this would make authenticated API calls
        let singles_url = format!(
            "https://api.cardmarket.com/ws/v2.0/expansions/{}/singles",
            expansion_id
        );

        // For now, return empty array but log the attempt
        println!("Would call MKM API: {}", singles_url);
        Python::with_gil(|py| {
            let empty_list = PyList::empty_bound(py);
            Ok(empty_list.into())
        })
    }
}

impl CardMarketProvider {
    /// Check if CardMarket configuration section exists
    fn check_cardmarket_config() -> bool {
        std::env::var("MKM_APP_TOKEN").is_ok()
            || std::env::var("MTGJSON_CARDMARKET_APP_TOKEN").is_ok()
    }

    /// Get configuration value
    fn get_config_value(section: &str, key: &str) -> Option<String> {
        let env_key = format!("MTGJSON_{}_{}", section.to_uppercase(), key.to_uppercase());
        std::env::var(env_key).ok()
    }

    /// Setup environment variables from configuration
    fn setup_environment_variables() {
        if let Ok(app_token) = std::env::var("MTGJSON_CARDMARKET_APP_TOKEN") {
            std::env::set_var("MKM_APP_TOKEN", app_token);
        }
        if let Ok(app_secret) = std::env::var("MTGJSON_CARDMARKET_APP_SECRET") {
            std::env::set_var("MKM_APP_SECRET", app_secret);
        }
        if let Ok(access_token) = std::env::var("MTGJSON_CARDMARKET_ACCESS_TOKEN") {
            std::env::set_var("MKM_ACCESS_TOKEN", access_token);
        }
        if let Ok(access_secret) = std::env::var("MTGJSON_CARDMARKET_ACCESS_TOKEN_SECRET") {
            std::env::set_var("MKM_ACCESS_TOKEN_SECRET", access_secret);
        }
    }

    /// Validate that required credentials are available
    fn validate_credentials() -> bool {
        std::env::var("MKM_APP_TOKEN").is_ok() && std::env::var("MKM_APP_SECRET").is_ok()
    }

    /// Use new MKM API to get MTG card prices
    fn get_card_market_data(&self) -> PyResult<HashMap<String, HashMap<String, Option<f64>>>> {
        if self.price_guide_url.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Unable to get CardMarket data: No price URL set",
            ));
        }

        let data = Python::with_gil(|py| self.download(py, self.price_guide_url.clone(), None))?;

        // Convert PyObject back to serde_json::Value for processing
        let json_str = Python::with_gil(|py| {
            let json_module: Bound<'_, PyModule> = pyo3::types::PyModule::import(py, "json")?;
            json_module
                .call_method1("dumps", (data,))?
                .extract::<String>()
        })?;

        let json_value: Value = serde_json::from_str(&json_str).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON parsing error: {}", e))
        })?;

        let price_guides = json_value
            .get("priceGuides")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "No priceGuides array found in response",
                )
            })?;

        let mut price_data = HashMap::new();

        for mkm_entry in price_guides {
            if let Some(entry_obj) = mkm_entry.as_object() {
                if let Some(product_id) = entry_obj.get("idProduct") {
                    let id_str = match product_id {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        _ => continue,
                    };

                    let trend = entry_obj.get("trend").and_then(|v| v.as_f64());
                    let trend_foil = entry_obj.get("trend-foil").and_then(|v| v.as_f64());

                    let mut price_entry = HashMap::new();
                    price_entry.insert("trend".to_string(), trend);
                    price_entry.insert("trend-foil".to_string(), trend_foil);

                    price_data.insert(id_str, price_entry);
                }
            }
        }

        Ok(price_data)
    }

    /// Construct a mapping for all set components from MKM
    fn init_set_map(&mut self) -> PyResult<()> {
        if !self.connection_available {
            return Ok(());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let expansions_url =
                "https://api.cardmarket.com/ws/v2.0/expansions/1/singles".to_string();

            match Python::with_gil(|py| self.download(py, expansions_url.clone(), None)) {
                Ok(response) => {
                    // Convert PyObject back to serde_json::Value for processing
                    let json_str = Python::with_gil(|py| {
                        let json_module: Bound<'_, PyModule> =
                            pyo3::types::PyModule::import(py, "json")?;
                        json_module
                            .call_method1("dumps", (response,))?
                            .extract::<String>()
                    })
                    .unwrap_or_else(|_| "{}".to_string());

                    let json_value: Value = serde_json::from_str(&json_str)
                        .unwrap_or_else(|_| Value::Object(Map::new()));

                    if let Some(expansions) = json_value.get("expansion").and_then(|v| v.as_array())
                    {
                        for set_content in expansions {
                            if let Some(set_obj) = set_content.as_object() {
                                if let (Some(name), Some(id)) = (
                                    set_obj.get("enName").and_then(|v| v.as_str()),
                                    set_obj.get("idExpansion").and_then(|v| v.as_i64()),
                                ) {
                                    let mut set_data = HashMap::new();
                                    set_data.insert(
                                        "mcmId".to_string(),
                                        Value::Number(serde_json::Number::from(id)),
                                    );
                                    set_data.insert(
                                        "mcmName".to_string(),
                                        Value::String(name.to_string()),
                                    );

                                    self.set_map.insert(name.to_lowercase(), set_data);
                                }
                            }
                        }
                    }

                    // Apply manual overrides
                    self.apply_manual_overrides();
                }
                Err(e) => {
                    error!("Unable to download MKM expansions: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Apply manual set name overrides from configuration
    fn apply_manual_overrides(&mut self) {
        // TODO: Load from mkm_set_name_fixes.json resource file
        // For now, use some common overrides as examples
        let manual_overrides = vec![
            ("ravnica allegiance", "ravnica allegiance"),
            ("guilds of ravnica", "guilds of ravnica"),
        ];

        for (old_name, new_name) in manual_overrides {
            if let Some(set_data) = self.set_map.remove(old_name) {
                self.set_map.insert(new_name.to_string(), set_data);
            }
        }
    }

    /// Fetch MKM cards for a specific set
    async fn fetch_mkm_cards(&self, mcm_id: i32) -> Result<HashMap<String, Vec<Value>>, String> {
        // TODO: Implement actual MKM API call for expansion singles
        // For now, return empty result
        info!("Would fetch cards for MKM set ID: {}", mcm_id);
        Ok(HashMap::new())
    }

    /// Load MKM set name fixes from resource file
    fn load_mkm_set_name_fixes() -> HashMap<String, String> {
        let resource_path = std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join("mtgjson5")
            .join("resources")
            .join("mkm_set_name_fixes.json");

        match std::fs::read_to_string(&resource_path) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_else(|e| {
                eprintln!("Warning: Failed to parse mkm_set_name_fixes.json: {}", e);
                HashMap::new()
            }),
            Err(e) => {
                eprintln!("Warning: Failed to read mkm_set_name_fixes.json: {}", e);
                HashMap::new()
            }
        }
    }
}

#[async_trait]
impl AbstractProvider for CardMarketProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }

    fn get_class_name(&self) -> &str {
        "CardMarketProvider"
    }

    fn build_http_header(&self) -> HashMap<String, String> {
        self._build_http_header().unwrap_or_default()
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
        println!(
            "Downloaded {} (Status: {})",
            response.url(),
            response.status()
        );
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
        // CardMarket has its own price generation logic
        HashMap::new()
    }
}
