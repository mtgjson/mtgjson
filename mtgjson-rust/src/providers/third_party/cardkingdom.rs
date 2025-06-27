use async_trait::async_trait;
use pyo3::prelude::*;
use reqwest::Response;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use regex::Regex;
use crate::classes::{MtgjsonPricesObject, MtgjsonSealedProductObject};
use crate::providers::{AbstractProvider, BaseProvider, ProviderError, ProviderResult};

#[pyclass(name = "CardKingdomProvider")]
pub struct CardKingdomProvider {
    base: BaseProvider,
}

impl CardKingdomProvider {
    const API_URL: &'static str = "https://api.cardkingdom.com/api/pricelist";
    const SEALED_URL: &'static str = "https://api.cardkingdom.com/api/sealed_pricelist";
}

#[pymethods]
impl CardKingdomProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let headers = HashMap::new(); // No special headers needed for Card Kingdom
        let base = BaseProvider::new("ck".to_string(), headers);
        
        Ok(Self { base })
    }
    
    /// Strip sealed product names for easier comparison
    #[staticmethod]
    pub fn strip_sealed_name(product_name: &str) -> String {
        let re_special = Regex::new(r"[^\w\s]").unwrap();
        let re_spaces = Regex::new(r" +").unwrap();
        
        let name = product_name.trim();
        let name = re_special.replace_all(name, "");
        let name = re_spaces.replace_all(&name, " ");
        
        name.to_lowercase()
    }
    
    /// Generate today's price dictionary
    pub fn generate_today_price_dict(&self, all_printings_path: &str) -> PyResult<HashMap<String, MtgjsonPricesObject>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.generate_today_price_dict_async(all_printings_path).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Price dict error: {}", e)))
    }
    
    /// Update sealed URLs for sealed products
    pub fn update_sealed_urls(&self, mut sealed_products: Vec<MtgjsonSealedProductObject>) -> PyResult<Vec<MtgjsonSealedProductObject>> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            self.update_sealed_urls_async(&mut sealed_products).await?;
            Ok(sealed_products)
        }).map_err(|e: ProviderError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Sealed URLs error: {}", e)))
    }
}

impl CardKingdomProvider {
    /// Generate today's price dictionary (async version)
    async fn generate_today_price_dict_async(&self, all_printings_path: &str) -> ProviderResult<HashMap<String, MtgjsonPricesObject>> {
        let api_response = self.base.download_json(Self::API_URL, None).await?;
        let price_data_rows = api_response.get("data")
            .and_then(|v| v.as_array())
            .unwrap_or(&Vec::<Value>::new())
            .clone();
        
        // Build mapping from Card Kingdom IDs to MTGJSON UUIDs
        let mut card_kingdom_id_to_mtgjson = self.generate_entity_mapping(
            all_printings_path,
            &["identifiers", "cardKingdomId"],
            &["uuid"],
        ).await?;
        
        // Add foil IDs
        let foil_mapping = self.generate_entity_mapping(
            all_printings_path,
            &["identifiers", "cardKingdomFoilId"],
            &["uuid"],
        ).await?;
        card_kingdom_id_to_mtgjson.extend(foil_mapping);
        
        // Add etched IDs
        let etched_mapping = self.generate_entity_mapping(
            all_printings_path,
            &["identifiers", "cardKingdomEtchedId"],
            &["uuid"],
        ).await?;
        card_kingdom_id_to_mtgjson.extend(etched_mapping);
        
        let default_prices_obj = MtgjsonPricesObject {
            currency: "USD".to_string(),
            date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            provider: "cardkingdom".to_string(),
            source: "paper".to_string(),
            buy_normal: None,
            buy_foil: None,
            buy_etched: None,
            sell_normal: None,
            sell_foil: None,
            sell_etched: None,
        };
        
        println!("Building CardKingdom buylist & retail data");
        Ok(self.generic_generate_today_price_dict(
            &card_kingdom_id_to_mtgjson,
            &price_data_rows,
            "id",
            &default_prices_obj,
            "is_foil",
            Some("price_retail"),
            Some("qty_retail"),
            Some("price_buy"),
            Some("qty_buying"),
            Some("variation"),
            Some("Foil Etched"),
        ))
    }
    
    /// Update sealed URLs (async version)
    async fn update_sealed_urls_async(&self, sealed_products: &mut Vec<MtgjsonSealedProductObject>) -> ProviderResult<()> {
        let api_data = self.base.download_json(Self::SEALED_URL, None).await?;
        
        for product in sealed_products {
            if let Some(ref identifiers) = product.identifiers {
                if let Some(card_kingdom_id) = &identifiers.card_kingdom_id {
                    if let Some(data_array) = api_data.get("data").and_then(|v| v.as_array()) {
                        for remote_product in data_array {
                            if let Some(remote_id) = remote_product.get("id").and_then(|v| v.as_str()) {
                                if remote_id == card_kingdom_id {
                                    if let (Some(base_url), Some(url_path)) = (
                                        api_data.get("meta")
                                            .and_then(|v| v.get("base_url"))
                                            .and_then(|v| v.as_str()),
                                        remote_product.get("url").and_then(|v| v.as_str())
                                    ) {
                                        let referral = "?partner=MTGJSONAffiliate&utm_source=MTGJSONAffiliate&utm_medium=affiliate&utm_campaign=MTGJSONAffiliate";
                                        let full_url = format!("{}{}{}", base_url, url_path, referral);
                                        let mut purchase_urls = crate::classes::MtgjsonPurchaseUrls::new();
                                        purchase_urls.card_kingdom = Some(full_url);
                                        product.raw_purchase_urls = Some(purchase_urls);
                                    }
                                    break;
                                }
                            }
                        }
                    } else {
                        if let Some(ref name) = product.name {
                            println!("No Card Kingdom URL found for product {}", name);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Generate entity mapping from all printings file
    async fn generate_entity_mapping(
        &self,
        all_printings_path: &str,
        id_path: &[&str],
        uuid_path: &[&str],
    ) -> ProviderResult<HashMap<String, HashSet<String>>> {
        let mut dump_map: HashMap<String, HashSet<String>> = HashMap::new();
        
        // Read and parse the all printings JSON file
        let content = match tokio::fs::read_to_string(all_printings_path).await {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Warning: Failed to read all printings file {}: {}", all_printings_path, e);
                return Ok(dump_map);
            }
        };
        
        let all_printings: Value = match serde_json::from_str(&content) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Warning: Failed to parse all printings JSON: {}", e);
                return Ok(dump_map);
            }
        };
        
        // Get all entities (sets and their cards)
        if let Some(data_obj) = all_printings.get("data").and_then(|v| v.as_object()) {
            for (_set_code, set_data) in data_obj {
                if let Some(cards_array) = set_data.get("cards").and_then(|v| v.as_array()) {
                    for card in cards_array {
                        self.extract_mapping_from_entity(card, id_path, uuid_path, &mut dump_map);
                    }
                }
                
                // Also check tokens if present
                if let Some(tokens_array) = set_data.get("tokens").and_then(|v| v.as_array()) {
                    for token in tokens_array {
                        self.extract_mapping_from_entity(token, id_path, uuid_path, &mut dump_map);
                    }
                }
            }
        }
        
        println!("Generated {} mappings from {}", dump_map.len(), all_printings_path);
        Ok(dump_map)
    }
    
    /// Extract mapping from a single entity (card/token)
    fn extract_mapping_from_entity(
        &self,
        entity: &Value,
        id_path: &[&str],
        uuid_path: &[&str],
        dump_map: &mut HashMap<String, HashSet<String>>,
    ) {
        // Navigate to the ID using the path
        let mut current = entity;
        for component in id_path {
            if let Some(next) = current.get(component) {
                current = next;
            } else {
                return; // Path doesn't exist
            }
        }
        
        // Extract the ID value
        let id_value = match current {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return, // Invalid ID type
        };
        
        // Navigate to the UUID using the path
        let mut current = entity;
        for component in uuid_path {
            if let Some(next) = current.get(component) {
                current = next;
            } else {
                return; // Path doesn't exist
            }
        }
        
        // Extract the UUID value
        let uuid_value = match current {
            Value::String(s) => s.clone(),
            Value::Array(arr) => {
                // Handle array of UUIDs
                for uuid_val in arr {
                    if let Value::String(uuid_str) = uuid_val {
                        dump_map.entry(id_value.clone()).or_insert_with(HashSet::new).insert(uuid_str.clone());
                    }
                }
                return;
            }
            _ => return, // Invalid UUID type
        };
        
        // Add the mapping
        dump_map.entry(id_value).or_insert_with(HashSet::new).insert(uuid_value);
    }
}

#[async_trait]
impl AbstractProvider for CardKingdomProvider {
    fn get_class_id(&self) -> &str {
        &self.base.class_id
    }
    
    fn get_class_name(&self) -> &str {
        "CardKingdomProvider"
    }
    
    fn build_http_header(&self) -> HashMap<String, String> {
        HashMap::new() // No special headers needed
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
                    
                    let prices = today_dict.entry(mtgjson_uuid.clone())
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