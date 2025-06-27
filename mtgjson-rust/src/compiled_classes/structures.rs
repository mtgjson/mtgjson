use crate::classes::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// MTGJSON Internal Object for Output Files
/// Rust equivalent of MtgjsonStructuresObject
#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass(name = "MtgjsonStructures")]
pub struct MtgjsonStructures {
    // Main compiled outputs
    #[pyo3(get)]
    pub all_printings: String,
    #[pyo3(get)]
    pub atomic_cards: String,
    #[pyo3(get)]
    pub all_prices: String,
    #[pyo3(get)]
    pub all_prices_today: String,

    // Directory outputs
    #[pyo3(get)]
    pub all_csvs_directory: String,
    #[pyo3(get)]
    pub all_parquets_directory: String,
    #[pyo3(get)]
    pub all_decks_directory: String,
    #[pyo3(get)]
    pub all_sets_directory: String,

    // Compiled files
    #[pyo3(get)]
    pub card_types: String,
    #[pyo3(get)]
    pub compiled_list: String,
    #[pyo3(get)]
    pub deck_list: String,
    #[pyo3(get)]
    pub key_words: String,
    #[pyo3(get)]
    pub enum_values: String,
    #[pyo3(get)]
    pub set_list: String,
    #[pyo3(get)]
    pub referral_database: String,
    #[pyo3(get)]
    pub version: String,
    #[pyo3(get)]
    pub all_identifiers: String,
    #[pyo3(get)]
    pub all_tcgplayer_skus: String,

    // Format-specific outputs
    #[pyo3(get)]
    pub all_printings_standard: String,
    #[pyo3(get)]
    pub all_printings_pioneer: String,
    #[pyo3(get)]
    pub all_printings_modern: String,
    #[pyo3(get)]
    pub all_printings_legacy: String,
    #[pyo3(get)]
    pub all_printings_vintage: String,

    // Atomic format-specific outputs
    #[pyo3(get)]
    pub atomic_cards_standard: String,
    #[pyo3(get)]
    pub atomic_cards_pioneer: String,
    #[pyo3(get)]
    pub atomic_cards_modern: String,
    #[pyo3(get)]
    pub atomic_cards_legacy: String,
    #[pyo3(get)]
    pub atomic_cards_vintage: String,
    #[pyo3(get)]
    pub atomic_cards_pauper: String,
}

#[pymethods]
impl MtgjsonStructures {
    #[new]
    pub fn new() -> Self {
        Self {
            all_printings: "AllPrintings".to_string(),
            atomic_cards: "AtomicCards".to_string(),
            all_prices: "AllPrices".to_string(),
            all_prices_today: "AllPricesToday".to_string(),
            all_csvs_directory: "AllPrintingsCSVFiles".to_string(),
            all_parquets_directory: "AllPrintingsParquetFiles".to_string(),
            all_decks_directory: "AllDeckFiles".to_string(),
            all_sets_directory: "AllSetFiles".to_string(),
            card_types: "CardTypes".to_string(),
            compiled_list: "CompiledList".to_string(),
            deck_list: "DeckList".to_string(),
            key_words: "Keywords".to_string(),
            enum_values: "EnumValues".to_string(),
            set_list: "SetList".to_string(),
            referral_database: "ReferralMap".to_string(),
            version: "Meta".to_string(),
            all_identifiers: "AllIdentifiers".to_string(),
            all_tcgplayer_skus: "TcgplayerSkus".to_string(),
            all_printings_standard: "Standard".to_string(),
            all_printings_pioneer: "Pioneer".to_string(),
            all_printings_modern: "Modern".to_string(),
            all_printings_legacy: "Legacy".to_string(),
            all_printings_vintage: "Vintage".to_string(),
            atomic_cards_standard: "StandardAtomic".to_string(),
            atomic_cards_pioneer: "PioneerAtomic".to_string(),
            atomic_cards_modern: "ModernAtomic".to_string(),
            atomic_cards_legacy: "LegacyAtomic".to_string(),
            atomic_cards_vintage: "VintageAtomic".to_string(),
            atomic_cards_pauper: "PauperAtomic".to_string(),
        }
    }

    /// Get all files that are compiled outputs
    pub fn get_all_compiled_file_names(&self) -> Vec<String> {
        vec![
            self.all_printings.clone(),
            self.atomic_cards.clone(),
            self.all_prices.clone(),
            self.all_prices_today.clone(),
            self.all_csvs_directory.clone(),
            self.all_parquets_directory.clone(),
            self.all_decks_directory.clone(),
            self.all_sets_directory.clone(),
            self.card_types.clone(),
            self.compiled_list.clone(),
            self.deck_list.clone(),
            self.key_words.clone(),
            self.enum_values.clone(),
            self.set_list.clone(),
            self.referral_database.clone(),
            self.version.clone(),
            self.all_identifiers.clone(),
            self.all_tcgplayer_skus.clone(),
            self.all_printings_standard.clone(),
            self.all_printings_pioneer.clone(),
            self.all_printings_modern.clone(),
            self.all_printings_legacy.clone(),
            self.all_printings_vintage.clone(),
            self.atomic_cards_standard.clone(),
            self.atomic_cards_pioneer.clone(),
            self.atomic_cards_modern.clone(),
            self.atomic_cards_legacy.clone(),
            self.atomic_cards_vintage.clone(),
            self.atomic_cards_pauper.clone(),
        ]
    }

    /// Get all files that should appear in CompiledList.json
    pub fn get_compiled_list_files(&self) -> Vec<String> {
        let mut files = self.get_all_compiled_file_names();
        files.retain(|f| f != &self.referral_database);
        files.sort();
        files
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }
}

impl Default for MtgjsonStructures {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObject for MtgjsonStructures {}