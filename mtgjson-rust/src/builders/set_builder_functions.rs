use crate::classes::card::MtgjsonCardObject;
use crate::builders::set_builder::{
    parse_card_types as inner_parse_card_types,
    get_card_colors as inner_get_card_colors,
    get_card_cmc as inner_get_card_cmc,
    is_number as inner_is_number,
    parse_legalities as inner_parse_legalities,
    build_mtgjson_set as inner_build_mtgjson_set,
    parse_foreign as inner_parse_foreign,
    parse_printings as inner_parse_printings,
    parse_rulings as inner_parse_rulings,
    mark_duel_decks as inner_mark_duel_decks,
    enhance_cards_with_metadata as inner_enhance_cards_with_metadata,
    build_base_mtgjson_cards as inner_build_base_mtgjson_cards,
};
use crate::classes::foreign_data::MtgjsonForeignDataObject;
use crate::classes::legalities::MtgjsonLegalitiesObject;
use crate::classes::rulings::MtgjsonRulingObject;
use crate::classes::set::MtgjsonSetObject;
use pyo3::prelude::*;
use std::collections::HashMap;

/// Parse card types into super types, types, and subtypes (PyO3 wrapper)
#[pyfunction]
pub fn parse_card_types(card_type: &str) -> PyResult<(Vec<String>, Vec<String>, Vec<String>)> {
    Ok(inner_parse_card_types(card_type))
}

/// Get card colors from mana cost (PyO3 wrapper)
#[pyfunction]
pub fn get_card_colors(mana_cost: &str) -> PyResult<Vec<String>> {
    Ok(inner_get_card_colors(mana_cost))
}

/// Get card's converted mana cost from mana cost string (PyO3 wrapper)
#[pyfunction]
pub fn get_card_cmc(mana_cost: &str) -> PyResult<f64> {
    Ok(inner_get_card_cmc(mana_cost))
}

/// Check if a string represents a number (PyO3 wrapper)
#[pyfunction]
pub fn is_number(string: &str) -> PyResult<bool> {
    Ok(inner_is_number(string))
}

/// Parse legalities from Scryfall format to MTGJSON format (PyO3 wrapper)
#[pyfunction]
pub fn parse_legalities(sf_card_legalities: HashMap<String, String>) -> PyResult<MtgjsonLegalitiesObject> {
    Ok(inner_parse_legalities(&sf_card_legalities))
}

/// Build MTGJSON set from set code (PyO3 wrapper)
#[pyfunction]
pub fn build_mtgjson_set(set_code: &str) -> PyResult<Option<MtgjsonSetObject>> {
    Ok(inner_build_mtgjson_set(set_code))
}

/// Parse foreign card data from Scryfall prints URL (PyO3 wrapper)
#[pyfunction]
pub fn parse_foreign(
    sf_prints_url: &str,
    card_name: &str,
    card_number: &str,
    set_name: &str,
) -> PyResult<Vec<MtgjsonForeignDataObject>> {
    Ok(inner_parse_foreign(sf_prints_url, card_name, card_number, set_name))
}

/// Parse printings from Scryfall prints URL (PyO3 wrapper)
#[pyfunction]
#[pyo3(signature = (sf_prints_url = None))]
pub fn parse_printings(sf_prints_url: Option<&str>) -> PyResult<Vec<String>> {
    Ok(inner_parse_printings(sf_prints_url))
}

/// Parse rulings from Scryfall URL (PyO3 wrapper)
#[pyfunction]
pub fn parse_rulings(rulings_url: &str) -> PyResult<Vec<MtgjsonRulingObject>> {
    Ok(inner_parse_rulings(rulings_url))
}

/// Mark duel deck assignments for cards (PyO3 wrapper)
#[pyfunction]
pub fn mark_duel_decks(set_code: &str, mtgjson_cards: Vec<MtgjsonCardObject>) -> PyResult<Vec<MtgjsonCardObject>> {
    let mut cards = mtgjson_cards;
    inner_mark_duel_decks(set_code, &mut cards);
    Ok(cards)
}

/// Enhance cards with additional metadata (PyO3 wrapper)
#[pyfunction]
pub fn enhance_cards_with_metadata(mtgjson_cards: Vec<MtgjsonCardObject>) -> PyResult<Vec<MtgjsonCardObject>> {
    let mut cards = mtgjson_cards;
    inner_enhance_cards_with_metadata(&mut cards);
    Ok(cards)
}

/// Build base MTGJSON cards (PyO3 wrapper)
#[pyfunction]
#[pyo3(signature = (set_code, additional_cards = None, is_token = false, set_release_date = ""))]
pub fn build_base_mtgjson_cards(
    set_code: &str,
    additional_cards: Option<String>, // Simplified to JSON string
    is_token: bool,
    set_release_date: &str,
) -> PyResult<Vec<MtgjsonCardObject>> {
    // Convert JSON string to HashMap if provided
    let parsed_additional_cards = if let Some(json_str) = additional_cards {
        match serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(&json_str) {
            Ok(cards) => Some(cards),
            Err(_) => None, // Invalid JSON, treat as None
        }
    } else {
        None
    };
    
    Ok(inner_build_base_mtgjson_cards(set_code, parsed_additional_cards, is_token, set_release_date))
}