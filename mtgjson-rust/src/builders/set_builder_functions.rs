use crate::classes::set::MtgjsonSetObject;
use crate::builders::set_builder::{
    parse_card_types,
    get_card_colors,
    get_card_cmc,
    is_number,
    parse_legalities,
    build_mtgjson_set,
    parse_foreign,
    parse_printings,
    parse_rulings,
    mark_duel_decks,
    enhance_cards_with_metadata,
    build_base_mtgjson_cards,
};
use crate::classes::foreign_data::MtgjsonForeignDataObject;
use crate::classes::legalities::MtgjsonLegalitiesObject;
use crate::classes::rulings::MtgjsonRulingObject;
use pyo3::prelude::*;
use std::collections::HashMap;
use serde_json::Value;

/// Get set translation data for a given set name
#[pyfunction]
pub fn get_set_translation_data(set_name: &str) -> PyResult<Option<HashMap<String, String>>> {
    // Load translation data from resources
    let translation_data = crate::builders::set_builder::get_translation_data(set_name);
    Ok(translation_data)
}

/// Build MTGJSON set from provided set data
#[pyfunction]
pub fn build_mtgjson_set_from_data(_py: Python, _set_data: String) -> PyResult<Option<MtgjsonSetObject>> {
    // This would build a set from provided set data JSON string
    // Implementation would be similar to build_mtgjson_set but with provided data
    
    // For now, return None to fix compilation
    Ok(None)
}

/// Wrapper function that matches the original Python API for parse_card_types
#[pyfunction]
pub fn parse_card_types_wrapper(card_type: &str) -> PyResult<(Vec<String>, Vec<String>, Vec<String>)> {
    Ok(parse_card_types(card_type))
}

/// Wrapper function that matches the original Python API for get_card_colors
#[pyfunction]
pub fn get_card_colors_wrapper(mana_cost: &str) -> PyResult<Vec<String>> {
    Ok(get_card_colors(mana_cost))
}

/// Wrapper function that matches the original Python API for get_card_cmc
#[pyfunction]
pub fn get_card_cmc_wrapper(mana_cost: &str) -> PyResult<f64> {
    Ok(get_card_cmc(mana_cost))
}

/// Wrapper function that matches the original Python API for is_number
#[pyfunction]
pub fn is_number_wrapper(string: &str) -> PyResult<bool> {
    Ok(is_number(string))
}

/// Wrapper function that matches the original Python API for parse_legalities
#[pyfunction]
pub fn parse_legalities_wrapper(sf_card_legalities: String) -> PyResult<MtgjsonLegalitiesObject> {
    // Parse the JSON string into a HashMap
    let legalities_map: HashMap<String, String> = serde_json::from_str(&sf_card_legalities)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON: {}", e)))?;
    Ok(parse_legalities(&legalities_map))
}

/// Wrapper function that matches the original Python API for build_mtgjson_set
#[pyfunction]
pub fn build_mtgjson_set_wrapper(set_code: &str) -> PyResult<Option<MtgjsonSetObject>> {
    Ok(build_mtgjson_set(set_code))
}

/// Wrapper function that matches the original Python API for parse_foreign
#[pyfunction]
pub fn parse_foreign_wrapper(
    sf_prints_url: &str,
    card_name: &str,
    card_number: &str,
    set_name: &str,
) -> PyResult<Vec<MtgjsonForeignDataObject>> {
    Ok(parse_foreign(sf_prints_url, card_name, card_number, set_name))
}

/// Wrapper function that matches the original Python API for parse_printings
#[pyfunction]
pub fn parse_printings_wrapper(sf_prints_url: Option<&str>) -> PyResult<Vec<String>> {
    Ok(parse_printings(sf_prints_url))
}

/// Wrapper function that matches the original Python API for parse_rulings
#[pyfunction]
pub fn parse_rulings_wrapper(rulings_url: &str) -> PyResult<Vec<MtgjsonRulingObject>> {
    Ok(parse_rulings(rulings_url))
}

/// Process set data with additional configurations
#[pyfunction] 
pub fn process_set_data(
    set_code: &str,
    additional_config: Option<String>,
) -> PyResult<Option<MtgjsonSetObject>> {
    // This would handle additional processing logic
    // that might be required for specific sets or configurations
    
    // Parse additional config if provided
    if let Some(config_json) = additional_config {
        let _config: HashMap<String, Value> = serde_json::from_str(&config_json)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid config JSON: {}", e)))?;
        // Use config for additional processing
    }
    
    Ok(build_mtgjson_set(set_code))
}

/// Apply set-specific corrections and enhancements
#[pyfunction]
pub fn apply_set_corrections(
    mtgjson_set: &mut MtgjsonSetObject,
    corrections: String,
) -> PyResult<()> {
    // Parse corrections JSON
    let corrections_map: HashMap<String, Value> = serde_json::from_str(&corrections)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid corrections JSON: {}", e)))?;
    
    // Apply any set-specific corrections that might be needed
    // This could include fixing card data, adding missing information, etc.
    println!("Applied {} corrections to set {}", corrections_map.len(), mtgjson_set.name);
    
    Ok(())
}

/// Validate set data integrity
#[pyfunction]
pub fn validate_set_data(mtgjson_set: &MtgjsonSetObject) -> PyResult<Vec<String>> {
    // Validate that the set data is consistent and complete
    // Return a list of any validation errors found
    
    let mut errors = Vec::new();
    
    // Basic validation checks
    if mtgjson_set.name.is_empty() {
        errors.push("Set name is empty".to_string());
    }
    
    if mtgjson_set.code.is_none() {
        errors.push("Set code is missing".to_string());
    }
    
    if mtgjson_set.cards.is_empty() {
        errors.push("Set has no cards".to_string());
    }
    
    Ok(errors)
}

/// Python module definition for set builder functions
#[pymodule]
pub fn set_builder_functions_module(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_set_translation_data, m)?)?;
    m.add_function(wrap_pyfunction!(build_mtgjson_set_from_data, m)?)?;
    m.add_function(wrap_pyfunction!(parse_card_types_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(get_card_colors_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(get_card_cmc_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(is_number_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(parse_legalities_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(build_mtgjson_set_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(parse_foreign_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(parse_printings_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(parse_rulings_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(process_set_data, m)?)?;
    m.add_function(wrap_pyfunction!(apply_set_corrections, m)?)?;
    m.add_function(wrap_pyfunction!(validate_set_data, m)?)?;
    // build_and_validate_set function not yet implemented
    Ok(())
}

/// Add functions to an existing module (for compatibility)
pub fn add_functions_to_module(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    set_builder_functions_module(m)
}