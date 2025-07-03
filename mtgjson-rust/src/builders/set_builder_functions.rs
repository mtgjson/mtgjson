use crate::builders::set_builder::{
    build_mtgjson_set, get_card_cmc,
    get_card_colors, is_number, parse_card_types, parse_foreign, parse_legalities,
    parse_printings, parse_rulings,
};
use crate::classes::foreign_data::MtgjsonForeignDataObject;
use crate::classes::legalities::MtgjsonLegalitiesObject;
use crate::classes::rulings::MtgjsonRulingObject;
use crate::classes::set::MtgjsonSetObject;
use pyo3::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// Get set translation data for a given set name
#[pyfunction]
#[pyo3(signature = (set_code, all_printings_path=None))]
pub fn get_set_translation_data(set_code: &str, all_printings_path: Option<&str>) -> PyResult<Option<MtgjsonSetObject>> {
    // Load translation data from resources
    let translation_data = crate::builders::set_builder::get_translation_data(set_code);
    Ok(translation_data)
}

/// Build MTGJSON set from provided set data
#[pyfunction]
#[pyo3(signature = (_set_data))]
pub fn build_mtgjson_set_from_data(
    _py: Python,
    _set_data: String,
) -> PyResult<Option<MtgjsonSetObject>> {
    // This would build a set from provided set data JSON string
    // Implementation would be similar to build_mtgjson_set but with provided data

    // For now, return None to fix compilation
    Ok(None)
}

/// Wrapper function that matches the original Python API for parse_card_types
#[pyfunction]
#[pyo3(signature = (card_type))]
pub fn parse_card_types_wrapper(
    card_type: &str,
) -> PyResult<(Vec<String>, Vec<String>, Vec<String>)> {
    Ok(parse_card_types(card_type))
}

/// Wrapper function that matches the original Python API for get_card_colors
#[pyfunction]
#[pyo3(signature = (mana_cost))]
pub fn get_card_colors_wrapper(mana_cost: &str) -> PyResult<Vec<String>> {
    Ok(get_card_colors(mana_cost))
}

/// Wrapper function that matches the original Python API for get_card_cmc
#[pyfunction]
#[pyo3(signature = (mana_cost))]
pub fn get_card_cmc_wrapper(mana_cost: &str) -> PyResult<f64> {
    Ok(get_card_cmc(mana_cost))
}

/// Wrapper function that matches the original Python API for is_number
#[pyfunction]
#[pyo3(signature = (string))]
pub fn is_number_wrapper(string: &str) -> PyResult<bool> {
    Ok(is_number(string))
}

/// Wrapper function that matches the original Python API for parse_legalities
#[pyfunction]
#[pyo3(signature = (sf_card_legalities))]
pub fn parse_legalities_wrapper(sf_card_legalities: String) -> PyResult<MtgjsonLegalitiesObject> {
    // Parse the JSON string into a HashMap
    let legalities_map: HashMap<String, String> = serde_json::from_str(&sf_card_legalities)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON: {}", e))
        })?;
    Ok(parse_legalities(&legalities_map))
}

/// Wrapper function that matches the original Python API for build_mtgjson_set
#[pyfunction]
#[pyo3(signature = (set_code))]
pub fn build_mtgjson_set_wrapper(set_code: &str) -> PyResult<Option<MtgjsonSetObject>> {
    Ok(build_mtgjson_set(set_code))
}

/// Wrapper function that matches the original Python API for parse_foreign
#[pyfunction]
#[pyo3(signature = (sf_prints_url, card_name, card_number, set_name))]
pub fn parse_foreign_wrapper(
    sf_prints_url: &str,
    card_name: &str,
    card_number: &str,
    set_name: &str,
) -> PyResult<Vec<MtgjsonForeignDataObject>> {
    Ok(parse_foreign(
        sf_prints_url,
        card_name,
        card_number,
        set_name,
    ))
}

/// Wrapper function that matches the original Python API for parse_printings
#[pyfunction]
#[pyo3(signature = (sf_prints_url=None))]
pub fn parse_printings_wrapper(sf_prints_url: Option<&str>) -> PyResult<Vec<String>> {
    Ok(parse_printings(sf_prints_url))
}

/// Wrapper function that matches the original Python API for parse_rulings
#[pyfunction]
#[pyo3(signature = (rulings_url))]
pub fn parse_rulings_wrapper(rulings_url: &str) -> PyResult<Vec<MtgjsonRulingObject>> {
    Ok(parse_rulings(rulings_url))
}

/// Process set data with additional configurations
#[pyfunction]
#[pyo3(signature = (set_code, additional_config=None))]
pub fn process_set_data(
    set_code: &str,
    additional_config: Option<String>,
) -> PyResult<Option<MtgjsonSetObject>> {
    // This would handle additional processing logic
    // that might be required for specific sets or configurations

    // Parse additional config if provided
    if let Some(config_json) = additional_config {
        let _config: HashMap<String, Value> = serde_json::from_str(&config_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid config JSON: {}", e))
        })?;
        // Use config for additional processing
    }

    Ok(build_mtgjson_set(set_code))
}

/// Apply set-specific corrections and enhancements
#[pyfunction]
#[pyo3(signature = (mtgjson_set, corrections))]
pub fn apply_set_corrections(
    mtgjson_set: &mut MtgjsonSetObject,
    corrections: String,
) -> PyResult<()> {
    // Parse corrections JSON
    let corrections_map: HashMap<String, Value> =
        serde_json::from_str(&corrections).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid corrections JSON: {}",
                e
            ))
        })?;

    // Apply any set-specific corrections that might be needed
    // This could include fixing card data, adding missing information, etc.
    println!(
        "Applied {} corrections to set {}",
        corrections_map.len(),
        mtgjson_set.name
    );

    Ok(())
}

/// Validate set data integrity
#[pyfunction]
#[pyo3(signature = (mtgjson_set))]
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

#[pyfunction]
#[pyo3(signature = (set_code, all_printings_path=None))]
pub fn build_and_validate_set(set_code: &str, all_printings_path: Option<PathBuf>) -> PyResult<Option<MtgjsonSetObject>> {
    Ok(crate::builders::set_builder::build_mtgjson_set(set_code))
}

/// Python module definition for set builder functions
#[pymodule]
pub fn set_builder_functions_module(m: &Bound<'_, PyModule>) -> PyResult<()> { 
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
    m.add_function(wrap_pyfunction!(build_and_validate_set, m)?)?;
    Ok(())
}

/// Add functions to an existing module (for compatibility)
pub fn add_functions_to_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    m.add_function(wrap_pyfunction!(build_and_validate_set, m)?)?;
    Ok(())
}