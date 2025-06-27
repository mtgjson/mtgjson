// PyO3 wrapper functions for utility functions
use pyo3::prelude::*;
use crate::base;
use crate::utils::MtgjsonUtils;

/// Convert string to camelCase (PyO3 wrapper)
#[pyfunction]
pub fn to_camel_case(string: &str) -> PyResult<String> {
    let mut result = String::new();
    let mut capitalize_next = false;
    
    for (i, ch) in string.chars().enumerate() {
        if ch == '_' || ch == '-' || ch == ' ' {
            capitalize_next = true;
        } else if i == 0 {
            result.push(ch.to_lowercase().next().unwrap_or(ch));
        } else if capitalize_next {
            result.push(ch.to_uppercase().next().unwrap_or(ch));
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    
    Ok(result)
}

/// Make a Windows-safe filename (PyO3 wrapper)
#[pyfunction]
pub fn make_windows_safe_filename(filename: &str) -> PyResult<String> {
    let invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
    let mut safe_filename = String::new();
    
    for ch in filename.chars() {
        if invalid_chars.contains(&ch) {
            safe_filename.push('_');
        } else {
            safe_filename.push(ch);
        }
    }
    
    // Handle reserved names
    let reserved_names = [
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    ];
    
    let uppercase_name = safe_filename.to_uppercase();
    if reserved_names.contains(&uppercase_name.as_str()) {
        safe_filename.push('_');
    }
    
    // Remove trailing dots and spaces
    safe_filename = safe_filename.trim_end_matches('.').trim_end().to_string();
    
    // Ensure not empty
    if safe_filename.is_empty() {
        safe_filename = "unnamed".to_string();
    }
    
    Ok(safe_filename)
}

/// Clean card number string (PyO3 wrapper)
#[pyfunction]
pub fn clean_card_number(card_number: &str) -> PyResult<String> {
    // Remove common prefixes and suffixes that clutter card numbers
    let mut cleaned = card_number.trim().to_string();
    
    // Remove leading zeros but preserve single zero
    if cleaned.len() > 1 {
        cleaned = cleaned.trim_start_matches('0').to_string();
        if cleaned.is_empty() {
            cleaned = "0".to_string();
        }
    }
    
    // Handle special characters commonly found in card numbers
    cleaned = cleaned.replace("★", "*");  // Replace star character
    cleaned = cleaned.replace("†", ""); // Remove dagger
    
    Ok(cleaned)
}