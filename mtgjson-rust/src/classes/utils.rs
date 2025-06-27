use regex::Regex;
use std::collections::HashSet;

/// Utility functions for MTGJSON processing
pub struct MtgjsonUtils;

impl MtgjsonUtils {
    /// Sanitize a deck name for use as a filename
    pub fn sanitize_deck_name(name: &str, code: &str) -> String {
        let word_characters_only = Regex::new(r"\W").unwrap();
        let capital_case: String = name
            .chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| c.to_uppercase().collect::<String>())
            .collect::<Vec<String>>()
            .join("");
        
        let deck_name_sanitized = word_characters_only.replace_all(&capital_case, "");
        format!("{}_{}", deck_name_sanitized, code)
    }
    
    /// Clean a card number for sorting purposes
    pub fn clean_card_number(number: &str) -> (u32, usize) {
        let digits_only: String = number.chars().filter(|c| c.is_ascii_digit()).collect();
        let number_int = digits_only.parse::<u32>().unwrap_or(100000);
        (number_int, digits_only.len())
    }
    
    /// Check if a filename would be problematic on Windows
    pub fn is_windows_safe_filename(filename: &str) -> bool {
        const BAD_NAMES: &[&str] = &[
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
        ];
        
        !BAD_NAMES.contains(&filename.to_uppercase().as_str())
    }
    
    /// Make a filename Windows-safe by appending underscore if needed
    pub fn make_windows_safe_filename(filename: &str) -> String {
        if Self::is_windows_safe_filename(filename) {
            filename.to_string()
        } else {
            format!("{}_", filename)
        }
    }
    
    /// Extract alpha-numeric characters only (for deck name matching)
    pub fn alpha_numeric_only(input: &str) -> String {
        input
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect::<String>()
            .to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_deck_name() {
        let result = MtgjsonUtils::sanitize_deck_name("Test Deck!", "ABC");
        assert_eq!(result, "TESTDECK_ABC");
    }

    #[test]
    fn test_clean_card_number() {
        let (num, len) = MtgjsonUtils::clean_card_number("123a");
        assert_eq!(num, 123);
        assert_eq!(len, 3);
    }

    #[test]
    fn test_windows_safe_filename() {
        assert!(!MtgjsonUtils::is_windows_safe_filename("CON"));
        assert!(MtgjsonUtils::is_windows_safe_filename("NORMAL"));
    }

    #[test]
    fn test_alpha_numeric_only() {
        let result = MtgjsonUtils::alpha_numeric_only("Test-Deck! 123");
        assert_eq!(result, "testdeck 123");
    }
}