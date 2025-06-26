use regex::Regex;

pub struct MtgjsonUtils;

impl MtgjsonUtils {
    /// Extract only alphanumeric characters from a string
    pub fn alpha_numeric_only(input: &str) -> String {
        input.chars()
            .filter(|c| c.is_alphanumeric())
            .collect::<String>()
            .to_lowercase()
    }

    /// Sanitize deck name for safe file naming
    pub fn sanitize_deck_name(name: &str, code: &str) -> String {
        let cleaned = name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        
        // Remove consecutive underscores and trim
        let re = Regex::new(r"_+").unwrap();
        let result = re.replace_all(&cleaned, "_");
        let trimmed = result.trim_matches('_');
        
        if trimmed.is_empty() {
            format!("deck_{}", code)
        } else {
            format!("{}_{}", trimmed, code)
        }
    }

    /// Make filename safe for Windows
    pub fn make_windows_safe_filename(filename: &str) -> String {
        let invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
        filename
            .chars()
            .map(|c| {
                if invalid_chars.contains(&c) {
                    '_'
                } else {
                    c
                }
            })
            .collect()
    }

    /// Clean card number for comparison, returning (cleaned_number, original_length)
    pub fn clean_card_number(number: &str) -> (i32, usize) {
        let original_len = number.len();
        
        // Extract numeric part from the beginning of the string
        let mut numeric_part = String::new();
        for c in number.chars() {
            if c.is_ascii_digit() {
                numeric_part.push(c);
            } else {
                break;
            }
        }
        
        // Parse the numeric part, defaulting to 0 if empty or invalid
        let cleaned_number = numeric_part.parse().unwrap_or(0);
        
        (cleaned_number, original_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpha_numeric_only() {
        assert_eq!(MtgjsonUtils::alpha_numeric_only("Hello123!@#"), "hello123");
        assert_eq!(MtgjsonUtils::alpha_numeric_only("Test-Name_42"), "testname42");
    }

    #[test]
    fn test_sanitize_deck_name() {
        assert_eq!(
            MtgjsonUtils::sanitize_deck_name("Test Deck!", "ABC"),
            "Test_Deck_ABC"
        );
        assert_eq!(
            MtgjsonUtils::sanitize_deck_name("", "XYZ"),
            "deck_XYZ"
        );
    }

    #[test]
    fn test_make_windows_safe_filename() {
        assert_eq!(
            MtgjsonUtils::make_windows_safe_filename("test<file>name"),
            "test_file_name"
        );
        assert_eq!(
            MtgjsonUtils::make_windows_safe_filename("normal_filename"),
            "normal_filename"
        );
    }
}