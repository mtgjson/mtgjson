use config::{Config, ConfigError, Environment, File};
use log::{info, warn};
use std::collections::HashMap;

/// Configuration management for MTGJSON
pub struct MtgjsonConfig {
    config: Config,
}

impl MtgjsonConfig {
    /// Create a new configuration instance
    pub fn new() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("mtgjson.properties").required(false))
            .add_source(Environment::with_prefix("MTGJSON"))
            .build()?;

        Ok(Self { config })
    }

    /// Check if a section exists in the configuration
    pub fn has_section(&self, section: &str) -> bool {
        self.config.get_table(section).is_ok()
    }

    /// Check if an option exists in a section
    pub fn has_option(&self, section: &str, option: &str) -> bool {
        let key = format!("{}.{}", section, option);
        self.config.get_string(&key).is_ok()
    }

    /// Get a string value from the configuration
    pub fn get_string(&self, section: &str, option: &str) -> Result<String, ConfigError> {
        let key = format!("{}.{}", section, option);
        self.config.get_string(&key)
    }
}

/// Construct the Authorization header for Scryfall
/// Returns: Authorization header as HashMap
pub fn build_http_header() -> HashMap<String, String> {
    let config = match MtgjsonConfig::new() {
        Ok(config) => config,
        Err(_) => {
            warn!("Failed to load configuration. Defaulting to non-authorized mode");
            return HashMap::new();
        }
    };

    if !config.has_section("Scryfall") {
        warn!("Scryfall section not established. Defaulting to non-authorized mode");
        return HashMap::new();
    }

    if !config.has_option("Scryfall", "client_secret") {
        warn!("Scryfall keys values missing. Defaulting to non-authorized mode");
        return HashMap::new();
    }

    match config.get_string("Scryfall", "client_secret") {
        Ok(client_secret) => {
            let mut headers = HashMap::new();
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {}", client_secret),
            );
            headers.insert("Connection".to_string(), "Keep-Alive".to_string());
            headers
        }
        Err(_) => {
            warn!("Failed to read Scryfall client_secret. Defaulting to non-authorized mode");
            HashMap::new()
        }
    }
}

/// Alternative simpler version using environment variables only
pub fn build_http_header_simple() -> HashMap<String, String> {
    match std::env::var("MTGJSON_SCRYFALL_CLIENT_SECRET") {
        Ok(client_secret) if !client_secret.is_empty() => {
            let mut headers = HashMap::new();
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {}", client_secret),
            );
            headers.insert("Connection".to_string(), "Keep-Alive".to_string());
            headers
        }
        _ => {
            warn!("Scryfall client secret not found in environment. Defaulting to non-authorized mode");
            HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_build_http_header_simple_with_env_var() {
        env::set_var("MTGJSON_SCRYFALL_CLIENT_SECRET", "test_secret_123");

        let headers = build_http_header_simple();

        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer test_secret_123".to_string())
        );
        assert_eq!(headers.get("Connection"), Some(&"Keep-Alive".to_string()));

        env::remove_var("MTGJSON_SCRYFALL_CLIENT_SECRET");
    }

    #[test]
    fn test_build_http_header_simple_without_env_var() {
        env::remove_var("MTGJSON_SCRYFALL_CLIENT_SECRET");

        let headers = build_http_header_simple();

        assert!(headers.is_empty());
    }

    #[test]
    fn test_mtgjson_config_has_section() {
        // This would require actual config file testing
        // For now, just test the structure works
        if let Ok(config) = MtgjsonConfig::new() {
            // Test that the methods exist and can be called
            let _has_section = config.has_section("TestSection");
            let _has_option = config.has_option("TestSection", "test_option");
        }
    }
}
