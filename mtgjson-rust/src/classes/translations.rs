use crate::base::{skip_if_empty_optional_string, JsonObject};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// MTGJSON Set.Translations Object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[pyclass(name = "MtgjsonTranslations")]
pub struct MtgjsonTranslations {
    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub chinese_simplified: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub chinese_traditional: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub french: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub german: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub italian: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub japanese: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub korean: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub portuguese_brazil: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub russian: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_optional_string")]
    #[pyo3(get, set)]
    pub spanish: Option<String>,
}

#[pymethods]
impl MtgjsonTranslations {
    #[new]
    #[pyo3(signature = (active_dict = None))]
    pub fn new(active_dict: Option<HashMap<String, String>>) -> Self {
        let mut translations = Self::default();

        if let Some(dict) = active_dict {
            translations.chinese_simplified = dict.get("Chinese Simplified").cloned();
            translations.chinese_traditional = dict.get("Chinese Traditional").cloned();
            translations.french = dict.get("French").or_else(|| dict.get("fr")).cloned();
            translations.german = dict.get("German").or_else(|| dict.get("de")).cloned();
            translations.italian = dict.get("Italian").or_else(|| dict.get("it")).cloned();
            translations.japanese = dict.get("Japanese").cloned();
            translations.korean = dict.get("Korean").cloned();
            translations.portuguese_brazil = dict.get("Portuguese (Brazil)").cloned();
            translations.russian = dict.get("Russian").cloned();
            translations.spanish = dict.get("Spanish").or_else(|| dict.get("es")).cloned();
        }

        translations
    }

    /// Custom parsing of translation keys
    #[staticmethod]
    pub fn parse_key(key: &str) -> String {
        let key = key.replace("_brazil", " (Brazil)");
        let components: Vec<&str> = key.split('_').collect();
        components
            .iter()
            .map(|&s| {
                let mut chars = s.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                }
            })
            .collect::<Vec<String>>()
            .join(" ")
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Convert to dictionary with parsed keys
    pub fn to_dict(&self) -> PyResult<HashMap<String, String>> {
        let mut result = HashMap::new();

        if let Some(ref val) = self.chinese_simplified {
            if !val.is_empty() {
                result.insert(Self::parse_key("chinese_simplified"), val.clone());
            }
        }
        if let Some(ref val) = self.chinese_traditional {
            if !val.is_empty() {
                result.insert(Self::parse_key("chinese_traditional"), val.clone());
            }
        }
        if let Some(ref val) = self.french {
            if !val.is_empty() {
                result.insert(Self::parse_key("french"), val.clone());
            }
        }
        if let Some(ref val) = self.german {
            if !val.is_empty() {
                result.insert(Self::parse_key("german"), val.clone());
            }
        }
        if let Some(ref val) = self.italian {
            if !val.is_empty() {
                result.insert(Self::parse_key("italian"), val.clone());
            }
        }
        if let Some(ref val) = self.japanese {
            if !val.is_empty() {
                result.insert(Self::parse_key("japanese"), val.clone());
            }
        }
        if let Some(ref val) = self.korean {
            if !val.is_empty() {
                result.insert(Self::parse_key("korean"), val.clone());
            }
        }
        if let Some(ref val) = self.portuguese_brazil {
            if !val.is_empty() {
                result.insert(Self::parse_key("portuguese_brazil"), val.clone());
            }
        }
        if let Some(ref val) = self.russian {
            if !val.is_empty() {
                result.insert(Self::parse_key("russian"), val.clone());
            }
        }
        if let Some(ref val) = self.spanish {
            if !val.is_empty() {
                result.insert(Self::parse_key("spanish"), val.clone());
            }
        }

        Ok(result)
    }

    /// Get all available languages
    pub fn get_available_languages(&self) -> Vec<String> {
        let mut languages = Vec::new();

        if self.chinese_simplified.is_some() {
            languages.push("Chinese Simplified".to_string());
        }
        if self.chinese_traditional.is_some() {
            languages.push("Chinese Traditional".to_string());
        }
        if self.french.is_some() {
            languages.push("French".to_string());
        }
        if self.german.is_some() {
            languages.push("German".to_string());
        }
        if self.italian.is_some() {
            languages.push("Italian".to_string());
        }
        if self.japanese.is_some() {
            languages.push("Japanese".to_string());
        }
        if self.korean.is_some() {
            languages.push("Korean".to_string());
        }
        if self.portuguese_brazil.is_some() {
            languages.push("Portuguese (Brazil)".to_string());
        }
        if self.russian.is_some() {
            languages.push("Russian".to_string());
        }
        if self.spanish.is_some() {
            languages.push("Spanish".to_string());
        }

        languages
    }

    /// Check if any translations are present
    pub fn has_translations(&self) -> bool {
        self.chinese_simplified.is_some()
            || self.chinese_traditional.is_some()
            || self.french.is_some()
            || self.german.is_some()
            || self.italian.is_some()
            || self.japanese.is_some()
            || self.korean.is_some()
            || self.portuguese_brazil.is_some()
            || self.russian.is_some()
            || self.spanish.is_some()
    }
}

impl JsonObject for MtgjsonTranslations {}
