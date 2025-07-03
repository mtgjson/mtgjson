use serde::Serialize;
use std::collections::HashSet;

/// Base trait for all MTGJSON objects (~Python JsonObject abstract base class)
pub trait JsonObject {
    /// Determine what keys should be avoided in the JSON dump
    fn build_keys_to_skip(&self) -> HashSet<String> {
        HashSet::new()
    }

    /// Convert to JSON string
    fn to_json_string(&self) -> Result<String, serde_json::Error>
    where
        Self: Serialize,
    {
        serde_json::to_string(self)
    }

    /// Convert to JSON value
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error>
    where
        Self: Serialize,
    {
        serde_json::to_value(self)
    }
}

/// Utility function to convert snake_case to camelCase
/// Equivalent to the Python to_camel_case function
pub fn to_camel_case(snake_str: &str) -> String {
    let mut result = String::with_capacity(snake_str.len()); // Pre-allocate capacity
    let mut capitalize_next = false;

    for c in snake_str.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Optimized serializer that skips empty/falsy values
#[inline]
pub fn skip_if_empty<T>(value: &Option<T>) -> bool
where
    T: Default + PartialEq,
{
    match value {
        Some(v) => *v == T::default(),
        None => true,
    }
}

/// Optimized serializer that skips empty vectors
#[inline]
pub fn skip_if_empty_vec<T>(value: &Vec<T>) -> bool {
    value.is_empty()
}

/// Optimized serializer that skips empty strings
#[inline]
pub fn skip_if_empty_string(value: &str) -> bool {
    value.is_empty()
}

/// Optimized serializer that skips empty optional strings
#[inline]
pub fn skip_if_empty_optional_string(value: &Option<String>) -> bool {
    match value {
        Some(s) => s.is_empty(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("snake_case"), "snakeCase");
        assert_eq!(to_camel_case("already_camel"), "alreadyCamel");
        assert_eq!(to_camel_case("single"), "single");
        assert_eq!(to_camel_case(""), "");
    }
}
