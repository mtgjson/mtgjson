//! MTGJSON Singular Card.LeadershipSkills Object
use crate::base::JsonObject;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

//! MtgjsonLeadershipSkills
//!
//! This struct represents the leadership skills for a single Magic: The Gathering card.
//! It is used to store all the data for a single card's leadership skills, including
//! its brawl, commander, and oathbreaker.
//!
//! Note: All fields are required.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[pyclass(name = "MtgjsonLeadershipSkills")]
pub struct MtgjsonLeadershipSkills {
    #[pyo3(get, set)]
    pub brawl: bool,
    
    #[pyo3(get, set)]
    pub commander: bool,
    
    #[pyo3(get, set)]
    pub oathbreaker: bool,
}

#[pymethods]
impl MtgjsonLeadershipSkills {
    #[new]
    pub fn new(brawl: bool, commander: bool, oathbreaker: bool) -> Self {
        Self {
            brawl,
            commander,
            oathbreaker,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization error: {}", e))
        })
    }

    /// Check if any leadership skills are present
    pub fn has_any_skills(&self) -> bool {
        self.brawl || self.commander || self.oathbreaker
    }

    /// Get list of available leadership skills
    pub fn get_available_skills(&self) -> Vec<String> {
        let mut skills = Vec::new();
        
        if self.brawl {
            skills.push("brawl".to_string());
        }
        if self.commander {
            skills.push("commander".to_string());
        }
        if self.oathbreaker {
            skills.push("oathbreaker".to_string());
        }
        
        skills
    }
}

impl JsonObject for MtgjsonLeadershipSkills {}