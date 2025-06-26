//! MTGJSON 5.5 - Rust Edition
//!
//! This crate provides Rust structs for all major MTGJSON classes,
//! as well as Python bindings for those structs.
//!
//! The crate is organized into modules, each corresponding to a
//! major MTGJSON class. The `base` module contains the base trait
//! for all MTGJSON objects, and the `utils` module contains utility
//! functions for working with MTGJSON data.
//! To be continued - 

use pyo3::prelude::*;

pub mod base;
pub mod card;
pub mod deck;
pub mod foreign_data;
pub mod game_formats;
pub mod identifiers;
pub mod leadership_skills;
pub mod legalities;
pub mod meta;
pub mod prices;
pub mod purchase_urls;
pub mod related_cards;
pub mod rulings;
pub mod sealed_product;
pub mod set;
pub mod translations;
pub mod utils;

// Re-export all major types
pub use base::*;
pub use card::*;
pub use deck::*;
pub use foreign_data::*;
pub use game_formats::*;
pub use identifiers::*;
pub use leadership_skills::*;
pub use legalities::*;
pub use meta::*;
pub use prices::*;
pub use purchase_urls::*;
pub use related_cards::*;
pub use rulings::*;
pub use sealed_product::*;
pub use set::*;
pub use translations::*;

/// Python module for MTGJSON Rust implementation
#[pymodule]
fn mtgjson_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes using PyO3 0.22 API
    m.add_class::<MtgjsonCard>()?;
    m.add_class::<MtgjsonDeck>()?;
    m.add_class::<MtgjsonDeckHeader>()?;
    m.add_class::<MtgjsonForeignData>()?;
    m.add_class::<MtgjsonGameFormats>()?;
    m.add_class::<MtgjsonIdentifiers>()?;
    m.add_class::<MtgjsonLeadershipSkills>()?;
    m.add_class::<MtgjsonLegalities>()?;
    m.add_class::<MtgjsonMeta>()?;
    m.add_class::<MtgjsonPrices>()?;
    m.add_class::<MtgjsonPurchaseUrls>()?;
    m.add_class::<MtgjsonRelatedCards>()?;
    m.add_class::<MtgjsonRuling>()?;
    m.add_class::<MtgjsonSealedProduct>()?;
    m.add_class::<MtgjsonSet>()?;
    m.add_class::<MtgjsonTranslations>()?;
    
    // Register enums
    m.add_class::<SealedProductCategory>()?;
    m.add_class::<SealedProductSubtype>()?;
    
    Ok(())
}