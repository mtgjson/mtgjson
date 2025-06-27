/// Builders module - Main module file for MTGJSON builders

pub mod set_builder;
pub mod set_builder_functions;
pub mod parallel_call;
pub mod output_generator;
pub mod price_builder;

// Re-export main types and functions for easier access
pub use set_builder::{
    parse_card_types, get_card_colors, get_card_cmc, is_number,
    parse_legalities, build_mtgjson_set, parse_foreign, parse_printings,
    parse_rulings, mark_duel_decks, enhance_cards_with_metadata,
    build_base_mtgjson_cards, add_uuid_placeholder, add_leadership_skills,
    parse_keyrune_code, get_translation_data,
};

pub use parallel_call::{
    BatchProcessor as ParallelProcessor,
    AsyncTaskQueue as ParallelIterator,
    CardBuildProcessor,
};

pub use set_builder_functions::{
    get_set_translation_data,
    build_mtgjson_set_from_data,
    parse_card_types_wrapper,
    get_card_colors_wrapper,
    get_card_cmc_wrapper,
    is_number_wrapper,
    parse_legalities_wrapper,
    build_mtgjson_set_wrapper,
    parse_foreign_wrapper,
    parse_printings_wrapper,
    parse_rulings_wrapper,
};

pub use output_generator::OutputGenerator;
pub use price_builder::PriceBuilder;