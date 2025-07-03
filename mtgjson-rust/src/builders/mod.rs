pub mod output_generator;
pub mod parallel_call;
pub mod price_builder;
/// Builders module - Main module file for MTGJSON builders
pub mod set_builder;
pub mod set_builder_functions;

// Re-export main types and functions for easier access
pub use set_builder::{
    add_leadership_skills, add_uuid_placeholder, build_base_mtgjson_cards, build_mtgjson_set,
    enhance_cards_with_metadata, get_card_cmc, get_card_colors, get_translation_data, is_number,
    mark_duel_decks, parse_card_types, parse_foreign, parse_keyrune_code, parse_legalities,
    parse_printings, parse_rulings,
};

pub use parallel_call::{
    AsyncTaskQueue as ParallelIterator, BatchProcessor as ParallelProcessor, CardBuildProcessor,
};

pub use set_builder_functions::{
    build_mtgjson_set_from_data, build_mtgjson_set_wrapper, get_card_cmc_wrapper,
    get_card_colors_wrapper, get_set_translation_data, is_number_wrapper, parse_card_types_wrapper,
    parse_foreign_wrapper, parse_legalities_wrapper, parse_printings_wrapper,
    parse_rulings_wrapper,
};

pub use output_generator::OutputGenerator;
pub use price_builder::PriceBuilder;
