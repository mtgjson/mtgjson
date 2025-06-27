/// Builders module - Main module file for MTGJSON builders

pub mod set_builder;
pub mod set_builder_functions;
pub mod parallel_call;
pub mod output_generator;

// Re-export main types and functions for easier access
pub use set_builder::{
    parse_card_types, get_card_colors, get_card_cmc, is_number,
    parse_legalities, build_mtgjson_set, parse_foreign, parse_printings,
    parse_rulings, mark_duel_decks, enhance_cards_with_metadata,
    build_base_mtgjson_cards, add_uuid, add_leadership_skills,
    parse_keyrune_code, get_translation_data,
};

pub use parallel_call::{
    parallel_call, parallel_map, parallel_starmap,
    BatchProcessor as ParallelProcessor,
    AsyncTaskQueue as ParallelIterator,
    CardBuildProcessor,
};

pub use set_builder_functions::{
    get_set_translation_data,
    build_mtgjson_set_from_data,
};

pub use output_generator::{
    generate_all_printings,
    generate_set_outputs,
};