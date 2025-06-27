// High-performance builders and computational modules
pub mod output_generator;
pub mod parallel_call;
pub mod price_builder;
pub mod set_builder;
pub mod set_builder_functions;

// Re-export main types for convenience
pub use output_generator::OutputGenerator;
pub use parallel_call::{ParallelProcessor, ParallelIterator};
pub use price_builder::PriceBuilder;

// Re-export set builder functions (avoiding duplicate names)
pub use set_builder_functions::{
    parse_card_types as sb_parse_card_types,
    get_card_colors as sb_get_card_colors,
    get_card_cmc as sb_get_card_cmc,
    is_number as sb_is_number,
    parse_legalities as sb_parse_legalities,
    parse_rulings as sb_parse_rulings,
    build_mtgjson_set,
    parse_foreign,
    parse_printings,
    mark_duel_decks,
    enhance_cards_with_metadata,
    build_base_mtgjson_cards,
};

