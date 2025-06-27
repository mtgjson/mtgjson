// Third-party provider modules
pub mod cardhoarder;
pub mod cardkingdom;
pub mod gatherer;
pub mod mtgban;
pub mod multiverse_bridge;
pub mod tcgplayer;
pub mod whats_in_standard;
pub mod wizards;

// Re-export all third-party providers
pub use cardhoarder::CardHoarderProvider;
pub use cardkingdom::CardKingdomProvider;
pub use gatherer::GathererProvider;
pub use mtgban::MTGBanProvider;
pub use multiverse_bridge::MultiverseBridgeProvider;
pub use tcgplayer::TCGPlayerProvider;
pub use whats_in_standard::WhatsInStandardProvider;
pub use wizards::WizardsProvider;