use crate::base::JsonObject;
use crate::card::MtgjsonCard;
use crate::deck::MtgjsonDeck;
use crate::foreign_data::MtgjsonForeignData;
use crate::game_formats::MtgjsonGameFormats;
use crate::leadership_skills::MtgjsonLeadershipSkills;
use crate::legalities::MtgjsonLegalities;
use crate::meta::MtgjsonMeta;
use crate::related_cards::MtgjsonRelatedCards;
use crate::rulings::MtgjsonRuling;
use crate::sealed_product::MtgjsonSealedProduct;
use crate::set::MtgjsonSet;
use crate::translations::MtgjsonTranslations;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Constants for card processing
pub struct Constants {
    pub language_map: HashMap<String, String>,
    pub basic_land_names: Vec<String>,
    pub super_types: Vec<String>,
    pub multi_word_sub_types: Vec<String>,
    pub foreign_sets: Vec<String>,
}

impl Constants {
    pub fn new() -> Self {
        let mut language_map = HashMap::new();
        language_map.insert("en".to_string(), "English".to_string());
        language_map.insert("es".to_string(), "Spanish".to_string());
        language_map.insert("fr".to_string(), "French".to_string());
        language_map.insert("de".to_string(), "German".to_string());
        language_map.insert("it".to_string(), "Italian".to_string());
        language_map.insert("pt".to_string(), "Portuguese".to_string());
        language_map.insert("ja".to_string(), "Japanese".to_string());
        language_map.insert("ko".to_string(), "Korean".to_string());
        language_map.insert("ru".to_string(), "Russian".to_string());
        language_map.insert("zhs".to_string(), "Chinese Simplified".to_string());
        language_map.insert("zht".to_string(), "Chinese Traditional".to_string());

        let basic_land_names = vec![
            "Plains".to_string(),
            "Island".to_string(),
            "Swamp".to_string(),
            "Mountain".to_string(),
            "Forest".to_string(),
            "Wastes".to_string(),
        ];

        let super_types = vec![
            "Basic".to_string(),
            "Legendary".to_string(),
            "Ongoing".to_string(),
            "Snow".to_string(),
            "World".to_string(),
        ];

        let multi_word_sub_types = vec![
            "Aura Curse".to_string(),
            "Equipment Vehicle".to_string(),
        ];

        let foreign_sets = vec![
            "4BB".to_string(),
            "FBB".to_string(),
            "REN".to_string(),
        ];

        Self {
            language_map,
            basic_land_names,
            super_types,
            multi_word_sub_types,
            foreign_sets,
        }
    }
}

/// Parse foreign card data from Scryfall prints URL
pub fn parse_foreign(
    sf_prints_url: &str,
    card_name: &str,
    card_number: &str,
    set_name: &str,
) -> Vec<MtgjsonForeignData> {
    let mut card_foreign_entries = Vec::new();
    
    // Add information to get all languages
    let modified_url = sf_prints_url.replace("&unique=prints", "+lang%3Aany&unique=prints");
    
    // TODO: Implement ScryfallProvider download_all_pages
    // For now, return empty vector as placeholder
    println!("Parsing foreign data for {} #{} in {}", card_name, card_number, set_name);
    
    card_foreign_entries
}

/// Parse card types into super types, types, and subtypes
pub fn parse_card_types(card_type: &str) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut sub_types = Vec::new();
    let mut super_types = Vec::new();
    let mut types = Vec::new();
    
    let constants = Constants::new();
    
    let supertypes_and_types: String;
    
    if !card_type.contains("—") {
        supertypes_and_types = card_type.to_string();
    } else {
        let split_type: Vec<&str> = card_type.split("—").collect();
        supertypes_and_types = split_type[0].to_string();
        let subtypes = split_type[1];
        
        // Planes are an entire sub-type, whereas normal cards are split by spaces
        if card_type.starts_with("Plane") {
            sub_types.push(subtypes.trim().to_string());
        } else {
            let mut modified_subtypes = subtypes.to_string();
            let mut special_case_found = false;
            
            for special_case in &constants.multi_word_sub_types {
                if subtypes.contains(special_case) {
                    modified_subtypes = modified_subtypes.replace(special_case, &special_case.replace(" ", "!"));
                    special_case_found = true;
                }
            }
            
            sub_types = modified_subtypes
                .split_whitespace()
                .filter(|x| !x.is_empty())
                .map(|x| x.to_string())
                .collect();
                
            if special_case_found {
                for sub_type in &mut sub_types {
                    *sub_type = sub_type.replace("!", " ");
                }
            }
        }
    }
    
    for value in supertypes_and_types.split_whitespace() {
        if constants.super_types.contains(&value.to_string()) {
            super_types.push(value.to_string());
        } else if !value.is_empty() {
            types.push(value.to_string());
        }
    }
    
    (super_types, types, sub_types)
}

/// Get card colors from mana cost
pub fn get_card_colors(mana_cost: &str) -> Vec<String> {
    let color_options = vec!["W", "U", "B", "R", "G"];
    let mut ret_val = Vec::new();
    
    for color in color_options {
        if mana_cost.contains(color) {
            ret_val.push(color.to_string());
        }
    }
    
    ret_val
}

/// Check if a string represents a number
pub fn is_number(string: &str) -> bool {
    // Try parsing as float
    if string.parse::<f64>().is_ok() {
        return true;
    }
    
    // Try unicode numeric parsing
    if string.chars().all(|c| c.is_numeric()) {
        return true;
    }
    
    false
}

/// Get card's converted mana cost from mana cost string
pub fn get_card_cmc(mana_cost: &str) -> f64 {
    let mut total = 0.0;
    
    let re = Regex::new(r"\{([^}]*)\}").unwrap();
    let symbols: Vec<String> = re
        .captures_iter(mana_cost.trim())
        .map(|cap| cap[1].to_string())
        .collect();
    
    for element in symbols {
        let mut element = element;
        
        // Address 2/W, G/W, etc as "higher" cost always first
        if element.contains('/') {
            element = element.split('/').next().unwrap().to_string();
        }
        
        if is_number(&element) {
            total += element.parse::<f64>().unwrap_or(0.0);
        } else if element == "X" || element == "Y" || element == "Z" {
            // Placeholder mana - continue without adding
            continue;
        } else if element.starts_with('H') {
            // Half mana
            total += 0.5;
        } else {
            total += 1.0;
        }
    }
    
    total
}

/// Parse printings from Scryfall prints URL
pub fn parse_printings(sf_prints_url: Option<&str>) -> Vec<String> {
    let mut card_sets = HashSet::new();
    
    if let Some(mut url) = sf_prints_url {
        // TODO: Implement actual Scryfall API calls
        // This is a placeholder implementation
        println!("Parsing printings from URL: {}", url);
        
        // For now, return empty vector
    }
    
    let mut result: Vec<String> = card_sets.into_iter().collect();
    result.sort();
    result
}

/// Parse legalities from Scryfall format to MTGJSON format
pub fn parse_legalities(sf_card_legalities: &HashMap<String, String>) -> MtgjsonLegalities {
    let mut card_legalities = MtgjsonLegalities::new();
    
    for (key, value) in sf_card_legalities {
        if value != "not_legal" {
            let capitalized_value = capitalize_first_letter(value);
            
            match key.to_lowercase().as_str() {
                "standard" => card_legalities.standard = Some(capitalized_value),
                "pioneer" => card_legalities.pioneer = Some(capitalized_value),
                "modern" => card_legalities.modern = Some(capitalized_value),
                "legacy" => card_legalities.legacy = Some(capitalized_value),
                "vintage" => card_legalities.vintage = Some(capitalized_value),
                "commander" => card_legalities.commander = Some(capitalized_value),
                "brawl" => card_legalities.brawl = Some(capitalized_value),
                "pauper" => card_legalities.pauper = Some(capitalized_value),
                "penny" => card_legalities.penny = Some(capitalized_value),
                "duel" => card_legalities.duel = Some(capitalized_value),
                _ => {} // Unknown format
            }
        }
    }
    
    card_legalities
}

/// Parse rulings from Scryfall URL
pub fn parse_rulings(rulings_url: &str) -> Vec<MtgjsonRuling> {
    let mut mtgjson_rules = Vec::new();
    
    // TODO: Implement actual Scryfall API call
    println!("Parsing rulings from URL: {}", rulings_url);
    
    // For now, return empty vector as placeholder
    
    // Sort rulings by date and text - TODO: implement after actual data loading
    // mtgjson_rules.sort_by(|a, b| {
    //     a.date.cmp(&b.date).then_with(|| a.text.cmp(&b.text))
    // });
    
    mtgjson_rules
}

/// Add UUID to MTGJSON objects (placeholder implementation)
pub fn add_uuid_placeholder(object_name: &str, is_token: bool, set_code: &str) -> String {
    // This is a simplified version - the actual implementation would need
    // access to all object fields to generate proper UUIDs
    
    // For now, generate a random UUID as placeholder
    // In real implementation, this would use specific object properties
    let uuid_v5 = Uuid::new_v4();
    
    println!("Generated UUID: {} for object {} in set {}", uuid_v5, object_name, set_code);
    uuid_v5.to_string()
}

/// Add leadership skills to a card
pub fn add_leadership_skills(mtgjson_card: &mut MtgjsonCard) {
    let override_cards = vec!["Grist, the Hunger Tide"];
    
    let is_commander_legal = override_cards.contains(&mtgjson_card.name.as_str())
        || (mtgjson_card.type_.contains("Legendary") 
            && mtgjson_card.type_.contains("Creature")
            && mtgjson_card.type_ != "flip"
            && (mtgjson_card.side.as_deref() == Some("a") || mtgjson_card.side.is_none()))
        || mtgjson_card.text.contains("can be your commander");
    
    let is_oathbreaker_legal = mtgjson_card.type_.contains("Planeswalker");
    
    // This would need access to WhatsInStandardProvider to determine brawl legality
    let is_brawl_legal = false; // Placeholder
    
    if is_commander_legal || is_oathbreaker_legal || is_brawl_legal {
        mtgjson_card.leadership_skills = Some(MtgjsonLeadershipSkills {
            brawl: is_brawl_legal,
            commander: is_commander_legal,
            oathbreaker: is_oathbreaker_legal,
        });
    }
}

/// Build MTGJSON set from set code
pub fn build_mtgjson_set(set_code: &str) -> Option<MtgjsonSet> {
    let mut mtgjson_set = MtgjsonSet::new();
    
    // TODO: Implement actual data fetching from Scryfall
    println!("Building MTGJSON set for: {}", set_code);
    
    // Set basic properties (placeholder)
    mtgjson_set.code = set_code.to_uppercase();
    mtgjson_set.name = format!("Set {}", set_code); // Placeholder
    
    // TODO: Implement the full build process:
    // 1. Get set data from Scryfall or local cache
    // 2. Build cards using build_base_mtgjson_cards
    // 3. Add various enhancements (starter cards, variations, etc.)
    // 4. Build tokens
    // 5. Add sealed products
    // 6. Set metadata
    
    Some(mtgjson_set)
}

/// Helper function to capitalize first letter
fn capitalize_first_letter(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Mark duel deck assignments for cards
pub fn mark_duel_decks(set_code: &str, mtgjson_cards: &mut [MtgjsonCard]) {
    println!("Marking duel deck status for {}", set_code);
    
    if set_code.starts_with("DD") || set_code == "GS1" {
        let mut land_pile_marked = false;
        let mut side_letter_as_number = b'a';
        
        let constants = Constants::new();
        
        // Sort cards for consistent processing - TODO: implement Ord trait
        // mtgjson_cards.sort();
        
        for card in mtgjson_cards.iter_mut() {
            if constants.basic_land_names.contains(&card.name) {
                land_pile_marked = true;
            } else if card.type_.contains("Token") || card.type_.contains("Emblem") {
                continue;
            } else if land_pile_marked {
                side_letter_as_number += 1;
                land_pile_marked = false;
            }
            
            card.duel_deck = Some((side_letter_as_number as char).to_string());
        }
    }
    
    println!("Finished marking duel deck status for {}", set_code);
}

/// Parse keyrune code from URL
pub fn parse_keyrune_code(url: &str) -> String {
    // Extract filename stem from URL
    let path = std::path::Path::new(url);
    let file_stem = path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_uppercase();
    
    // TODO: Load keyrune_code_overrides.json
    // For now, return the file stem as-is
    file_stem
}

/// Get translation data for a set name
pub fn get_translation_data(mtgjson_set_name: &str) -> Option<HashMap<String, String>> {
    // TODO: Load mkm_set_name_translations.json
    // For now, return None as placeholder
    println!("Getting translation data for: {}", mtgjson_set_name);
    None
}

/// Add variations and alternative fields to cards within a set
pub fn add_variations_and_alternative_fields(mtgjson_set: &mut MtgjsonSet) {
    if mtgjson_set.cards.is_empty() {
        return;
    }

    println!("Adding variations for {}", mtgjson_set.code);

    let mut distinct_card_printings_found: HashSet<String> = HashSet::new();
    let constants = Constants::new();
    
    // We need to work with indices to avoid borrowing issues
    let card_count = mtgjson_set.cards.len();
    
    for i in 0..card_count {
        // Collect variations for this card
        let mut variations = Vec::new();
        let current_card_name = mtgjson_set.cards[i].name.split(" (").next().unwrap_or(&mtgjson_set.cards[i].name).to_string();
        let current_face_name = mtgjson_set.cards[i].face_name.clone();
        let current_uuid = mtgjson_set.cards[i].uuid.clone();
        let current_number = mtgjson_set.cards[i].number.clone();
        
        for j in 0..card_count {
            if i == j {
                continue;
            }
            
            let other_card_name = mtgjson_set.cards[j].name.split(" (").next().unwrap_or(&mtgjson_set.cards[j].name).to_string();
            let other_face_name = mtgjson_set.cards[j].face_name.clone();
            let other_uuid = mtgjson_set.cards[j].uuid.clone();
            let other_number = mtgjson_set.cards[j].number.clone();
            
            if current_card_name == other_card_name
                && current_face_name == other_face_name
                && current_uuid != other_uuid
                && (other_number != current_number || other_number.is_empty())
            {
                variations.push(other_uuid);
            }
        }
        
        if !variations.is_empty() {
            mtgjson_set.cards[i].variations = variations;
        }
        
        // Add alternative tag - ignore singleton printings and basics
        let has_variations = !mtgjson_set.cards[i].variations.is_empty();
        if !has_variations || constants.basic_land_names.contains(&mtgjson_set.cards[i].name) {
            continue;
        }
        
        // In each set, a card has to be unique by all of these attributes
        let distinct_card_printing = format!(
            "{}|{}|{}|{}|{}",
            mtgjson_set.cards[i].name,
            mtgjson_set.cards[i].border_color,
            mtgjson_set.cards[i].frame_version,
            mtgjson_set.cards[i].frame_effects.join(","),
            mtgjson_set.cards[i].side.as_deref().unwrap_or("")
        );
        
        // Special handling for certain sets
        if mtgjson_set.code == "UNH" || mtgjson_set.code == "10E" {
            let finishes = mtgjson_set.cards[i].finishes.join(",");
            let distinct_card_printing = format!("{}|{}", distinct_card_printing, finishes);
        }
        
        if distinct_card_printings_found.contains(&distinct_card_printing) {
            mtgjson_set.cards[i].is_alternative = Some(true);
        } else {
            distinct_card_printings_found.insert(distinct_card_printing);
        }
    }

    println!("Finished adding variations for {}", mtgjson_set.code);
}

/// Add other face IDs to all cards within a group
pub fn add_other_face_ids(cards_to_act_on: &mut [MtgjsonCard]) {
    if cards_to_act_on.is_empty() {
        return;
    }

    println!("Adding otherFaceIds to group");
    
    let card_count = cards_to_act_on.len();
    
    for i in 0..card_count {
        let current_names = cards_to_act_on[i].get_names();
        if current_names.is_empty() {
            continue;
        }
        
        let mut other_face_ids = Vec::new();
        let current_uuid = cards_to_act_on[i].uuid.clone();
        let current_layout = cards_to_act_on[i].layout.clone();
        let current_side = cards_to_act_on[i].side.clone();
        let current_number = cards_to_act_on[i].number.clone();
        
        for j in 0..card_count {
            if i == j {
                continue;
            }
            
            let other_face_name = cards_to_act_on[j].face_name.as_deref().unwrap_or("");
            let other_uuid = cards_to_act_on[j].uuid.clone();
            let other_side = cards_to_act_on[j].side.clone();
            let other_number = cards_to_act_on[j].number.clone();
            
            if !current_names.contains(&other_face_name.to_string()) {
                continue;
            }
            
            if current_uuid == other_uuid {
                continue;
            }
            
            if current_layout == "meld" {
                // Meld cards should account for the other sides
                if current_side != other_side {
                    other_face_ids.push(other_uuid);
                }
            } else if !other_number.is_empty() {
                // Most split cards should have the same number
                if other_number == current_number {
                    other_face_ids.push(other_uuid);
                }
            } else {
                // No number? No problem, just add it!
                other_face_ids.push(other_uuid);
            }
        }
        
        if !other_face_ids.is_empty() {
            cards_to_act_on[i].other_face_ids = other_face_ids;
        }
    }
    
    println!("Finished adding otherFaceIds to group");
}

/// Link same card with different details (foil/non-foil versions)
pub fn link_same_card_different_details(mtgjson_set: &mut MtgjsonSet) {
    println!("Linking multiple printings for {}", mtgjson_set.code);
    
    let mut cards_seen: HashMap<String, usize> = HashMap::new();
    let card_count = mtgjson_set.cards.len();
    
    for i in 0..card_count {
        let illustration_id = mtgjson_set.cards[i].identifiers.scryfall_illustration_id
            .as_deref()
            .unwrap_or("")
            .to_string();
            
        if let Some(&other_index) = cards_seen.get(&illustration_id) {
            let has_nonfoil = mtgjson_set.cards[i].finishes.contains(&"nonfoil".to_string());
            let other_uuid = mtgjson_set.cards[other_index].uuid.clone();
            let current_uuid = mtgjson_set.cards[i].uuid.clone();
            
            if has_nonfoil {
                mtgjson_set.cards[other_index].identifiers.mtgjson_non_foil_version_id = Some(current_uuid);
                mtgjson_set.cards[i].identifiers.mtgjson_foil_version_id = Some(other_uuid);
            } else {
                mtgjson_set.cards[other_index].identifiers.mtgjson_foil_version_id = Some(current_uuid);
                mtgjson_set.cards[i].identifiers.mtgjson_non_foil_version_id = Some(other_uuid);
            }
        } else {
            cards_seen.insert(illustration_id, i);
        }
    }
    
    println!("Finished linking multiple printings for {}", mtgjson_set.code);
}

/// Build base MTGJSON cards from a set
pub fn build_base_mtgjson_cards(
    set_code: &str,
    additional_cards: Option<Vec<HashMap<String, serde_json::Value>>>,
    is_token: bool,
    set_release_date: &str,
) -> Vec<MtgjsonCard> {
    println!("Building cards for {}", set_code);
    
    // TODO: Implement actual Scryfall API call
    // let cards = ScryfallProvider::download_cards(set_code);
    
    let mut mtgjson_cards = Vec::new();
    
    // For now, return empty vector as placeholder
    // In real implementation, this would:
    // 1. Download cards from Scryfall
    // 2. Process each card through build_mtgjson_card
    // 3. Sort cards consistently
    
    println!("Finished building cards for {}", set_code);
    mtgjson_cards
}

/// Add rebalanced to original linkage for Alchemy cards
pub fn add_rebalanced_to_original_linkage(mtgjson_set: &mut MtgjsonSet) {
    println!("Linking rebalanced cards for {}", mtgjson_set.code);
    
    let card_count = mtgjson_set.cards.len();
    
    for i in 0..card_count {
        if !mtgjson_set.cards[i].is_rebalanced.unwrap_or(false) {
            continue;
        }
        
        let rebalanced_name = mtgjson_set.cards[i].name.clone();
        let original_card_name_to_find = rebalanced_name.replace("A-", "");
        let mut original_card_uuids = Vec::new();
        
        for j in 0..card_count {
            if mtgjson_set.cards[j].name == original_card_name_to_find {
                // Link these cards bidirectionally
                original_card_uuids.push(mtgjson_set.cards[j].uuid.clone());
                
                // Add rebalanced printing to original card
                mtgjson_set.cards[j].rebalanced_printings.push(mtgjson_set.cards[i].uuid.clone());
            }
        }
        
        mtgjson_set.cards[i].original_printings = original_card_uuids;
    }
    
    println!("Finished linking rebalanced cards for {}", mtgjson_set.code);
}

/// Relocate miscellaneous tokens from cards to tokens array
pub fn relocate_miscellaneous_tokens(mtgjson_set: &mut MtgjsonSet) {
    println!("Relocate tokens for {}", mtgjson_set.code);
    
    let token_types = vec!["token", "double_faced_token", "emblem", "art_series"];
    
    // Identify unique tokens from cards
    let mut tokens_found = HashSet::new();
    for card in &mtgjson_set.cards {
        if token_types.contains(&card.layout.as_str()) {
            if let Some(ref scryfall_id) = card.identifiers.scryfall_id {
                tokens_found.insert(scryfall_id.clone());
            }
        }
    }
    
    // Remove tokens from cards array
    mtgjson_set.cards.retain(|card| !token_types.contains(&card.layout.as_str()));
    
    // Store Scryfall IDs for later token processing
    // TODO: Download Scryfall objects for these tokens
    println!("Found {} tokens to relocate", tokens_found.len());
    
    println!("Finished relocating tokens for {}", mtgjson_set.code);
}

/// Get the base and total set sizes
pub fn get_base_and_total_set_sizes(mtgjson_set: &MtgjsonSet) -> (i32, i32) {
    // TODO: Load base_set_sizes.json for manual corrections
    let mut base_set_size = mtgjson_set.cards.len() as i32;
    
    // Use knowledge of Boosterfun being the first non-numbered card
    // BoosterFun started with Throne of Eldraine in Oct 2019
    if mtgjson_set.release_date.as_ref().unwrap_or(&String::new()) > "2019-10-01" {
        for card in &mtgjson_set.cards {
            if card.promo_types.contains(&"boosterfun".to_string()) {
                // Extract number from card number
                let re = Regex::new(r"([0-9]+)").unwrap();
                if let Some(captures) = re.captures(&card.number) {
                    if let Some(number_match) = captures.get(1) {
                        if let Ok(card_number) = number_match.as_str().parse::<i32>() {
                            base_set_size = card_number - 1;
                            break;
                        }
                    }
                }
            }
        }
    }
    
    let total_set_size = mtgjson_set.cards.iter()
        .filter(|card| !card.is_rebalanced.unwrap_or(false))
        .count() as i32;
    
    (base_set_size, total_set_size)
}

/// Add starter card designation to cards not available in boosters
pub fn add_is_starter_option(
    set_code: &str,
    search_url: &str,
    mtgjson_cards: &mut [MtgjsonCard],
) {
    println!("Add starter data to {}", set_code);
    
    let starter_card_url = search_url.replace("&unique=", "++not:booster&unique=");
    
    // TODO: Download starter cards from Scryfall
    // let starter_cards = ScryfallProvider::download(starter_card_url);
    
    // For now, placeholder implementation
    println!("Would check for starter cards at: {}", starter_card_url);
    
    println!("Finished adding starter data to {}", set_code);
}

/// Build a single MTGJSON card from Scryfall data
pub fn build_mtgjson_card(
    sf_card: &HashMap<String, serde_json::Value>,
    sf_set: &HashMap<String, serde_json::Value>,
    set_release_date: &str,
) -> Option<MtgjsonCard> {
    let mut mtgjson_card = MtgjsonCard::new(false);
    
    // Extract basic card information
    mtgjson_card.name = sf_card.get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    mtgjson_card.number = sf_card.get("collector_number")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    mtgjson_card.type_ = sf_card.get("type_line")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    // Parse types
    let (super_types, types, sub_types) = parse_card_types(&mtgjson_card.type_);
    mtgjson_card.supertypes = super_types;
    mtgjson_card.types = types;
    mtgjson_card.subtypes = sub_types;
    
    // Extract mana cost and colors
    if let Some(mana_cost) = sf_card.get("mana_cost").and_then(|v| v.as_str()) {
        mtgjson_card.mana_cost = mana_cost.to_string();
        mtgjson_card.colors = get_card_colors(mana_cost);
        mtgjson_card.converted_mana_cost = get_card_cmc(mana_cost);
    }
    
    // Extract Oracle text
    mtgjson_card.text = sf_card.get("oracle_text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    // Extract layout
    mtgjson_card.layout = sf_card.get("layout")
        .and_then(|v| v.as_str())
        .unwrap_or("normal")
        .to_string();
    
    // Extract rarity
    mtgjson_card.rarity = sf_card.get("rarity")
        .and_then(|v| v.as_str())
        .unwrap_or("common")
        .to_string();
    
    // Extract power and toughness for creatures
    if let Some(power) = sf_card.get("power").and_then(|v| v.as_str()) {
        mtgjson_card.power = power.to_string();
    }
    if let Some(toughness) = sf_card.get("toughness").and_then(|v| v.as_str()) {
        mtgjson_card.toughness = toughness.to_string();
    }
    
    // Extract loyalty for planeswalkers
    if let Some(loyalty) = sf_card.get("loyalty").and_then(|v| v.as_str()) {
        mtgjson_card.loyalty = Some(loyalty.to_string());
    }
    
    // Extract finishes
    if let Some(finishes) = sf_card.get("finishes").and_then(|v| v.as_array()) {
        mtgjson_card.finishes = finishes.iter()
            .filter_map(|f| f.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    
    // Extract border color
    mtgjson_card.border_color = sf_card.get("border_color")
        .and_then(|v| v.as_str())
        .unwrap_or("black")
        .to_string();
    
    // Extract frame version
    mtgjson_card.frame_version = sf_card.get("frame")
        .and_then(|v| v.as_str())
        .unwrap_or("2015")
        .to_string();
    
    // Extract frame effects
    if let Some(frame_effects) = sf_card.get("frame_effects").and_then(|v| v.as_array()) {
        mtgjson_card.frame_effects = frame_effects.iter()
            .filter_map(|f| f.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    
    // Extract promo types
    if let Some(promo_types) = sf_card.get("promo_types").and_then(|v| v.as_array()) {
        mtgjson_card.promo_types = promo_types.iter()
            .filter_map(|f| f.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    
    // Build identifiers
    // TODO: Implement full identifier extraction
    
    // Build legalities
    if let Some(legalities) = sf_card.get("legalities").and_then(|v| v.as_object()) {
        let legalities_map: HashMap<String, String> = legalities.iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
        mtgjson_card.legalities = parse_legalities(&legalities_map);
    }
    
    // Extract ruling information
    if let Some(rulings_uri) = sf_card.get("rulings_uri").and_then(|v| v.as_str()) {
        mtgjson_card.rulings = Some(parse_rulings(rulings_uri));
    }
    
    // Extract printings
    if let Some(prints_uri) = sf_card.get("prints_search_uri").and_then(|v| v.as_str()) {
        mtgjson_card.printings = parse_printings(Some(prints_uri));
    }
    
    // Parse foreign data
    if let Some(prints_uri) = sf_card.get("prints_search_uri").and_then(|v| v.as_str()) {
        mtgjson_card.foreign_data = parse_foreign(
            prints_uri,
            &mtgjson_card.name,
            &mtgjson_card.number,
            sf_set.get("name").and_then(|v| v.as_str()).unwrap_or("")
        );
    }
    
    // Generate UUID
    mtgjson_card.uuid = add_uuid_placeholder(&mtgjson_card.name, false, "");
    
    Some(mtgjson_card)
}

/// Complete the set building process with all enhancements
pub fn complete_set_building(
    set_code: &str,
    mtgjson_set: &mut MtgjsonSet,
    set_object: &HashMap<String, serde_json::Value>,
) {
    println!("Completing set building for {}", set_code);
    
    // Get release date
    let release_date = set_object.get("released_at")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    // Build cards
    mtgjson_set.cards = build_base_mtgjson_cards(
        set_code,
        None,
        false,
        &release_date
    );
    
    // Add various enhancements to cards
    add_variations_and_alternative_fields(mtgjson_set);
    add_other_face_ids(&mut mtgjson_set.cards);
    link_same_card_different_details(mtgjson_set);
    add_rebalanced_to_original_linkage(mtgjson_set);
    
    // Process tokens
    relocate_miscellaneous_tokens(mtgjson_set);
    
    // Mark starter cards
    let search_url = format!("https://api.scryfall.com/cards/search?q=set:{}", set_code);
    add_is_starter_option(set_code, &search_url, &mut mtgjson_set.cards);
    
    // Add leadership skills to eligible cards
    for card in &mut mtgjson_set.cards {
        add_leadership_skills(card);
    }
    
    // Mark duel deck assignments if applicable
    mark_duel_decks(set_code, &mut mtgjson_set.cards);
    
    // Calculate set sizes
    let (base_set_size, total_set_size) = get_base_and_total_set_sizes(mtgjson_set);
    mtgjson_set.base_set_size = base_set_size;
    mtgjson_set.total_set_size = total_set_size;
    
    // TODO: Add tokens building
    // TODO: Add sealed products
    // TODO: Add decks
    
    println!("Finished building set {}", set_code);
}

/// Build sealed products for a set
pub fn build_sealed_products(set_code: &str) -> Vec<MtgjsonSealedProduct> {
    println!("Building sealed products for {}", set_code);
    
    let mut sealed_products = Vec::new();
    
    // TODO: Implement actual sealed product building
    // This would involve:
    // 1. Getting sealed product data from various providers
    // 2. Creating MtgjsonSealedProduct objects
    // 3. Linking products to sets
    
    println!("Finished building sealed products for {}", set_code);
    sealed_products
}

/// Build decks for a set 
pub fn build_decks(set_code: &str) -> Vec<MtgjsonDeck> {
    println!("Building decks for {}", set_code);
    
    let mut decks = Vec::new();
    
    // TODO: Implement actual deck building
    // This would involve:
    // 1. Getting deck data from GitHub provider
    // 2. Creating MtgjsonDeck objects
    // 3. Linking decks to sets
    
    println!("Finished building decks for {}", set_code);
    decks
}

/// Enhance cards with additional metadata
pub fn enhance_cards_with_metadata(mtgjson_cards: &mut [MtgjsonCard]) {
    println!("Enhancing cards with metadata");
    
    for card in mtgjson_cards.iter_mut() {
        // Add color identity for commanders
        if card.type_.contains("Legendary") && card.type_.contains("Creature") {
            card.color_identity = card.colors.clone();
        }
        
        // Mark basic lands
        let constants = Constants::new();
        if constants.basic_land_names.contains(&card.name) {
            card.supertypes.push("Basic".to_string());
        }
        
        // Calculate EDH rec rank (placeholder)
        // TODO: Implement actual EDHREC integration
        
        // Add purchase URLs (placeholder)
        // TODO: Implement actual purchase URL building
    }
    
    println!("Finished enhancing cards");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_card_types_basic() {
        let (super_types, types, sub_types) = parse_card_types("Creature — Human Wizard");
        assert_eq!(super_types, Vec::<String>::new());
        assert_eq!(types, vec!["Creature"]);
        assert_eq!(sub_types, vec!["Human", "Wizard"]);
    }

    #[test]
    fn test_parse_card_types_legendary() {
        let (super_types, types, sub_types) = parse_card_types("Legendary Creature — Human Wizard");
        assert_eq!(super_types, vec!["Legendary"]);
        assert_eq!(types, vec!["Creature"]);
        assert_eq!(sub_types, vec!["Human", "Wizard"]);
    }

    #[test]
    fn test_get_card_colors() {
        let colors = get_card_colors("{2}{W}{U}");
        assert_eq!(colors, vec!["W", "U"]);
    }

    #[test]
    fn test_get_card_cmc_simple() {
        assert_eq!(get_card_cmc("{3}"), 3.0);
        assert_eq!(get_card_cmc("{2}{W}{U}"), 4.0);
    }

    #[test]
    fn test_get_card_cmc_hybrid() {
        assert_eq!(get_card_cmc("{2/W}"), 2.0); // Takes higher cost
    }

    #[test]
    fn test_is_number() {
        assert!(is_number("123"));
        assert!(is_number("12.5"));
        assert!(!is_number("abc"));
        assert!(!is_number("X"));
    }
}