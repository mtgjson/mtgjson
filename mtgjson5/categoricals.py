""" Categorical column definitions for processing Scryfall data in Polars. """
from typing import Dict, List

# These require .list.eval() for casting
LIST_CATEGORICAL_COLS = [
     "colors",
     "color_identity",
     "color_indicator",
     "produced_mana",
     "frame_effects",
     "finishes",
     "games",
     "promo_types",
     "keywords",
 ]

# Columns that are simple strings (Utf8)
SCALAR_CATEGORICAL_COLS = [
     "rarity",
     "layout",
     "border_color",
     "frame",
     "security_stamp",
     "set_type",
     "image_status",
     "lang",
 ]

# This enables O(1) integer comparisons instead of string comparisons
STATIC_CATEGORICALS: Dict[str, List[str]] = {
     "colors": ["W", "U", "B", "R", "G"],
     "color_identity": ["W", "U", "B", "R", "G"],
     "color_indicator": ["W", "U", "B", "R", "G"],
     "rarity": ["common", "uncommon", "rare", "mythic", "special", "bonus"],
     "layout": [
         "normal", "split", "flip", "transform", "modal_dfc", "meld",
         "leveler", "class", "case", "saga", "adventure", "mutate",
         "prototype", "battle", "planar", "scheme", "vanguard", "token",
         "double_faced_token", "emblem", "augment", "host", "art_series",
         "reversible_card",
     ],
     "border_color": ["black", "white", "borderless", "silver", "gold"],
     "frame": ["1993", "1997", "2003", "2015", "future"],
     "frame_effects": [
         "legendary", "miracle", "enchantment", "draft", "devoid",
         "tombstone", "colorshifted", "inverted", "sunmoondfc",
         "compasslanddfc", "originpwdfc", "mooneldrazidfc",
         "waxingandwaningmoondfc", "showcase", "extendedart",
         "companion", "etched", "snow", "lesson", "shatteredglass",
         "convertdfc", "fandfc", "upsidedowndfc", "spree",
     ],
     "security_stamp": ["oval", "triangle", "acorn", "circle", "arena", "heart"],
     "lang": [
         "en", "es", "fr", "de", "it", "pt", "ja", "ko", "ru",
         "zhs", "zht", "he", "la", "grc", "ar", "sa", "ph",
     ],
     "finishes": ["foil", "nonfoil", "etched", "glossy"],
     "games": ["paper", "arena", "mtgo", "astral", "sega"],
     "image_status": ["missing", "placeholder", "lowres", "highres_scan"],
     "set_type": [
         "core", "expansion", "masters", "alchemy", "masterpiece", "arsenal",
         "from_the_vault", "spellbook", "premium_deck", "duel_deck",
         "draft_innovation", "treasure_chest", "commander", "planechase",
         "archenemy", "vanguard", "funny", "starter", "box", "promo",
         "token", "memorabilia", "minigame",
     ],
     "legalities": [
        "alchemy",
        "brawl",
        "commander",
        "duel",
        "explorer",
        "future",
        "gladiator",
        "historic",
        "historicbrawl",
        "legacy",
        "modern",
        "oathbreaker",
        "oldschool",
        "pauper",
        "paupercommander",
        "penny",
        "pioneer",
        "predh",
        "premodern",
        "standard",
        "standardbrawl",
        "timeless",
        "vintage",
    ],
 }
