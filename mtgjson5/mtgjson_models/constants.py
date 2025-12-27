"""
MTGJSON field conventions and constants.
"""

# Fields whose list values should be sorted alphabetically
# Note: subtypes, supertypes, types, variations, finishes preserve original order from source
SORTED_LIST_FIELDS: frozenset[str] = frozenset({
    "artistIds",
    "attractionLights",
    "availability",
    "boosterTypes",
    "cardParts",
    "colorIdentity",
    "colorIndicator",
    "colors",
    "frameEffects",
    "keywords",
    "originalPrintings",
    "otherFaceIds",
    "printings",
    "promoTypes",
    "rebalancedPrintings",
    "reverseRelated",
    "subsets",
})

# Required list fields - should be present as [] even when empty (card-level)
REQUIRED_CARD_LIST_FIELDS: frozenset[str] = frozenset({
    "availability",
    "colorIdentity",
    "colors",
    "finishes",
    "foreignData",
    "printings",
    "subtypes",
    "supertypes",
    "types",
})

# Required list fields for deck structures
REQUIRED_DECK_LIST_FIELDS: frozenset[str] = frozenset({
    "mainBoard",
    "sideBoard",
})

# Combined required list fields
REQUIRED_LIST_FIELDS: frozenset[str] = REQUIRED_CARD_LIST_FIELDS | REQUIRED_DECK_LIST_FIELDS

# Fields where empty list should be OMITTED
OMIT_EMPTY_LIST_FIELDS: frozenset[str] = frozenset({
    "artistIds",
    "attractionLights",
    "boosterTypes",
    "cardParts",
    "frameEffects",
    "keywords",
    "originalPrintings",
    "otherFaceIds",
    "promoTypes",
    "rebalancedPrintings",
    "reverseRelated",
    "rulings",
    "sealedProductUuids",
    "subsets",
    "variations",
})

# Optional boolean fields - omit unless True
OPTIONAL_BOOL_FIELDS: frozenset[str] = frozenset({
    "hasAlternativeDeckLimit",
    "hasContentWarning",
    "isAlternative",
    "isFullArt",
    "isFunny",
    "isGameChanger",
    "isOnlineOnly",
    "isOversized",
    "isPromo",
    "isRebalanced",
    "isReprint",
    "isReserved",
    "isStarter",
    "isStorySpotlight",
    "isTextless",
    "isTimeshifted",
})

# Other optional fields - omit if empty/null/zero
OTHER_OPTIONAL_FIELDS: frozenset[str] = frozenset({
    "asciiName",
    "colorIndicator",
    "defense",
    "duelDeck",
    "edhrecSaltiness",
    "faceConvertedManaCost",
    "faceManaValue",
    "faceName",
    "flavorName",
    "hand",
    "leadershipSkills",
    "life",
    "loyalty",
    "manaCost",
    "power",
    "printedName",
    "printedType",
    "relatedCards",
    "securityStamp",
    "side",
    "sourceProducts",
    "toughness",
    "watermark",
})

# Fields to exclude from AllPrintings output (not in source schema)
# Note: relatedCards is intentionally NOT excluded - it's on tokens
EXCLUDE_FROM_OUTPUT: frozenset[str] = frozenset({
    "cardParts",
    "isPromo",
    "originalReleaseDate",
})

# Combined set of all fields to potentially omit
OMIT_FIELDS: frozenset[str] = OPTIONAL_BOOL_FIELDS | OMIT_EMPTY_LIST_FIELDS | OTHER_OPTIONAL_FIELDS

# Legacy alias for backward compatibility
ALLOW_IF_FALSEY: frozenset[str] = frozenset({
    # Required string fields
    "uuid", "setCode", "text", "type", "layout", "frameVersion",
    "language", "name", "number", "borderColor", "rarity",
    # Required list fields
    *REQUIRED_LIST_FIELDS,
    # Numeric fields that should be present even if 0
    "convertedManaCost", "manaValue", "faceConvertedManaCost",
    "faceManaValue", "count",
    # Boolean fields that should be present even if false
    "hasFoil", "hasNonFoil", "isFoil",
})

# TypedDict field aliases for pipeline -> model conversion
# Maps (TypedDict_name, source_field) -> target_field
TYPEDDICT_FIELD_ALIASES: dict[tuple[str, str], str] = {
    # Rulings: Scryfall uses publishedAt/comment, MTGJSON uses date/text
    ("Rulings", "publishedAt"): "date",
    ("Rulings", "comment"): "text",
}

# Required set-level boolean fields - always present even when False
REQUIRED_SET_BOOL_FIELDS: frozenset[str] = frozenset({
    "isFoilOnly",
    "isOnlineOnly",
})

# All supported languages
LANGUAGE_MAP: dict[str, str] = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese (Brazil)",
    "ja": "Japanese",
    "ko": "Korean",
    "ru": "Russian",
    "zhs": "Chinese Simplified",
    "zht": "Chinese Traditional",
    "he": "Hebrew",
    "la": "Latin",
    "grc": "Ancient Greek",
    "ar": "Arabic",
    "sa": "Sanskrit",
    "ph": "Phyrexian",
}
