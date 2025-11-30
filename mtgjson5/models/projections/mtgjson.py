"""
MTGJSON schema definitions using field exclusion patterns.
"""

from typing import FrozenSet, Set
import polars as pl

# Fields that always exist on all card types
ALWAYS_EXIST_FIELDS: FrozenSet[str] = frozenset({
    "availability",
    "borderColor",
    "colorIdentity",
    "colors",
    "convertedManaCost",
    "finishes",
    "frameVersion",
    "hasFoil",
    "hasnonFoil",
    "identifiers"
    "language",
    "layout",
    "legalities",
    "manaValue",
    "name",
    "number",
    "purchaseUrls",
    "rarity",
    "setCode",
    "subtypes",
    "supertypes",
    "type",
    "types",
    "uuid"
})

# All possible card fields (superset of all card types)
ALL_CARD_FIELDS: FrozenSet[str] = frozenset({
    # Identity
    "uuid",
    "name",
    "asciiName",
    "faceName",
    "faceFlavorName",
    "flavorName",
    "setCode",
    "number",
    "side",
    
    # Mana & Colors
    "manaCost",
    "manaValue",
    "convertedManaCost",
    "faceManaValue",
    "faceConvertedManaCost",
    "colors",
    "colorIdentity",
    "colorIndicator",
    
    # Types
    "type",
    "supertypes",
    "types",
    "subtypes",
    
    # Text
    "text",
    "flavorText",
    "originalText",
    "originalType",
    
    # Stats
    "power",
    "toughness",
    "loyalty",
    "defense",
    "hand",
    "life",
    
    # Appearance
    "artist",
    "artistIds",
    "borderColor",
    "frameVersion",
    "frameEffects",
    "securityStamp",
    "watermark",
    "signature",
    "orientation",
    
    # Finishes & Availability
    "finishes",
    "hasFoil",
    "hasNonFoil",
    "availability",
    "boosterTypes",
    
    # Boolean Flags
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
    
    # Gameplay
    "keywords",
    "layout",
    "rarity",
    "edhrecRank",
    "edhrecSaltiness",
    "attractionLights",
    "duelDeck",
    
    # Printing Info
    "language",
    "printedName",
    "printedText",
    "printedType",
    "originalReleaseDate",
    "promoTypes",
    
    # Relations
    "otherFaceIds",
    "variations",
    "cardParts",
    "printings",
    "firstPrinting",
    "originalPrintings",
    "rebalancedPrintings",
    "reverseRelated",
    "subsets",
    
    # Nested Objects
    "identifiers",
    "legalities",
    "leadershipSkills",
    "purchaseUrls",
    "relatedCards",
    "rulings",
    "foreignData",
    "sourceProducts",
    
    # Deck-specific
    "count",
    "isFoil",
})

# Fields NOT present on CardToken
TOKEN_EXCLUDE: FrozenSet[str] = frozenset({
    # No mana value on tokens
    "manaValue",
    "convertedManaCost",
    "faceManaValue",
    "faceConvertedManaCost",
    
    # No gameplay metadata
    "rarity",
    "edhrecRank",
    "legalities",
    "leadershipSkills",
    "purchaseUrls",
    "rulings",
    "foreignData",
    "printings",
    "firstPrinting",
    
    # No rebalancing
    "isRebalanced",
    "originalPrintings",
    "rebalancedPrintings",
    
    # No starter/reserved status
    "isStarter",
    "isReserved",
    "isTimeshifted",
    "isAlternative",
    "isGameChanger",
    
    # No deck limit info
    "hasAlternativeDeckLimit",
    "hasContentWarning",
    
    # Other missing
    "duelDeck",
    "variations",
    "hand",
    "life",
    "printedName",
    "printedText",
    "printedType",
    "originalReleaseDate",
    
    # Deck-specific
    "count",
    "isFoil",
})

# Fields NOT present on CardAtomic (oracle-level, no printing info)
ATOMIC_EXCLUDE: FrozenSet[str] = frozenset({
    # No printing-specific data
    "setCode",
    "number",
    "artist",
    "artistIds",
    "borderColor",
    "frameVersion",
    "frameEffects",
    "securityStamp",
    "watermark",
    "signature",
    "orientation",
    "finishes",
    "hasFoil",
    "hasNonFoil",
    "availability",
    "boosterTypes",
    "flavorText",
    "flavorName",
    "faceFlavorName",
    "originalReleaseDate",
    "promoTypes",
    "rarity",
    "language",
    "printedName",
    "printedText",
    "printedType",
    "duelDeck",
    
    # No per-printing flags
    "isAlternative",
    "isFullArt",
    "isOnlineOnly",
    "isOversized",
    "isPromo",
    "isRebalanced",
    "isReprint",
    "isStarter",
    "isStorySpotlight",
    "isTextless",
    "isTimeshifted",
    "hasContentWarning",
    
    # No per-printing relations
    "otherFaceIds",
    "variations",
    "originalPrintings",
    "rebalancedPrintings",
    "reverseRelated",
    "sourceProducts",
    "foreignData",
    
    # No per-printing rankings
    "edhrecRank",
    
    # Deck-specific
    "count",
    "isFoil",
})

# Fields NOT present on CardSet (the standard card type)
CARD_SET_EXCLUDE: FrozenSet[str] = frozenset({
    # Token-only
    "orientation",
    "reverseRelated",  # On CardSet this is inside relatedCards
    
    # Atomic-only
    "firstPrinting",
    
    # Deck-only
    "count",
    "isFoil",
})

# Fields NOT present on CardDeck
CARD_DECK_EXCLUDE: FrozenSet[str] = frozenset({
    # Token-only
    "orientation",
    
    # Atomic-only
    "firstPrinting",
})

# Additional fields for CardDeck (added, not excluded)
CARD_DECK_EXTRA: FrozenSet[str] = frozenset({
    "count",
    "isFoil",
})


def get_card_set_fields() -> Set[str]:
    """Get fields for CardSet (standard set card)."""
    return ALL_CARD_FIELDS - CARD_SET_EXCLUDE


def get_card_token_fields() -> Set[str]:
    """Get fields for CardToken."""
    return ALL_CARD_FIELDS - TOKEN_EXCLUDE


def get_card_atomic_fields() -> Set[str]:
    """Get fields for CardAtomic (oracle card)."""
    return ALL_CARD_FIELDS - ATOMIC_EXCLUDE


def get_card_deck_fields() -> Set[str]:
    """Get fields for CardDeck (card in a deck list)."""
    return (ALL_CARD_FIELDS - CARD_DECK_EXCLUDE) | CARD_DECK_EXTRA


IDENTIFIER_FIELDS: FrozenSet[str] = frozenset({
    "abuId",
    "cardKingdomEtchedId",
    "cardKingdomFoilId",
    "cardKingdomId",
    "cardsphereId",
    "cardsphereFoilId",
    "cardtraderId",
    "csiId",
    "mcmId",
    "mcmMetaId",
    "miniaturemarketId",
    "mtgArenaId",
    "mtgjsonFoilVersionId",
    "mtgjsonNonFoilVersionId",
    "mtgjsonV4Id",
    "mtgoFoilId",
    "mtgoId",
    "multiverseId",
    "scgId",
    "scryfallId",
    "scryfallCardBackId",
    "scryfallOracleId",
    "scryfallIllustrationId",
    "tcgplayerProductId",
    "tcgplayerEtchedProductId",
    "tntId",
})


LEGALITY_FORMATS: FrozenSet[str] = frozenset({
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
})


PURCHASE_URL_FIELDS: FrozenSet[str] = frozenset({
    "cardKingdom",
    "cardKingdomEtched",
    "cardKingdomFoil",
    "cardmarket",
    "tcgplayer",
    "tcgplayerEtched",
})


OPTIONAL_BOOL_FIELDS: FrozenSet[str] = frozenset({
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

# Required boolean fields (must be present as true/false)
REQUIRED_BOOL_FIELDS: FrozenSet[str] = frozenset({
    "hasFoil",
    "hasNonFoil",
})


def select_columns_for_type(
    df: pl.DataFrame,
    card_type: str = 'card_set'
) -> pl.DataFrame:
    """
    Select and order columns for a specific card type.
    
    :param df: DataFrame with card data
    :param card_type: One of "card_set", "card_token", "card_atomic", "card_deck"
    :return: DataFrame with only the appropriate columns
    """
    
    type_to_fields = {
        "card_set": get_card_set_fields,
        "card_token": get_card_token_fields,
        "card_atomic": get_card_atomic_fields,
        "card_deck": get_card_deck_fields,
    }
    
    if card_type not in type_to_fields:
        raise ValueError(f"Unknown card type: {card_type}")
    
    allowed_fields = type_to_fields[card_type]()
    existing_allowed = [c for c in df.columns if c in allowed_fields]
    existing_allowed.sort()
    
    return df.select(existing_allowed)