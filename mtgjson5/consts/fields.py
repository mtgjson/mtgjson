"""
Field name constants and mappings.

Centralizes all field name transformations between Scryfall and MTGJSON.
Single source of truth - conventions.py re-exports from here.
"""

from __future__ import annotations

from typing import Final


# =============================================================================
# List Field Classifications
# =============================================================================

# Fields whose list values should be sorted alphabetically
# Note: 'finishes' is NOT included - it uses canonical order (nonfoil, foil, etched)
# Note: 'subtypes', 'artistIds', 'promoTypes' are NOT sorted - they preserve source order
# Note: 'types', 'supertypes' preserve type_line order (e.g. "Enchantment Creature" not "Creature Enchantment")
# Note: 'variations', 'otherFaceIds' preserve source/discovery order, not alphabetical
# Note: 'colors', 'colorIdentity', 'colorIndicator' ARE sorted alphabetically (source behavior)
SORTED_LIST_FIELDS: Final[frozenset[str]] = frozenset({
	"attractionLights",
	"availability",
	"boosterTypes",
	"cardParts",
	"colorIdentity",
	"colorIndicator",
	"colors",
	"frameEffects",
	"games",
	"keywords",
	"originalPrintings",
	"printings",
	"rebalancedPrintings",
	"reverseRelated",
	"subsets",
})
"""List fields sorted alphabetically in output."""

# Required list fields - should be present as [] even when empty (card-level)
REQUIRED_CARD_LIST_FIELDS: Final[frozenset[str]] = frozenset({
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
"""Card-level list fields that must be [] not null."""

# Required list fields for deck structures
REQUIRED_DECK_LIST_FIELDS: Final[frozenset[str]] = frozenset({
	"mainBoard",
	"sideBoard",
})
"""Deck-level list fields that must be [] not null."""

# Combined required list fields
REQUIRED_LIST_FIELDS: Final[frozenset[str]] = REQUIRED_CARD_LIST_FIELDS | REQUIRED_DECK_LIST_FIELDS
"""All list fields that must be [] not null."""

# Fields where empty list should be OMITTED
OMIT_EMPTY_LIST_FIELDS: Final[frozenset[str]] = frozenset({
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
"""List fields omitted when empty."""


# =============================================================================
# Boolean Field Classifications
# =============================================================================

# Optional boolean fields - omit unless True
OPTIONAL_BOOL_FIELDS: Final[frozenset[str]] = frozenset({
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
"""Boolean fields omitted from output when False."""

# Required set-level boolean fields - always present even when False
REQUIRED_SET_BOOL_FIELDS: Final[frozenset[str]] = frozenset({
	"isFoilOnly",
	"isOnlineOnly",
})
"""Set-level boolean fields always present."""


# =============================================================================
# Other Optional Field Classifications
# =============================================================================

# Other optional fields - omit if empty/null/zero
OTHER_OPTIONAL_FIELDS: Final[frozenset[str]] = frozenset({
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
"""Other fields omitted if empty/null/zero."""

# Fields to exclude from AllPrintings output
# Note: This set is intentionally EMPTY - all fields should be included by default.
# If a field needs exclusion, document why before adding it here.
EXCLUDE_FROM_OUTPUT: Final[frozenset[str]] = frozenset()
"""Fields excluded from output (currently empty)."""

# Combined set of all fields to potentially omit
OMIT_FIELDS: Final[frozenset[str]] = OPTIONAL_BOOL_FIELDS | OMIT_EMPTY_LIST_FIELDS | OTHER_OPTIONAL_FIELDS
"""Combined set of all fields that may be omitted."""


# =============================================================================
# Always-Include Fields
# =============================================================================

# List of fields that should always be included even if they are falsey
# (empty string, 0, False, empty list/dict)
ALLOW_IF_FALSEY: Final[frozenset[str]] = frozenset({
	# Required string fields
	"uuid",
	"setCode",
	"type",
	"layout",
	"frameVersion",
	"language",
	"name",
	"number",
	"borderColor",
	"rarity",
	# Required list fields
	*REQUIRED_LIST_FIELDS,
	# Numeric fields that should be present even if 0
	"convertedManaCost",
	"manaValue",
	"faceConvertedManaCost",
	"faceManaValue",
	"count",
	# Boolean fields that should be present even if false
	"hasFoil",
	"hasNonFoil",
	"isFoil",
	"isEtched",
})
"""Fields always included even when falsey."""


# =============================================================================
# Identifier Field Sources
# =============================================================================

IDENTIFIERS_FIELD_SOURCES: Final[dict[str, str | tuple[str, ...]]] = {
	"scryfallId": "scryfallId",
	"scryfallOracleId": ("_face_data.oracle_id", "oracleId"),  # coalesce
	"scryfallIllustrationId": ("_face_data.illustration_id", "illustrationId"),
	"scryfallCardBackId": "cardBackId",
	"mcmId": "mcmId",
	"mcmMetaId": "mcmMetaId",
	"mtgArenaId": "arenaId",
	"mtgoId": "mtgoId",
	"mtgoFoilId": "mtgoFoilId",
	"multiverseId": "multiverseIds[faceId]",  # Special: indexed per face
	"tcgplayerProductId": "tcgplayerId",
	"tcgplayerEtchedProductId": "tcgplayerEtchedId",
	"cardKingdomId": "cardKingdomId",
	"cardKingdomFoilId": "cardKingdomFoilId",
	"cardKingdomEtchedId": "cardKingdomEtchedId",
}
"""Mapping of identifiers struct fields to source columns."""
