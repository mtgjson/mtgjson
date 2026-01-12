"""
Field name constants and mappings.

Centralizes all field name transformations between Scryfall and MTGJSON.
"""

from __future__ import annotations

from typing import Final


SORTED_LIST_FIELDS: Final[frozenset[str]] = frozenset({
	"attractionLights",
	"availability",
	"boosterTypes",
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

# Required list fields for deck structures
REQUIRED_DECK_LIST_FIELDS: Final[frozenset[str]] = frozenset({
	"mainBoard",
	"sideBoard",
})

# Combined required list fields
REQUIRED_LIST_FIELDS: Final[frozenset[str]] = REQUIRED_CARD_LIST_FIELDS | REQUIRED_DECK_LIST_FIELDS

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
	"rulings",
	"sealedProductUuids",
	"subsets",
	"variations",
})

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

# Required set-level boolean fields - always present even when False
REQUIRED_SET_BOOL_FIELDS: Final[frozenset[str]] = frozenset({
	"isFoilOnly",
	"isOnlineOnly",
})

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

# Fields to exclude from AllPrintings output
# If a field needs exclusion, document why before adding it here.
EXCLUDE_FROM_OUTPUT: Final[frozenset[str]] = frozenset()

# Combined set of all fields to potentially omit
OMIT_FIELDS: Final[frozenset[str]] = OPTIONAL_BOOL_FIELDS | OMIT_EMPTY_LIST_FIELDS | OTHER_OPTIONAL_FIELDS



# List of fields that should always be included even if they are falsey
ALLOW_IF_FALSEY: Final[frozenset[str]] = frozenset({
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
	*REQUIRED_LIST_FIELDS,
	"convertedManaCost",
	"manaValue",
	"faceConvertedManaCost",
	"faceManaValue",
	"count",
	"hasFoil",
	"hasNonFoil",
	"isFoil",
	"isEtched",
})

# Mapping of identifiers struct fields to source columns
IDENTIFIERS_FIELD_SOURCES: Final[dict[str, str | tuple[str, ...]]] = {
	"scryfallId": "scryfallId",
	"scryfallOracleId": ("_face_data.oracle_id", "oracleId"),
	"scryfallIllustrationId": ("_face_data.illustration_id", "illustrationId"),
	"scryfallCardBackId": "cardBackId",
	"mcmId": "mcmId",
	"mcmMetaId": "mcmMetaId",
	"mtgArenaId": "arenaId",
	"mtgoId": "mtgoId",
	"mtgoFoilId": "mtgoFoilId",
	"multiverseId": "multiverseIds[faceId]",
	"tcgplayerProductId": "tcgplayerId",
	"tcgplayerEtchedProductId": "tcgplayerEtchedId",
	"cardKingdomId": "cardKingdomId",
	"cardKingdomFoilId": "cardKingdomFoilId",
	"cardKingdomEtchedId": "cardKingdomEtchedId",
}
