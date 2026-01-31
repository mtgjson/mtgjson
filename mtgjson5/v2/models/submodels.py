"""
MTGJSON TypedDict sub-models.

These are lightweight dict-based types used as nested structures
in card and set models. Using TypedDict instead of BaseModel
for ~2.5x faster parsing performance.
"""

from __future__ import annotations

from typing_extensions import Required, TypedDict  # noqa: UP035

# =============================================================================
# Core Card Sub-Models
# =============================================================================


class ForeignDataIdentifiers(TypedDict, total=False):
    """Identifiers for foreign card data."""

    multiverseId: str
    scryfallId: str


class ForeignData(TypedDict, total=False):
    """Localized card data."""

    faceName: str
    flavorText: str
    identifiers: ForeignDataIdentifiers
    language: Required[str]
    multiverseId: int  # Deprecated - top level multiverse ID
    name: Required[str]
    text: str
    type: str
    uuid: str


class Identifiers(TypedDict, total=False):
    """External identifiers for a card or sealed product."""

    # Sealed product identifiers
    abuId: str
    cardtraderId: str
    csiId: str
    miniaturemarketId: str
    mvpId: str
    scgId: str
    tntId: str
    # Card identifiers
    cardKingdomEtchedId: str
    cardKingdomFoilId: str
    cardKingdomId: str
    cardsphereId: str
    cardsphereFoilId: str
    deckboxId: str
    mcmId: str
    mcmMetaId: str
    mtgArenaId: str
    mtgjsonFoilVersionId: str
    mtgjsonNonFoilVersionId: str
    mtgjsonV4Id: str
    mtgoFoilId: str
    mtgoId: str
    multiverseId: str
    scryfallId: str
    scryfallCardBackId: str
    scryfallIllustrationId: str
    scryfallOracleId: str
    tcgplayerEtchedProductId: str
    tcgplayerProductId: str


class LeadershipSkills(TypedDict):
    """Commander/Brawl/Oathbreaker legality."""

    brawl: bool
    commander: bool
    oathbreaker: bool


class Legalities(TypedDict, total=False):
    """Format legalities for a card."""

    alchemy: str
    brawl: str
    commander: str
    duel: str
    explorer: str
    future: str
    gladiator: str
    historic: str
    historicbrawl: str
    legacy: str
    modern: str
    oathbreaker: str
    oldschool: str
    pauper: str
    paupercommander: str
    penny: str
    pioneer: str
    predh: str
    premodern: str
    standard: str
    standardbrawl: str
    timeless: str
    vintage: str


class PurchaseUrls(TypedDict, total=False):
    """Purchase URLs for a card."""

    cardKingdom: str
    cardKingdomEtched: str
    cardKingdomFoil: str
    cardmarket: str
    tcgplayer: str
    tcgplayerEtched: str


class RelatedCards(TypedDict, total=False):
    """Related cards (spellbook, reverse related)."""

    reverseRelated: list[str]
    spellbook: list[str]


class Rulings(TypedDict):
    """Card ruling entry."""

    date: str
    text: str


class SourceProducts(TypedDict, total=False):
    """Sealed products containing this card."""

    etched: list[str]
    foil: list[str]
    nonfoil: list[str]


# =============================================================================
# Meta/Translations
# =============================================================================


class Meta(TypedDict):
    """MTGJSON file metadata."""

    date: str
    version: str


class Translations(TypedDict, total=False):
    """Set name translations by language."""

    AncientGreek: str | None
    Arabic: str | None
    ChineseSimplified: str | None
    ChineseTraditional: str | None
    French: str | None
    German: str | None
    Hebrew: str | None
    Italian: str | None
    Japanese: str | None
    Korean: str | None
    Latin: str | None
    Phyrexian: str | None
    PortugueseBrazil: str | None
    Russian: str | None
    Sanskrit: str | None
    Spanish: str | None


class TcgplayerSkus(TypedDict):
    """TCGPlayer SKU information."""

    condition: str
    finish: str
    language: str
    printing: str
    productId: str
    skuId: str


# =============================================================================
# Booster Configuration
# =============================================================================


class BoosterSheet(TypedDict, total=False):
    """Single sheet in a booster configuration."""

    allowDuplicates: bool
    balanceColors: bool
    cards: Required[dict[str, int]]  # card_uuid -> weight
    foil: Required[bool]
    fixed: bool
    totalWeight: Required[int]


class BoosterPack(TypedDict):
    """Single booster pack configuration."""

    contents: dict[str, int]  # sheet_name -> count
    weight: int


class BoosterConfig(TypedDict, total=False):
    """Complete booster configuration for a set."""

    boosters: Required[list[BoosterPack]]
    boostersTotalWeight: Required[int]
    name: str
    sheets: Required[dict[str, BoosterSheet]]
    sourceSetCodes: Required[list[str]]


# =============================================================================
# Price Data
# =============================================================================


class PricePoints(TypedDict, total=False):
    """Price points by finish type."""

    etched: dict[str, float]
    foil: dict[str, float]
    normal: dict[str, float]


class PriceList(TypedDict, total=False):
    """Price list from a provider."""

    buylist: PricePoints
    currency: Required[str]
    retail: PricePoints


class PriceFormats(TypedDict, total=False):
    """Prices by format (paper/mtgo) and provider."""

    mtgo: dict[str, PriceList]
    paper: dict[str, PriceList]


# =============================================================================
# Sealed Product Contents
# =============================================================================


class SealedProductCard(TypedDict, total=False):
    """Card in sealed product."""

    foil: bool
    name: Required[str]
    number: Required[str]
    set: Required[str]
    uuid: Required[str]


class SealedProductDeck(TypedDict):
    """Deck in sealed product."""

    name: str
    set: str


class SealedProductOther(TypedDict):
    """Non-card item in sealed product."""

    name: str


class SealedProductPack(TypedDict):
    """Booster pack in sealed product."""

    code: str
    set: str


class SealedProductSealed(TypedDict, total=False):
    """Nested sealed product."""

    count: Required[int]
    name: Required[str]
    set: Required[str]
    uuid: str


class SealedProductVariableConfig(TypedDict, total=False):
    """Variable configuration weights."""

    chance: int
    weight: int


class SealedProductVariableItem(TypedDict, total=False):
    """Single variable configuration option."""

    card: list[SealedProductCard]
    deck: list[SealedProductDeck]
    other: list[SealedProductOther]
    pack: list[SealedProductPack]
    sealed: list[SealedProductSealed]
    variable_config: list[SealedProductVariableConfig]


class SealedProductVariableEntry(TypedDict, total=False):
    """Variable contents entry with configs."""

    configs: list[SealedProductVariableItem]


class SealedProductContents(TypedDict, total=False):
    """All possible contents of a sealed product."""

    card: list[SealedProductCard]
    deck: list[SealedProductDeck]
    other: list[SealedProductOther]
    pack: list[SealedProductPack]
    sealed: list[SealedProductSealed]
    variable: list[SealedProductVariableEntry]


# =============================================================================
# Compiled Data Structures
# =============================================================================


class Keywords(TypedDict):
    """All keyword types in MTG."""

    abilityWords: list[str]
    keywordAbilities: list[str]
    keywordActions: list[str]


class CardType(TypedDict):
    """Type with its valid subtypes and supertypes."""

    subTypes: list[str]
    superTypes: list[str]


class CardTypes(TypedDict):
    """All card types and their valid sub/supertypes."""

    artifact: CardType
    battle: CardType
    conspiracy: CardType
    creature: CardType
    enchantment: CardType
    instant: CardType
    land: CardType
    phenomenon: CardType
    plane: CardType
    planeswalker: CardType
    scheme: CardType
    sorcery: CardType
    tribal: CardType
    vanguard: CardType


# =============================================================================
# Registry for TypeScript generation
# =============================================================================

TYPEDDICT_REGISTRY: list[type] = [
    # Core card sub-models
    ForeignDataIdentifiers,
    ForeignData,
    Identifiers,
    LeadershipSkills,
    Legalities,
    PurchaseUrls,
    RelatedCards,
    Rulings,
    SourceProducts,
    # Meta/translations
    Meta,
    Translations,
    TcgplayerSkus,
    # Booster
    BoosterSheet,
    BoosterPack,
    BoosterConfig,
    # Prices
    PricePoints,
    PriceList,
    PriceFormats,
    # Sealed product contents
    SealedProductCard,
    SealedProductDeck,
    SealedProductOther,
    SealedProductPack,
    SealedProductSealed,
    SealedProductContents,
    # Compiled
    Keywords,
    CardType,
    CardTypes,
]
