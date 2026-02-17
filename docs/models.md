# MTGJSON Pydantic Models

**Directory**: `mtgjson5/models/`

The models directory contains a comprehensive Pydantic-based data model system for representing Magic: The Gathering card data. The architecture uses a sophisticated inheritance hierarchy with both BaseModel and TypedDict structures for optimal performance and serialization.

## Overview

```
models/
├── base.py          # PolarsMixin, MtgjsonFileBase
├── cards.py         # Card models (CardSet, CardAtomic, CardDeck, CardToken)
├── sets.py          # Set models (MtgSet, SetList, DeckSet)
├── decks.py         # Deck models (Deck, DeckList)
├── sealed.py        # SealedProduct model and assemblers
├── files.py         # File wrapper models (AllPrintingsFile, etc.)
├── compiled.py      # Compiled data models (Keywords, CardTypes)
├── submodels.py     # TypedDict sub-models (Identifiers, Legalities, etc.)
├── schemas.py       # Polars schema generation
├── adapters.py      # Pydantic TypeAdapters for parsing
├── doc_indices.py   # Documentation index generation
├── utils.py         # PolarsConverter, TypeScriptGenerator
├── _typing.py       # TypedDict utilities
└── scryfall/        # Scryfall-specific models
    └── models.py
```

## Base Classes

### PolarsMixin (`models/base.py`)

Foundation mixin providing Polars DataFrame integration and TypeScript generation.

```python
class PolarsMixin:
    # Class variables for output control
    _sorted_list_fields: ClassVar[set[str]]  # Fields with sorted lists
    _allow_if_falsey: ClassVar[set[str]]     # Include even if empty

    # Core methods
    def to_polars_dict(
        self,
        use_alias: bool = True,
        sort_keys: bool = True,
        sort_lists: bool = True,
        exclude_none: bool = False,
    ) -> dict

    @classmethod
    def polars_schema(cls) -> pl.Schema

    @classmethod
    def to_dataframe(cls, items: list[Self]) -> pl.DataFrame

    @classmethod
    def to_lazyframe(cls, items: list[Self]) -> pl.LazyFrame

    @classmethod
    def from_polars_row(cls, row: dict) -> Self

    @classmethod
    def from_dataframe(cls, df: pl.DataFrame) -> list[Self]

    @classmethod
    def from_lazyframe(cls, lf: pl.LazyFrame) -> list[Self]

    @classmethod
    def to_typescript(cls) -> str
```

**Key Features**:
- WUBRG color ordering preservation for split/adventure layouts
- Recursive dict conversion with alias application
- Polars struct/list handling for nested types

### MtgjsonFileBase (`models/base.py`)

Base class for all MTGJSON file structures (JSON files with meta + data).

```python
class MtgjsonFileBase(PolarsMixin, BaseModel):
    meta: dict[str, str]

    @classmethod
    def make_meta(cls) -> dict[str, str]

    @classmethod
    def with_meta(cls, **kwargs) -> Self

    def write(self, path: Path, pretty: bool = False) -> None

    @classmethod
    def read(cls, path: Path) -> Self
```

### RecordFileBase & ListFileBase (`models/base.py`)

```python
# For files with data: Record<string, T>
class RecordFileBase(MtgjsonFileBase):
    @classmethod
    def from_items(cls, items: dict) -> Self

# For files with data: T[]
class ListFileBase(MtgjsonFileBase):
    @classmethod
    def from_items(cls, items: list) -> Self
```

## Card Models (`models/cards.py`)

### Inheritance Hierarchy

```
CardBase (common fields)
├── CardAtomicBase (oracle properties)
│   └── CardAtomic (final atomic model)
└── CardPrintingBase (printing-specific)
    └── CardPrintingFull (atomic + printing combined)
        ├── CardSet (card in set)
        └── CardDeck (card in deck with count/foil)

CardToken (separate token model, extends CardPrintingBase)
CardSetDeck (minimal reference for decks)
```

### CardBase

Common fields for all card types:

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Card name |
| `asciiName` | str? | ASCII-normalized name |
| `faceName` | str? | Face name for multi-face |
| `type` | str | Full type line |
| `types` | list[str] | Card types |
| `subtypes` | list[str] | Subtypes |
| `supertypes` | list[str] | Supertypes |
| `colors` | list[str] | Card colors |
| `colorIdentity` | list[str] | Color identity |
| `colorIndicator` | list[str]? | Color indicator |
| `manaCost` | str? | Mana cost |
| `text` | str? | Oracle text |
| `layout` | str | Card layout |
| `side` | str? | Side (a/b/c) |
| `power` | str? | Power |
| `toughness` | str? | Toughness |
| `loyalty` | str? | Planeswalker loyalty |
| `keywords` | list[str] | Keywords |
| `identifiers` | Identifiers | External IDs |
| `isFunny` | bool? | Silver-bordered |
| `edhrecSaltiness` | float? | Salt score |
| `subsets` | list[str]? | Secret Lair subsets |

### CardAtomicBase

Extends CardBase with oracle-level properties:

| Field | Type | Description |
|-------|------|-------------|
| `manaValue` | float? | Converted mana cost |
| `convertedManaCost` | float? | Legacy CMC field |
| `faceConvertedManaCost` | float? | Face CMC |
| `faceManaValue` | float? | Face mana value |
| `defense` | str? | Battle defense |
| `attractionLights` | list[int]? | Un-set attractions |
| `hand` | str? | Vanguard hand modifier |
| `life` | str? | Vanguard life modifier |
| `edhrecRank` | int? | EDHREC popularity |
| `foreignData` | list[ForeignData]? | Foreign printings |
| `legalities` | Legalities? | Format legalities |
| `leadershipSkills` | LeadershipSkills? | Commander legal |
| `rulings` | list[Rulings]? | Card rulings |
| `hasAlternativeDeckLimit` | bool? | Relentless Rats |
| `isReserved` | bool? | Reserved list |
| `isGameChanger` | bool? | Game changer card |
| `printings` | list[str]? | Set codes |
| `purchaseUrls` | PurchaseUrls? | Purchase links |
| `relatedCards` | RelatedCards? | Related cards |

### CardPrintingBase

Extends CardBase with printing-specific properties:

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | str | MTGJSON UUID |
| `setCode` | str | Set code |
| `number` | str | Collector number |
| `artist` | str? | Artist name |
| `artistIds` | list[str]? | Scryfall artist IDs |
| `borderColor` | str? | Border color |
| `frameVersion` | str? | Frame version |
| `frameEffects` | list[str]? | Frame effects |
| `watermark` | str? | Watermark |
| `signature` | str? | Signature |
| `securityStamp` | str? | Security stamp |
| `flavorText` | str? | Flavor text |
| `flavorName` | str? | Godzilla-style name |
| `originalText` | str? | Original text |
| `originalType` | str? | Original type line |
| `availability` | list[str]? | paper/mtgo/arena |
| `boosterTypes` | list[str]? | Booster types |
| `finishes` | list[str]? | Foil finishes |
| `hasFoil` | bool? | Has foil printing |
| `hasNonFoil` | bool? | Has nonfoil printing |
| `promoTypes` | list[str]? | Promo types |
| `isFullArt` | bool? | Full art |
| `isOnlineOnly` | bool? | Online only |
| `isOversized` | bool? | Oversized |
| `isPromo` | bool? | Promo card |
| `isReprint` | bool? | Reprint |
| `isTextless` | bool? | Textless |
| `otherFaceIds` | list[str]? | Other face UUIDs |
| `cardParts` | list[str]? | Meld parts |
| `language` | str | Language (default "English") |
| `sourceProducts` | list[str]? | Source products |

### CardPrintingFull

Multiple inheritance combining CardPrintingBase and CardAtomicBase:

| Field | Type | Description |
|-------|------|-------------|
| `rarity` | str | Card rarity |
| `duelDeck` | str? | Duel deck side |
| `isRebalanced` | bool? | Rebalanced card |
| `originalPrintings` | list[str]? | Original UUIDs |
| `rebalancedPrintings` | list[str]? | Rebalanced UUIDs |
| `originalReleaseDate` | str? | Original release |
| `isAlternative` | bool? | Alternative art |
| `isStarter` | bool? | Starter deck |
| `isStorySpotlight` | bool? | Story spotlight |
| `isTimeshifted` | bool? | Timeshifted |
| `hasContentWarning` | bool? | Content warning |
| `variations` | list[str]? | Variation UUIDs |

### Final Card Models

**CardAtomic**
```python
class CardAtomic(CardAtomicBase):
    first_printing: str | None = Field(None, alias="firstPrinting")
```

**CardSet**
```python
class CardSet(CardPrintingFull):
    source_products: SourceProducts | None = Field(None, alias="sourceProducts")
```

**CardDeck**
```python
class CardDeck(CardPrintingFull):
    count: int  # Required
    is_foil: bool | None = Field(None, alias="isFoil")
    is_etched: bool | None = Field(None, alias="isEtched")
    source_products: SourceProducts | None = Field(None, alias="sourceProducts")
```

**CardToken**
```python
class CardToken(CardPrintingBase):  # NOT CardAtomicBase
    orientation: str | None = None
    reverse_related: list[str] | None = Field(None, alias="reverseRelated")
    related_cards: RelatedCards | None = Field(None, alias="relatedCards")
    edhrec_saltiness: float | None = Field(None, alias="edhrecSaltiness")
    source_products: SourceProducts | None = Field(None, alias="sourceProducts")
```

**CardSetDeck** (minimal reference)
```python
class CardSetDeck(PolarsMixin, BaseModel):
    count: int  # Required
    is_foil: bool | None = Field(None, alias="isFoil")
    uuid: str  # Required
```

## Set Models (`models/sets.py`)

### SetList

Set metadata without cards:

| Field | Type | Description |
|-------|------|-------------|
| `code` | str | Set code |
| `name` | str | Set name |
| `type` | str | Set type |
| `releaseDate` | str | Release date |
| `baseSetSize` | int | Base set size |
| `totalSetSize` | int | Total set size |
| `keyruneCode` | str? | Keyrune icon code |
| `translations` | Translations? | Localized names |
| `block` | str? | Block name |
| `parentCode` | str? | Parent set code |
| `mtgoCode` | str? | MTGO set code |
| `tokenSetCode` | str? | Token set code |
| `mcmId` | int? | CardMarket ID |
| `mcmName` | str? | CardMarket name |
| `tcgplayerGroupId` | int? | TCGPlayer group ID |
| `isFoilOnly` | bool? | Foil only set |
| `isNonFoilOnly` | bool? | Non-foil only |
| `isOnlineOnly` | bool? | Online only |
| `isPaperOnly` | bool? | Paper only |
| `isForeignOnly` | bool? | Foreign only |
| `isPartialPreview` | bool? | Partial preview |
| `languages` | list[str]? | Available languages |

### MtgSet

Extends SetList with card data:

```python
class MtgSet(SetList):
    cards: list[CardSet] = Field(default_factory=list)
    tokens: list[CardToken] = Field(default_factory=list)
    booster: dict[str, BoosterConfig] | None = None
    decks: list[DeckSet] = Field(default_factory=list)
    sealed_product: list[SealedProduct] | None = Field(None, alias="sealedProduct")
```

### DeckSet

Minimal deck representation used in Set.decks:

```python
class DeckSet(PolarsMixin, BaseModel):
    code: str
    name: str
    type: str
    release_date: str | None = Field(None, alias="releaseDate")
    sealed_product_uuids: list[str] | None = Field(None, alias="sealedProductUuids")

    # Card boards (all use CardSetDeck)
    main_board: list[CardSetDeck] = Field(default_factory=list, alias="mainBoard")
    side_board: list[CardSetDeck] = Field(default_factory=list, alias="sideBoard")
    commander: list[CardSetDeck] = Field(default_factory=list)
    display_commander: list[CardSetDeck] = Field(default_factory=list, alias="displayCommander")
    tokens: list[CardSetDeck] = Field(default_factory=list)
    planes: list[CardSetDeck] = Field(default_factory=list)
    schemes: list[CardSetDeck] = Field(default_factory=list)

    source_set_codes: list[str] = Field(default_factory=list, alias="sourceSetCodes")
```

## Deck Models (`models/decks.py`)

### DeckList

Deck metadata for DeckList.json:

```python
class DeckList(PolarsMixin, BaseModel):
    code: str
    name: str
    file_name: str = Field(alias="fileName")
    type: str
    release_date: str | None = Field(None, alias="releaseDate")
```

### Deck

Full deck with expanded card data:

```python
class Deck(PolarsMixin, BaseModel):
    code: str
    name: str
    type: str
    release_date: str | None = Field(None, alias="releaseDate")
    sealed_product_uuids: list[str] | None = Field(None, alias="sealedProductUuids")

    # Card boards (all use CardDeck - full card data)
    main_board: list[CardDeck] = Field(default_factory=list, alias="mainBoard")
    side_board: list[CardDeck] = Field(default_factory=list, alias="sideBoard")
    commander: list[CardDeck] = Field(default_factory=list)
    display_commander: list[CardDeck] = Field(default_factory=list, alias="displayCommander")
    planes: list[CardDeck] = Field(default_factory=list)
    schemes: list[CardDeck] = Field(default_factory=list)
    tokens: list[CardToken] = Field(default_factory=list)

    source_set_codes: list[str] = Field(default_factory=list, alias="sourceSetCodes")
```

## Sealed Product Models (`models/sealed.py`)

### SealedProduct

```python
class SealedProduct(PolarsMixin, BaseModel):
    uuid: str
    name: str
    category: str | None = None
    subtype: str | None = None
    language: str | None = None
    release_date: str | None = Field(None, alias="releaseDate")
    card_count: int | None = Field(None, alias="cardCount")
    product_size: int | None = Field(None, alias="productSize")
    contents: SealedProductContents | None = None
    identifiers: Identifiers | None = None
    purchase_urls: PurchaseUrls | None = Field(None, alias="purchaseUrls")

    _allow_if_falsey = {"identifiers"}  # Always include even if empty
```

### SealedProductAssembler

Utility class for assembling sealed products from DataFrames:

```python
class SealedProductAssembler:
    @staticmethod
    def from_dataframe(df: pl.DataFrame) -> list[dict]

    @staticmethod
    def assemble_contents(
        product_uuid: str,
        cards_df: pl.DataFrame,
    ) -> SealedProductContents
```

### BoosterAssembler

Utility class for assembling booster configurations:

```python
class BoosterAssembler:
    @staticmethod
    def build_sheet(cards: list[CardSet], weights: dict[str, int] | None = None) -> BoosterSheet

    @staticmethod
    def build_sheet_from_df(df: pl.DataFrame, foil: bool = False) -> BoosterSheet

    @staticmethod
    def build_pack(sheets: dict[str, int], weight: int = 1) -> BoosterPack

    @staticmethod
    def build_config(
        boosters: list[BoosterPack],
        sheets: dict[str, BoosterSheet],
        source_set_codes: list[str],
    ) -> BoosterConfig

    @staticmethod
    def build_draft_booster(cards_df: pl.DataFrame) -> BoosterConfig

    @staticmethod
    def from_json(data: dict) -> BoosterConfig
```

## File Models (`models/files.py`)

### Record-Based Files

```python
class AllPrintingsFile(RecordFileBase):
    data: dict[str, MtgSet]  # SET_CODE -> MtgSet

    def iter_sets(self) -> Iterator[tuple[str, MtgSet]]
    def get_set(self, code: str) -> MtgSet | None

class AtomicCardsFile(RecordFileBase):
    data: dict[str, list[CardAtomic]]  # CARD_NAME -> [CardAtomic, ...]

    def iter_cards(self) -> Iterator[tuple[str, list[CardAtomic]]]
    def get_card(self, name: str) -> list[CardAtomic] | None

class AllIdentifiersFile(RecordFileBase):
    data: dict[str, CardSet]  # UUID -> CardSet

    def get_by_uuid(self, uuid: str) -> CardSet | None

class AllPricesFile(RecordFileBase):
    data: dict[str, PriceFormats]  # UUID -> PriceFormats

    def get_prices(self, uuid: str) -> PriceFormats | None

class IndividualSetFile(RecordFileBase):
    data: dict[str, Any]

    @classmethod
    def from_set_data(cls, set_data: dict) -> Self
```

### List-Based Files

```python
class SetListFile(ListFileBase):
    data: list[SetList]

    def iter_sets(self) -> Iterator[SetList]
    def get_by_code(self, code: str) -> SetList | None

class DeckListFile(ListFileBase):
    data: list[DeckList]

    def iter_decks(self) -> Iterator[DeckList]
```

### Format-Specific Files

```python
class FormatPrintingsFile(RecordFileBase):
    format_name: str

    @classmethod
    def for_format(cls, all_printings: AllPrintingsFile, format_name: str) -> Self

class FormatAtomicFile(RecordFileBase):
    format_name: str

    @classmethod
    def for_format(cls, atomic_cards: AtomicCardsFile, format_name: str) -> Self
```

### FormatFilter

Utility for filtering cards by format legality:

```python
class FormatFilter:
    @staticmethod
    def is_legal(card: dict | BaseModel, format_name: str) -> bool
        # Returns True if legalities[format] in ("Legal", "Restricted")
```

## Compiled Data Models (`models/compiled.py`)

```python
class CompiledListFile(MtgjsonFileBase):
    data: list[str]

class KeywordsFile(MtgjsonFileBase):
    data: Keywords  # TypedDict with abilityWords, keywordAbilities, keywordActions

class CardTypesFile(MtgjsonFileBase):
    data: CardTypes  # TypedDict with all card types

class EnumValuesFile(MtgjsonFileBase):
    data: dict[str, dict[str, list[str]]]
```

## TypedDict Sub-Models (`models/submodels.py`)

Lightweight dict-based types for nested structures (~2.5x faster parsing than BaseModel).

### Core Card Sub-Models

```python
class ForeignData(TypedDict, total=False):
    language: Required[str]
    name: Required[str]
    faceName: str
    flavorText: str
    identifiers: ForeignDataIdentifiers
    multiverseId: int
    text: str
    type: str
    uuid: str

class Identifiers(TypedDict, total=False):
    # Card identifiers
    cardKingdomId: str
    cardKingdomFoilId: str
    cardKingdomEtchedId: str
    mcmId: str
    mcmMetaId: str
    mtgArenaId: int
    mtgoId: int
    mtgoFoilId: int
    multiverseId: int
    scryfallId: str
    scryfallOracleId: str
    scryfallIllustrationId: str
    tcgplayerProductId: int
    tcgplayerEtchedProductId: int
    # ... more identifiers

class Legalities(TypedDict, total=False):
    alchemy: str
    brawl: str
    commander: str
    duel: str
    explorer: str
    gladiator: str
    historic: str
    legacy: str
    modern: str
    oathbreaker: str
    pauper: str
    pioneer: str
    standard: str
    vintage: str
    # ... more formats

class PurchaseUrls(TypedDict, total=False):
    cardKingdom: str
    cardKingdomEtched: str
    cardKingdomFoil: str
    cardmarket: str
    tcgplayer: str
    tcgplayerEtched: str

class Rulings(TypedDict):
    date: str
    text: str

class LeadershipSkills(TypedDict):
    brawl: bool
    commander: bool
    oathbreaker: bool

class RelatedCards(TypedDict, total=False):
    reverseRelated: list[str]
    spellbook: list[str]

class SourceProducts(TypedDict, total=False):
    etched: list[str]
    foil: list[str]
    nonfoil: list[str]
```

### Booster Configuration

```python
class BoosterSheet(TypedDict, total=False):
    cards: Required[dict[str, int]]  # uuid -> weight
    foil: Required[bool]
    totalWeight: Required[int]
    allowDuplicates: bool
    balanceColors: bool
    fixed: bool

class BoosterPack(TypedDict):
    contents: dict[str, int]  # sheet_name -> count
    weight: int

class BoosterConfig(TypedDict, total=False):
    boosters: Required[list[BoosterPack]]
    boostersTotalWeight: Required[int]
    sheets: Required[dict[str, BoosterSheet]]
    sourceSetCodes: Required[list[str]]
    name: str
```

### Sealed Product Contents

```python
class SealedProductContents(TypedDict, total=False):
    card: list[SealedProductCard]
    deck: list[SealedProductDeck]
    other: list[SealedProductOther]
    pack: list[SealedProductPack]
    sealed: list[SealedProductSealed]
    variable: list[SealedProductVariableEntry]
```

### Translations

```python
class Translations(TypedDict, total=False):
    AncientGreek: str
    Arabic: str
    ChineseSimplified: str
    ChineseTraditional: str
    French: str
    German: str
    Hebrew: str
    Italian: str
    Japanese: str
    Korean: str
    Latin: str
    Phyrexian: str
    PortugueseBrazil: str
    Russian: str
    Sanskrit: str
    Spanish: str
```

## Utilities

### PolarsConverter (`models/utils.py`)

Convert Python/Pydantic types to Polars types:

```python
class PolarsConverter:
    @staticmethod
    def python_to_polars(python_type: type) -> pl.DataType

    @staticmethod
    def typeddict_to_struct(td: type) -> pl.Struct

    @staticmethod
    def model_to_struct(model: type[BaseModel]) -> pl.Struct
```

### TypeScriptGenerator (`models/utils.py`)

Generate TypeScript interfaces from Python types:

```python
class TypeScriptGenerator:
    @staticmethod
    def python_to_ts(python_type: type) -> str

    @staticmethod
    def from_typeddict(td: type) -> str

    @staticmethod
    def from_model(model: type[BaseModel]) -> str
```

### TypedDictUtils (`models/_typing.py`)

Utilities for working with TypedDicts:

```python
def is_typeddict(tp: type) -> bool
def get_fields(td: type) -> dict[str, type]
def is_field_required(td: type, field: str) -> bool
def filter_none(d: dict) -> dict
def apply_aliases(d: dict, model: type) -> dict
```

## Adapters (`models/adapters.py`)

Module-level TypeAdapters for efficient parsing:

```python
# Card adapters
CardAtomicAdapter = TypeAdapter(CardAtomic)
CardSetAdapter = TypeAdapter(CardSet)
CardDeckAdapter = TypeAdapter(CardDeck)
CardTokenAdapter = TypeAdapter(CardToken)

# List adapters
CardSetListAdapter = TypeAdapter(list[CardSet])
CardAtomicListAdapter = TypeAdapter(list[CardAtomic])

# File adapters
AllPrintingsFileAdapter = TypeAdapter(AllPrintingsFile)
AtomicCardsFileAdapter = TypeAdapter(AtomicCardsFile)

# Parse functions
def parse_card_set(data: dict) -> CardSet
def parse_cards_set(data: list[dict]) -> list[CardSet]
def parse_all_printings(data: dict) -> AllPrintingsFile
# ... more parse functions
```

## Key Design Patterns

### 1. Multiple Inheritance
CardPrintingFull combines CardPrintingBase and CardAtomicBase for complete card data.

### 2. Field Aliasing
All models use `populate_by_name=True` with camelCase aliases:
```python
color_identity: list[str] = Field(default_factory=list, alias="colorIdentity")
```

### 3. TypedDict vs BaseModel Trade-off
- **TypedDict**: Sub-models requiring maximum performance (~2.5x faster)
- **BaseModel**: Top-level models requiring validation and Polars integration

#### Choosing TypedDict vs BaseModel

Use this decision guide when adding new data structures:

| Choose... | When... | Examples |
|-----------|---------|----------|
| **TypedDict** | Nested sub-structure inside a card/set model | `Identifiers`, `Legalities`, `PurchaseUrls`, `ForeignData` |
| **TypedDict** | Struct fields in Polars DataFrames | `BoosterSheet`, `BoosterPack`, `SealedProductContents` |
| **TypedDict** | Performance-critical paths (parsing thousands of cards) | Any sub-model instantiated per-card |
| **BaseModel** | Top-level model with PolarsMixin integration | `CardSet`, `MtgSet`, `SealedProduct` |
| **BaseModel** | Needs validation, `write()`/`read()`, or TypeScript generation | `AllPrintingsFile`, `SetListFile` |
| **BaseModel** | File wrapper models (meta + data pattern) | All `*File` classes in `files.py` |

**Rule of thumb**: Never nest BaseModel inside BaseModel unless strictly required. TypedDict sub-models parse ~2.5x faster than equivalent BaseModel sub-models because they skip Pydantic's validation machinery.

### 4. Conditional Field Inclusion
```python
_allow_if_falsey = {"legalities", "purchaseUrls"}  # Always include
_sorted_list_fields = {"colors", "colorIdentity"}   # Sort these lists
```

### 5. Registry System
```python
CARD_MODEL_REGISTRY = [CardSetDeck, CardToken, CardAtomic, CardSet, CardDeck]
SET_MODEL_REGISTRY = [DeckSet, SetList, MtgSet]
DECK_MODEL_REGISTRY = [DeckList, Deck]
FILE_MODEL_REGISTRY = [AllPrintingsFile, AtomicCardsFile, ...]
```

### 6. WUBRG Color Ordering
Special handling in `to_polars_dict()` preserves W-U-B-R-G order for split/adventure layouts instead of alphabetical.

## Scryfall Models (`models/scryfall/`)

Separate models for Scryfall API data with strong typing:

### Enums
- `Color`: W, U, B, R, G
- `Rarity`: common, uncommon, rare, mythic, special, bonus
- `Layout`: 23 layouts (normal, split, flip, transform, modal_dfc, meld, etc.)
- `Frame`: 1993, 1997, 2003, 2015, future
- `Legality`: legal, not_legal, restricted, banned
- `Finish`: foil, nonfoil, etched, glossy

### Main Models
- `ScryfallCard`: Complete Scryfall card object (55+ fields)
- `CardFace`: Single face of multiface card
- `SetMetadata`: Scryfall set object
- `ImageUris`: Image URL fields
- `Prices`: Price fields (usd, eur, tix)
