"""
MTGJSON Models

Pydantic models for MTGJSON data structures with Polars serialization
and TypeScript generation support.

Usage:
    from mtgjson_models import (
        # Cards
        CardAtomic, CardSet, CardDeck, CardToken, CardSetDeck,
        
        # Sets
        MtgSet, SetList, SealedProduct,
        
        # Decks
        Deck, DeckList, DeckSet,
        
        # Files
        AllPrintingsFile, AtomicCardsFile, SetListFile,
        
        # Parsing
        parse_cards_set, parse_cards_atomic, parse_set,
        
        # Building
        MtgjsonFileBuilder, AssemblyContext,
        
        # TypeScript
        generate_typescript_interfaces,
    )
"""

from ._typing import (
    TypedDictUtils,
    is_union_type,
    unwrap_optional,
)
from .adapters import (
    # File adapters
    AllPrintingsFileAdapter,
    AtomicCardsFileAdapter,
    # Card adapters
    CardAtomicAdapter,
    CardAtomicListAdapter,
    CardDeckAdapter,
    CardDeckListAdapter,
    CardSetAdapter,
    CardSetDeckAdapter,
    CardSetDeckListAdapter,
    CardSetListAdapter,
    CardTokenAdapter,
    CardTokenListAdapter,
    # Deck adapters
    DeckAdapter,
    DeckListAdapter,
    DeckSetListAdapter,
    # Set adapters
    SealedProductAdapter,
    SealedProductListAdapter,
    SetAdapter,
    SetListAdapter,
    # Parse functions
    parse_all_printings,
    parse_atomic_cards,
    parse_atomic_cards_file,
    parse_card_atomic,
    parse_card_deck,
    parse_card_set,
    parse_card_token,
    parse_cards_atomic,
    parse_cards_deck,
    parse_cards_set,
    parse_cards_token,
    parse_deck,
    parse_sealed_product,
    parse_sealed_products,
    parse_set,
)
from .base import (
    ListFileBase,
    MtgjsonFileBase,
    PolarsMixin,
    RecordFileBase,
)
from .builder import (
    AssemblyContext,
    AtomicCardsAssembler,
    DeckAssembler,
    MtgjsonFileBuilder,
    SetAssembler,
    SetListAssembler,
)
from .cards import (
    CARD_MODEL_REGISTRY,
    CardAtomic,
    CardAtomicBase,
    CardBase,
    CardDeck,
    CardPrintingBase,
    CardPrintingFull,
    CardSet,
    CardSetDeck,
    CardToken,
)
from .compiled import (
    COMPILED_MODEL_REGISTRY,
    CardTypesFile,
    CompiledListFile,
    EnumValuesFile,
    KeywordsFile,
)
from .constants import (
    ALLOW_IF_FALSEY,
    LANGUAGE_MAP,
    OMIT_EMPTY_LIST_FIELDS,
    OMIT_FIELDS,
    OPTIONAL_BOOL_FIELDS,
    OTHER_OPTIONAL_FIELDS,
    REQUIRED_CARD_LIST_FIELDS,
    SORTED_LIST_FIELDS,
)
from .decks import (
    DECK_MODEL_REGISTRY,
    Deck,
    DeckList,
)
from .files import (
    FILE_MODEL_REGISTRY,
    AllIdentifiersFile,
    AllPricesFile,
    AllPrintingsFile,
    AtomicCardsFile,
    DeckListFile,
    FormatAtomicFile,
    FormatFilter,
    FormatPrintingsFile,
    LegacyAtomicFile,
    LegacyFile,
    ModernAtomicFile,
    ModernFile,
    PauperAtomicFile,
    PioneerAtomicFile,
    PioneerFile,
    SetListFile,
    StandardAtomicFile,
    StandardFile,
    VintageAtomicFile,
    VintageFile,
)
from .schemas import (
    # Field sets
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    CARD_SET_EXCLUDE,
    REQUIRED_DECK_LIST_FIELDS,
    TOKEN_EXCLUDE,
    get_card_atomic_schema,
    get_card_deck_schema,
    # Schema generators
    get_card_set_schema,
    get_card_token_schema,
    pydantic_model_to_schema,
    pydantic_model_to_struct,
    # Utility functions
    pydantic_type_to_polars,
)
from .serialize import (
    atomic_to_json,
    cards_to_json,
    clean_nested,
    dataframe_to_cards_list,
    prepare_cards_for_json,
    tokens_to_json,
)
from .sets import (
    SET_MODEL_REGISTRY,
    DeckSet,
    MtgSet,
    SealedProduct,
    SetList,
)
from .submodels import (
    # Registry
    TYPEDDICT_REGISTRY,
    # Booster
    BoosterConfig,
    BoosterPack,
    BoosterSheet,
    # Compiled
    CardType,
    CardTypes,
    # Core card sub-models
    ForeignData,
    Identifiers,
    Keywords,
    LeadershipSkills,
    Legalities,
    # Meta
    Meta,
    # Prices
    PriceFormats,
    PriceList,
    PricePoints,
    PurchaseUrls,
    RelatedCards,
    Rulings,
    # Sealed contents
    SealedProductCard,
    SealedProductContents,
    SealedProductDeck,
    SealedProductOther,
    SealedProductPack,
    SealedProductSealed,
    SourceProducts,
    TcgplayerSkus,
    Translations,
)
from .utils import (
    PolarsConverter,
    TypeScriptGenerator,
)


# =============================================================================
# Model Rebuild
# =============================================================================

for model in [
    *CARD_MODEL_REGISTRY,
    *SET_MODEL_REGISTRY,
    *DECK_MODEL_REGISTRY,
    *COMPILED_MODEL_REGISTRY,
    *FILE_MODEL_REGISTRY,
]:
    model.model_rebuild()

# =============================================================================
# TypeScript Generation API
# =============================================================================

def generate_typescript_interfaces(
    include_sub_models: bool = True,
    include_card_models: bool = True,
    include_set_models: bool = True,
    include_file_models: bool = True,
) -> str:
    """Generate TypeScript interfaces for all MTGJSON models."""
    sections = [
        "// MTGJSON TypeScript Definitions",
        "// Auto-generated from Pydantic models\n",
    ]

    if include_sub_models:
        sections.append("// === Sub-Models ===\n")
        for td in TYPEDDICT_REGISTRY:
            sections.append(TypeScriptGenerator.from_typeddict(td))
            sections.append("")

    if include_card_models:
        sections.append("// === Card Models ===\n")
        for model in CARD_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")

    if include_set_models:
        sections.append("// === Set Models ===\n")
        for model in SET_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")
        for model in DECK_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")

    if include_file_models:
        sections.append("// === File Models ===\n")
        for model in COMPILED_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")
        for model in FILE_MODEL_REGISTRY:
            if hasattr(model, "__name__"):
                sections.append(TypeScriptGenerator.from_model(model))
                sections.append("")

    return "\n".join(sections)


def write_typescript_interfaces(path: str, **kwargs) -> None:
    """Write TypeScript interfaces to file."""
    with open(path, "w") as f:
        f.write(generate_typescript_interfaces(**kwargs))


# =============================================================================
# Version
# =============================================================================

__version__ = "1.0.0"

__all__ = [  # noqa: RUF022
    # Version
    "__version__",
    # Constants
    "ALLOW_IF_FALSEY",
    "LANGUAGE_MAP",
    "OMIT_EMPTY_LIST_FIELDS",
    "OMIT_FIELDS",
    "OPTIONAL_BOOL_FIELDS",
    "OTHER_OPTIONAL_FIELDS",
    "REQUIRED_CARD_LIST_FIELDS",
    "SORTED_LIST_FIELDS",
    # Submodels
    "ForeignData",
    "Identifiers",
    "LeadershipSkills",
    "Legalities",
    "PurchaseUrls",
    "RelatedCards",
    "Rulings",
    "SourceProducts",
    "Meta",
    "Translations",
    "TcgplayerSkus",
    "BoosterConfig",
    "BoosterPack",
    "BoosterSheet",
    "PriceFormats",
    "PriceList",
    "PricePoints",
    "SealedProductCard",
    "SealedProductContents",
    "SealedProductDeck",
    "SealedProductOther",
    "SealedProductPack",
    "SealedProductSealed",
    "CardType",
    "CardTypes",
    "Keywords",
    # Base classes
    "PolarsMixin",
    "MtgjsonFileBase",
    "RecordFileBase",
    "ListFileBase",
    # Card models
    "CardBase",
    "CardAtomicBase",
    "CardPrintingBase",
    "CardPrintingFull",
    "CardAtomic",
    "CardSet",
    "CardDeck",
    "CardToken",
    "CardSetDeck",
    # Set models
    "SetList",
    "MtgSet",
    "SealedProduct",
    "DeckSet",
    # Deck models
    "DeckList",
    "Deck",
    # Compiled models
    "CompiledListFile",
    "KeywordsFile",
    "CardTypesFile",
    "EnumValuesFile",
    # File models
    "AllPrintingsFile",
    "AtomicCardsFile",
    "AllIdentifiersFile",
    "AllPricesFile",
    "SetListFile",
    "DeckListFile",
    "FormatPrintingsFile",
    "FormatAtomicFile",
    "FormatFilter",
    "LegacyFile",
    "ModernFile",
    "PioneerFile",
    "StandardFile",
    "VintageFile",
    "LegacyAtomicFile",
    "ModernAtomicFile",
    "PauperAtomicFile",
    "PioneerAtomicFile",
    "StandardAtomicFile",
    "VintageAtomicFile",
    # Adapters
    "CardAtomicAdapter",
    "CardAtomicListAdapter",
    "CardSetAdapter",
    "CardSetListAdapter",
    "CardDeckAdapter",
    "CardDeckListAdapter",
    "CardTokenAdapter",
    "CardTokenListAdapter",
    "CardSetDeckAdapter",
    "CardSetDeckListAdapter",
    "SetAdapter",
    "SetListAdapter",
    "DeckAdapter",
    "DeckListAdapter",
    "DeckSetListAdapter",
    "SealedProductAdapter",
    "SealedProductListAdapter",
    "AllPrintingsFileAdapter",
    "AtomicCardsFileAdapter",
    # Parsers
    "parse_card_atomic",
    "parse_cards_atomic",
    "parse_card_set",
    "parse_cards_set",
    "parse_card_deck",
    "parse_cards_deck",
    "parse_card_token",
    "parse_cards_token",
    "parse_set",
    "parse_deck",
    "parse_sealed_product",
    "parse_sealed_products",
    "parse_all_printings",
    "parse_atomic_cards",
    "parse_atomic_cards_file",
    # Utilities
    "PolarsConverter",
    "TypeScriptGenerator",
    "TypedDictUtils",
    "is_union_type",
    "unwrap_optional",
    # Schemas
    "get_card_set_schema",
    "get_card_atomic_schema",
    "get_card_deck_schema",
    "get_card_token_schema",
    "ALL_CARD_FIELDS",
    "ATOMIC_EXCLUDE",
    "CARD_DECK_EXCLUDE",
    "CARD_SET_EXCLUDE",
    "TOKEN_EXCLUDE",
    "REQUIRED_DECK_LIST_FIELDS",
    "pydantic_type_to_polars",
    "pydantic_model_to_struct",
    "pydantic_model_to_schema",
    # Builders
    "AssemblyContext",
    "SetAssembler",
    "DeckAssembler",
    "AtomicCardsAssembler",
    "SetListAssembler",
    "MtgjsonFileBuilder",
    # Generators
    "generate_typescript_interfaces",
    "write_typescript_interfaces",
    # Serialization
    "atomic_to_json",
    "cards_to_json",
    "clean_nested",
    "dataframe_to_cards_list",
    "prepare_cards_for_json",
    "tokens_to_json",
    # Registries
    "TYPEDDICT_REGISTRY",
    "CARD_MODEL_REGISTRY",
    "SET_MODEL_REGISTRY",
    "DECK_MODEL_REGISTRY",
    "COMPILED_MODEL_REGISTRY",
    "FILE_MODEL_REGISTRY",
]
