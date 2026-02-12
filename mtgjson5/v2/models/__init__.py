"""
MTGJSON Models

Pydantic models for MTGJSON data structures with Polars serialization
and TypeScript generation support.

Usage:
    from mtgjson5.v2.models import (
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

        # TypeScript
        generate_typescript_interfaces,
    )

    # For output building, use mtgjson5.v2.build:
    from mtgjson5.v2.build import AssemblyContext, OutputWriter
"""

from typing import Any

from ._typing import (
    TypedDictUtils,
    is_union_type,
    unwrap_optional,
)
from .adapters import (  # File adapters; Card adapters; Deck adapters; Parse functions; Set adapters
    Adapters,
    AllPrintingsFileAdapter,
    AtomicCardsFileAdapter,
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
    DeckAdapter,
    DeckListAdapter,
    DeckSetListAdapter,
    Parsers,
    SealedProductAdapter,
    SealedProductListAdapter,
    SetAdapter,
    SetListAdapter,
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
from .cards import (
    CARD_MODEL_REGISTRY,
    CardAtomic,
    CardAtomicBase,
    CardBase,
    CardDeck,
    CardPrintingBase,
    CardPrintingFull,
    Cards,
    CardSet,
    CardSetDeck,
    CardToken,
)
from .compiled import (
    COMPILED_MODEL_REGISTRY,
    CardTypesFile,
    Compiled,
    CompiledListFile,
    EnumValuesFile,
    KeywordsFile,
)
from .decks import (
    DECK_MODEL_REGISTRY,
    Deck,
    DeckList,
    Decks,
)
from .files import (
    FILE_MODEL_REGISTRY,
    AllIdentifiersFile,
    AllPricesFile,
    AllPrintingsFile,
    AtomicCardsFile,
    DeckListFile,
    Files,
    FormatAtomicFile,
    FormatFilter,
    FormatPrintingsFile,
    IndividualSetFile,
    SetListFile,
)
from .schemas import (  # Field sets; Schema generators; Utility functions
    ALL_CARD_FIELDS,
    ATOMIC_EXCLUDE,
    CARD_DECK_EXCLUDE,
    REQUIRED_DECK_LIST_FIELDS,
    TOKEN_EXCLUDE,
    get_card_atomic_schema,
    get_card_deck_schema,
    get_card_set_schema,
    get_card_token_schema,
    pydantic_model_to_schema,
    pydantic_model_to_struct,
    pydantic_type_to_polars,
)
from .sealed import (
    SEALED_MODEL_REGISTRY,
)
from .sets import (
    SET_MODEL_REGISTRY,
    DeckSet,
    MtgSet,
    SealedProduct,
    SetList,
    Sets,
)
from .submodels import (  # Registry; Booster; Compiled; Core card sub-models; Meta; Prices; Sealed contents
    TYPEDDICT_REGISTRY,
    BoosterConfig,
    BoosterPack,
    BoosterSheet,
    CardType,
    CardTypes,
    ForeignData,
    Identifiers,
    Keywords,
    LeadershipSkills,
    Legalities,
    Meta,
    PriceFormats,
    PriceList,
    PricePoints,
    PurchaseUrls,
    RelatedCards,
    Rulings,
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
    MarkdownDocGenerator,
    PolarsConverter,
    TypeScriptGenerator,
)

# =============================================================================
# Model Rebuild
# =============================================================================

for model in [
    *CARD_MODEL_REGISTRY,
    *SET_MODEL_REGISTRY,
    *SEALED_MODEL_REGISTRY,
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
        for model in SEALED_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")
        for model in DECK_MODEL_REGISTRY:
            sections.append(TypeScriptGenerator.from_model(model))
            sections.append("")

    if include_file_models:
        sections.append("// === File Models ===\n")
        file_lines: list[str] = []
        for model in COMPILED_MODEL_REGISTRY:
            file_lines.append(TypeScriptGenerator.from_file_model(model))
        for model in FILE_MODEL_REGISTRY:
            file_lines.append(TypeScriptGenerator.from_file_model(model))
        for name in ["StandardFile", "ModernFile", "PioneerFile", "VintageFile", "LegacyFile"]:
            file_lines.append(f"export type {name} = {{ meta: Meta; data: Record<string, Set>; }};")
        for name in [
            "StandardAtomicFile",
            "ModernAtomicFile",
            "PauperAtomicFile",
            "PioneerAtomicFile",
            "VintageAtomicFile",
            "LegacyAtomicFile",
        ]:
            file_lines.append(f"export type {name} = {{ meta: Meta; data: Record<string, CardAtomic>; }};")
        file_lines.append("export type AllPricesTodayFile = { meta: Meta; data: Record<string, PriceFormats>; };")
        sections.extend(sorted(file_lines))

    return "\n".join(sections)


def write_typescript_interfaces(path: str, **kwargs: Any) -> None:
    """Write TypeScript interfaces to file, plus individual types/{Name}.ts files."""
    import os
    import re

    content = generate_typescript_interfaces(**kwargs)
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)

    # Split into individual per-model files in types/ subdirectory
    types_dir = os.path.join(os.path.dirname(path), "types")
    os.makedirs(types_dir, exist_ok=True)

    # Split on "export type " at start of line
    blocks = re.split(r"\n(?=export type )", content)
    for block in blocks:
        m = re.match(r"export type (\w+)", block)
        if not m:
            continue
        type_name = m.group(1)
        type_content = block.strip() + "\n"
        with open(os.path.join(types_dir, f"{type_name}.ts"), "w") as f:
            f.write(type_content)


def write_doc_pages(output_dir: str) -> list[str]:
    """Generate markdown doc pages for models that have __doc_title__ metadata.

    Returns list of paths written.
    """
    import os

    all_models = [
        *CARD_MODEL_REGISTRY,
        *SET_MODEL_REGISTRY,
        *SEALED_MODEL_REGISTRY,
        *DECK_MODEL_REGISTRY,
        *COMPILED_MODEL_REGISTRY,
        *FILE_MODEL_REGISTRY,
    ]

    from .doc_indices import INDEX_PAGES

    written: list[str] = []

    # Index pages
    for page in INDEX_PAGES:
        slug = page["slug"]
        if slug:
            doc_dir = os.path.join(output_dir, "data-models", slug)
        else:
            doc_dir = os.path.join(output_dir, "data-models")
        os.makedirs(doc_dir, exist_ok=True)
        doc_path = os.path.join(doc_dir, "index.md")
        content = MarkdownDocGenerator.index_page(
            title=page["title"],
            description=page["description"],
            keywords=page["keywords"],
            body=page["body"],
        )
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(content)
        written.append(doc_path)

    # Pydantic BaseModel pages
    for model in all_models:
        title = getattr(model, "__doc_title__", None)
        if not title:
            continue
        slug = getattr(model, "__doc_slug__", None) or MarkdownDocGenerator.slug_from_title(title)
        doc_dir = os.path.join(output_dir, "data-models", slug)
        os.makedirs(doc_dir, exist_ok=True)
        doc_path = os.path.join(doc_dir, "index.md")
        content = MarkdownDocGenerator.from_model(model)
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(content)
        written.append(doc_path)

    # TypedDict pages
    for td in TYPEDDICT_REGISTRY:
        title = getattr(td, "__doc_title__", None)
        if not title:
            continue
        slug = getattr(td, "__doc_slug__", None) or MarkdownDocGenerator.slug_from_title(title)
        doc_dir = os.path.join(output_dir, "data-models", slug)
        os.makedirs(doc_dir, exist_ok=True)
        doc_path = os.path.join(doc_dir, "index.md")
        content = MarkdownDocGenerator.from_typeddict(td)
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(content)
        written.append(doc_path)

    return written


# =============================================================================
# Version
# =============================================================================

__version__ = "1.0.0"

__all__ = [  # noqa: RUF022
    # Version
    "__version__",
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
    "Cards",
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
    "Sets",
    "SetList",
    "MtgSet",
    "SealedProduct",
    "DeckSet",
    # Deck models
    "Decks",
    "DeckList",
    "Deck",
    # Compiled models
    "Compiled",
    "CompiledListFile",
    "KeywordsFile",
    "CardTypesFile",
    "EnumValuesFile",
    # File models
    "Files",
    "AllPrintingsFile",
    "AtomicCardsFile",
    "AllIdentifiersFile",
    "AllPricesFile",
    "SetListFile",
    "DeckListFile",
    "IndividualSetFile",
    "FormatPrintingsFile",
    "FormatAtomicFile",
    "FormatFilter",
    # Adapters
    "Adapters",
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
    "Parsers",
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
    "MarkdownDocGenerator",
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
    "TOKEN_EXCLUDE",
    "REQUIRED_DECK_LIST_FIELDS",
    "pydantic_type_to_polars",
    "pydantic_model_to_struct",
    "pydantic_model_to_schema",
    # Generators
    "generate_typescript_interfaces",
    "write_typescript_interfaces",
    "write_doc_pages",
    # Registries
    "TYPEDDICT_REGISTRY",
    "CARD_MODEL_REGISTRY",
    "SET_MODEL_REGISTRY",
    "DECK_MODEL_REGISTRY",
    "COMPILED_MODEL_REGISTRY",
    "FILE_MODEL_REGISTRY",
    "SEALED_MODEL_REGISTRY",
]
