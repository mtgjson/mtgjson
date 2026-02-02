"""
MTGJSON file structure models.

These models represent the complete structure of MTGJSON output files.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from .base import ListFileBase, RecordFileBase
from .submodels import PriceFormats

if TYPE_CHECKING:
    from .cards import CardAtomic, CardSet
    from .decks import DeckList
    from .sets import MtgSet, SetList


# =============================================================================
# Core Record Files (data: Record<string, T>)
# =============================================================================


class MetaFile(RecordFileBase):
    """Meta.json: { meta, data: { date, version } }"""

    data: dict[str, str]


class IndividualSetFile(RecordFileBase):
    """Individual set file: { meta, data: Set }"""

    data: dict[str, Any]

    @classmethod
    def from_set_data(
        cls, set_data: dict[str, Any], meta: dict[str, str] | None = None
    ) -> IndividualSetFile:
        """Create from set data dict."""
        return cls.with_meta(set_data, meta)  # type: ignore[return-value]


class AllPrintingsFile(RecordFileBase):
    """AllPrintings.json: { meta, data: { SET_CODE: Set } }"""

    data: dict[str, MtgSet]

    def iter_sets(self) -> Iterator[tuple[str, MtgSet]]:
        """Iterate over (set_code, set_data) pairs."""
        yield from self.data.items()

    def get_set(self, code: str) -> MtgSet | None:
        """Get a single set by code."""
        return self.data.get(code.upper())


class AtomicCardsFile(RecordFileBase):
    """AtomicCards.json: { meta, data: { CARD_NAME: [CardAtomic, ...] } }"""

    data: dict[str, list[CardAtomic]]

    def iter_cards(self) -> Iterator[tuple[str, list[CardAtomic]]]:
        """Iterate over (card_name, variants) pairs."""
        yield from self.data.items()

    def get_card(self, name: str) -> list[CardAtomic] | None:
        """Get all variants of a card by name."""
        return self.data.get(name)


class AllIdentifiersFile(RecordFileBase):
    """AllIdentifiers.json: { meta, data: { UUID: CardSet } }"""

    data: dict[str, CardSet]

    def get_by_uuid(self, uuid: str) -> CardSet | None:
        """Get card by UUID."""
        return self.data.get(uuid)


class AllPricesFile(RecordFileBase):
    """AllPrices.json: { meta, data: { UUID: PriceFormats } }"""

    data: dict[str, PriceFormats]

    def get_prices(self, uuid: str) -> PriceFormats | None:
        """Get prices for a card by UUID."""
        return self.data.get(uuid)


class TcgplayerSkusFile(RecordFileBase):
    """TcgplayerSkus.json: { meta, data: { UUID: [SkuEntry, ...] } }

    Maps card UUIDs to their TCGPlayer SKU information.
    Each SKU entry contains:
    - skuId: TCGPlayer SKU ID (string)
    - productId: TCGPlayer product ID (string)
    - language: Language name (e.g., "ENGLISH")
    - printing: "FOIL" or "NON FOIL"
    - condition: e.g., "NEAR MINT", "LIGHTLY PLAYED"
    - finish: (optional) Special finish like "ETCHED"
    """

    data: dict[str, list[dict[str, Any]]]

    def get_skus(self, uuid: str) -> list[dict[str, Any]] | None:
        """Get SKUs for a card by UUID."""
        return self.data.get(uuid)


# =============================================================================
# List Files (data: T[])
# =============================================================================


class SetListFile(ListFileBase):
    """SetList.json: { meta, data: [SetList, ...] }"""

    data: list[SetList]

    def iter_sets(self) -> Iterator[SetList]:
        """Iterate over sets."""
        yield from self.data

    def get_by_code(self, code: str) -> SetList | None:
        """Get set by code."""
        code = code.upper()
        return next((s for s in self.data if s.get("code") == code), None)  # type: ignore[attr-defined]


class DeckListFile(ListFileBase):
    """DeckList.json: { meta, data: [DeckList, ...] }"""

    data: list[DeckList]

    def iter_decks(self) -> Iterator[DeckList]:
        """Iterate over decks."""
        yield from self.data


# =============================================================================
# Format-Specific Files
# =============================================================================


class FormatPrintingsFile(RecordFileBase):
    """Base for format-specific printings (Legacy, Modern, etc.)."""

    data: dict[str, dict[str, Any]]
    format_name: str = ""

    @classmethod
    def for_format(
        cls,
        format_name: str,
        all_printings: AllPrintingsFile,
        format_legal_sets: set[str] | None = None,
    ) -> FormatPrintingsFile:
        """Filter AllPrintings to format-legal cards.

        Args:
                format_name: The format to filter for (e.g., "standard", "modern")
                all_printings: The AllPrintings file to filter
                format_legal_sets: Optional set of set codes that are format-legal.
                        If provided, only sets in this list are included.
                        This enables proper filtering by set type and requiring ALL cards
                        to be legal (matching legacy behavior).
        """
        filtered: dict[str, dict[str, Any]] = {}
        for set_code, set_data in all_printings.iter_sets():
            # Skip sets not in format-legal list
            if format_legal_sets is not None and set_code not in format_legal_sets:
                continue

            if isinstance(set_data, BaseModel):
                set_dict = set_data.model_dump(by_alias=True, exclude_none=True)
            else:
                set_dict = set_data

            # Filter to only legal cards
            cards = [
                c
                for c in set_dict.get("cards", [])
                if FormatFilter.is_legal(c, format_name)
            ]
            if cards:
                filtered[set_code] = {**set_dict, "cards": cards}
        return cls(meta=all_printings.meta, data=filtered, format_name=format_name)


class FormatAtomicFile(RecordFileBase):
    """Base for format-specific atomic cards."""

    data: dict[str, list[dict[str, Any]]]
    format_name: str = ""

    @classmethod
    def for_format(
        cls,
        format_name: str,
        atomic: AtomicCardsFile,
    ) -> FormatAtomicFile:
        """Filter AtomicCards to format-legal cards."""
        filtered: dict[str, list[dict[str, Any]]] = {}
        for name, variants in atomic.iter_cards():
            legal = []
            for v in variants:
                if FormatFilter.is_legal(v, format_name):
                    # Convert Pydantic models to dicts
                    if isinstance(v, BaseModel):
                        legal.append(v.model_dump(by_alias=True, exclude_none=True))
                    else:
                        legal.append(v)
            if legal:
                filtered[name] = legal
        return cls(meta=atomic.meta, data=filtered, format_name=format_name)


# =============================================================================
# Format Filter Utility
# =============================================================================


class FormatFilter:
    """Filters for format-legal cards."""

    @staticmethod
    def is_legal(card: dict[str, Any] | BaseModel, format_name: str) -> bool:
        """Check if card is legal in format.

        Handles both raw dicts and Pydantic model instances.
        """
        if isinstance(card, BaseModel):
            legalities = getattr(card, "legalities", None) or {}
        else:
            legalities = card.get("legalities", {})

        if isinstance(legalities, dict):
            status = legalities.get(format_name)
        else:
            status = None
        return status in ("Legal", "Restricted")


# =============================================================================
# Namespace for File Models
# =============================================================================


class Files:
    """Namespace for all file models."""

    AllPrintingsFile = AllPrintingsFile
    AtomicCardsFile = AtomicCardsFile
    AllIdentifiersFile = AllIdentifiersFile
    AllPricesFile = AllPricesFile
    TcgplayerSkusFile = TcgplayerSkusFile
    SetListFile = SetListFile
    DeckListFile = DeckListFile
    IndividualSetFile = IndividualSetFile
    MetaFile = MetaFile


# =============================================================================
# Registry for TypeScript generation
# =============================================================================

FILE_MODEL_REGISTRY: list[type[BaseModel]] = [
    AllPrintingsFile,
    AtomicCardsFile,
    AllIdentifiersFile,
    AllPricesFile,
    TcgplayerSkusFile,
    SetListFile,
    DeckListFile,
    IndividualSetFile,
    MetaFile,
]

__all__ = [
    "FILE_MODEL_REGISTRY",
    "Files",
]
