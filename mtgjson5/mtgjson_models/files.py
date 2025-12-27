"""
MTGJSON file structure models.

These models represent the complete structure of MTGJSON output files.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, ClassVar

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


# =============================================================================
# List Files (data: T[])
# =============================================================================

class SetListFile(ListFileBase):
    """SetList.json: { meta, data: [SetList, ...] }"""
    data: list[SetList]

    def iter_sets(self) -> Iterator[SetList]:
        yield from self.data

    def get_by_code(self, code: str) -> SetList | None:
        code = code.upper()
        return next((s for s in self.data if s.get("code") == code), None)


class DeckListFile(ListFileBase):
    """DeckList.json: { meta, data: [DeckList, ...] }"""
    data: list[DeckList]

    def iter_decks(self) -> Iterator[DeckList]:
        yield from self.data


# =============================================================================
# Format-Specific Files
# =============================================================================

class FormatPrintingsFile(RecordFileBase):
    """Base for format-specific printings (Legacy, Modern, etc.)."""
    data: dict[str, dict[str, Any]]
    format_name: ClassVar[str] = ""

    @classmethod
    def from_all_printings(
        cls,
        all_printings: AllPrintingsFile,
        filter_fn: Callable[[str, dict[str, Any]], bool],
    ) -> FormatPrintingsFile:
        """Filter AllPrintings to format-legal cards."""
        from pydantic import BaseModel

        filtered: dict[str, dict[str, Any]] = {}
        for set_code, set_data in all_printings.iter_sets():
            # Convert Pydantic model to dict if needed
            if isinstance(set_data, BaseModel):
                set_dict = set_data.model_dump(by_alias=True, exclude_none=True)
            else:
                set_dict = set_data

            cards = [
                c for c in set_dict.get("cards", [])
                if filter_fn(set_code, c)
            ]
            if cards:
                filtered[set_code] = {**set_dict, "cards": cards}
        return cls(meta=all_printings.meta, data=filtered)


class LegacyFile(FormatPrintingsFile):
    format_name: ClassVar[str] = "legacy"


class ModernFile(FormatPrintingsFile):
    format_name: ClassVar[str] = "modern"


class PioneerFile(FormatPrintingsFile):
    format_name: ClassVar[str] = "pioneer"


class StandardFile(FormatPrintingsFile):
    format_name: ClassVar[str] = "standard"


class VintageFile(FormatPrintingsFile):
    format_name: ClassVar[str] = "vintage"


class FormatAtomicFile(RecordFileBase):
    """Base for format-specific atomic cards."""
    data: dict[str, list[dict[str, Any]]]
    format_name: ClassVar[str] = ""

    @classmethod
    def from_atomic_cards(
        cls,
        atomic: AtomicCardsFile,
        filter_fn: Callable[[str, dict[str, Any]], bool],
    ) -> FormatAtomicFile:
        """Filter AtomicCards to format-legal cards."""
        filtered: dict[str, list[dict[str, Any]]] = {}
        for name, variants in atomic.iter_cards():
            legal = [v for v in variants if filter_fn(name, v)]
            if legal:
                filtered[name] = legal
        return cls(meta=atomic.meta, data=filtered)


class LegacyAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "legacy"


class ModernAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "modern"


class PauperAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "pauper"


class PioneerAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "pioneer"


class StandardAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "standard"


class VintageAtomicFile(FormatAtomicFile):
    format_name: ClassVar[str] = "vintage"


# =============================================================================
# Format Filter Utility
# =============================================================================

class FormatFilter:
    """Filters for format-legal cards."""

    @staticmethod
    def is_legal(card: dict[str, Any], format_name: str) -> bool:
        """Check if card is legal in format."""
        legalities = card.get("legalities", {})
        status = legalities.get(format_name)
        return status in ("Legal", "Restricted")

    @staticmethod
    def legacy(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "legacy")

    @staticmethod
    def modern(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "modern")

    @staticmethod
    def pioneer(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "pioneer")

    @staticmethod
    def standard(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "standard")

    @staticmethod
    def vintage(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "vintage")

    @staticmethod
    def pauper(set_code: str, card: dict[str, Any]) -> bool:
        return FormatFilter.is_legal(card, "pauper")


# =============================================================================
# Registry
# =============================================================================

FILE_MODEL_REGISTRY: list[type[BaseModel]] = [
    AllPrintingsFile,
    AtomicCardsFile,
    AllIdentifiersFile,
    AllPricesFile,
    SetListFile,
    DeckListFile,
    FormatPrintingsFile,
    LegacyFile,
    ModernFile,
    PioneerFile,
    StandardFile,
    VintageFile,
    FormatAtomicFile,
    LegacyAtomicFile,
    ModernAtomicFile,
    PauperAtomicFile,
    PioneerAtomicFile,
    StandardAtomicFile,
    VintageAtomicFile,
]
