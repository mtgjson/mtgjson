"""
MTGJSON TypeAdapters and parse functions.

Module-level TypeAdapters for efficient parsing of MTGJSON models.
"""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .cards import CardAtomic, CardDeck, CardSet, CardSetDeck, CardToken
from .decks import Deck, DeckList
from .files import AllPrintingsFile, AtomicCardsFile
from .sets import DeckSet, MtgSet, SealedProduct, SetList

CardAtomicAdapter: TypeAdapter[CardAtomic] = TypeAdapter(CardAtomic)
CardAtomicListAdapter: TypeAdapter[list[CardAtomic]] = TypeAdapter(list[CardAtomic])

CardSetAdapter: TypeAdapter[CardSet] = TypeAdapter(CardSet)
CardSetListAdapter: TypeAdapter[list[CardSet]] = TypeAdapter(list[CardSet])

CardDeckAdapter: TypeAdapter[CardDeck] = TypeAdapter(CardDeck)
CardDeckListAdapter: TypeAdapter[list[CardDeck]] = TypeAdapter(list[CardDeck])

CardTokenAdapter: TypeAdapter[CardToken] = TypeAdapter(CardToken)
CardTokenListAdapter: TypeAdapter[list[CardToken]] = TypeAdapter(list[CardToken])

CardSetDeckAdapter: TypeAdapter[CardSetDeck] = TypeAdapter(CardSetDeck)
CardSetDeckListAdapter: TypeAdapter[list[CardSetDeck]] = TypeAdapter(list[CardSetDeck])

SetAdapter: TypeAdapter[MtgSet] = TypeAdapter(MtgSet)
SetListAdapter: TypeAdapter[list[SetList]] = TypeAdapter(list[SetList])

SealedProductAdapter: TypeAdapter[SealedProduct] = TypeAdapter(SealedProduct)
SealedProductListAdapter: TypeAdapter[list[SealedProduct]] = TypeAdapter(
    list[SealedProduct]
)

DeckAdapter: TypeAdapter[Deck] = TypeAdapter(Deck)
DeckListAdapter: TypeAdapter[list[DeckList]] = TypeAdapter(list[DeckList])
DeckSetListAdapter: TypeAdapter[list[DeckSet]] = TypeAdapter(list[DeckSet])

AllPrintingsFileAdapter: TypeAdapter[AllPrintingsFile] = TypeAdapter(AllPrintingsFile)
AtomicCardsFileAdapter: TypeAdapter[AtomicCardsFile] = TypeAdapter(AtomicCardsFile)


def parse_card_atomic(data: dict[str, Any]) -> CardAtomic:
    """Parse a single CardAtomic from dict."""
    return CardAtomicAdapter.validate_python(data)


def parse_cards_atomic(data: list[dict[str, Any]]) -> list[CardAtomic]:
    """Parse a list of CardAtomic from dicts."""
    return CardAtomicListAdapter.validate_python(data)


def parse_card_set(data: dict[str, Any]) -> CardSet:
    """Parse a single CardSet from dict."""
    return CardSetAdapter.validate_python(data)


def parse_cards_set(data: list[dict[str, Any]]) -> list[CardSet]:
    """Parse a list of CardSet from dicts."""
    return CardSetListAdapter.validate_python(data)


def parse_card_deck(data: dict[str, Any]) -> CardDeck:
    """Parse a single CardDeck from dict."""
    return CardDeckAdapter.validate_python(data)


def parse_cards_deck(data: list[dict[str, Any]]) -> list[CardDeck]:
    """Parse a list of CardDeck from dicts."""
    return CardDeckListAdapter.validate_python(data)


def parse_card_token(data: dict[str, Any]) -> CardToken:
    """Parse a single CardToken from dict."""
    return CardTokenAdapter.validate_python(data)


def parse_cards_token(data: list[dict[str, Any]]) -> list[CardToken]:
    """Parse a list of CardToken from dicts."""
    return CardTokenListAdapter.validate_python(data)


def parse_set(data: dict[str, Any]) -> MtgSet:
    """Parse a Set from dict."""
    return SetAdapter.validate_python(data)


def parse_deck(data: dict[str, Any]) -> Deck:
    """Parse a Deck from dict."""
    return DeckAdapter.validate_python(data)


def parse_sealed_product(data: dict[str, Any]) -> SealedProduct:
    """Parse a SealedProduct from dict."""
    return SealedProductAdapter.validate_python(data)


def parse_sealed_products(data: list[dict[str, Any]]) -> list[SealedProduct]:
    """Parse a list of SealedProduct from dicts."""
    return SealedProductListAdapter.validate_python(data)


def parse_all_printings(data: dict[str, Any]) -> AllPrintingsFile:
    """Parse AllPrintings.json content."""
    return AllPrintingsFileAdapter.validate_python(data)


def parse_atomic_cards(data: dict[str, Any]) -> AtomicCardsFile:
    """Parse AtomicCards.json content."""
    return AtomicCardsFileAdapter.validate_python(data)


def parse_atomic_cards_file(
    data: dict[str, list[dict[str, Any]]],
) -> dict[str, list[CardAtomic]]:
    """Parse AtomicCards data section (name -> [CardAtomic, ...])."""
    return {
        name: CardAtomicListAdapter.validate_python(cards)
        for name, cards in data.items()
    }


class Adapters:
    """Namespace for all TypeAdapters."""

    card_atomic = staticmethod(CardAtomicAdapter.validate_python)
    cards_atomic = staticmethod(CardAtomicListAdapter.validate_python)
    card_set = staticmethod(CardSetAdapter.validate_python)
    cards_set = staticmethod(CardSetListAdapter.validate_python)
    card_deck = staticmethod(CardDeckAdapter.validate_python)
    cards_deck = staticmethod(CardDeckListAdapter.validate_python)
    card_token = staticmethod(CardTokenAdapter.validate_python)
    cards_token = staticmethod(CardTokenListAdapter.validate_python)
    card_set_deck = staticmethod(CardSetDeckAdapter.validate_python)
    cards_set_deck = staticmethod(CardSetDeckListAdapter.validate_python)
    mtg_set = staticmethod(SetAdapter.validate_python)
    set_list = staticmethod(SetListAdapter.validate_python)
    sealed_product = staticmethod(SealedProductAdapter.validate_python)
    sealed_products = staticmethod(SealedProductListAdapter.validate_python)
    deck = staticmethod(DeckAdapter.validate_python)
    deck_list = staticmethod(DeckListAdapter.validate_python)
    deck_set_list = staticmethod(DeckSetListAdapter.validate_python)
    all_printings_file = staticmethod(AllPrintingsFileAdapter.validate_python)
    atomic_cards_file = staticmethod(AtomicCardsFileAdapter.validate_python)


class Parsers:
    """Namespace for all parse functions."""

    parse_card_atomic = staticmethod(parse_card_atomic)
    parse_cards_atomic = staticmethod(parse_cards_atomic)
    parse_card_set = staticmethod(parse_card_set)
    parse_cards_set = staticmethod(parse_cards_set)
    parse_card_deck = staticmethod(parse_card_deck)
    parse_cards_deck = staticmethod(parse_cards_deck)
    parse_card_token = staticmethod(parse_card_token)
    parse_cards_token = staticmethod(parse_cards_token)
    parse_set = staticmethod(parse_set)
    parse_deck = staticmethod(parse_deck)
    parse_sealed_product = staticmethod(parse_sealed_product)
    parse_sealed_products = staticmethod(parse_sealed_products)
    parse_all_printings = staticmethod(parse_all_printings)
    parse_atomic_cards = staticmethod(parse_atomic_cards)
    parse_atomic_cards_file = staticmethod(parse_atomic_cards_file)
