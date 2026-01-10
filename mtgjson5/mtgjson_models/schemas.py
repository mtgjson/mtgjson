"""
Polars schema generation and field constants.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .cards import CardAtomic, CardDeck, CardSet, CardToken
from .utils import PolarsConverter


if TYPE_CHECKING:
	from polars import Schema


# =============================================================================
# Schema Generation
# =============================================================================


def get_card_set_schema() -> Schema:
	"""Get Polars schema for CardSet."""
	return CardSet.polars_schema()


def get_card_atomic_schema() -> Schema:
	"""Get Polars schema for CardAtomic."""
	return CardAtomic.polars_schema()


def get_card_deck_schema() -> Schema:
	"""Get Polars schema for CardDeck."""
	return CardDeck.polars_schema()


def get_card_token_schema() -> Schema:
	"""Get Polars schema for CardToken."""
	return CardToken.polars_schema()


# Lazy schema objects (computed on first access)
class _LazySchema:
	_card_set: Schema | None = None
	_card_atomic: Schema | None = None
	_card_deck: Schema | None = None
	_card_token: Schema | None = None

	@classmethod
	def card_set(cls) -> Schema:
		if cls._card_set is None:
			cls._card_set = get_card_set_schema()
		return cls._card_set

	@classmethod
	def card_atomic(cls) -> Schema:
		if cls._card_atomic is None:
			cls._card_atomic = get_card_atomic_schema()
		return cls._card_atomic

	@classmethod
	def card_deck(cls) -> Schema:
		if cls._card_deck is None:
			cls._card_deck = get_card_deck_schema()
		return cls._card_deck

	@classmethod
	def card_token(cls) -> Schema:
		if cls._card_token is None:
			cls._card_token = get_card_token_schema()
		return cls._card_token


# Compatibility aliases
CardSetSchema = property(lambda self: _LazySchema.card_set())
CardAtomicSchema = property(lambda self: _LazySchema.card_atomic())
CardDeckSchema = property(lambda self: _LazySchema.card_deck())
CardTokenSchema = property(lambda self: _LazySchema.card_token())


# =============================================================================
# Field Sets
# =============================================================================


def _get_model_fields(model: type) -> set[str]:
	"""Get all field names (using aliases) from a model."""
	return {(info.alias or name) for name, info in model.model_fields.items()}


# All fields that can appear on any card type
ALL_CARD_FIELDS: frozenset[str] = frozenset(
	_get_model_fields(CardSet)
	| _get_model_fields(CardAtomic)
	| _get_model_fields(CardToken)
	| _get_model_fields(CardDeck)
)

# Fields to EXCLUDE when creating atomic cards from set cards
ATOMIC_EXCLUDE: frozenset[str] = frozenset(_get_model_fields(CardSet) - _get_model_fields(CardAtomic))

# Fields to EXCLUDE when creating deck cards from set cards
CARD_DECK_EXCLUDE: frozenset[str] = frozenset(_get_model_fields(CardSet) - _get_model_fields(CardDeck))

# Fields to EXCLUDE when creating tokens from set cards
TOKEN_EXCLUDE: frozenset[str] = frozenset(_get_model_fields(CardSet) - _get_model_fields(CardToken))

# Required fields for deck list entries
REQUIRED_DECK_LIST_FIELDS: frozenset[str] = frozenset(
	{
		"code",
		"name",
		"releaseDate",
		"type",
	}
)


# =============================================================================
# Utility Functions
# =============================================================================


def pydantic_type_to_polars(python_type):
	"""Convert Python type to Polars dtype."""
	return PolarsConverter.python_to_polars(python_type)


def pydantic_model_to_struct(model):
	"""Convert Pydantic model to Polars Struct."""
	return PolarsConverter.model_to_struct(model)


def pydantic_model_to_schema(model):
	"""Convert Pydantic model to Polars Schema."""
	if hasattr(model, "polars_schema"):
		return model.polars_schema()
	return PolarsConverter.model_to_struct(model)
