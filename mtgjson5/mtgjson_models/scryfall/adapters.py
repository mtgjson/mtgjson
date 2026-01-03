"""
Scryfall TypeAdapters for validated parsing.

Use when you need Pydantic validation but want to avoid
repeated model instantiation overhead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import TypeAdapter


if TYPE_CHECKING:
	from .submodels import ScryfallCard


# Lazy init to avoid import-time cost
_card_adapter: TypeAdapter | None = None
_cards_adapter: TypeAdapter | None = None
_set_adapter: TypeAdapter | None = None
_sets_adapter: TypeAdapter | None = None


def get_card_adapter() -> TypeAdapter:
	"""Get or create card TypeAdapter."""
	global _card_adapter
	if _card_adapter is None:
		from .submodels import ScryfallCardModel

		_card_adapter = TypeAdapter(ScryfallCardModel)
	return _card_adapter


def get_cards_adapter() -> TypeAdapter:
	"""Get or create cards list TypeAdapter."""
	global _cards_adapter
	if _cards_adapter is None:
		from .submodels import ScryfallCardModel

		_cards_adapter = TypeAdapter(list[ScryfallCardModel])
	return _cards_adapter


def get_set_adapter() -> TypeAdapter:
	"""Get or create set TypeAdapter."""
	global _set_adapter
	if _set_adapter is None:
		from .submodels import ScryfallSetModel

		_set_adapter = TypeAdapter(ScryfallSetModel)
	return _set_adapter


def get_sets_adapter() -> TypeAdapter:
	"""Get or create sets list TypeAdapter."""
	global _sets_adapter
	if _sets_adapter is None:
		from .submodels import ScryfallSetModel

		_sets_adapter = TypeAdapter(list[ScryfallSetModel])
	return _sets_adapter


def validate_card(data: dict) -> ScryfallCard:
	"""Validate a single card dict."""
	return get_card_adapter().validate_python(data)


def validate_cards(data: list[dict]) -> list[ScryfallCard]:
	"""Validate a list of card dicts."""
	return get_cards_adapter().validate_python(data)
