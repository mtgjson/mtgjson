"""
MTGJSON compiled data models (Keywords, CardTypes, EnumValues, etc.).
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel

from .base import MtgjsonFileBase
from .submodels import CardTypes, Keywords


class CompiledListFile(MtgjsonFileBase):
    """File containing a simple list of strings."""

    data: list[str]


class KeywordsFile(MtgjsonFileBase):
    """Keywords.json structure."""

    data: Keywords


class CardTypesFile(MtgjsonFileBase):
    """CardTypes.json structure."""

    data: CardTypes


class EnumValuesFile(MtgjsonFileBase):
    """EnumValues.json structure."""

    __ts_name__: ClassVar[str] = "EnumValues"

    data: dict[str, dict[str, list[str]]]


class Compiled:
    """Namespace for all compiled data models."""

    CompiledListFile = CompiledListFile
    KeywordsFile = KeywordsFile
    CardTypesFile = CardTypesFile
    EnumValuesFile = EnumValuesFile


COMPILED_MODEL_REGISTRY: list[type[BaseModel]] = [
    CompiledListFile,
    KeywordsFile,
    CardTypesFile,
    EnumValuesFile,
]

__all__ = [
    "COMPILED_MODEL_REGISTRY",
    "Compiled",
]
