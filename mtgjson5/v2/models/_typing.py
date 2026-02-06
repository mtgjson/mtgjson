"""
Type utilities for MTGJSON models.
"""

from __future__ import annotations

import sys
import types
import typing
from typing import Any, get_args, get_origin

from typing_extensions import Required  # noqa: UP035


class TypedDictUtils:
    """Utilities for working with TypedDicts."""

    @staticmethod
    def is_typeddict(tp: Any) -> bool:
        """Check if a type is a TypedDict."""
        return isinstance(tp, type) and issubclass(tp, dict) and hasattr(tp, "__annotations__")

    @staticmethod
    def get_fields(td: type) -> dict[str, Any]:
        """Get resolved type hints from a TypedDict."""
        annotations = {}
        for base in reversed(td.__mro__):
            if hasattr(base, "__annotations__"):
                annotations.update(base.__annotations__)

        try:
            module = sys.modules.get(td.__module__, None)
            globalns = getattr(module, "__dict__", {}) if module else {}
            localns = {
                td.__name__: td,
                "Required": Required,
                "list": list,
                "dict": dict,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
            }
            return typing.get_type_hints(td, globalns=globalns, localns=localns)
        except Exception:
            return annotations

    @staticmethod
    def is_field_required(td: type, field_name: str) -> bool:
        """Check if a TypedDict field is required."""
        if field_name in getattr(td, "__required_keys__", set()):
            return True
        # Check raw annotation for Required[] wrapper
        raw = {}
        for base in reversed(td.__mro__):
            if hasattr(base, "__annotations__"):
                raw.update(base.__annotations__)
        raw_type = raw.get(field_name)
        if isinstance(raw_type, typing.ForwardRef):
            return "Required[" in raw_type.__forward_arg__
        return getattr(raw_type, "__origin__", None) is Required

    @staticmethod
    def filter_none(d: dict[str, Any]) -> dict[str, Any]:
        """Remove None values from dict (for Polars struct reconstruction)."""
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def apply_aliases(
        cls,
        td: type,
        d: dict[str, Any],
        aliases: dict[tuple[str, str], str],
    ) -> dict[str, Any]:
        """Apply field aliases and filter None values recursively.

        Args:
            td: The TypedDict type
            d: The dict to transform
            aliases: Mapping of (TypedDict_name, source_field) -> target_field

        Returns:
            New dict with aliased field names and None values removed
        """
        td_name = td.__name__
        result: dict[str, Any] = {}
        for key, value in d.items():
            target_key = aliases.get((td_name, key), key)
            if value is None:
                continue
            if isinstance(value, dict):
                cleaned = cls._clean_nested(value, aliases)
                if cleaned:
                    result[target_key] = cleaned
            elif isinstance(value, list):
                cleaned_list = cls._clean_list(value, aliases)
                if cleaned_list:
                    result[target_key] = cleaned_list
            else:
                result[target_key] = value
        return result

    @classmethod
    def _clean_nested(
        cls,
        d: dict[str, Any],
        aliases: dict[tuple[str, str], str],
    ) -> dict[str, Any]:
        """Recursively clean nested dict, removing None values."""
        result: dict[str, Any] = {}
        for key, value in d.items():
            if value is None:
                continue
            if isinstance(value, dict):
                cleaned = cls._clean_nested(value, aliases)
                if cleaned:
                    result[key] = cleaned
            elif isinstance(value, list):
                cleaned_list = cls._clean_list(value, aliases)
                if cleaned_list:
                    result[key] = cleaned_list
            else:
                result[key] = value
        return result

    @classmethod
    def _clean_list(
        cls,
        lst: list[Any],
        aliases: dict[tuple[str, str], str],
    ) -> list[Any]:
        """Recursively clean list items, removing None values from nested dicts."""
        result: list[Any] = []
        for item in lst:
            if item is None:
                continue
            if isinstance(item, dict):
                cleaned = cls._clean_nested(item, aliases)
                if cleaned:
                    result.append(cleaned)
            elif isinstance(item, list):
                cleaned_list = cls._clean_list(item, aliases)
                if cleaned_list:
                    result.append(cleaned_list)
            else:
                result.append(item)
        return result


def is_union_type(tp: Any) -> bool:
    """Check if type is a Union (including X | Y syntax)."""
    origin = get_origin(tp)
    return origin is typing.Union or (hasattr(types, "UnionType") and isinstance(tp, types.UnionType))


def unwrap_optional(tp: Any) -> tuple[Any, bool]:
    """
    Unwrap Optional[T] to T.

    Returns:
        (inner_type, is_optional)
    """
    if not is_union_type(tp):
        return tp, False

    args = get_args(tp)
    non_none = [a for a in args if a is not type(None)]

    if len(non_none) == len(args):
        return tp, False  # Not optional

    if len(non_none) == 1:
        return non_none[0], True

    # Multiple non-None types
    return tp, True
