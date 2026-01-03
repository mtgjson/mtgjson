"""
Type converters and generators for MTGJSON models.
"""

from __future__ import annotations

import typing
from typing import TYPE_CHECKING, Any, ClassVar, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import Required  # noqa: UP035

from ._typing import TypedDictUtils, is_union_type


try:
	import polars as pl

	POLARS_AVAILABLE = True
except ImportError:
	POLARS_AVAILABLE = False
	pl = None

if TYPE_CHECKING:
	from polars.datatypes import DataType, Struct


class PolarsConverter:
	"""Converts between Python/Pydantic types and Polars types."""

	@classmethod
	def python_to_polars(cls, python_type: Any) -> DataType:
		"""Map Python type to Polars dtype."""
		if not POLARS_AVAILABLE:
			raise ImportError("Polars required")

		origin = get_origin(python_type)
		args = get_args(python_type)

		# Handle Union / Optional
		if is_union_type(python_type):
			non_none = [a for a in args if a is not type(None)]
			if len(non_none) == 1:
				return cls.python_to_polars(non_none[0])
			return pl.String

		# Handle list
		if origin is list:
			inner = cls.python_to_polars(args[0]) if args else pl.String
			return pl.List(inner)

		# Handle TypedDict
		if TypedDictUtils.is_typeddict(python_type):
			return cls.typeddict_to_struct(python_type)

		# Handle Pydantic model
		if isinstance(python_type, type) and issubclass(python_type, BaseModel):
			return cls.model_to_struct(python_type)

		# Primitives
		if python_type is str:
			return pl.String
		if python_type is int:
			return pl.Int64
		if python_type is float:
			return pl.Float64
		if python_type is bool:
			return pl.Boolean

		return pl.String

	@classmethod
	def typeddict_to_struct(cls, td: type) -> Struct:
		"""Convert TypedDict to Polars Struct."""
		if not POLARS_AVAILABLE:
			raise ImportError("Polars required")
		fields = []
		for name in sorted(TypedDictUtils.get_fields(td).keys()):
			field_type = TypedDictUtils.get_fields(td)[name]
			fields.append(pl.Field(name, cls.python_to_polars(field_type)))
		return pl.Struct(fields)

	@classmethod
	def model_to_struct(cls, model: type[BaseModel]) -> Struct:
		"""Convert Pydantic model to Polars Struct."""
		if not POLARS_AVAILABLE:
			raise ImportError("Polars required")
		fields = []
		for name in sorted(model.model_fields.keys()):
			info = model.model_fields[name]
			output_name = info.alias or name
			fields.append(pl.Field(output_name, cls.python_to_polars(info.annotation)))
		return pl.Struct(fields)


class TypeScriptGenerator:
	"""Generates TypeScript interfaces from Python types."""

	PRIMITIVE_MAP: ClassVar[dict[type, str]] = {
		str: "string",
		int: "number",
		float: "number",
		bool: "boolean",
		type(None): "null",
	}

	@classmethod
	def python_to_ts(cls, python_type: Any) -> str:
		"""Convert Python type to TypeScript type string."""
		# Handle ForwardRef
		if isinstance(python_type, typing.ForwardRef):
			s = python_type.__forward_arg__
			m = {"str": "string", "int": "number", "float": "number", "bool": "boolean", "None": "null"}
			if s in m:
				return m[s]
			if s.startswith("list["):
				inner = s[5:-1]
				return f"{m.get(inner, inner)}[]"
			if s.startswith("Required["):
				inner = s[9:-1]
				return m.get(inner, inner)
			return s

		origin = get_origin(python_type)
		args = get_args(python_type)

		# Handle Union
		if is_union_type(python_type):
			non_none = [a for a in args if a is not type(None)]
			if len(non_none) == 1:
				return cls.python_to_ts(non_none[0])
			return " | ".join(cls.python_to_ts(a) for a in non_none)

		if origin is Required:
			return cls.python_to_ts(args[0]) if args else "any"

		if origin is list:
			return f"{cls.python_to_ts(args[0])}[]" if args else "any[]"

		if origin is dict:
			if args and len(args) == 2:
				return f"Record<{cls.python_to_ts(args[0])}, {cls.python_to_ts(args[1])}>"
			return "Record<string, any>"

		if TypedDictUtils.is_typeddict(python_type):
			return python_type.__name__

		if isinstance(python_type, type) and issubclass(python_type, BaseModel):
			return python_type.__name__

		return cls.PRIMITIVE_MAP.get(python_type, "any")

	@classmethod
	def from_typeddict(cls, td: type, indent: str = "  ") -> str:
		"""Generate TypeScript type from TypedDict."""
		lines = [f"export type {td.__name__} = {{"]
		fields = TypedDictUtils.get_fields(td)
		for name in sorted(fields.keys()):
			required = TypedDictUtils.is_field_required(td, name)
			ts_type = cls.python_to_ts(fields[name])
			opt = "" if required else "?"
			lines.append(f"{indent}{name}{opt}: {ts_type};")
		lines.append("};")
		return "\n".join(lines)

	@classmethod
	def from_model(cls, model: type[BaseModel], indent: str = "  ") -> str:
		"""Generate TypeScript type from Pydantic model."""
		import sys

		lines = [f"export type {model.__name__} = {{"]

		# Get resolved hints
		try:
			module = sys.modules.get(model.__module__, None)
			globalns = getattr(module, "__dict__", {}) if module else {}
			hints = typing.get_type_hints(model, globalns=globalns)
		except Exception:
			hints = {}

		sorted_fields = sorted(model.model_fields.items(), key=lambda x: x[1].alias or x[0])

		for name, info in sorted_fields:
			output_name = info.alias or name
			annotation = hints.get(name, info.annotation)

			# Determine optionality
			is_optional = info.default is not None or info.default_factory is not None or not info.is_required()

			# Check for Optional in annotation
			if is_union_type(annotation):
				args = get_args(annotation)
				non_none = [a for a in args if a is not type(None)]
				if len(non_none) < len(args):
					is_optional = True
					if len(non_none) == 1:
						annotation = non_none[0]

			ts_type = cls.python_to_ts(annotation)
			opt = "?" if is_optional else ""
			lines.append(f"{indent}{output_name}{opt}: {ts_type};")

		lines.append("};")
		return "\n".join(lines)
