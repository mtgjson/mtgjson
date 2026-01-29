"""
MTGJSON base classes and mixins.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any, ClassVar, get_args, get_origin

from pydantic import BaseModel

from mtgjson5.conventions import (
	ALLOW_IF_FALSEY,
	EXCLUDE_FROM_OUTPUT,
	OMIT_EMPTY_LIST_FIELDS,
	SORTED_LIST_FIELDS,
	TYPEDDICT_FIELD_ALIASES,
)
from mtgjson5.mtgjson_models._typing import TypedDictUtils, is_union_type


if TYPE_CHECKING:
	import polars as pl
	from polars.dataframe import DataFrame
	from polars.lazyframe import LazyFrame
	from polars.schema import Schema

try:
	import polars as pl

	POLARS_AVAILABLE = True
except ImportError:
	POLARS_AVAILABLE = False
	pl = None  # type: ignore

try:
	import orjson
except ImportError:
	orjson = None  # type: ignore


class PolarsMixin:
	"""Mixin providing Polars DataFrame serialization for Pydantic models."""

	_sorted_list_fields: ClassVar[set[str]] = SORTED_LIST_FIELDS
	_allow_if_falsey: ClassVar[set[str]] = ALLOW_IF_FALSEY

	@classmethod
	def polars_schema(cls) -> Schema:
		"""Generate Polars schema for this model."""
		if not POLARS_AVAILABLE:
			raise ImportError("Polars required")
		from .utils import PolarsConverter

		schema = {}
		for name in sorted(cls.model_fields.keys()):
			info = cls.model_fields[name]
			output_name = info.alias or name
			schema[output_name] = PolarsConverter.python_to_polars(info.annotation)
		return pl.Schema(schema)

	@classmethod
	def json_schema(
		cls,
		by_alias: bool = True,
		ref_template: str = "#/$defs/{model}",
		mode: str = "serialization",
	) -> dict[str, Any]:
		"""Generate JSON Schema for this model.

		Args:
		    by_alias: Use field aliases in schema (default True for MTGJSON output names)
		    ref_template: Template for $ref URIs
		    mode: 'serialization' (output) or 'validation' (input)

		Returns:
		    JSON Schema as dict
		"""
		return cls.model_json_schema(
			by_alias=by_alias,
			ref_template=ref_template,
			mode=mode,
		)

	@classmethod
	def write_json_schema(
		cls,
		path: pathlib.Path,
		by_alias: bool = True,
		pretty: bool = True,
	) -> None:
		"""Write JSON Schema to file.

		Args:
		    path: Output path
		    by_alias: Use field aliases in schema
		    pretty: Pretty-print with indentation
		"""
		schema = cls.json_schema(by_alias=by_alias)

		if orjson is not None:
			opts = orjson.OPT_SORT_KEYS
			if pretty:
				opts |= orjson.OPT_INDENT_2
			with path.open("wb") as f:
				f.write(orjson.dumps(schema, option=opts))
		else:
			import json

			with path.open("w", encoding="utf-8") as f:
				json.dump(schema, f, indent=2 if pretty else None, sort_keys=True)

	def to_polars_dict(
		self,
		use_alias: bool = True,
		sort_keys: bool = True,
		sort_lists: bool = True,
		exclude_none: bool = False,
		keep_empty_lists: bool = False,
	) -> dict[str, Any]:
		"""
		Convert instance to dict suitable for Polars.
		"""
		return self._to_dict_recursive(self, use_alias, sort_keys, sort_lists, exclude_none, keep_empty_lists)

	@classmethod
	def _to_dict_recursive(
		cls,
		instance: BaseModel,
		use_alias: bool,
		sort_keys: bool,
		sort_lists: bool,
		exclude_none: bool,
		keep_empty_lists: bool = False,
	) -> dict[str, Any]:
		"""Recursively convert model to dict."""
		result = {}
		items = list(type(instance).model_fields.items())
		if sort_keys:
			items = sorted(items, key=lambda x: (x[1].alias or x[0]) if use_alias else x[0])

		for field_name, info in items:
			key = (info.alias or field_name) if use_alias else field_name

			if key in EXCLUDE_FROM_OUTPUT or info.exclude:
				continue

			value = getattr(instance, field_name)

			if value is None:
				if key in ("legalities", "purchaseUrls"):
					result[key] = {}
				elif field_name in cls._allow_if_falsey or not exclude_none:
					result[key] = None
				continue

			if isinstance(value, BaseModel):
				nested = cls._to_dict_recursive(value, use_alias, sort_keys, sort_lists, exclude_none, keep_empty_lists)
				if nested or field_name in cls._allow_if_falsey or not exclude_none:
					result[key] = nested
			elif isinstance(value, dict):
				if key in ("legalities", "purchaseUrls") and not value:
					result[key] = {}
				elif value or field_name in cls._allow_if_falsey or not exclude_none:
					result[key] = dict(sorted(value.items())) if sort_keys else value
			elif isinstance(value, list):
				if not value and exclude_none and key in OMIT_EMPTY_LIST_FIELDS and not keep_empty_lists:
					continue
				if value and isinstance(value[0], BaseModel):
					result[key] = [
						cls._to_dict_recursive(v, use_alias, sort_keys, sort_lists, exclude_none, keep_empty_lists) for v in value
					]
				elif value and isinstance(value[0], dict):
					sorted_list = [dict(sorted(v.items())) for v in value] if sort_keys else list(value)
					if key == "rulings":
						sorted_list = sorted(sorted_list, key=lambda r: (r.get("date", ""), r.get("text", "")))
					result[key] = sorted_list
				else:
					result[key] = cls._sort_list(key, value) if sort_lists else list(value)
			else:
				if exclude_none and value is False and key not in cls._allow_if_falsey:
					continue
				if exclude_none and value == "" and key not in cls._allow_if_falsey:
					continue
				result[key] = value

		return result

	@staticmethod
	def _sort_list(key: str, value: list[Any]) -> list[Any]:
		"""Sort list alphabetically for sortable fields."""
		if key in SORTED_LIST_FIELDS and value:
			try:
				return sorted(value)
			except TypeError:
				return value
		return value

	@classmethod
	def to_dataframe(cls, instances: list[PolarsMixin]) -> DataFrame:
		"""Convert list of instances to DataFrame."""
		if not POLARS_AVAILABLE:
			raise ImportError("Polars required")
		if not instances:
			return pl.DataFrame(schema=cls.polars_schema())
		data = [inst.to_polars_dict() for inst in instances]
		return pl.DataFrame(data, schema=cls.polars_schema())

	@classmethod
	def to_lazyframe(cls, instances: list[PolarsMixin]) -> LazyFrame:
		"""Convert list of instances to LazyFrame."""
		return cls.to_dataframe(instances).lazy()

	@classmethod
	def from_polars_row(cls, row: dict[str, Any]) -> PolarsMixin:
		"""Reconstruct model from Polars row dict."""
		converted = cls._from_row_recursive(row, cls)
		return cls.model_validate(converted)

	@classmethod
	def _from_row_recursive(cls, row: dict[str, Any], model: type[BaseModel]) -> dict[str, Any]:
		"""Recursively convert row dict to model-compatible dict."""
		alias_map = {(info.alias or name): name for name, info in model.model_fields.items()}
		result = {}

		for key, value in row.items():
			field_name = alias_map.get(key, key)
			if field_name not in model.model_fields:
				continue

			info = model.model_fields[field_name]
			annotation = info.annotation

			# Unwrap Optional
			origin = get_origin(annotation)
			args = get_args(annotation)
			if is_union_type(annotation):
				non_none = [a for a in args if a is not type(None)]
				if non_none:
					annotation = non_none[0]
					origin = get_origin(annotation)
					args = get_args(annotation)

			if isinstance(value, dict):
				if isinstance(annotation, type) and issubclass(annotation, BaseModel):
					result[field_name] = cls._from_row_recursive(value, annotation)
				elif TypedDictUtils.is_typeddict(annotation):
					result[field_name] = TypedDictUtils.apply_aliases(annotation, value, TYPEDDICT_FIELD_ALIASES)
				else:
					result[field_name] = value
			elif isinstance(value, list) and value and isinstance(value[0], dict):
				if origin is list and args:
					inner = args[0]
					if isinstance(inner, type) and issubclass(inner, BaseModel):
						result[field_name] = [cls._from_row_recursive(v, inner) for v in value]
					elif TypedDictUtils.is_typeddict(inner):
						result[field_name] = [
							TypedDictUtils.apply_aliases(inner, v, TYPEDDICT_FIELD_ALIASES) for v in value
						]
					else:
						result[field_name] = value
				else:
					result[field_name] = value
			else:
				result[field_name] = value

		return result

	@classmethod
	def from_dataframe(cls, df: DataFrame) -> list[PolarsMixin]:
		"""Reconstruct list of models from DataFrame."""
		return [cls.from_polars_row(row) for row in df.iter_rows(named=True)]

	@classmethod
	def from_lazyframe(cls, lf: LazyFrame) -> list[PolarsMixin]:
		"""Reconstruct list of models from LazyFrame."""
		return cls.from_dataframe(lf.collect())

	@classmethod
	def to_typescript(cls, indent: str = "  ") -> str:
		"""Generate TypeScript interface for this model."""
		from .utils import TypeScriptGenerator

		return TypeScriptGenerator.from_model(cls, indent)


class MtgjsonFileBase(PolarsMixin, BaseModel):
	"""Base for all MTGJSON file structures."""

	model_config = {"populate_by_name": True}

	meta: dict[str, str]

	@classmethod
	def make_meta(cls) -> dict[str, str]:
		"""Create meta dict with current date/version."""
		from datetime import date

		# Note: Version would come from package, hardcode for now
		return {"date": date.today().isoformat(), "version": "5.3.0"}

	@classmethod
	def with_meta(cls, data: Any, meta: dict[str, str] | None = None) -> MtgjsonFileBase:
		"""Create file with auto-generated meta if not provided."""
		if meta is None:
			meta = cls.make_meta()
		return cls(meta=meta, data=data)

	def write(self, path: pathlib.Path, pretty: bool = False) -> None:
		"""Write to JSON file."""
		if not POLARS_AVAILABLE or orjson is None:
			raise ImportError("orjson required")
		opts = orjson.OPT_SORT_KEYS | (orjson.OPT_INDENT_2 if pretty else 0)
		with path.open("wb") as f:
			f.write(orjson.dumps(self.model_dump(by_alias=True, exclude_none=True), option=opts))

	@classmethod
	def read(cls, path: pathlib.Path) -> MtgjsonFileBase:
		"""Read from JSON file."""
		if orjson is None:
			raise ImportError("orjson required")
		with path.open("rb") as f:
			return cls.model_validate(orjson.loads(f.read()))

	@classmethod
	def write_all_schemas(cls, output_dir: pathlib.Path, pretty: bool = True) -> list[pathlib.Path]:
		"""Write JSON schemas for this file type and all nested models.

		Args:
		    output_dir: Directory to write schema files
		    pretty: Pretty-print with indentation

		Returns:
		    List of written schema file paths
		"""
		output_dir.mkdir(parents=True, exist_ok=True)
		written = []

		# Write main schema
		schema = cls.json_schema(by_alias=True)
		main_path = output_dir / f"{cls.__name__}.schema.json"
		cls.write_json_schema(main_path, pretty=pretty)
		written.append(main_path)

		# Extract and write $defs as separate files
		defs = schema.get("$defs", {})
		for name, definition in defs.items():
			def_path = output_dir / f"{name}.schema.json"
			standalone = {
				"$schema": "https://json-schema.org/draft/2020-12/schema",
				"$id": f"https://mtgjson.com/schemas/{name}.schema.json",
				**definition,
			}
			if orjson is not None:
				opts = orjson.OPT_SORT_KEYS
				if pretty:
					opts |= orjson.OPT_INDENT_2
				with def_path.open("wb") as f:
					f.write(orjson.dumps(standalone, option=opts))
			else:
				import json

				with def_path.open("w", encoding="utf-8") as f:
					json.dump(standalone, f, indent=2 if pretty else None, sort_keys=True)
			written.append(def_path)

		return written


class RecordFileBase(MtgjsonFileBase):
	"""Base for files with data: Record<string, T> structure."""

	@classmethod
	def from_items(cls, items: dict[str, Any], meta: dict[str, str] | None = None) -> RecordFileBase:
		return cls.with_meta(items, meta)


class ListFileBase(MtgjsonFileBase):
	"""Base for files with data: T[] structure."""

	@classmethod
	def from_items(cls, items: list[Any], meta: dict[str, str] | None = None) -> ListFileBase:
		return cls.with_meta(items, meta)
