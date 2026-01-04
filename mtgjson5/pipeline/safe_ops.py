"""
Safe DataFrame operations with explicit error handling.

Replaces silent strict=False patterns with explicit validation and logging.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
	from polars import LazyFrame

from mtgjson5.utils import LOGGER


def safe_drop(
	lf: LazyFrame,
	columns: list[str] | set[str],
	*,
	warn_missing: bool = True,
) -> LazyFrame:
	"""
	Drop columns that exist, optionally warn about missing.

	Unlike df.drop(strict=False), this explicitly tracks what's missing.

	Args:
	    lf: LazyFrame to modify
	    columns: Column names to drop
	    warn_missing: If True, log missing columns at DEBUG level

	Returns:
	    LazyFrame with existing columns dropped
	"""
	schema = lf.collect_schema()
	existing = set(schema.names())

	to_drop = set(columns) & existing
	missing = set(columns) - existing

	if missing and warn_missing:
		LOGGER.debug(f"safe_drop: columns not present: {missing}")

	if to_drop:
		return lf.drop(list(to_drop))
	return lf


def safe_rename(
	lf: LazyFrame,
	mapping: dict[str, str],
	*,
	warn_missing: bool = True,
) -> LazyFrame:
	"""
	Rename columns that exist, optionally warn about missing.

	Unlike df.rename(strict=False), this explicitly tracks what's missing.

	Args:
	    lf: LazyFrame to modify
	    mapping: Dict of old_name -> new_name
	    warn_missing: If True, log missing columns at DEBUG level

	Returns:
	    LazyFrame with existing columns renamed
	"""
	schema = lf.collect_schema()
	existing = set(schema.names())

	valid_renames = {k: v for k, v in mapping.items() if k in existing}
	missing = set(mapping.keys()) - existing

	if missing and warn_missing:
		LOGGER.debug(f"safe_rename: columns not present: {missing}")

	if valid_renames:
		return lf.rename(valid_renames)
	return lf


def safe_struct_field(
	col: str,
	field_name: str,
	*,
	default: pl.Expr | None = None,
) -> pl.Expr:
	"""
	Access struct field with fallback if field doesn't exist.

	Args:
	    col: Struct column name
	    field_name: Field name within struct
	    default: Default expression if field missing (None = null)

	Returns:
	    Expression that safely accesses the field
	"""
	if default is None:
		default = pl.lit(None)

	# Use when().then() pattern since struct.field() errors on missing
	return (
		pl.when(pl.col(col).is_not_null())
		.then(pl.col(col).struct.field(field_name))
		.otherwise(default)
	)


def require_columns(
	lf: LazyFrame,
	columns: set[str],
	context: str = "",
) -> LazyFrame:
	"""
	Assert columns exist, raise informative error if not.

	Args:
	    lf: LazyFrame to check
	    columns: Required column names
	    context: Description for error message

	Returns:
	    The input LazyFrame (unchanged)

	Raises:
	    ValueError: If any columns missing
	"""
	schema = lf.collect_schema()
	existing = set(schema.names())
	missing = columns - existing

	if missing:
		ctx = f" ({context})" if context else ""
		raise ValueError(f"Missing required columns{ctx}: {missing}")

	return lf


__all__ = [
	"safe_drop",
	"safe_rename",
	"safe_struct_field",
	"require_columns",
]
