"""
Pipeline schema validation.

Validates DataFrame schemas at pipeline stage boundaries to catch
issues early rather than producing corrupt output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
	from polars import LazyFrame


class PipelineValidationError(Exception):
	"""Raised when pipeline validation fails."""

	def __init__(
		self,
		stage: str,
		message: str,
		missing_columns: set[str] | None = None,
		type_mismatches: dict[str, tuple[pl.DataType, pl.DataType]] | None = None,
	):
		self.stage = stage
		self.missing_columns = missing_columns or set()
		self.type_mismatches = type_mismatches or {}
		super().__init__(f"[{stage}] {message}")


@dataclass
class ColumnSpec:
	"""Specification for a required column."""

	name: str
	dtype: pl.DataType | None = None  # None = any type
	nullable: bool = True


@dataclass
class StageSchema:
	"""Schema requirements for a pipeline stage."""

	name: str
	required: list[ColumnSpec] = field(default_factory=list)
	forbidden: set[str] = field(default_factory=set)  # Columns that should be dropped by now

	def validate(self, lf: LazyFrame) -> list[str]:
		"""
		Validate LazyFrame against this schema.

		Returns list of warning messages (empty if valid).
		Raises PipelineValidationError for critical failures.
		"""
		schema = lf.collect_schema()
		existing = set(schema.names())
		warnings = []

		# Check required columns
		missing = set()
		type_mismatches = {}

		for spec in self.required:
			if spec.name not in existing:
				missing.add(spec.name)
			elif spec.dtype is not None:
				actual = schema[spec.name]
				if actual != spec.dtype:
					# Allow compatible types
					if not self._types_compatible(actual, spec.dtype):
						type_mismatches[spec.name] = (spec.dtype, actual)

		if missing:
			raise PipelineValidationError(
				self.name,
				f"Missing required columns: {missing}",
				missing_columns=missing,
			)

		if type_mismatches:
			msg = ", ".join(f"{k}: expected {v[0]}, got {v[1]}" for k, v in type_mismatches.items())
			raise PipelineValidationError(
				self.name,
				f"Type mismatches: {msg}",
				type_mismatches=type_mismatches,
			)

		# Warn about forbidden columns still present
		still_present = self.forbidden & existing
		if still_present:
			warnings.append(f"Columns should be dropped by {self.name}: {still_present}")

		return warnings

	@staticmethod
	def _types_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
		"""Check if types are compatible (e.g., Int64 vs Int32)."""
		# Same type
		if actual == expected:
			return True
		# Numeric compatibility
		if actual.is_numeric() and expected.is_numeric():
			return True
		# String compatibility
		if actual in (pl.String, pl.Utf8) and expected in (pl.String, pl.Utf8):
			return True
		return False


# =============================================================================
# Pre-defined Stage Schemas
# =============================================================================

STAGE_POST_EXPLODE = StageSchema(
	name="post_explode_card_faces",
	required=[
		ColumnSpec("_row_id", pl.UInt32),
		ColumnSpec("faceId", pl.Int64),
		ColumnSpec("side", pl.String),
		ColumnSpec("_face_data"),  # Struct type varies
	],
)

STAGE_POST_BASIC_FIELDS = StageSchema(
	name="post_basic_fields",
	required=[
		ColumnSpec("name", pl.String),
		ColumnSpec("setCode", pl.String),
		ColumnSpec("number", pl.String),
		ColumnSpec("scryfallId", pl.String),
		ColumnSpec("language", pl.String),
		ColumnSpec("type", pl.String),
		ColumnSpec("manaValue", pl.Float64),
	],
	forbidden={"lang", "frame", "cmc", "typeLine"},
)

STAGE_POST_IDENTIFIERS = StageSchema(
	name="post_identifiers",
	required=[
		ColumnSpec("identifiers"),  # Struct
		ColumnSpec("uuid", pl.String),
	],
	forbidden={"illustrationId", "arenaId", "mtgoId", "tcgplayerId"},
)

STAGE_PRE_SINK = StageSchema(
	name="pre_sink",
	required=[
		ColumnSpec("uuid", pl.String),
		ColumnSpec("name", pl.String),
		ColumnSpec("setCode", pl.String),
	],
	forbidden={
		# All temp columns should be dropped
		"_row_id",
		"_face_data",
		"_all_keywords",
		"_all_parts",
		"_meld_face_name",
		"_cardReleasedAt",
		"_setReleaseDate",
	},
)


def validate_stage(
	lf: LazyFrame,
	stage: StageSchema,
	*,
	strict: bool = True,
) -> LazyFrame:
	"""
	Validate LazyFrame at pipeline stage.

	Args:
	    lf: LazyFrame to validate
	    stage: Stage schema to validate against
	    strict: If True, raise on warnings; if False, log warnings

	Returns:
	    The input LazyFrame (unchanged)

	Raises:
	    PipelineValidationError: If validation fails
	"""
	from mtgjson5.utils import LOGGER

	warnings = stage.validate(lf)

	if warnings:
		if strict:
			raise PipelineValidationError(stage.name, "; ".join(warnings))
		for w in warnings:
			LOGGER.warning(f"[{stage.name}] {w}")

	return lf


__all__ = [
	"PipelineValidationError",
	"ColumnSpec",
	"StageSchema",
	"validate_stage",
	"STAGE_POST_EXPLODE",
	"STAGE_POST_BASIC_FIELDS",
	"STAGE_POST_IDENTIFIERS",
	"STAGE_PRE_SINK",
]
