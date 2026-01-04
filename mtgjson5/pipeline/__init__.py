"""
MTGJSON Pipeline Utilities.

Schema validation and safe operations for the card transformation pipeline.
"""

from __future__ import annotations

from mtgjson5.pipeline.validation import (
	STAGE_POST_BASIC_FIELDS,
	STAGE_POST_EXPLODE,
	STAGE_POST_IDENTIFIERS,
	STAGE_PRE_SINK,
	ColumnSpec,
	PipelineValidationError,
	StageSchema,
	validate_stage,
)
from mtgjson5.pipeline.safe_ops import (
	require_columns,
	safe_drop,
	safe_rename,
	safe_struct_field,
)

__all__ = [
	# Validation
	"PipelineValidationError",
	"ColumnSpec",
	"StageSchema",
	"validate_stage",
	"STAGE_POST_EXPLODE",
	"STAGE_POST_BASIC_FIELDS",
	"STAGE_POST_IDENTIFIERS",
	"STAGE_PRE_SINK",
	# Safe operations
	"safe_drop",
	"safe_rename",
	"safe_struct_field",
	"require_columns",
]
