"""
MTGJSON Pipeline Utilities.

Schema validation, safe operations, and vectorized expressions
for the card transformation pipeline.
"""

from __future__ import annotations

from mtgjson5.pipeline.expressions import (
	ascii_name_expr,
	calculate_cmc_expr,
	extract_colors_from_mana_expr,
	extract_mana_symbols_expr,
	filter_keywords_expr,
	filter_keywords_join,
	order_finishes_expr,
)
from mtgjson5.pipeline.lookups import (
	add_meld_other_face_ids,
)
from mtgjson5.pipeline.safe_ops import (
	require_columns,
	safe_drop,
	safe_rename,
	safe_struct_field,
)
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
	# Vectorized expressions
	"order_finishes_expr",
	"extract_mana_symbols_expr",
	"calculate_cmc_expr",
	"extract_colors_from_mana_expr",
	"filter_keywords_expr",
	"filter_keywords_join",
	"ascii_name_expr",
	# Lookups
	"add_meld_other_face_ids",
]
