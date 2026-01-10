"""
MTGJSON Pipeline Utilities.

Schema validation, safe operations, and vectorized expressions
for the card transformation pipeline.
"""

from __future__ import annotations

from mtgjson5.pipeline.bridge import (
	assemble_from_cache,
	assemble_json_outputs,
	assemble_with_models,
	write_all_formats,
)
from mtgjson5.pipeline.core import (
	build_sealed_products_lf,
	build_set_metadata_df,
)
from mtgjson5.pipeline.expressions import (
	calculate_cmc_expr,
	extract_colors_from_mana_expr,
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
	"STAGE_POST_BASIC_FIELDS",
	"STAGE_POST_EXPLODE",
	"STAGE_POST_IDENTIFIERS",
	"STAGE_PRE_SINK",
	"ColumnSpec",
	"PipelineValidationError",
	"StageSchema",
	"add_meld_other_face_ids",
	"assemble_from_cache",
	"assemble_json_outputs",
	"assemble_with_models",
	"build_sealed_products_lf",
	"build_set_metadata_df",
	"calculate_cmc_expr",
	"extract_colors_from_mana_expr",
	"order_finishes_expr",
	"require_columns",
	"safe_drop",
	"safe_rename",
	"safe_struct_field",
	"validate_stage",
	"write_all_formats",
]
