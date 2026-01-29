"""
MTGJSON Pipeline Utilities.

Schema validation, safe operations, and vectorized expressions
for the card transformation pipeline.
"""

from __future__ import annotations

from mtgjson5.v2.build.writer import (
    assemble_json_outputs,
    assemble_with_models,
)
from mtgjson5.v2.pipeline.core import (
    build_cards,
    build_expanded_decks_df,
    build_sealed_products_lf,
    build_set_metadata_df,
)
from mtgjson5.v2.pipeline.expressions import (
    calculate_cmc_expr,
    extract_colors_from_mana_expr,
    order_finishes_expr,
    sort_colors_wubrg_expr,
)
from mtgjson5.v2.pipeline.lookups import (
    add_meld_other_face_ids,
)
from mtgjson5.v2.pipeline.safe_ops import (
    require_columns,
    safe_drop,
    safe_rename,
    safe_struct_field,
)
from mtgjson5.v2.pipeline.validation import (
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
    "assemble_json_outputs",
    "assemble_with_models",
    "build_cards",
    "build_expanded_decks_df",
    "build_sealed_products_lf",
    "build_set_metadata_df",
    "calculate_cmc_expr",
    "extract_colors_from_mana_expr",
    "order_finishes_expr",
    "sort_colors_wubrg_expr",
    "require_columns",
    "safe_drop",
    "safe_rename",
    "safe_struct_field",
    "validate_stage",
]
