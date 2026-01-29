"""
MTGJSON v2 Pipeline Module.

This module contains the modern Polars-based pipeline for MTGJSON,
including data providers, cache, context, pipeline transforms, and build system.

Key components:
- data/: GlobalCache and PipelineContext for loading and transforming data
- pipeline/: Card transformation logic (build_cards)
- build/: Assembly and output generation
- providers/: V2 async data providers
- models/: Pydantic models for cards, sets, tokens, etc.
"""

from .data import GLOBAL_CACHE, GlobalCache, PipelineContext

__all__ = [
    "GLOBAL_CACHE",
    "GlobalCache",
    "PipelineContext",
]
