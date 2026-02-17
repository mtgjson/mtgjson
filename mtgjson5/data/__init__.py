"""
MTGJSON V2 Data Layer.

GlobalCache for loading provider data, PipelineContext for transformation state.
"""

from .cache import GLOBAL_CACHE, GlobalCache
from .context import PipelineContext

__all__ = [
    "GLOBAL_CACHE",
    "GlobalCache",
    "PipelineContext",
]
