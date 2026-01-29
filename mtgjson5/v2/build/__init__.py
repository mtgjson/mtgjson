"""MTGJSON Output Writer System."""

from .assemble import (
    AtomicCardsAssembler,
    DeckAssembler,
    DeckListAssembler,
    SetAssembler,
    SetListAssembler,
    TableAssembler,
)
from .context import AssemblyContext
from .writer import (
    OutputWriter,
    UnifiedOutputWriter,
    assemble_json_outputs,
    assemble_with_models,
)


__all__ = [
    "AssemblyContext",
    "AtomicCardsAssembler",
    "DeckAssembler",
    "DeckListAssembler",
    "OutputWriter",
    "SetAssembler",
    "SetListAssembler",
    "TableAssembler",
    "UnifiedOutputWriter",
    "assemble_json_outputs",
    "assemble_with_models",
]
