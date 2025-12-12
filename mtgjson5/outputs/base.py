"""Base classes for MTGJSON output system."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext

ExportFormatType = Literal["json", "sql", "sqlite", "psql", "csv", "parquet"]
OutputTypeStr = Literal["atomic", "deck", "set"]

EXPORT_FORMAT_REGISTRY: dict[str, type["ExportFormat"]] = {}
OUTPUT_TYPE_REGISTRY: dict[str, type["OutputType"]] = {}


def register_export_format(cls: type["ExportFormat"]) -> type["ExportFormat"]:
    """Decorator to register an export format class."""
    if hasattr(cls, "NAME"):
        EXPORT_FORMAT_REGISTRY[cls.NAME.lower()] = cls
    return cls


def register_output_type(cls: type["OutputType"]) -> type["OutputType"]:
    """Decorator to register an output type class."""
    if hasattr(cls, "NAME"):
        OUTPUT_TYPE_REGISTRY[cls.NAME.lower()] = cls
    return cls


class ExportFormat(ABC):
    """
    Base class for export formats (JSON, SQL, CSV, etc.).

    Subclasses must implement the write() method.
    """

    NAME: ClassVar[str] = ""
    FILE_NAME: ClassVar[str] = ""

    @abstractmethod
    def write(self, ctx: "PipelineContext", output_path: Path) -> Path | None:
        """
        Write this export format.

        Args:
            ctx: Pipeline context containing data
            output_path: Directory to write output files

        Returns:
            Path to written file, or None if failed
        """
        raise NotImplementedError


class OutputType(ABC):
    """
    Base class for output types (Atomic, Deck, Set).

    Subclasses must implement the build() method.
    """

    NAME: ClassVar[str] = ""

    @abstractmethod
    def build(self, ctx: "PipelineContext") -> Path | None:
        """
        Build this output type.

        Args:
            ctx: Pipeline context containing data

        Returns:
            Path to built output, or None if failed
        """
        raise NotImplementedError


class ExportSchema:
    """
    Base class for export format schemas.

    Mirrors the CardSchema pattern - each format is defined by class attributes.
    """

    FORMAT: ClassVar[ExportFormatType] = "json"
    FILE_NAME: ClassVar[str] = "AllPrintings.json"


class JsonSchema(ExportSchema):
    """AllPrintings.json export."""

    FORMAT: ClassVar[ExportFormatType] = "json"
    FILE_NAME: ClassVar[str] = "AllPrintings.json"

class SqlSchema(ExportSchema):
    """SQLite text dump export."""

    FORMAT: ClassVar[ExportFormatType] = "sql"
    FILE_NAME: ClassVar[str] = "AllPrintings.sql"


class SqliteSchema(ExportSchema):
    """SQLite database export."""

    FORMAT: ClassVar[ExportFormatType] = "sqlite"
    FILE_NAME: ClassVar[str] = "AllPrintings.sqlite"


class PsqlSchema(ExportSchema):
    """PostgreSQL dump export."""

    FORMAT: ClassVar[ExportFormatType] = "psql"
    FILE_NAME: ClassVar[str] = "AllPrintings.psql"


class CsvSchema(ExportSchema):
    """CSV files export."""

    FORMAT: ClassVar[ExportFormatType] = "csv"
    FILE_NAME: ClassVar[str] = "cards.csv"


class ParquetSchema(ExportSchema):
    """Parquet files export."""

    FORMAT: ClassVar[ExportFormatType] = "parquet"
    FILE_NAME: ClassVar[str] = "cards.parquet"


# Schema registry by format name
EXPORT_SCHEMAS: dict[str, type[ExportSchema]] = {
    "json": JsonSchema,
    "sql": SqlSchema,
    "sqlite": SqliteSchema,
    "psql": PsqlSchema,
    "csv": CsvSchema,
    "parquet": ParquetSchema,
}
