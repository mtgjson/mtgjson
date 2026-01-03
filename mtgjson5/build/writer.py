"""Unified output writer dispatching to format-specific builders."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import LOGGER

from .context import AssemblyContext
from .formats import CSVBuilder, JsonOutputBuilder, ParquetBuilder, PostgresBuilder, SQLiteBuilder


if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext


FormatType = Literal["json", "sqlite", "sql", "psql", "csv", "parquet"]


class UnifiedOutputWriter:
    """
    Unified output writer that dispatches to format-specific builders.

    Usage:
        # From AssemblyContext (preferred)
        ctx = AssemblyContext.from_pipeline(pipeline_ctx)
        writer = UnifiedOutputWriter(ctx)
        writer.write("json")
        writer.write_all(["json", "sqlite", "parquet"])

        # From PipelineContext (convenience wrapper)
        writer = UnifiedOutputWriter.from_pipeline(pipeline_ctx)
        writer.write_all()
    """

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    @classmethod
    def from_pipeline(cls, ctx: PipelineContext) -> UnifiedOutputWriter:
        """Create writer from PipelineContext."""
        assembly_ctx = AssemblyContext.from_pipeline(ctx)
        return cls(assembly_ctx)

    @classmethod
    def from_cache(cls, cache_dir: Path | None = None) -> UnifiedOutputWriter | None:
        """Create writer from cached assembly context."""
        assembly_ctx = AssemblyContext.from_cache(cache_dir)
        if assembly_ctx is None:
            return None
        return cls(assembly_ctx)

    def write(self, format_type: FormatType) -> Path | None:
        """
        Write output in the specified format.

        Args:
            format_type: One of "json", "sqlite", "sql", "psql", "csv", "parquet"

        Returns:
            Path to the written output, or None if failed
        """
        LOGGER.info(f"Writing {format_type} format...")

        try:
            if format_type == "json":
                builder = JsonOutputBuilder(self.ctx)
                builder.write_all(self.ctx.output_path)
                return self.ctx.output_path / "AllPrintings.json"

            elif format_type == "sqlite":
                builder = SQLiteBuilder(self.ctx)
                return builder.write()

            elif format_type == "sql":
                builder = SQLiteBuilder(self.ctx)
                return builder.write_text_dump()

            elif format_type == "psql":
                builder = PostgresBuilder(self.ctx)
                return builder.write()

            elif format_type == "csv":
                builder = CSVBuilder(self.ctx)
                return builder.write()

            elif format_type == "parquet":
                builder = ParquetBuilder(self.ctx)
                return builder.write()

            else:
                LOGGER.error(f"Unknown format: {format_type}")
                return None

        except Exception as e:
            LOGGER.error(f"Failed to write {format_type}: {e}")
            return None

    def write_all(self, formats: list[FormatType] | None = None) -> dict[str, Path | None]:
        """
        Write output in multiple formats.

        Args:
            formats: List of format types. If None, writes all formats.

        Returns:
            Dict mapping format name to output path (or None if failed)
        """
        if formats is None:
            formats = ["json", "sqlite", "psql", "csv", "parquet"]

        results: dict[str, Path | None] = {}
        for fmt in formats:
            results[fmt] = self.write(fmt)

        return results


class OutputWriter:
    """
    Legacy output writer for backwards compatibility.

    Use UnifiedOutputWriter for new code.
    """

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.output_path = MtgjsonConfig().output_path
        self._formats: list[str] = []

    @classmethod
    def from_args(cls, ctx: PipelineContext) -> OutputWriter:
        """Create writer with formats from ctx.args.export."""
        writer = cls(ctx)
        if ctx.args:
            formats = getattr(ctx.args, "export", None) or []
            writer._formats = [f.lower() for f in formats]
        return writer

    def write(self, format_name: str) -> Path | None:
        """Write a single export format."""
        assembly_ctx = AssemblyContext.from_pipeline(self.ctx)
        unified = UnifiedOutputWriter(assembly_ctx)
        return unified.write(format_name)  # type: ignore[arg-type]

    def write_all(self, formats: list[str] | None = None) -> dict[str, Path | None]:
        """Write multiple export formats."""
        formats = formats or self._formats
        if not formats:
            LOGGER.info("No formats to write")
            return {}

        assembly_ctx = AssemblyContext.from_pipeline(self.ctx)
        unified = UnifiedOutputWriter(assembly_ctx)
        return unified.write_all(formats)  # type: ignore[arg-type]
