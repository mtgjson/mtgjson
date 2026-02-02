"""Unified output writer dispatching to format-specific builders."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Union

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import LOGGER

from .context import AssemblyContext
from .formats import CSVBuilder, JsonOutputBuilder, ParquetBuilder, PostgresBuilder, SQLiteBuilder

# Type alias for all format builders
FormatBuilder = Union[JsonOutputBuilder, SQLiteBuilder, PostgresBuilder, CSVBuilder, ParquetBuilder]


if TYPE_CHECKING:
    from mtgjson5.v2.data import PipelineContext


FormatType = Literal["json", "sqlite", "sql", "psql", "csv", "parquet"]


# -----------------------------------------------------------------------------
# Entry point functions (formerly in bridge.py)
# -----------------------------------------------------------------------------


def assemble_with_models(
    ctx: PipelineContext,
    streaming: bool = True,
    set_codes: list[str] | None = None,
    outputs: set[str] | None = None,
    pretty: bool = False,
    sets_only: bool = False,
) -> dict[str, int]:
    """
    Assemble MTGJSON outputs using the model-based approach.

    Uses Pydantic models for validation and serialization.
    This is the preferred path for new builds.

    Args:
        ctx: PipelineContext with processed card data
        streaming: Use streaming writes for large files
        set_codes: Optional filter for specific set codes
        outputs: Optional set of output types to build (e.g., {"AllPrintings"}).
                If None or empty, builds all outputs.
        pretty: Pretty-print JSON output with indentation
        sets_only: When True, only build individual set files (skip compiled outputs
                  unless explicitly listed in ``outputs``)

    Returns:
        Dict mapping output file names to record counts
    """
    assembly_ctx = AssemblyContext.from_pipeline(ctx)
    assembly_ctx.pretty = pretty
    assembly_ctx.save_cache()

    json_builder = JsonOutputBuilder(assembly_ctx)
    results = json_builder.write_all(
        output_dir=assembly_ctx.output_path,
        set_codes=set_codes,
        streaming=streaming,
        include_decks=True,
        outputs=outputs,
        sets_only=sets_only,
    )

    LOGGER.info(f"Model assembly complete: {sum(results.values())} total records")
    return results


def assemble_json_outputs(
    ctx: PipelineContext,
    include_referrals: bool = False,
    parallel: bool = True,
    max_workers: int = 30,
    set_codes: list[str] | None = None,
    pretty: bool = False,
    sets_only: bool = False,
) -> dict[str, int]:
    """
    Assemble MTGJSON JSON outputs from pipeline context.

    This function bridges the pipeline processing stage to the output
    generation stage. It creates an AssemblyContext and uses the
    JsonOutputBuilder to generate all standard outputs.

    Args:
        ctx: PipelineContext with processed card data
        include_referrals: Include referral URL processing (not yet implemented)
        parallel: Use parallel processing for set file generation
        max_workers: Maximum parallel workers
        set_codes: Optional filter for specific set codes
        pretty: Pretty-print JSON output with indentation
        sets_only: When True, only build individual set files (skip compiled outputs)

    Returns:
        Dict mapping output file names to record counts
    """
    LOGGER.info("Assembling JSON outputs from pipeline...")

    assembly_ctx = AssemblyContext.from_pipeline(ctx)
    assembly_ctx.pretty = pretty
    assembly_ctx.save_cache()

    output_path = assembly_ctx.output_path
    output_path.mkdir(parents=True, exist_ok=True)

    results: dict[str, int] = {}

    # Build individual set files
    LOGGER.info("Building individual set files...")
    codes_to_build = set_codes or sorted(assembly_ctx.set_meta.keys())
    valid_codes = [c for c in codes_to_build if c in assembly_ctx.set_meta]

    if parallel and len(valid_codes) > 1:
        results["sets"] = _write_sets_parallel(
            assembly_ctx, valid_codes, output_path, max_workers
        )
    else:
        results["sets"] = _write_sets_sequential(assembly_ctx, valid_codes, output_path)

    if sets_only:
        LOGGER.info(
            f"Sets-only mode: built {results.get('sets', 0)} set files, "
            "skipping compiled outputs"
        )
        return results

    json_builder = JsonOutputBuilder(assembly_ctx)

    # Build AllPrintings.json (streaming)
    LOGGER.info("Building AllPrintings.json...")
    all_printings_path = output_path / "AllPrintings.json"
    count = json_builder.write_all_printings(
        all_printings_path,
        set_codes=set_codes,
        streaming=True,
    )
    results["AllPrintings"] = (
        count
        if isinstance(count, int)
        else len(count.data)  # pylint: disable=no-member
    )

    # Build AtomicCards.json
    LOGGER.info("Building AtomicCards.json...")
    atomic = json_builder.write_atomic_cards(output_path / "AtomicCards.json")
    results["AtomicCards"] = len(atomic.data)

    # Build SetList.json
    LOGGER.info("Building SetList.json...")
    set_list = json_builder.write_set_list(output_path / "SetList.json")
    results["SetList"] = len(set_list.data)

    # Build deck files
    LOGGER.info("Building deck files...")
    deck_count = json_builder.write_decks(output_path / "decks", set_codes=set_codes)
    results["decks"] = deck_count

    deck_list = assembly_ctx.deck_list.build()
    from mtgjson5.v2.models.files import DeckListFile

    deck_list_file = DeckListFile.with_meta(deck_list, assembly_ctx.meta)
    deck_list_file.write(output_path / "DeckList.json", pretty=assembly_ctx.pretty)
    results["DeckList"] = len(deck_list)

    if include_referrals:
        LOGGER.info("Referral processing requested but delegated to legacy path")

    LOGGER.info(f"JSON assembly complete: {sum(results.values())} total records")
    return results


def _write_sets_parallel(
    ctx: AssemblyContext,
    set_codes: list[str],
    output_path: Path,
    max_workers: int,
) -> int:
    """Write individual set files in parallel."""
    from mtgjson5.v2.models.files import IndividualSetFile

    def write_one(code: str) -> bool:
        try:
            set_data = ctx.sets.build(code)
            single = IndividualSetFile.from_set_data(set_data, ctx.meta)
            single.write(output_path / f"{code}.json", pretty=ctx.pretty)
            return True
        except Exception as e:
            LOGGER.error(f"Failed to write set {code}: {e}")
            return False

    count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(write_one, code): code for code in set_codes}
        for future in as_completed(futures):
            if future.result():
                count += 1

    return count


def _write_sets_sequential(
    ctx: AssemblyContext,
    set_codes: list[str],
    output_path: Path,
) -> int:
    """Write individual set files sequentially."""
    from mtgjson5.v2.models.files import IndividualSetFile

    count = 0
    for code in set_codes:
        try:
            set_data = ctx.sets.build(code)
            single = IndividualSetFile.from_set_data(set_data, ctx.meta)
            single.write(output_path / f"{code}.json", pretty=ctx.pretty)
            count += 1
        except Exception as e:
            LOGGER.error(f"Failed to write set {code}: {e}")

    return count


# -----------------------------------------------------------------------------
# Writer classes
# -----------------------------------------------------------------------------


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
                json_builder = JsonOutputBuilder(self.ctx)
                json_builder.write_all(self.ctx.output_path)
                return self.ctx.output_path / "AllPrintings.json"

            elif format_type == "sqlite":
                sqlite_builder = SQLiteBuilder(self.ctx)
                return sqlite_builder.write()

            elif format_type == "sql":
                sql_builder = SQLiteBuilder(self.ctx)
                return sql_builder.write_text_dump()

            elif format_type == "psql":
                psql_builder = PostgresBuilder(self.ctx)
                return psql_builder.write()

            elif format_type == "csv":
                csv_builder = CSVBuilder(self.ctx)
                return csv_builder.write()

            elif format_type == "parquet":
                parquet_builder = ParquetBuilder(self.ctx)
                return parquet_builder.write()

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
        if self.ctx.args:
            assembly_ctx.pretty = getattr(self.ctx.args, "pretty", False)
        unified = UnifiedOutputWriter(assembly_ctx)
        return unified.write(format_name)  # type: ignore[arg-type]

    def write_all(self, formats: list[str] | None = None) -> dict[str, Path | None]:
        """Write multiple export formats."""
        formats = formats or self._formats
        if not formats:
            LOGGER.info("No formats to write")
            return {}

        assembly_ctx = AssemblyContext.from_pipeline(self.ctx)
        if self.ctx.args:
            assembly_ctx.pretty = getattr(self.ctx.args, "pretty", False)
        unified = UnifiedOutputWriter(assembly_ctx)
        return unified.write_all(formats)  # type: ignore[arg-type]
