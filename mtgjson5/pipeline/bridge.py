"""
Bridge between pipeline processing and output generation.

Provides functions to assemble JSON outputs from PipelineContext using
the build/* modules. These are the main entry points called from __main__.py.
"""

from __future__ import annotations

import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from mtgjson5.build import AssemblyContext, UnifiedOutputWriter
from mtgjson5.build.formats import JsonOutputBuilder
from mtgjson5.utils import LOGGER


if TYPE_CHECKING:
	from mtgjson5.context import PipelineContext


def assemble_with_models(
	ctx: PipelineContext,
	streaming: bool = True,
	set_codes: list[str] | None = None,
	outputs: set[str] | None = None,
	pretty: bool = False,
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

	Returns:
	    Dict mapping output file names to record counts
	"""
	LOGGER.info("Assembling outputs with model-based approach...")

	# Build assembly context from pipeline
	assembly_ctx = AssemblyContext.from_pipeline(ctx)
	assembly_ctx.pretty = pretty

	# Cache the context for fast rebuilds
	assembly_ctx.save_cache()

	# Build JSON outputs
	json_builder = JsonOutputBuilder(assembly_ctx)
	results = json_builder.write_all(
		output_dir=assembly_ctx.output_path,
		set_codes=set_codes,
		streaming=streaming,
		include_decks=True,
		outputs=outputs,
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

	Returns:
	    Dict mapping output file names to record counts
	"""
	LOGGER.info("Assembling JSON outputs from pipeline...")

	# Build assembly context from pipeline
	assembly_ctx = AssemblyContext.from_pipeline(ctx)
	assembly_ctx.pretty = pretty

	# Cache for fast rebuilds
	assembly_ctx.save_cache()

	output_path = assembly_ctx.output_path
	output_path.mkdir(parents=True, exist_ok=True)

	results: dict[str, int] = {}
	json_builder = JsonOutputBuilder(assembly_ctx)

	# Build AllPrintings.json (streaming)
	LOGGER.info("Building AllPrintings.json...")
	all_printings_path = output_path / "AllPrintings.json"
	count = json_builder.write_all_printings(
		all_printings_path,
		set_codes=set_codes,
		streaming=True,
	)
	results["AllPrintings"] = count if isinstance(count, int) else len(count.data)

	# Build AtomicCards.json
	LOGGER.info("Building AtomicCards.json...")
	atomic = json_builder.write_atomic_cards(output_path / "AtomicCards.json")
	results["AtomicCards"] = len(atomic.data)

	# Build SetList.json
	LOGGER.info("Building SetList.json...")
	set_list = json_builder.write_set_list(output_path / "SetList.json")
	results["SetList"] = len(set_list.data)

	# Build individual set files
	LOGGER.info("Building individual set files...")
	codes_to_build = set_codes or sorted(assembly_ctx.set_meta.keys())
	valid_codes = [c for c in codes_to_build if c in assembly_ctx.set_meta]

	if parallel and len(valid_codes) > 1:
		results["sets"] = _write_sets_parallel(assembly_ctx, valid_codes, output_path, max_workers)
	else:
		results["sets"] = _write_sets_sequential(assembly_ctx, valid_codes, output_path)

	# Build deck files
	LOGGER.info("Building deck files...")
	deck_count = json_builder.write_decks(output_path / "decks", set_codes=set_codes)
	results["decks"] = deck_count

	deck_list = assembly_ctx.deck_list.build()
	from mtgjson5.mtgjson_models.files import DeckListFile

	deck_list_file = DeckListFile.with_meta(deck_list, assembly_ctx.meta)
	deck_list_file.write(output_path / "DeckList.json")
	results["DeckList"] = len(deck_list)

	# Handle referrals if requested
	if include_referrals:
		LOGGER.info("Referral processing requested but delegated to legacy path")

	LOGGER.info(f"JSON assembly complete: {sum(results.values())} total records")
	return results


def _write_sets_parallel(
	ctx: AssemblyContext,
	set_codes: list[str],
	output_path: pathlib.Path,
	max_workers: int,
) -> int:
	"""Write individual set files in parallel."""
	from mtgjson5.mtgjson_models.files import IndividualSetFile

	def write_one(code: str) -> bool:
		try:
			set_data = ctx.sets.build(code)
			single = IndividualSetFile.from_set_data(set_data, ctx.meta)
			single.write(output_path / f"{code}.json")
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
	output_path: pathlib.Path,
) -> int:
	"""Write individual set files sequentially."""
	from mtgjson5.mtgjson_models.files import IndividualSetFile

	count = 0
	for code in set_codes:
		try:
			set_data = ctx.sets.build(code)
			single = IndividualSetFile.from_set_data(set_data, ctx.meta)
			single.write(output_path / f"{code}.json")
			count += 1
		except Exception as e:
			LOGGER.error(f"Failed to write set {code}: {e}")

	return count


def write_all_formats(
	ctx: PipelineContext,
	formats: list[str] | None = None,
) -> dict[str, pathlib.Path | None]:
	"""
	Write outputs in multiple formats (json, sqlite, csv, parquet, etc).

	Args:
	    ctx: PipelineContext with processed card data
	    formats: List of format names. If None, writes all formats.

	Returns:
	    Dict mapping format name to output path (or None if failed)
	"""
	writer = UnifiedOutputWriter.from_pipeline(ctx)
	return writer.write_all(formats)  # type: ignore[arg-type]


def assemble_from_cache(
	output_path: pathlib.Path | None = None,
	formats: list[str] | None = None,
) -> dict[str, Any]:
	"""
	Assemble outputs from cached assembly context.

	Fast path for regenerating outputs without re-running the pipeline.
	Requires a previous successful build that saved the assembly cache.

	Args:
	    output_path: Override output directory
	    formats: List of format names to generate

	Returns:
	    Dict with results per format
	"""
	assembly_ctx = AssemblyContext.from_cache()
	if assembly_ctx is None:
		raise ValueError("No cached assembly context found. Run full build first.")

	if output_path:
		assembly_ctx.output_path = output_path

	writer = UnifiedOutputWriter(assembly_ctx)

	if formats is None:
		formats = ["json"]

	return writer.write_all(formats)  # type: ignore[arg-type]
