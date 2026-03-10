"""
MTGJSON Main Executor
"""

import argparse
import gc
import logging
import traceback
from typing import Any

import requests
import urllib3.exceptions

from mtgjson5.utils import init_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
init_logger()
LOGGER: logging.Logger = logging.getLogger(__name__)

from mtgjson5 import constants
from mtgjson5.data import GlobalCache
from mtgjson5.utils import load_local_set_data

SCRYFALL_SETS_URL = "https://api.scryfall.com/sets/"


def _run_subprocess(
    target: Any,
    args: tuple[Any, ...],
    label: str,
    profile: bool = False,
) -> None:
    """Run a function in an isolated subprocess with error/profile handling.

    Polars' memory allocator (jemalloc) never returns memory to the OS within
    a process. By running each phase in its own child, all allocations are
    fully reclaimed when the child exits.

    Uses the ``spawn`` start method explicitly so Linux doesn't default to
    ``fork`` (which would COW-copy the parent's address space).
    """
    import multiprocessing

    from mtgjson5.utils import get_log_file

    mp_ctx = multiprocessing.get_context("spawn")
    error_queue: multiprocessing.Queue[str] = mp_ctx.Queue()
    profile_queue: multiprocessing.Queue[dict] | None = mp_ctx.Queue() if profile else None

    LOGGER.info("Launching %s subprocess...", label)
    proc = mp_ctx.Process(
        target=target,
        args=(*args, error_queue, get_log_file(), profile, profile_queue),
    )
    proc.start()
    proc.join()

    # Collect subprocess profile before error check
    if profile_queue is not None and not profile_queue.empty():
        from mtgjson5.profiler import get_profiler

        sp_profile = profile_queue.get_nowait()
        get_profiler().add_subprocess_profile(sp_profile)

    # Propagate child errors to parent
    if not error_queue.empty():
        error_msg = error_queue.get_nowait()
        raise RuntimeError(f"{label} subprocess failed:\n{error_msg}")

    if proc.exitcode != 0:
        raise RuntimeError(f"{label} subprocess exited with code {proc.exitcode}")

    LOGGER.info("%s subprocess completed successfully", label)


def get_sets_to_build(args: argparse.Namespace) -> list[str]:
    """
    Determine which sets to build based on CLI args.
    Fetches set codes from Scryfall when building all sets.
    :param args: CLI args
    :return: List of set codes to build, alphabetically sorted
    """
    if args.resume_build:
        from mtgjson5.mtgjson_config import MtgjsonConfig

        # Exclude sets already built
        json_files = list(MtgjsonConfig().output_path.glob("**/*.json"))
        already_built = [f.stem[:-1] if f.stem[:-1] in constants.BAD_FILE_NAMES else f.stem for f in json_files]
        args.skip_sets.extend(already_built)

    if not args.all_sets:
        return sorted(set(args.sets) - set(args.skip_sets))

    # Fetch all set codes from Scryfall
    response = requests.get(SCRYFALL_SETS_URL, timeout=30)
    response.raise_for_status()
    scryfall_sets = [s["code"].upper() for s in response.json().get("data", [])]

    # Remove token sets (Txxx where xxx is also a set)
    scryfall_set_codes = set(scryfall_sets)
    non_token_sets = {s for s in scryfall_set_codes if not (s.startswith("T") and s[1:] in scryfall_set_codes)}

    return sorted(non_token_sets - set(args.skip_sets))


def dispatcher(args: argparse.Namespace) -> None:
    """
    MTGJSON Dispatcher
    """
    # Generate types/docs only (no other build flags)
    generate_types = getattr(args, "generate_types", None) is not None
    generate_docs = getattr(args, "generate_docs", False)
    if (generate_types or generate_docs) and not (args.sets or args.all_sets or args.full_build or args.price_build):
        from mtgjson5.models import write_doc_pages, write_typescript_interfaces
        from mtgjson5.mtgjson_config import MtgjsonConfig

        if generate_types:
            output_path = args.generate_types or str(MtgjsonConfig().output_path / "AllMTGJSONTypes.ts")
            write_typescript_interfaces(output_path)
            LOGGER.info(f"TypeScript definitions written to {output_path}")

        if generate_docs:
            output_dir = str(MtgjsonConfig().output_path)
            written = write_doc_pages(output_dir)
            for p in written:
                LOGGER.info(f"Doc page written to {p}")

        return

    from mtgjson5.build.writer import assemble_json_outputs, assemble_with_models
    from mtgjson5.compress_generator import compress_mtgjson_contents
    from mtgjson5.data import PipelineContext
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler
    from mtgjson5.pipeline.core import build_cards
    from mtgjson5.profiler import init_profiler
    from mtgjson5.utils import generate_output_file_hashes

    use_tracemalloc = getattr(args, "profile_tracemalloc", False)
    profiler = init_profiler(
        enabled=getattr(args, "profile", False) or use_tracemalloc,
        use_tracemalloc=use_tracemalloc,
    )

    sets_to_build = get_sets_to_build(args)

    # Check if only specific outputs or formats requested
    outputs_requested = {o.lower() for o in (args.outputs or [])}
    export_formats = {f.lower() for f in (args.export or [])} if args.export else None

    # Sets-only mode: specific sets requested without --all-sets or --full-build
    # Only builds individual set files, skipping compiled outputs and exports
    # Override with --outputs, --export, or --full-build to include more
    sets_only = bool(args.sets) and not args.all_sets and not args.full_build

    # Load global cache
    # Pass set_codes to filter aggregation computations to only requested sets
    set_filter = None
    if sets_to_build and not args.all_sets:
        # Include token sets (T{code}) for each requested set
        set_filter = []
        for code in sets_to_build:
            set_filter.append(code.upper())
            set_filter.append(f"T{code.upper()}")
        set_filter = sorted(set(set_filter))
    GlobalCache().load_all(
        set_codes=set_filter,
        output_types=outputs_requested,
        export_formats=export_formats,
        skip_mcm=args.skip_mcm,
    )
    profiler.checkpoint("cache_loaded", top_n=10)

    # Start background price fetch (overlaps with pipeline + assembly)
    raw_fetcher = None
    if args.price_build or (export_formats and "parquet" in export_formats) or args.full_build:
        from mtgjson5.build.prices.price_fetcher import PriceFetcher

        raw_fetcher = PriceFetcher.start_background()
        LOGGER.info("Background price raw fetch started")

    if args.all_sets:
        additional_set_keys = set(load_local_set_data().keys())
        additional_set_keys -= set(args.skip_sets)
        sets_to_build = list(set(sets_to_build).union(additional_set_keys))
        args.sets = sorted(sets_to_build)

    decks_only = outputs_requested == {"decks"}

    # Create pipeline context
    ctx = PipelineContext.from_global_cache(args=args)
    profiler.checkpoint("context_created")
    ctx.consolidate_lookups()
    profiler.checkpoint("lookups_consolidated")

    assembly_ctx = None  # Shared across assembly, exports, and referrals

    if sets_to_build or decks_only:
        batch_size = getattr(args, "batch_size", "auto")
        build_cards(ctx, batch_size=batch_size)
        profiler.checkpoint("pipeline_complete", top_n=10)

        # Enrich sealed contents with card UUIDs and build card-to-products
        # mapping now that pipeline parquet output is available.
        ctx.enrich_sealed_data()
        profiler.checkpoint("sealed_enrichment")

        # Release pipeline-only frames before assembly
        # Data is now on disk as partitioned parquet — free the in-memory
        # LazyFrames/DataFrames that were only needed for the card pipeline.
        ctx.release_pipeline_data()
        GlobalCache().release_pipeline_frames()
        profiler.checkpoint("pipeline_frames_released")

        if decks_only:
            # Only build deck files, skip set JSON assembly
            from mtgjson5.pipeline import build_expanded_decks_df

            decks_df = build_expanded_decks_df(ctx)
            LOGGER.info(f"Built expanded decks DataFrame: {len(decks_df)} rows")
        elif args.use_models:
            # Model-based assembly with Pydantic types
            outputs_set = set(args.outputs) if args.outputs else None
            set_codes = sets_to_build if sets_only else None
            results, assembly_ctx = assemble_with_models(
                ctx,
                streaming=True,
                set_codes=set_codes,
                outputs=outputs_set,
                pretty=args.pretty,
                sets_only=sets_only,
            )
            LOGGER.info(f"Model assembly results: {results}")
        else:
            set_codes = sets_to_build if sets_only else None
            _, assembly_ctx = assemble_json_outputs(
                ctx,
                parallel=True,
                max_workers=30,
                set_codes=set_codes,
                pretty=args.pretty,
                sets_only=sets_only,
            )
        profiler.checkpoint("assembly_complete")

    # --outputs or --export implies --full-build (unless only decks requested)
    # In sets-only mode, only export when explicit formats were requested
    should_export = args.full_build or export_formats or (args.outputs and not decks_only)
    if sets_only and not export_formats:
        should_export = False

    # Referral map build
    if args.referrals:
        from mtgjson5.build.referral_builder import build_and_write_referral_map

        LOGGER.info("Building referral map...")
        if assembly_ctx is None:
            from mtgjson5.build.context import AssemblyContext

            assembly_ctx = AssemblyContext.from_cache() or AssemblyContext.from_pipeline(ctx)
        referral_count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=assembly_ctx.parquet_dir,
            sealed_df=assembly_ctx.sealed_df,
            output_path=MtgjsonConfig().output_path,
        )
        LOGGER.info(f"Referral map written: {referral_count:,} entries")

    if should_export or args.price_build:
        # Wait for background price fetch to complete
        raw_prices_ready = False
        if raw_fetcher is not None:
            LOGGER.info("Waiting for background price fetch to complete...")
            raw_fetcher.wait()
            raw_fetcher.raise_if_error()
            if raw_fetcher.timings:
                LOGGER.info("Background fetch timings: %s", raw_fetcher.timings)
            raw_prices_ready = True
            profiler.checkpoint("raw_price_fetch_complete")

        # Release parent memory before subprocess
        if assembly_ctx is not None:
            del assembly_ctx
        del ctx
        if raw_fetcher is not None:
            del raw_fetcher
        GlobalCache().clear()
        gc.collect()
        profiler.checkpoint("pre_export_cleanup")

        from mtgjson5._subprocess_exports import _run_price_build, run_exports

        has_parquet = bool(export_formats and "parquet" in export_formats)
        fmt_list = list(export_formats) if export_formats else None

        # Phase 1: Format exports subprocess (parquet data, sqlite, csv, etc.)
        if fmt_list:
            profiler.checkpoint_with_children("pre_exports_subprocess")
            _run_subprocess(
                target=run_exports,
                args=(fmt_list,),
                label="exports",
                profile=args.profile,
            )
            profiler.checkpoint_with_children("post_exports_subprocess")

        # Phase 2: Price build subprocess (separate process = clean jemalloc heap)
        if args.price_build:
            parquet_dir = str(MtgjsonConfig().output_path / "parquet") if has_parquet else None
            profiler.checkpoint_with_children("pre_price_subprocess")
            _run_subprocess(
                target=_run_price_build,
                args=(parquet_dir, True, raw_prices_ready),
                label="prices",
                profile=args.profile,
            )
            profiler.checkpoint_with_children("post_price_subprocess")
    else:
        if raw_fetcher is not None:
            raw_fetcher.wait()
            del raw_fetcher
        if assembly_ctx is not None:
            del assembly_ctx
        del ctx
        GlobalCache().clear()
        gc.collect()
        profiler.checkpoint("pre_export_cleanup")

    if args.compress:
        compress_mtgjson_contents(MtgjsonConfig().output_path)
        profiler.checkpoint("compression_complete")

    generate_output_file_hashes(MtgjsonConfig().output_path)
    profiler.checkpoint("hashes_complete")

    if generate_types:
        from mtgjson5.models import write_typescript_interfaces

        output_path = args.generate_types or str(MtgjsonConfig().output_path / "AllMTGJSONTypes.ts")
        write_typescript_interfaces(output_path)
        LOGGER.info(f"TypeScript definitions written to {output_path}")

    if generate_docs:
        from mtgjson5.models import write_doc_pages

        written = write_doc_pages(str(MtgjsonConfig().output_path))
        for p in written:
            LOGGER.info(f"Doc page written to {p}")

    if args.aws_s3_upload_bucket:
        MtgjsonS3Handler().upload_directory(
            MtgjsonConfig().output_path, args.aws_s3_upload_bucket, {"Prunable": "true"}
        )

    profiler.finish()
    profiler.write_report(MtgjsonConfig().output_path)


def main() -> None:
    """
    MTGJSON safe main call
    """
    from mtgjson5.arg_parser import parse_args
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.utils import send_push_notification

    args = parse_args()
    MtgjsonConfig()

    LOGGER.info(f"Starting {MtgjsonConfig().mtgjson_version} on {constants.MTGJSON_BUILD_DATE}")

    try:
        if not args.no_alerts:
            send_push_notification(f"Starting build\n{args}")
        dispatcher(args)
        if not args.no_alerts:
            send_push_notification("Build finished")
    except Exception as error:
        LOGGER.fatal(f"Exception caught: {error} {traceback.format_exc()}")
        if not args.no_alerts:
            send_push_notification(f"Build failed: {error}\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
