"""
MTGJSON Main Executor
"""

import argparse
import logging
import traceback

import urllib3.exceptions

from mtgjson5.utils import init_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
init_logger()
LOGGER: logging.Logger = logging.getLogger(__name__)

from mtgjson5 import constants
from mtgjson5.utils import load_local_set_data
from mtgjson5.v2.data import GlobalCache


def build_mtgjson_sets(
    sets_to_build: set[str] | list[str],
    output_pretty: bool,
    include_referrals: bool,
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param output_pretty: Should we dump minified
    :param include_referrals: Should we include referrals
    """
    from mtgjson5.output_generator import write_to_file
    from mtgjson5.providers import GathererProvider, WhatsInStandardProvider
    from mtgjson5.referral_builder import (
        build_and_write_referral_map,
        fixup_referral_map,
    )
    from mtgjson5.set_builder import build_mtgjson_set

    LOGGER.info(f"Building {len(sets_to_build)} Sets: {', '.join(sets_to_build)}")

    # Prime lookups
    _ = WhatsInStandardProvider()
    _ = GathererProvider()

    for set_to_build in sets_to_build:
        # Build the full set
        mtgjson_set = build_mtgjson_set(set_to_build)
        if not mtgjson_set:
            continue

        # Handle referral components
        if include_referrals:
            build_and_write_referral_map(mtgjson_set)

        # Dump set out to file
        write_to_file(
            file_name=mtgjson_set.get_windows_safe_set_code(),
            file_contents=mtgjson_set,
            pretty_print=output_pretty,
        )

    if sets_to_build and include_referrals:
        fixup_referral_map()


def dispatcher(args: argparse.Namespace) -> None:
    """
    MTGJSON Dispatcher
    """
    # Generate types/docs only (no other build flags)
    generate_types = getattr(args, "generate_types", None) is not None
    generate_docs = getattr(args, "generate_docs", False)
    if (generate_types or generate_docs) and not (args.sets or args.all_sets or args.full_build or args.price_build):
        from mtgjson5.mtgjson_config import MtgjsonConfig
        from mtgjson5.v2.models import write_doc_pages, write_typescript_interfaces

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

    from mtgjson5.compress_generator import (
        compress_mtgjson_contents,
        compress_mtgjson_contents_parallel,
    )
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler
    from mtgjson5.output_generator import (
        generate_compiled_output_files,
        generate_compiled_prices_output,
        generate_output_file_hashes,
    )
    from mtgjson5.price_builder import PriceBuilder
    from mtgjson5.providers import GitHubMTGSqliteProvider, ScryfallProvider
    from mtgjson5.v2.build.writer import assemble_json_outputs, assemble_with_models
    from mtgjson5.v2.data import PipelineContext
    from mtgjson5.v2.pipeline.core import build_cards

    # Legacy price-only build (non-v2) - build prices and exit
    if args.price_build and not args.polars:
        all_prices, today_prices = PriceBuilder().build_prices()
        generate_compiled_prices_output(all_prices, today_prices, args.pretty)
        if args.compress:
            compress_mtgjson_contents(MtgjsonConfig().output_path)
        generate_output_file_hashes(MtgjsonConfig().output_path)
        return

    sets_to_build = ScryfallProvider().get_sets_to_build(args)

    # Check if only specific outputs or formats requested
    outputs_requested = {o.lower() for o in (args.outputs or [])}
    export_formats = {f.lower() for f in (args.export or [])} if args.export else None

    # Sets-only mode: specific sets requested without --all-sets or --full-build
    # Only builds individual set files, skipping compiled outputs and exports
    # Override with --outputs, --export, or --full-build to include more
    sets_only = bool(args.sets) and not args.all_sets and not args.full_build

    # Load global cache only when using Polars pipeline
    # Pass set_codes to filter aggregation computations to only requested sets
    if args.polars:
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
    if args.all_sets:
        additional_set_keys = set(load_local_set_data().keys())
        additional_set_keys -= set(args.skip_sets)
        sets_to_build = list(set(sets_to_build).union(additional_set_keys))
        args.sets = sorted(sets_to_build)

    decks_only = outputs_requested == {"decks"}

    # Create context for Polars pipeline (needed for builds and/or exports)
    ctx = None
    if args.polars:
        ctx = PipelineContext.from_global_cache(args=args)
        ctx.consolidate_lookups()

    if sets_to_build or decks_only:
        if args.polars:
            if ctx is None:
                raise ValueError("PipelineContext not initialized")

            build_cards(ctx)

            if decks_only:
                # Only build deck files, skip set JSON assembly
                from mtgjson5.v2.pipeline import build_expanded_decks_df

                decks_df = build_expanded_decks_df(ctx)
                LOGGER.info(f"Built expanded decks DataFrame: {len(decks_df)} rows")
            elif args.use_models:
                # Model-based assembly with Pydantic types
                # Pass outputs if specified (use original case from args.outputs)
                outputs_set = set(args.outputs) if args.outputs else None
                set_codes = sets_to_build if sets_only else None
                results = assemble_with_models(
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
                assemble_json_outputs(
                    ctx,
                    parallel=True,
                    max_workers=30,
                    set_codes=set_codes,
                    pretty=args.pretty,
                    sets_only=sets_only,
                )
        else:
            build_mtgjson_sets(sorted(sets_to_build), args.pretty, args.referrals)

    # --outputs or --export implies --full-build (unless only decks requested)
    # In sets-only mode, only export when explicit formats were requested
    should_export = args.full_build or export_formats or (args.outputs and not decks_only)
    if sets_only and not export_formats:
        should_export = False
    if should_export:
        if args.polars:
            if ctx is None:
                raise ValueError("PipelineContext not initialized")

            from mtgjson5.v2.build.writer import OutputWriter

            OutputWriter.from_args(ctx).write_all()
        else:
            generate_compiled_output_files(args.pretty)
            GitHubMTGSqliteProvider().build_alternative_formats()

    # V2 referral map build (runs after card/export build if --referrals specified)
    if args.referrals and args.polars:
        if ctx is None:
            raise ValueError("PipelineContext not initialized for referral build")

        from mtgjson5.v2.build.context import AssemblyContext
        from mtgjson5.v2.build.referral_builder import build_and_write_referral_map

        LOGGER.info("Building referral map...")
        assembly_ctx = AssemblyContext.from_pipeline(ctx)
        referral_count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=assembly_ctx.parquet_dir,
            sealed_df=assembly_ctx.sealed_df,
            output_path=MtgjsonConfig().output_path,
        )
        LOGGER.info(f"Referral map written: {referral_count:,} entries")

    # V2 price build (runs after card build if --price-build specified)
    if args.price_build and args.polars:
        from mtgjson5.v2.build.price_builder import PolarsPriceBuilder

        LOGGER.info("Building prices...")
        all_prices_path, today_prices_path = PolarsPriceBuilder().build_prices()
        if all_prices_path is None:
            LOGGER.error("Price build failed")
        else:
            LOGGER.info(f"Price files written: {all_prices_path}, {today_prices_path}")

    if args.compress:
        if args.parallel | args.polars:
            compress_mtgjson_contents_parallel(MtgjsonConfig().output_path)
        else:
            compress_mtgjson_contents(MtgjsonConfig().output_path)

    generate_output_file_hashes(MtgjsonConfig().output_path)

    if generate_types:
        from mtgjson5.v2.models import write_typescript_interfaces

        output_path = args.generate_types or str(MtgjsonConfig().output_path / "AllMTGJSONTypes.ts")
        write_typescript_interfaces(output_path)
        LOGGER.info(f"TypeScript definitions written to {output_path}")

    if generate_docs:
        from mtgjson5.v2.models import write_doc_pages

        written = write_doc_pages(str(MtgjsonConfig().output_path))
        for p in written:
            LOGGER.info(f"Doc page written to {p}")

    if args.aws_s3_upload_bucket:
        MtgjsonS3Handler().upload_directory(
            MtgjsonConfig().output_path, args.aws_s3_upload_bucket, {"Prunable": "true"}
        )


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
