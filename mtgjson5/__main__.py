"""
MTGJSON Main Executor
"""

import argparse
import logging
import traceback

import urllib3.exceptions

from mtgjson5 import constants
from mtgjson5.utils import init_logger, load_local_set_data
from mtgjson5.v2.data import GlobalCache

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

init_logger()
LOGGER: logging.Logger = logging.getLogger(__name__)


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


def validate_config_file_in_place() -> None:
    """
    Check to see if the MTGJSON config file was found.
    If not, kill the system with an error message.
    """
    if not constants.CONFIG_PATH.exists():
        LOGGER.error(
            f"{constants.CONFIG_PATH.name} was not found ({constants.CONFIG_PATH}). "
            "Please create this file and re-run the program. "
            "You can copy paste the example file into the "
            "correct location and (optionally) fill in your keys."
        )
        raise ValueError("ConfigPath not found")


def dispatcher(args: argparse.Namespace) -> None:
    """
    MTGJSON Dispatcher
    """
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

    # If a price build, simply build prices and exit
    if args.price_build:
        if args.polars:
            # Use Polars price builder for v2 pipeline
            from mtgjson5.v2.build.price_builder import PolarsPriceBuilder

            all_prices_path, today_prices_path = PolarsPriceBuilder().build_prices()
            if all_prices_path is None:
                LOGGER.error("Price build failed")
                return
            LOGGER.info(f"Price files written: {all_prices_path}, {today_prices_path}")
        else:
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
                results = assemble_with_models(
                    ctx, streaming=True, outputs=outputs_set, pretty=args.pretty
                )
                LOGGER.info(f"Model assembly results: {results}")
            else:
                assemble_json_outputs(
                    ctx,
                    include_referrals=args.referrals,
                    parallel=True,
                    max_workers=30,
                    pretty=args.pretty,
                )
        else:
            build_mtgjson_sets(sorted(sets_to_build), args.pretty, args.referrals)

    # --outputs or --export implies --full-build (unless only decks requested)
    should_export = (
        args.full_build or export_formats or (args.outputs and not decks_only)
    )
    if should_export:
        if args.polars:
            if ctx is None:
                raise ValueError("PipelineContext not initialized")

            from mtgjson5.v2.build.writer import OutputWriter

            OutputWriter.from_args(ctx).write_all()
        else:
            generate_compiled_output_files(args.pretty)
            GitHubMTGSqliteProvider().build_alternative_formats()

    if args.compress:
        if args.parallel | args.polars:
            compress_mtgjson_contents_parallel(MtgjsonConfig().output_path)
        else:
            compress_mtgjson_contents(MtgjsonConfig().output_path)

    generate_output_file_hashes(MtgjsonConfig().output_path)

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
    if args.aws_ssm_download_config:
        MtgjsonConfig(args.aws_ssm_download_config)
    else:
        validate_config_file_in_place()
        MtgjsonConfig()

    LOGGER.info(
        f"Starting {MtgjsonConfig().mtgjson_version} on {constants.MTGJSON_BUILD_DATE}"
    )

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
