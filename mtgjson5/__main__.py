"""
MTGJSON Main Executor
"""

import argparse
import logging
import traceback

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
    from mtgjson5.compress_generator import (
        compress_mtgjson_contents,
        compress_mtgjson_contents_parallel,
    )
    from mtgjson5.data import PipelineContext
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler
    from mtgjson5.pipeline.core import build_cards
    from mtgjson5.utils import generate_output_file_hashes

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

    if args.all_sets:
        additional_set_keys = set(load_local_set_data().keys())
        additional_set_keys -= set(args.skip_sets)
        sets_to_build = list(set(sets_to_build).union(additional_set_keys))
        args.sets = sorted(sets_to_build)

    decks_only = outputs_requested == {"decks"}

    # Create pipeline context
    ctx = PipelineContext.from_global_cache(args=args)
    ctx.consolidate_lookups()

    if sets_to_build or decks_only:
        build_cards(ctx)

        if decks_only:
            # Only build deck files, skip set JSON assembly
            from mtgjson5.pipeline import build_expanded_decks_df

            decks_df = build_expanded_decks_df(ctx)
            LOGGER.info(f"Built expanded decks DataFrame: {len(decks_df)} rows")
        elif args.use_models:
            # Model-based assembly with Pydantic types
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

    # --outputs or --export implies --full-build (unless only decks requested)
    # In sets-only mode, only export when explicit formats were requested
    should_export = args.full_build or export_formats or (args.outputs and not decks_only)
    if sets_only and not export_formats:
        should_export = False
    if should_export:
        from mtgjson5.build.writer import OutputWriter

        OutputWriter.from_args(ctx).write_all()

    # Referral map build (runs after card/export build if --referrals specified)
    if args.referrals:
        from mtgjson5.build.context import AssemblyContext
        from mtgjson5.build.referral_builder import build_and_write_referral_map

        LOGGER.info("Building referral map...")
        assembly_ctx = AssemblyContext.from_pipeline(ctx)
        referral_count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=assembly_ctx.parquet_dir,
            sealed_df=assembly_ctx.sealed_df,
            output_path=MtgjsonConfig().output_path,
        )
        LOGGER.info(f"Referral map written: {referral_count:,} entries")

    # Price build (runs after card build if --price-build specified)
    if args.price_build:
        from mtgjson5.build.price_builder import PolarsPriceBuilder

        LOGGER.info("Building prices...")
        all_prices_path, today_prices_path = PolarsPriceBuilder().build_prices()
        if all_prices_path is None:
            LOGGER.error("Price build failed")
        else:
            LOGGER.info(f"Price files written: {all_prices_path}, {today_prices_path}")

    if args.compress:
        if args.parallel:
            compress_mtgjson_contents_parallel(MtgjsonConfig().output_path)
        else:
            compress_mtgjson_contents(MtgjsonConfig().output_path)

    generate_output_file_hashes(MtgjsonConfig().output_path)

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
