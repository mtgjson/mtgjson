"""
MTGJSON Main Executor
"""
import gevent.monkey  # isort:skip

gevent.monkey.patch_all()  # isort:skip


import argparse
import logging
import sys
import traceback
from typing import List, Set, Union

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.compress_generator import compress_mtgjson_contents
from mtgjson5.consts import CONFIG_PATH, MTGJSON_VERSION, OUTPUT_PATH
from mtgjson5.output_generator import (
    generate_compiled_output_files,
    generate_compiled_prices_output,
    generate_output_file_hashes,
    write_set_file,
)
from mtgjson5.price_builder import build_prices
from mtgjson5.providers import GitHubMTGSqliteProvider, WhatsInStandardProvider
from mtgjson5.referral_builder import build_and_write_referral_map, fixup_referral_map
from mtgjson5.set_builder import build_mtgjson_set
from mtgjson5.utils import init_logger, send_push_notification


def build_mtgjson_sets(
    sets_to_build: Union[Set[str], List[str]],
    output_pretty: bool,
    include_referrals: bool,
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param output_pretty: Should we dump minified?
    :param include_referrals: Should we include referrals?
    """
    LOGGER.info(f"Building {len(sets_to_build)} Sets: {', '.join(sets_to_build)}")

    # Prime WhatsInStandard lookup
    _ = WhatsInStandardProvider().standard_legal_set_codes

    for set_to_build in sets_to_build:
        # Build the full set
        compiled_set = build_mtgjson_set(set_to_build)
        if not compiled_set:
            continue

        # Handle referral components
        if include_referrals:
            build_and_write_referral_map(compiled_set)

        # Dump set out to file
        write_set_file(compiled_set, output_pretty)

    if sets_to_build and include_referrals:
        fixup_referral_map()


def validate_config_file_in_place() -> None:
    """
    Check to see if the MTGJSON config file was found.
    If not, kill the system with an error message.
    """
    if not CONFIG_PATH.exists():
        LOGGER.error(
            f"{CONFIG_PATH.name} was not found ({CONFIG_PATH}). "
            "Please create this file and re-run the program. "
            "You can copy paste the example file into the "
            "correct location and (optionally) fill in your keys."
        )
        sys.exit(1)


def dispatcher(args: argparse.Namespace) -> None:
    """
    MTGJSON Dispatcher
    """
    # If a price build, simply build prices and exit
    if args.price_build:
        generate_compiled_prices_output(build_prices(), args.pretty)
        if args.compress:
            compress_mtgjson_contents(OUTPUT_PATH)
        generate_output_file_hashes(OUTPUT_PATH)
        return

    sets_to_build = get_sets_to_build(args)
    if sets_to_build:
        build_mtgjson_sets(sets_to_build, args.pretty, args.referrals)

    if args.full_build:
        generate_compiled_output_files(args.pretty)
        GitHubMTGSqliteProvider().build_sql_and_csv_files()

    if args.compress:
        compress_mtgjson_contents(OUTPUT_PATH)
    generate_output_file_hashes(OUTPUT_PATH)


def main() -> None:
    """
    MTGJSON safe main call
    """
    LOGGER.info(f"Starting MTGJSON {MTGJSON_VERSION}")
    args = parse_args()

    validate_config_file_in_place()

    try:
        if not args.no_alerts:
            send_push_notification(f"Starting build\n{args}")
        dispatcher(args)
        if not args.no_alerts:
            send_push_notification("Build finished")
    except Exception:
        LOGGER.fatal(f"Exception caught: {traceback.format_exc()}")
        if not args.no_alerts:
            send_push_notification(f"Build failed\n{traceback.format_exc()}")


if __name__ == "__main__":
    init_logger()
    LOGGER = logging.getLogger(__name__)
    main()
