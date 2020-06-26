"""
MTGJSON Main Executor
"""
import gevent.monkey  # isort:skip

gevent.monkey.patch_all()  # isort:skip


import argparse
import logging
from typing import List, Set, Union

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.compress_generator import compress_mtgjson_contents
from mtgjson5.consts import MTGJSON_VERSION, OUTPUT_PATH
from mtgjson5.output_generator import (
    generate_compiled_output_files,
    generate_compiled_prices_output,
    generate_output_file_hashes,
    write_set_file,
)
from mtgjson5.price_builder import build_prices
from mtgjson5.providers import GitHubMTGSqliteProvider, WhatsInStandardProvider
from mtgjson5.referral_builder import build_and_write_referral_map
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

    try:
        if not args.no_alerts:
            send_push_notification(f"Starting build\n{args}")
        dispatcher(args)
        if not args.no_alerts:
            send_push_notification("Build finished")
    except Exception as exception:
        LOGGER.fatal(f"Exception caught: {exception}")
        if not args.no_alerts:
            send_push_notification(f"Build failed\n{exception}")


if __name__ == "__main__":
    init_logger()
    LOGGER = logging.getLogger(__name__)
    main()
