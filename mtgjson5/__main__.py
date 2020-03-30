"""
MTGJSON Main Executor
"""
import datetime
import logging
from typing import List, Set, Union

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.compress_generator import compress_mtgjson_contents
from mtgjson5.consts import CACHE_PATH, OUTPUT_PATH
from mtgjson5.output_generator import (
    generate_compiled_output_files,
    generate_compiled_prices_output,
    write_set_file,
)
from mtgjson5.price_builder import build_prices, get_price_archive_data
from mtgjson5.providers import GithubMTGSqliteProvider, WhatsInStandardProvider
from mtgjson5.referral_builder import build_and_write_referral_map
from mtgjson5.set_builder import build_mtgjson_set
from mtgjson5.utils import init_logger


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


def should_build_new_prices() -> bool:
    """
    Determine if prices were built recently enough that there
    is no reason to build them again
    :return: Should prices be rebuilt
    """
    cache_file = CACHE_PATH.joinpath("last_price_build_time")

    if not cache_file.is_file():
        return True

    stat_time = cache_file.stat().st_mtime
    last_price_build_time = datetime.datetime.fromtimestamp(stat_time)
    twelve_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=12)
    return twelve_hours_ago > last_price_build_time


def main() -> None:
    """
    MTGJSON Main Executor
    """
    args = parse_args()

    # If a price build, simply build prices and exit
    if args.pricing:
        LOGGER.info("Prices Build - Building Prices")
        price_data_cache = build_prices()
        generate_compiled_prices_output(price_data_cache, args.pretty)
        return

    # If a full build, build prices then build sets
    # Otherwise just load up the prices cache
    price_data_cache = {}
    if args.full_build:
        if should_build_new_prices():
            LOGGER.info("Full Build - Building Prices")
            price_data_cache = build_prices()
        else:
            LOGGER.info("Full Build - Installing Price Cache")
            price_data_cache = get_price_archive_data()

    sets_to_build = get_sets_to_build(args)
    if sets_to_build:
        LOGGER.info(f"Building Sets: {sets_to_build}")
        build_mtgjson_sets(sets_to_build, args.pretty, args.referrals)

    if args.full_build:
        LOGGER.info("Building Compiled Outputs")
        generate_compiled_output_files(price_data_cache, args.pretty)
        GithubMTGSqliteProvider().build_sql_and_csv_files()

    if args.compress:
        LOGGER.info("Compressing MTGJSON")
        compress_mtgjson_contents(OUTPUT_PATH)


if __name__ == "__main__":
    init_logger()
    LOGGER = logging.getLogger(__name__)
    main()
