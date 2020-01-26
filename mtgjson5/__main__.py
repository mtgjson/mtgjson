"""
MTGJSON Main Executor
"""
import logging
from typing import Dict, List, Set, Union

from mtgjson5.consts import OUTPUT_PATH
from mtgjson5.output_generator import generate_compiled_output_files, write_set_file
from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.price_builder import (
    add_prices_to_mtgjson_set,
    build_prices,
    get_price_archive_data,
)
from mtgjson5.referral_builder import build_and_write_referral_map
from mtgjson5.set_builder import build_mtgjson_set
from mtgjson5.utils import init_logger

LOGGER = logging.getLogger(__name__)


def build_mtgjson_sets(
    sets_to_build: Union[Set[str], List[str]],
    price_data_cache: Dict[str, Dict[str, float]],
    output_pretty: bool,
    include_referrals: bool,
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param price_data_cache: Data cache
    :param output_pretty: Should we dump minified?
    :param include_referrals: Should we include referrals?
    """
    for set_to_build in sets_to_build:
        # Build the full set
        compiled_set = build_mtgjson_set(set_to_build)

        # Add single price lines to each card entry
        add_prices_to_mtgjson_set(compiled_set, price_data_cache)

        # Handle referral components
        if include_referrals:
            build_and_write_referral_map(compiled_set)

        # Dump set out to file
        write_set_file(compiled_set, output_pretty)


def main() -> None:
    """
    MTGJSON Main Executor
    """
    args = parse_args()

    OUTPUT_PATH.mkdir(exist_ok=True)

    if args.pricing:
        build_prices()
        return

    sets_to_build = get_sets_to_build(args)

    if sets_to_build:
        LOGGER.info("Installing Price Cache")
        price_data_cache = get_price_archive_data()

        LOGGER.info(f"Building Sets: {sets_to_build}")
        build_mtgjson_sets(sets_to_build, price_data_cache, args.pretty, args.referrals)

    if args.full_build:
        LOGGER.info("Building Compiled Outputs")
        generate_compiled_output_files(args.pretty)


if __name__ == "__main__":
    init_logger()
    main()
