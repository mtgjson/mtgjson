"""
MTGJSON Main Executor
"""
import logging
from typing import List, Set, Union

from mtgjson5.arg_parser import parse_args, get_sets_to_build
from mtgjson5.globals import init_thread_logger, OUTPUT_PATH
from mtgjson5.set_builder import build_mtgjson_set
import simplejson


def build_price_stuff() -> None:
    """
    TODO LATER
    :return:
    """
    return


def build_mtgjson_sets(sets_to_build: Union[Set[str], List[str]]) -> None:
    """
    TEMP BUILDER
    :param sets_to_build:
    :return:
    """
    for set_to_build in sets_to_build:
        compiled_set = build_mtgjson_set(set_to_build)
        with OUTPUT_PATH.joinpath(f"{set_to_build}.json").open("w") as f:
            simplejson.dump(compiled_set, f, for_json=True, sort_keys=True, indent=4)


def main() -> None:
    """
    MTGJSON Main Executor
    """
    init_thread_logger()
    args = parse_args()

    if args.pricing:
        build_price_stuff()
        return

    logging.info(args)

    sets_to_build = get_sets_to_build(args)
    logging.info(f"Building Sets: {sets_to_build}")

    build_mtgjson_sets(sets_to_build)


if __name__ == "__main__":
    main()
