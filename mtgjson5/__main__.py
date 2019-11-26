"""
MTGJSON Main Executor
"""
import logging
from typing import List, Set, Union

from mtgjson5.arg_parser import parse_args
from mtgjson5.globals import init_thread_logger
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
        with open(set_to_build + ".json", "w") as f:
            simplejson.dump(
                build_mtgjson_set(set_to_build),
                f,
                for_json=True,
                indent=4,
                sort_keys=True,
            )


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

    sets_to_build = set(args.sets) - set(args.skip_sets)
    build_mtgjson_sets(sets_to_build)


if __name__ == "__main__":
    main()
