"""
MTGJSON Main Executor
"""
import logging

import simplejson

from mtgjson5.arg_parser import parse_args
from mtgjson5.globals import init_logger
from mtgjson5.set_builder import build_mtgjson_set


def build_price_stuff():
    pass


def build_mtgjson_sets(sets_to_build, is_privileged):
    for set_to_build in sets_to_build:
        with open(set_to_build + ".json", "w") as f:
            simplejson.dump(
                build_mtgjson_set(set_to_build, is_privileged),
                f,
                for_json=True,
                indent=4,
                sort_keys=True,
            )


def main() -> None:
    """
    MTGJSON Main Executor
    """
    init_logger()
    args = parse_args()

    if args.pricing:
        build_price_stuff()
        return

    logging.info(args)

    sets_to_build = set(args.sets) - set(args.skip_sets)
    build_mtgjson_sets(sets_to_build, not args.skip_keys)


if __name__ == "__main__":
    main()
