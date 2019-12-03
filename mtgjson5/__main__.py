"""
MTGJSON Main Executor
"""
import logging
from typing import List, Set, Union

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.globals import OUTPUT_PATH, init_thread_logger, set_cache
from mtgjson5.set_builder import build_mtgjson_set
import simplejson


def build_price_stuff() -> None:
    """
    TODO LATER
    :return:
    """
    return


def build_mtgjson_sets_part_1(
    sets_to_build: Union[Set[str], List[str]], output_pretty: bool = False
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param output_pretty: Should we dump minified? (Default=False)
    """
    for set_to_build in sets_to_build:
        compiled_set = build_mtgjson_set(set_to_build)
        with OUTPUT_PATH.joinpath(f"{set_to_build}.json").open("w") as f:
            simplejson.dump(
                obj=compiled_set,
                fp=f,
                for_json=True,
                sort_keys=True,
                indent=(4 if output_pretty else None),
                ensure_ascii=False,
            )


def main() -> None:
    """
    MTGJSON Main Executor
    """
    init_thread_logger()
    args = parse_args()

    set_cache(not args.skip_cache)
    OUTPUT_PATH.mkdir(exist_ok=True)

    if args.pricing:
        build_price_stuff()
        return

    logging.info(args)

    sets_to_build = get_sets_to_build(args)
    logging.info(f"Building Sets: {sets_to_build}")

    build_mtgjson_sets_part_1(sets_to_build, args.pretty)


if __name__ == "__main__":
    main()
