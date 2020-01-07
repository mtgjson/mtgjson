"""
MTGJSON Main Executor
"""
from typing import List, Set, Union

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.consts import OUTPUT_PATH
from mtgjson5.set_builder import build_mtgjson_set
from mtgjson5.utils import get_thread_logger
import simplejson

LOGGER = get_thread_logger()


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
        with OUTPUT_PATH.joinpath(f"{set_to_build}.json").open("w") as file:
            simplejson.dump(
                obj=compiled_set,
                fp=file,
                for_json=True,
                sort_keys=True,
                indent=(4 if output_pretty else None),
                ensure_ascii=False,
            )


def main() -> None:
    """
    MTGJSON Main Executor
    """
    get_thread_logger()
    args = parse_args()

    OUTPUT_PATH.mkdir(exist_ok=True)

    if args.pricing:
        return

    LOGGER.info(args)

    sets_to_build = get_sets_to_build(args)
    LOGGER.info(f"Building Sets: {sets_to_build}")

    build_mtgjson_sets_part_1(sets_to_build, args.pretty)


if __name__ == "__main__":
    main()
