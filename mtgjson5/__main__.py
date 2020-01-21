"""
MTGJSON Main Executor
"""
from typing import List, Set, Union, Dict, Any

from mtgjson5.arg_parser import get_sets_to_build, parse_args
from mtgjson5.classes import MtgjsonSetObject, MtgjsonPricesObject
from mtgjson5.consts import OUTPUT_PATH
from mtgjson5.price_builder import build_prices, get_price_archive_data
from mtgjson5.set_builder import build_mtgjson_set
from mtgjson5.utils import get_thread_logger
import simplejson as json

LOGGER = get_thread_logger()


def build_mtgjson_sets_part_2(
    mtgjson_part_1_set: MtgjsonSetObject, price_data_cache: Dict[str, Any]
) -> None:
    """
    Add the final pieces to the set (i.e. Price data)
    :param mtgjson_part_1_set: Part 1 build
    :param price_data_cache: Data cache to pull entries from
    """
    for mtgjson_card_object in mtgjson_part_1_set.cards:
        mtgjson_card_object.prices = MtgjsonPricesObject(mtgjson_card_object.uuid)

        data_entry = price_data_cache.get(mtgjson_card_object.uuid, {})
        for key, value in data_entry.items():
            if not isinstance(value, dict):
                continue

            if value:
                max_value = max(value)
                data_entry[key] = {max_value: value[max_value]}

        mtgjson_card_object.prices = MtgjsonPricesObject(
            mtgjson_card_object.uuid, data_entry
        )


def build_mtgjson_sets_part_1(
    sets_to_build: Union[Set[str], List[str]],
    price_data_cache: Dict[str, Any],
    output_pretty: bool = False,
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param price_data_cache: Data cache
    :param output_pretty: Should we dump minified? (Default=False)
    """
    for set_to_build in sets_to_build:
        compiled_set = build_mtgjson_set(set_to_build)
        build_mtgjson_sets_part_2(compiled_set, price_data_cache)

        with OUTPUT_PATH.joinpath(f"{set_to_build}.json").open("w") as file:
            json.dump(
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
        build_prices()
        return

    LOGGER.info("Installing Price Cache")
    price_data_cache = get_price_archive_data()

    sets_to_build = get_sets_to_build(args)
    LOGGER.info(f"Building Sets: {sets_to_build}")

    build_mtgjson_sets_part_1(sets_to_build, price_data_cache, args.pretty)


if __name__ == "__main__":
    main()
