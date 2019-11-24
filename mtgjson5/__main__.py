"""
MTGJSON Main Executor
"""
import json

from mtgjson5.arg_parser import parse_args
from mtgjson5.globals import CACHE_PATH
from mtgjson5.providers.scryfall_provider import ScryfallProvider


def init_paths():
    CACHE_PATH.mkdir(exist_ok=True)


def build_price_stuff():
    pass


def build_mtgjson_sets(sets_to_build):
    sf = ScryfallProvider()

    for set_to_build in sets_to_build:
        print(
            json.dumps(
                sf.download(f"https://api.scryfall.com/sets/{set_to_build}"), indent=4
            )
        )


def main() -> None:
    """
    MTGJSON Main Executor
    """
    init_paths()
    args = parse_args()

    if args.pricing:
        build_price_stuff()
        return

    print(args)

    sets_to_build = set(args.sets) - set(args.skip_sets)
    build_mtgjson_sets(sets_to_build)


if __name__ == "__main__":
    main()
