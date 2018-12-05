"""MTGJSON Version 4 Compiler"""
# pylint: disable=too-many-lines

import argparse
import logging
import pathlib
import sys
from typing import Any, Dict, List, Optional

from mtgjson4 import compile_mtg
import mtgjson4.outputter
from mtgjson4.provider import scryfall

LOGGER = logging.getLogger(__name__)


def find_file(name: str, path: pathlib.Path) -> Optional[pathlib.Path]:
    """
    Function finds where on the path tree a specific file
    can be found. Useful for set_configs as we use sub
    directories to better organize data.
    """
    for file in path.glob("**/*.json"):
        if name == file.name:
            return file

    return None


def get_all_sets() -> List[str]:
    """
    Grab the set codes (~3 letters) for all sets found
    in the config database.
    :return: List of all set codes found, sorted
    """
    downloaded = mtgjson4.provider.scryfall.download(scryfall.SCRYFALL_API_SETS)
    if downloaded["object"] == "error":
        LOGGER.error("Downloading Scryfall data failed: {}".format(downloaded))
        return []

    # Get _ALL_ Scryfall sets
    set_codes: List[str] = [set_obj["code"] for set_obj in downloaded["data"]]

    # Remove Scryfall token sets (but leave extra sets)
    set_codes = [s for s in set_codes if not (s.startswith("t") and s[1:] in set_codes)]

    return sorted(set_codes)


def get_compiled_sets() -> List[str]:
    """
    Grab the official set codes for all sets that have already been
    compiled and are awaiting use in the set_outputs dir.
    :return: List of all set codes found
    """
    all_paths: List[pathlib.Path] = list(mtgjson4.COMPILED_OUTPUT_DIR.glob("**/*.json"))
    all_sets_found: List[str] = [
        str(card_set).split("/")[-1][:-5].lower() for card_set in all_paths
    ]

    all_sets_found = [
        x[:-1] if x[:-1].upper() in mtgjson4.BANNED_FILE_NAMES else x
        for x in all_sets_found
    ]

    return all_sets_found


def main() -> None:
    """
    Main Method
    """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-s", metavar="SET", nargs="*", type=str)
    parser.add_argument("-a", "--all-sets", action="store_true")
    parser.add_argument("-c", "--compiled-outputs", action="store_true")
    parser.add_argument("--skip-rebuild", action="store_true")
    parser.add_argument("--skip-cached", action="store_true")

    # Ensure there are args
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)
    else:
        args = parser.parse_args()

    if not pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
        LOGGER.warning(
            "No properties file found at {}. Will download without authentication".format(
                mtgjson4.CONFIG_PATH
            )
        )

    if not args.skip_rebuild:
        # Determine sets to build, whether they're passed in as args or all sets in our configs
        args_s = args.s if args.s else []
        set_list: List[str] = get_all_sets() if args.all_sets else args_s

        LOGGER.info("Sets to compile: {}".format(set_list))

        # If we had to kill mid-rebuild, we can skip the sets that already were done
        if args.skip_cached:
            sets_compiled_already: List[str] = get_compiled_sets()
            set_list = [s for s in set_list if s not in sets_compiled_already]
            LOGGER.info(
                "Sets to skip compilation for: {}".format(sets_compiled_already)
            )
            LOGGER.info(
                "Sets to compile, after cached sets removed: {}".format(set_list)
            )

        for set_code in set_list:
            sf_set: List[Dict[str, Any]] = scryfall.get_set(set_code)
            compiled: Dict[str, Any] = mtgjson4.compile_mtg.build_output_file(sf_set, set_code)

            # If we have at least 1 card, dump to file SET.json
            if compiled["cards"]:
                mtgjson4.outputter.write_to_file(set_code.upper(), compiled, do_cleanup=True)

    if args.compiled_outputs:
        LOGGER.info("Compiling Additional Outputs")
        mtgjson4.outputter.create_and_write_compiled_outputs()


if __name__ == "__main__":
    main()
