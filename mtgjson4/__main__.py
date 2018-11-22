"""MTGJSON Version 4 Compiler"""
# pylint: disable=too-many-lines

import argparse
import json
import logging
import pathlib
import sys
from typing import Any, Dict, List, Optional

import mtgjson4
from mtgjson4 import compile_mtg
from mtgjson4.provider import scryfall, wizards

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


def win_os_fix(set_name: str) -> str:
    """
    In the Windows OS, there are certain file names that are not allowed.
    In case we have a set with such a name, we will add a _ to the end to allow its existence
    on Windows.
    :param set_name: Set name
    :return: Set name with a _ if necessary
    """
    if set_name in mtgjson4.BANNED_FILE_NAMES:
        return set_name + "_"

    return set_name


def write_to_file(set_name: str, file_contents: Any, do_cleanup: bool = False) -> None:
    """
    Write the compiled data to a file with the set's code
    Will ensure the output directory exists first
    """
    mtgjson4.COMPILED_OUTPUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(
        mtgjson4.COMPILED_OUTPUT_DIR, win_os_fix(set_name) + ".json"
    ).open("w", encoding="utf-8") as f:
        if do_cleanup and isinstance(file_contents, dict):
            if "cards" in file_contents:
                file_contents["cards"] = compile_mtg.remove_unnecessary_fields(
                    file_contents["cards"]
                )
            if "tokens" in file_contents:
                file_contents["tokens"] = compile_mtg.remove_unnecessary_fields(
                    file_contents["tokens"]
                )
        json.dump(file_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
        return


def get_all_sets() -> List[str]:
    """
    Grab the set codes (~3 letters) for all sets found
    in the config database.
    :return: List of all set codes found, sorted
    """
    downloaded = scryfall.download(scryfall.SCRYFALL_API_SETS)
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


def compile_and_write_outputs() -> None:
    """
    This method class will create the combined output files
    of AllSets.json and AllCards.json
    """
    # Files that should not be combined into compiled outputs
    files_to_ignore: List[str] = [
        mtgjson4.ALL_SETS_OUTPUT,
        mtgjson4.ALL_CARDS_OUTPUT,
        mtgjson4.SET_CODES_OUTPUT,
        mtgjson4.SET_LIST_OUTPUT,
        mtgjson4.KEY_WORDS_OUTPUT,
    ]

    # Actual compilation process of the method
    # The ordering _shouldn't necessarily_ matter
    # but it works as is, so no need to tweak it
    all_sets = create_all_sets(files_to_ignore)
    write_to_file(mtgjson4.ALL_SETS_OUTPUT, all_sets)

    all_cards = create_all_cards(files_to_ignore)
    write_to_file(mtgjson4.ALL_CARDS_OUTPUT, all_cards)

    all_set_codes = get_all_set_names(files_to_ignore)
    write_to_file(mtgjson4.SET_CODES_OUTPUT, all_set_codes)

    set_list_info = get_all_set_list(files_to_ignore)
    write_to_file(mtgjson4.SET_LIST_OUTPUT, set_list_info)

    key_words = wizards.compile_comp_output()
    write_to_file(mtgjson4.KEY_WORDS_OUTPUT, key_words)


def create_all_sets(files_to_ignore: List[str]) -> Dict[str, Any]:
    """
    This will create the AllSets.json file
    by pulling the compile data from the
    compiled sets and combining them into
    one conglomerate file.
    """
    all_sets_data: Dict[str, Any] = {}

    for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json"):
        if set_file.name[:-5] in files_to_ignore:
            continue

        with set_file.open("r", encoding="utf-8") as f:
            file_content = json.load(f)
            set_name = set_file.name.split(".")[0]
            all_sets_data[set_name] = file_content

    return all_sets_data


def create_all_cards(files_to_ignore: List[str]) -> Dict[str, Any]:
    """
    This will create the AllCards.json file
    by pulling the compile data from the
    compiled sets and combining them into
    one conglomerate file.
    """
    all_cards_data: Dict[str, Any] = {}

    for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json"):
        if set_file.name[:-5] in files_to_ignore:
            continue

        with set_file.open("r", encoding="utf-8") as f:
            file_content = json.load(f)

            duplicate_cards: Dict[str, int] = {}

            for card in file_content["cards"]:
                # Only if a card is duplicated in a set will it get the (a), (b) appended
                if (
                    card["name"] in duplicate_cards
                    or file_content["cards"].count(card["name"]) > 1
                ):
                    if card["name"] in mtgjson4.BASIC_LANDS:
                        pass
                    elif card["name"] in duplicate_cards:
                        duplicate_cards[card["name"]] += 1
                    else:
                        duplicate_cards[card["name"]] = 98  # 'b'
                        # Replace "Original" => "Original (a)"
                        all_cards_data[
                            "{0} ({1})".format(card["name"], "a")
                        ] = all_cards_data[card["name"]]
                        del all_cards_data[card["name"]]

                # Since these can vary from printing to printing, we do not include them in the output
                card.pop("artist", None)
                card.pop("borderColor", None)
                card.pop("cardHash", None)
                card.pop("flavorText", None)
                card.pop("frameVersion", None)
                card.pop("hasFoil", None)
                card.pop("hasNonFoil", None)
                card.pop("isOnlineOnly", None)
                card.pop("isOversized", None)
                card.pop("multiverseId", None)
                card.pop("number", None)
                card.pop("originalText", None)
                card.pop("originalType", None)
                card.pop("rarity", None)
                card.pop("reserved", None)
                card.pop("timeshifted", None)
                card.pop("variations", None)
                card.pop("watermark", None)

                for foreign in card["foreignData"]:
                    foreign.pop("multiverseId", None)

                key = card["name"]
                if duplicate_cards.get(card["name"], 0) > 0:
                    key += " ({0})".format(chr(duplicate_cards[card["name"]]))

                all_cards_data[key] = card

    return all_cards_data


def get_all_set_names(files_to_ignore: List[str]) -> List[str]:
    """
    This will create the SetCodes.json file
    by getting the name of all the files in
    the set_outputs folder and combining
    them into a list.
    :param files_to_ignore: Files to ignore in set_outputs folder
    :return: List of all set names
    """
    all_sets_data: List[str] = []

    for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json"):
        if set_file.name[:-5] in files_to_ignore:
            continue
        all_sets_data.append(set_file.name.split(".")[0].upper())

    return sorted(all_sets_data)


def get_all_set_list(files_to_ignore: List[str]) -> List[Dict[str, str]]:
    """
    This will create the SetList.json file
    by getting the info from all the files in
    the set_outputs folder and combining
    them into the old v3 structure.
    :param files_to_ignore: Files to ignore in set_outputs folder
    :return: List of all set dicts
    """
    all_sets_data: List[Dict[str, str]] = []

    for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json"):
        if set_file.name[:-5] in files_to_ignore:
            continue

        with set_file.open("r", encoding="utf-8") as f:
            file_content = json.load(f)
            all_sets_data.append(
                {
                    "name": file_content.get("name", None),
                    "code": file_content.get("code", None),
                    "releaseDate": file_content.get("releaseDate", None),
                }
            )

    return sorted(all_sets_data, key=lambda set_info: set_info["name"])


def create_version_file() -> None:
    """
    :return: nothing
    """
    # TODO
    return


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
            compiled: Dict[str, Any] = compile_mtg.build_output_file(sf_set, set_code)

            # If we have at least 1 card, print out to file
            if compiled["cards"]:
                write_to_file(set_code.upper(), compiled, do_cleanup=True)

    if args.compiled_outputs:
        LOGGER.info("Compiling AllSets, AllCards, SetCodes, and SetList")
        compile_and_write_outputs()


if __name__ == "__main__":
    main()
