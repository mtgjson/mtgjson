"""
Functions used to generate outputs and write out
"""
import json
import pathlib
from typing import Any, Dict, List

import requests

import mtgjson4
from mtgjson4 import compile_mtg
from mtgjson4.provider import wizards


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
            set_name = get_set_name_from_file_name(set_file.name.split(".")[0])
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

            for card in file_content["cards"]:
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
                card.pop("isTimeshifted", None)
                card.pop("variations", None)
                card.pop("watermark", None)

                for foreign in card["foreignData"]:
                    foreign.pop("multiverseId", None)

                all_cards_data[card["name"]] = card

    return all_cards_data


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
        all_sets_data.append(
            get_set_name_from_file_name(set_file.name.split(".")[0].upper())
        )

    return sorted(all_sets_data)


def get_set_name_from_file_name(set_name: str) -> str:
    """
    Some files on Windows break down, such as CON. This is our reverse mapping.
    :param set_name: File name to convert to MTG format
    :return: Real MTG set code
    """
    return set_name[:-1] if set_name[:-1] in mtgjson4.BANNED_FILE_NAMES else set_name


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


def get_version_info() -> Dict[str, str]:
    """
    Create a version file for updating purposes
    :return: Version file
    """
    return {"version": mtgjson4.__VERSION__, "date": mtgjson4.__VERSION_DATE__}


def create_standard_only_output(files_to_ignore: List[str]) -> Dict[str, Any]:
    standard_data: Dict[str, Any] = {}

    # Get all sets currently in standard
    standard_url_content = requests.get("https://whatsinstandard.com/api/v5/sets.json")
    standard_json = [
        set_obj["code"]
        for set_obj in json.loads(standard_url_content)["sets"]
    ]

    for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json"):
        if set_file.name[:-5] in files_to_ignore:
            continue

        if set_file.name[:-5] in standard_json:
            with set_file.open("r", encoding="utf-8") as f:
                file_content = json.load(f)
                set_name = get_set_name_from_file_name(set_file.name.split(".")[0])
                standard_data[set_name] = file_content

    return standard_data


def create_and_write_compiled_outputs() -> None:
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
        mtgjson4.VERSION_OUTPUT,
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

    version_info = get_version_info()
    write_to_file(mtgjson4.VERSION_OUTPUT, version_info)
