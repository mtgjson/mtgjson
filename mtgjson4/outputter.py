"""
Functions used to generate outputs and write out
"""
import contextvars
import datetime
import json
import logging
import pathlib
from typing import Any, Dict, List

import mtgjson4
from mtgjson4 import util
import mtgjson4.providers

STANDARD_API_URL: str = "https://whatsinstandard.com/api/v5/sets.json"

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")


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
                file_contents["cards"] = remove_unnecessary_fields(
                    file_contents["cards"]
                )
            if "tokens" in file_contents:
                file_contents["tokens"] = remove_unnecessary_fields(
                    file_contents["tokens"]
                )
        json.dump(file_contents, f, indent=4, sort_keys=True, ensure_ascii=False)


def remove_unnecessary_fields(card_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove invalid field entries to shrink JSON output size
    """

    fixed_dict: List[Dict[str, Any]] = []
    remove_field_if_false: List[str] = [
        "isOversized",
        "isOnlineOnly",
        "isTimeshifted",
        "isReserved",
        "frameEffect",
    ]

    for card_entry in card_list:
        insert_value = {}

        for key, value in card_entry.items():
            if value is not None:
                if (key in remove_field_if_false and value is False) or (value == ""):
                    continue
                if key == "foreignData":
                    value = fix_foreign_entries(value)

                insert_value[key] = value

        fixed_dict.append(insert_value)

    return fixed_dict


def fix_foreign_entries(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Foreign entries may have bad values, such as missing flavor text. This removes them.
    :param values: List of foreign entries dicts
    :return: Pruned foreign entries
    """
    # List of dicts
    fd_insert_list = []
    for foreign_info in values:
        fd_insert_dict = {}

        name_found: bool = False
        for fd_key, fd_value in foreign_info.items():
            if fd_value is not None:
                fd_insert_dict[fd_key] = fd_value

                if fd_key == "name":
                    name_found = True

        if name_found:
            fd_insert_list.append(fd_insert_dict)

    return fd_insert_list


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
                    "code": file_content.get("code", None).upper(),
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


def create_standard_only_output() -> Dict[str, Any]:
    """
    Use whatsinstandard to determine all sets that are legal in
    the standard format. Return an AllSets version that only
    has Standard legal sets.
    :return: AllSets for Standard only
    """
    standard_data: Dict[str, Any] = {}

    # Get all sets currently in standard
    standard_url_content = util.get_generic_session().get(STANDARD_API_URL)
    standard_json = [
        set_obj["code"].upper()
        for set_obj in json.loads(standard_url_content.text)["sets"]
        if str(set_obj["enter_date"])
        < datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        < str(set_obj["exit_date"])
    ]

    for set_code in standard_json:
        set_file = mtgjson4.COMPILED_OUTPUT_DIR.joinpath(win_os_fix(set_code) + ".json")

        if not set_file.is_file():
            LOGGER.warning(
                "Set {} not found in compiled outputs (Standard)".format(set_code)
            )
            continue

        with set_file.open("r", encoding="utf-8") as f:
            file_content = json.load(f)
            standard_data[set_code] = file_content

    return standard_data


def create_modern_only_output() -> Dict[str, Any]:
    """
    Use gamepedia to determine all sets that are legal in
    the modern format. Return an AllSets version that only
    has Modern legal sets.
    :return: AllSets for Modern only
    """
    modern_data: Dict[str, Any] = {}

    for set_code in mtgjson4.providers.GAMEPEDIA.get_modern_sets():
        set_file = mtgjson4.COMPILED_OUTPUT_DIR.joinpath(win_os_fix(set_code) + ".json")

        if not set_file.is_file():
            LOGGER.warning(
                "Set {} not found in compiled outputs (Modern)".format(set_code)
            )
            continue

        with set_file.open("r", encoding="utf-8") as f:
            file_content = json.load(f)
            modern_data[set_code] = file_content

    return modern_data


def get_funny_sets() -> List[str]:
    """
    This will determine all of the "joke" sets and give
    back a list of their set codes
    :return: List of joke set codes
    """
    return [
        x["code"].upper()
        for x in mtgjson4.providers.SCRYFALL.download(
            mtgjson4.providers.SCRYFALL.api_sets
        )["data"]
        if str(x["set_type"]) in ["funny", "memorabilia"]
    ]


def create_all_sets_no_funny(files_to_ignore: List[str]) -> Dict[str, Any]:
    """
    Create all sets, but ignore additional sets
    :param files_to_ignore: Files to default ignore in the output
    :return: AllSets without funny
    """
    return create_all_sets(files_to_ignore + get_funny_sets())


def create_all_cards_no_funny(files_to_ignore: List[str]) -> Dict[str, Any]:
    """
    Create all cards, but ignore additional sets
    :param files_to_ignore: Files to default ignore in the output
    :return: AllCards without funny
    """
    return create_all_cards(files_to_ignore + get_funny_sets())


def create_and_write_compiled_outputs() -> None:
    """
    This method class will create the combined output files
    (ex: AllSets.json, AllCards.json, Standard.json)
    """
    # Files that should not be combined into compiled outputs
    files_to_ignore: List[str] = [
        mtgjson4.ALL_SETS_OUTPUT,
        mtgjson4.ALL_CARDS_OUTPUT,
        mtgjson4.SET_LIST_OUTPUT,
        mtgjson4.KEY_WORDS_OUTPUT,
        mtgjson4.VERSION_OUTPUT,
        mtgjson4.STANDARD_OUTPUT,
        mtgjson4.MODERN_OUTPUT,
        mtgjson4.ALL_CARDS_NO_FUN_OUTPUT,
        mtgjson4.ALL_SETS_NO_FUN_OUTPUT,
        mtgjson4.REFERRAL_DB_OUTPUT,
    ]

    # AllSets.json
    all_sets = create_all_sets(files_to_ignore)
    write_to_file(mtgjson4.ALL_SETS_OUTPUT, all_sets)

    # AllCards.json
    all_cards = create_all_cards(files_to_ignore)
    write_to_file(mtgjson4.ALL_CARDS_OUTPUT, all_cards)

    # SetList.json
    set_list_info = get_all_set_list(files_to_ignore)
    write_to_file(mtgjson4.SET_LIST_OUTPUT, set_list_info)

    # Keywords.json
    key_words = mtgjson4.providers.WIZARDS.compile_comp_output()
    write_to_file(mtgjson4.KEY_WORDS_OUTPUT, key_words)

    # version.json
    version_info = get_version_info()
    write_to_file(mtgjson4.VERSION_OUTPUT, version_info)

    # Standard.json
    write_to_file(mtgjson4.STANDARD_OUTPUT, create_standard_only_output())

    # Modern.json
    write_to_file(mtgjson4.MODERN_OUTPUT, create_modern_only_output())

    # AllSetsNoUn.json
    all_sets_no_fun = create_all_sets_no_funny(files_to_ignore)
    write_to_file(mtgjson4.ALL_SETS_NO_FUN_OUTPUT, all_sets_no_fun)

    # AllCardsNoUn.json
    all_cards_no_fun = create_all_cards_no_funny(files_to_ignore)
    write_to_file(mtgjson4.ALL_CARDS_NO_FUN_OUTPUT, all_cards_no_fun)
