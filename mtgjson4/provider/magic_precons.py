"""Card information provider for Precons."""
import json
import logging
import multiprocessing
import pathlib
import re
from typing import Any, Dict, List

import mtgjson4
from mtgjson4.outputter import win_os_fix, write_deck_to_file

LOGGER = logging.getLogger(__name__)


def build_and_write_decks(precon_path: str) -> None:
    """
    Given the path to the precons list, this will
    compile them in MTGJSONv4 format and write out
    the decks to the "decks/" folder.
    """
    with pathlib.Path(precon_path).open("r", encoding="utf-8") as f:
        content = json.load(f)

    for deck in content:
        deck_to_output = {
            "name": deck["name"],
            "code": deck["set_code"].upper(),
            "type": deck["type"],
            "mainBoard": [],
            "sideBoard": [],
        }

        with multiprocessing.Pool(processes=8) as pool:
            # Pool main board first
            results: List[Any] = pool.map(build_single_card, deck["cards"])
            for cards in results:
                for card in cards:
                    deck_to_output["mainBoard"].append(card)

            # Now pool side board
            results = pool.map(build_single_card, deck["sideboard"])
            for cards in results:
                for card in cards:
                    deck_to_output["sideBoard"].append(card)

        write_deck_to_file(capital_case_without_symbols(deck["name"]), deck_to_output)
        LOGGER.info("Finished set {}".format(deck["name"]))


def capital_case_without_symbols(name: str) -> str:
    """
    Determine the name of the output file by stripping
    all special characters and capital casing the words.
    :param name: Deck name (unsanitized)
    :return: Sanitized deck name
    """
    word_characters_only_regex = re.compile(r"[^\w]")
    capital_case = "".join(x for x in name.title() if not x.isspace())

    return word_characters_only_regex.sub("", capital_case)


def get_mtgjson_set_code(set_code: str) -> str:
    """
    Some set codes are wrong, so this will sanitize
    the set_code passed in
    :param set_code: Set code (unsanitized)
    :return: Sanitized set code
    """
    with mtgjson4.RESOURCE_PATH.joinpath("gatherer_set_codes.json").open(
        "r", encoding="utf-8"
    ) as f:
        json_dict = json.load(f)
        for key, value in json_dict.items():
            if set_code == value:
                return str(key)

    return win_os_fix(set_code)


def build_single_card(precon_card: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build the MTGJSONv4 card for each pre-con card
    in the deck.
    :param precon_card: Card to build Precon format
    :return: Card(s) built from the card MTGJSONv4 format
    """
    with mtgjson4.COMPILED_OUTPUT_DIR.joinpath(
        get_mtgjson_set_code(precon_card["set_code"].upper()) + ".json"
    ).open("r") as f:
        compiled_file = json.load(f)

    cards = []
    for mtgjson_card in compiled_file["cards"]:
        if "//" in precon_card["name"]:
            if precon_card["number"][-1].isalpha():
                precon_card["number"] = precon_card["number"][:-1]

        if mtgjson_card["number"] == precon_card["number"]:
            mtgjson_card["count"] = precon_card["count"]
            mtgjson_card["isFoil"] = precon_card["foil"]
            cards.append(mtgjson_card)

    if not cards:
        LOGGER.warning("No match for {}".format(precon_card))

    return cards


if __name__ == "__main__":
    mtgjson4.init_logger()
    build_and_write_decks("/Users/zachary/Downloads/export_decks.json")
