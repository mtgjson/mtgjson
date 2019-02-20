"""Card information provider for Precons."""
import json
import logging
import multiprocessing
import pathlib
from typing import Any, Dict, Generator, List

import mtgjson4
import mtgjson4.util

LOGGER = logging.getLogger(__name__)


def build_and_write_decks(decks_path: str) -> Generator[Dict[str, Any], None, None]:
    """
    Given the path to the precons list, this will
    compile them in MTGJSONv4 format and write out
    the decks to the "decks/" folder.
    """
    with pathlib.Path(decks_path).open("r", encoding="utf-8") as f:
        content = json.load(f)

    for deck in content:
        deck_to_output = {
            "name": deck["name"],
            "code": deck["set_code"].upper(),
            "type": deck["type"],
            "mainBoard": [],
            "sideBoard": [],
            "meta": {
                "version": mtgjson4.__VERSION__,
                "date": mtgjson4.__VERSION_DATE__,
            },
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

        LOGGER.info("Finished set {}".format(deck["name"]))
        yield deck_to_output


def build_single_card(deck_card: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build the MTGJSONv4 card for each pre-con card
    in the deck.
    :param deck_card: Card to build Precon format
    :return: Card(s) built from the card MTGJSONv4 format
    """
    with mtgjson4.COMPILED_OUTPUT_DIR.joinpath(
        mtgjson4.util.get_mtgjson_set_code(deck_card["set_code"].upper()) + ".json"
    ).open("r") as f:
        compiled_file = json.load(f)

    cards = []
    for mtgjson_card in compiled_file["cards"]:
        if "//" in deck_card["name"]:
            if deck_card["number"][-1].isalpha():
                deck_card["number"] = deck_card["number"][:-1]

        if mtgjson_card["number"] == deck_card["number"]:
            mtgjson_card["count"] = deck_card["count"]
            mtgjson_card["isFoil"] = deck_card["foil"]
            cards.append(mtgjson_card)

    if not cards:
        LOGGER.warning("No match for {}".format(deck_card))

    return cards
