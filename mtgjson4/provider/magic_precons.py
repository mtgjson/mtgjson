"""Card information provider for Precons."""
import contextvars
import json
import logging
import multiprocessing
import pathlib
from typing import Any, Dict, Iterator, List

import requests

import mtgjson4
import mtgjson4.util

LOGGER = logging.getLogger(__name__)

SET_SESSION: contextvars.ContextVar = contextvars.ContextVar("ALL_SETS_SESSION")


def build_and_write_decks(decks_url: str) -> Iterator[Dict[str, Any]]:
    """
    Given the URL to the precons list, this will
    compile them in MTGJSONv4 format and write out
    the decks to the "decks/" folder.
    :return Each deck completed, one by one
    """
    decks_content: Any = requests.get(decks_url).json()
    LOGGER.info("Downloaded: {} (Cache = {})".format(decks_url, False))

    # Location of AllSets.json -- Must be compiled before decks!
    all_sets_path: pathlib.Path = mtgjson4.COMPILED_OUTPUT_DIR.joinpath(
        mtgjson4.ALL_SETS_OUTPUT + ".json"
    )

    file_loaded: bool = False
    # Does the file exist
    if all_sets_path.is_file():
        # Is the file > 100MB? (Ensure we have all sets in it)
        if all_sets_path.stat().st_size > 1e8:
            with all_sets_path.open("r") as f:
                SET_SESSION.set(json.load(f))
                file_loaded = True

    if not file_loaded:
        LOGGER.warning("AllSets must be fully compiled before decks. Aborting.")
        return

    with multiprocessing.Pool(processes=8) as pool:
        for deck in decks_content:
            deck_to_output = {
                "name": deck["name"],
                "code": deck["set_code"].upper(),
                "type": deck["type"],
                "releaseDate": deck["release_date"],
                "mainBoard": [],
                "sideBoard": [],
                "meta": {
                    "version": mtgjson4.__VERSION__,
                    "date": mtgjson4.__VERSION_DATE__,
                },
            }

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

            LOGGER.info("Finished deck {}".format(deck["name"]))
            yield deck_to_output


def build_single_card(deck_card: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build the MTGJSONv4 card for each pre-con card
    in the deck.
    :param deck_card: Card to build Precon format
    :return: Card(s) built from the card MTGJSONv4 format
    """
    cards = []
    for mtgjson_card in SET_SESSION.get()[
        mtgjson4.util.get_mtgjson_set_code(deck_card["set_code"].upper())
    ]["cards"]:
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
