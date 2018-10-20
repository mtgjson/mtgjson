"""Scryfall retrieval and processing."""

import configparser
import contextvars
import logging
import multiprocessing
import pathlib
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import requests.adapters

import mtgjson4
from mtgjson4 import compile_mtg
from mtgjson4 import util

LOGGER = logging.getLogger(__name__)

SESSION = contextvars.ContextVar("SESSION")
AUTH_STATUS = contextvars.ContextVar("AUTH_STATUS")

SCRYFALL_API_SETS: str = "https://api.scryfall.com/sets/"


def attach_session():
    """
    The session can be static, so this will be a singleton creation
    """
    session = requests.Session()

    auth_status: str = "with authentication"
    if pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
        # Open and read MTGJSON secret properties
        config = configparser.RawConfigParser()
        config.read(mtgjson4.CONFIG_PATH)
        header_auth: Dict[str, str] = {
            "Authorization": "Bearer {}".format(config.get("Scryfall", "client_secret"))
        }
        session.headers.update(header_auth)
    else:
        auth_status = "WITHOUT authentication"

    SESSION.set(session)
    AUTH_STATUS.set(auth_status)


def download(scryfall_url: str) -> Dict[str, Any]:
    """
    Get the data from Scryfall in JSON format using our secret keys
    :param scryfall_url: URL to download JSON data from
    :return: JSON object of the Scryfall data
    """
    session = util.retryable_session(SESSION.get())
    response = session.get(url=scryfall_url, timeout=5.0)
    request_api_json: Dict[str, Any] = response.json()

    LOGGER.info("Downloaded URL: {0}".format(scryfall_url))

    return request_api_json


def convert_to_mtgjson(sf_cards: List[Dict[str, Any]]) -> List[Any]:
    """
    Parallel method to build each card in the set
    :param sf_cards: cards to build
    :return: list of cards built
    """
    with multiprocessing.Pool(processes=8) as pool:
        results: List[Any] = pool.map(compile_mtg.build_mtgjson_card, sf_cards)

        all_cards: List[Dict[str, Any]] = []
        for cards in results:
            for card in cards:
                all_cards.append(card)
    return all_cards


def get_set(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    LOGGER.info("Downloading set {} information".format(set_code))
    set_api_json: Dict[str, Any] = download(SCRYFALL_API_SETS + set_code)
    if set_api_json["object"] == "error":
        LOGGER.warning(
            "Set api download failed for {0}: {1}".format(set_code, set_api_json)
        )
        return []

    cards_api_url: Optional[str] = set_api_json.get("search_uri")

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = []

    # For each page, append all the data, go to next page
    page_downloaded: int = 1
    while cards_api_url is not None:
        LOGGER.info(
            "Downloading page {0} of card data for {1}".format(
                page_downloaded, set_code
            )
        )
        page_downloaded += 1

        cards_api_json: Dict[str, Any] = download(cards_api_url)
        if cards_api_json["object"] == "error":
            LOGGER.error("Error downloading {0}: {1}".format(set_code, cards_api_json))
            return scryfall_cards

        for card in cards_api_json["data"]:
            scryfall_cards.append(card)

        if cards_api_json.get("has_more"):
            cards_api_url = cards_api_json.get("next_page")
        else:
            cards_api_url = None

    return scryfall_cards


def parse_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = download(rulings_url)
    if rules_api_json["object"] == "error":
        LOGGER.error(
            "Error downloading URL {0}: {1}".format(rulings_url, rules_api_json)
        )

    sf_rules: List[Dict[str, str]] = []
    mtgjson_rules: List[Dict[str, str]] = []

    for rule in rules_api_json["data"]:
        sf_rules.append(rule)

    for sf_rule in sf_rules:
        mtgjson_rule: Dict[str, str] = {
            "date": sf_rule["published_at"],
            "text": sf_rule["comment"],
        }
        mtgjson_rules.append(mtgjson_rule)

    return mtgjson_rules


def parse_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Given a card type string, split it up into its raw components: super, sub, and type
    :param card_type: Card type string to parse
    :return: Tuple (super, type, sub) of the card's attributes
    """
    sub_types: List[str] = []
    super_types: List[str] = []
    types: List[str] = []

    if "—" in card_type:
        split_type: List[str] = card_type.split("—")
        supertypes_and_types: str = split_type[0]
        subtypes: str = split_type[1]
        sub_types = [x for x in subtypes.split(" ") if x]
    else:
        supertypes_and_types = card_type

    for value in supertypes_and_types.split(" "):
        if value in mtgjson4.SUPERTYPES:
            super_types.append(value)
        elif value:
            types.append(value)

    return super_types, types, sub_types


def parse_legalities(sf_card_legalities: Dict[str, str]) -> Dict[str, str]:
    """
    Given a Scryfall legalities dictionary, convert it to MTGJSON format
    :param sf_card_legalities: Scryfall legalities
    :return: MTGJSON legalities
    """
    card_legalities: Dict[str, str] = {}
    for key, value in sf_card_legalities.items():
        if value != "not_legal":
            card_legalities[key] = value.capitalize()

    return card_legalities


def parse_foreign(
    sf_prints_url: str, card_name: str, set_name: str
) -> List[Dict[str, str]]:
    """
    Get the foreign printings information for a specific card
    :param sf_prints_url: URL to get prints from
    :param card_name: Card name to parse (needed for double faced)
    :param set_name: Set name
    :return: Foreign entries object
    """
    card_foreign_entries: List[Dict[str, str]] = []

    # Add information to get all languages
    sf_prints_url = sf_prints_url.replace("&unique=prints", "+lang%3Aany&unique=prints")

    prints_api_json: Dict[str, Any] = download(sf_prints_url)
    if prints_api_json["object"] == "error":
        LOGGER.error(
            "No data found for {0}: {1}".format(sf_prints_url, prints_api_json)
        )
        return []

    for foreign_card in prints_api_json["data"]:
        if set_name != foreign_card["set"] or foreign_card["lang"] == "en":
            continue

        card_foreign_entry: Dict[str, str] = {}
        try:
            card_foreign_entry["language"] = mtgjson4.LANGUAGE_MAP[foreign_card["lang"]]
        except IndexError:
            LOGGER.warning("Error trying to get language {}".format(foreign_card))

        try:
            card_foreign_entry["multiverseId"] = foreign_card["multiverse_ids"][0]
        except IndexError:
            LOGGER.warning("Error trying to get multiverseId {}".format(foreign_card))

        if "card_faces" in foreign_card:
            if card_name.lower() == foreign_card["name"].split("/")[0].strip().lower():
                face = 0
            else:
                face = 1

            foreign_card = foreign_card["card_faces"][face]
            LOGGER.info(
                "Split card found: Using face {0} for {1}".format(face, card_name)
            )

        card_foreign_entry["name"] = foreign_card.get("printed_name")
        card_foreign_entry["text"] = foreign_card.get("printed_text")
        card_foreign_entry["flavorText"] = foreign_card.get("flavor_text")
        card_foreign_entry["type"] = foreign_card.get("printed_type_line")

        card_foreign_entries.append(card_foreign_entry)

    return card_foreign_entries


def parse_printings(sf_prints_url: str) -> List[str]:
    """
    Given a Scryfall printings URL, extract all sets a card was printed in
    :param sf_prints_url: URL to extract data from
    :return: List of all sets a specific card was printed in
    """
    card_sets: Set[str] = set()

    prints_api_json: Dict[str, Any] = download(sf_prints_url)
    if prints_api_json["object"] == "error":
        LOGGER.error("Bad download: {}".format(sf_prints_url))
        return []

    for card in prints_api_json["data"]:
        card_sets.add(card.get("set").upper())

    return list(card_sets)
