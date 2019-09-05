"""Scryfall retrieval and processing."""

import configparser
import contextvars
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import requests.adapters
import requests_cache

import mtgjson4
from mtgjson4 import util

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_SCRYFALL")


SCRYFALL_API_SETS: str = "https://api.scryfall.com/sets/"
SCRYFALL_API_CARD: str = "https://api.scryfall.com/cards/"
SCRYFALL_API_CATALOG: str = "https://api.scryfall.com/catalog/{0}-types"
SCRYFALL_VARIATIONS: str = "https://api.scryfall.com/cards/search?q=is%3Avariation%20set%3A{0}&unique=prints"
SCRYFALL_SET_SIZE: str = "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20is:booster%20unique:prints"
SCRYFALL_API_SEARCH: str = "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)"

BASE_SET_FILE_CACHE: contextvars.ContextVar = contextvars.ContextVar(
    "BASE_SET_FILE_CACHE"
)


def __get_session() -> requests.Session:
    """Get or create a requests session for scryfall."""
    if mtgjson4.USE_CACHE.get():
        requests_cache.install_cache(
            str(mtgjson4.PROJECT_CACHE_PATH.joinpath("scryfall_cache")),
            expire_after=mtgjson4.SESSION_CACHE_EXPIRE_SCRYFALL,
        )

    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
        session = requests.Session()

        if mtgjson4.CONFIG_PATH.is_file():
            # Open and read MTGJSON secret properties
            config = configparser.RawConfigParser()
            config.read(mtgjson4.CONFIG_PATH)

            if config.get("Scryfall", "client_secret"):
                header_auth = {
                    "Authorization": "Bearer " + config.get("Scryfall", "client_secret")
                }
                session.headers.update(header_auth)
                LOGGER.info("Fetching from Scryfall with authentication")
            else:
                LOGGER.warning("Fetching from Scryfall WITHOUT authentication")
        else:
            LOGGER.warning("Fetching from Scryfall WITHOUT authentication")

        session = util.retryable_session(session)
        SESSION.set(session)
    return session


def get_base_set_size(set_code: str) -> int:
    """
    Get the size of a set from scryfall or corrections file
    :param set_code: Set code, upper case
    :return: Amount of cards in set
    """
    # Load cache if not loaded
    if not BASE_SET_FILE_CACHE.get(None):
        with mtgjson4.RESOURCE_PATH.joinpath("base_set_sizes.json").open(
            "r", encoding="utf-8"
        ) as f:
            BASE_SET_FILE_CACHE.set(json.load(f))

    # Manual correction
    if set_code in BASE_SET_FILE_CACHE.get().keys():
        return int(BASE_SET_FILE_CACHE.get()[set_code])

    # Download on the fly
    content = download(SCRYFALL_SET_SIZE.format(set_code))
    if content["object"] == "error":
        LOGGER.warning(f"Unable to get set size for {set_code}")
        return 0

    return int(content["total_cards"])


def download(scryfall_url: str) -> Dict[str, Any]:
    """
    Get the data from Scryfall in JSON format using our secret keys
    :param scryfall_url: URL to download JSON data from
    :return: JSON object of the Scryfall data
    """
    session = __get_session()
    response: Any = session.get(url=scryfall_url, timeout=8.0)
    request_api_json: Dict[str, Any] = response.json()
    util.print_download_status(response)
    session.close()
    return request_api_json


def get_set_header(set_name: str) -> Dict[str, Any]:
    """
    Get just the header (not card contents) of a set by its name
    :param set_name:
    :return:
    """
    set_api_json: Dict[str, Any] = download(SCRYFALL_API_SETS + set_name)
    if set_api_json["object"] == "error":
        LOGGER.warning(f"Set header api download failed for {set_name}: {set_api_json}")
        return {}

    return set_api_json


def get_set(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    LOGGER.info(f"Downloading set {set_code} information")
    set_api_json: Dict[str, Any] = download(SCRYFALL_API_SETS + set_code)
    if set_api_json["object"] == "error":
        if not set_api_json["details"].startswith("No Magic set found"):
            LOGGER.warning(f"Set api download failed for {set_code}: {set_api_json}")
        return []

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = []

    # Download both normal card and variations
    for cards_api_url in [
        set_api_json.get("search_uri"),
        SCRYFALL_VARIATIONS.format(set_code),
    ]:
        # For each page, append all the data, go to next page
        page_downloaded: int = 1
        while cards_api_url:
            LOGGER.info(
                f"Downloading page {page_downloaded} of card data for {set_code}"
            )
            page_downloaded += 1

            cards_api_json: Dict[str, Any] = download(cards_api_url)
            if cards_api_json["object"] == "error":
                if not cards_api_json["details"].startswith("Your query didn’t match"):
                    LOGGER.warning(f"Error downloading {set_code}: {cards_api_json}")
                break

            # Append all cards on this page
            for card_obj in cards_api_json["data"]:
                scryfall_cards.append(card_obj)

            # Go to the next page, if it exists
            if not cards_api_json.get("has_more"):
                break

            cards_api_url = cards_api_json.get("next_page")

    # Return sorted by card name, and by card number if the same name is found
    return sorted(
        scryfall_cards, key=lambda card: (card["name"], card["collector_number"])
    )


def parse_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = download(rulings_url)
    if rules_api_json["object"] == "error":
        LOGGER.error(f"Error downloading URL {rulings_url}: {rules_api_json}")

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

        # Planes are an entire sub-type, whereas normal cards
        # are split by spaces
        if card_type.startswith("Plane"):
            sub_types = [subtypes.strip()]
        else:
            sub_types = [x.strip() for x in subtypes.split(" ") if x]
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
    sf_prints_url: str, card_name: str, card_number: str, set_name: str
) -> List[Dict[str, str]]:
    """
    Get the foreign printings information for a specific card
    :param card_number: Card's number
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
        LOGGER.error(f"No data found for {sf_prints_url}: {prints_api_json}")
        return []

    for foreign_card in prints_api_json["data"]:
        if (
            set_name != foreign_card["set"]
            or card_number != foreign_card["collector_number"]
            or foreign_card["lang"] == "en"
        ):
            continue

        card_foreign_entry: Dict[str, str] = {}
        try:
            card_foreign_entry["language"] = mtgjson4.LANGUAGE_MAP[foreign_card["lang"]]
        except IndexError:
            LOGGER.warning(f"Unable to get language {foreign_card}")

        try:
            card_foreign_entry["multiverseId"] = foreign_card["multiverse_ids"][0]
        except IndexError:
            LOGGER.warning(f"Unable to get multiverseId {foreign_card['name']}")

        if "card_faces" in foreign_card:
            if card_name.lower() == foreign_card["name"].split("/")[0].strip().lower():
                face = 0
            else:
                face = 1

            foreign_card = foreign_card["card_faces"][face]
            LOGGER.info(f"Split card found: Using face {face} for {card_name}")

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

    while sf_prints_url:
        prints_api_json: Dict[str, Any] = download(sf_prints_url)
        if prints_api_json["object"] == "error":
            LOGGER.error(f"Bad download: {sf_prints_url}")
            break

        for card in prints_api_json["data"]:
            card_sets.add(card.get("set").upper())

        if not prints_api_json.get("has_more"):
            break

        sf_prints_url = prints_api_json.get("next_page", "")

    return list(card_sets)


def get_catalog(key: str) -> List[str]:
    """
    Grab the Scryfall catalog of appropriate types
    :param key: Type to find
    :return: List of values found
    """
    return list(download(SCRYFALL_API_CATALOG.format(key))["data"])


def get_cards_without_limit() -> Set[str]:
    """
    Grab all cards that can have as many copies
    in a deck as the player wants
    :return: Set of valid cards
    """
    return {
        card["name"]
        for card in download(
            "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named%20or%20t:basic)"
        )["data"]
    }
