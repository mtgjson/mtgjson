"""CardHoarder retrieval and processing."""

import configparser
import contextvars
import datetime
import json
import logging
import pathlib
from typing import Any, Dict, Iterator, List, Optional

import dateutil.relativedelta
import requests

import mtgjson4
from mtgjson4 import util

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_CARDHOARDER")
SESSION_TOKEN: contextvars.ContextVar = contextvars.ContextVar("CH_TOKEN")

CH_API_URL: str = "https://www.cardhoarder.com/affiliates/pricefile/{}"
CH_PRICE_DATA: Dict[str, Dict[str, str]] = {}

GH_API_USER = ""
GH_API_KEY = ""
GH_DB_KEY = ""
GH_DB_URL = ""
GH_DB_FILE = ""

TODAY_DATE = datetime.datetime.today().strftime("%Y-%m-%d")


def __get_session() -> requests.Session:
    """
    Get or create a requests session for CardHoarder.
    :return Session data
    """
    global GH_DB_URL, GH_DB_KEY, GH_API_KEY, GH_API_USER, GH_DB_FILE

    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
        session = requests.Session()

        if mtgjson4.CONFIG_PATH.is_file():
            # Open and read MTGJSON secret properties
            config = configparser.RawConfigParser()
            config.read(mtgjson4.CONFIG_PATH)
            SESSION_TOKEN.set(config.get("CardHoarder", "token"))

            GH_API_USER = config.get("CardHoarder", "gh_api_user")
            GH_API_KEY = config.get("CardHoarder", "gh_api_key")
            GH_DB_KEY = config.get("CardHoarder", "gh_db_key")
            GH_DB_FILE = config.get("CardHoarder", "gh_db_file")
            GH_DB_URL = f"https://gist.github.com/{GH_DB_KEY}"

        session = util.retryable_session(session)
        SESSION.set(session)
    return session


def prune_ch_database(
    database: Dict[str, Dict[str, Dict[str, float]]], months: int = 3
) -> None:
    """
    Prune entries from the CardHoarder database in which entries are
    older than X months
    :param database: Database to modify
    :param months: How many months back should we keep (default = 3)
    """
    prune_date = datetime.date.today() + dateutil.relativedelta.relativedelta(
        months=-months
    )

    for format_dicts in database.values():
        for value_dates in format_dicts.values():
            if not isinstance(value_dates, dict):
                continue

            # Determine keys to remove
            keys_to_prune = [
                key_date
                for key_date in value_dates.keys()
                if datetime.datetime.strptime(key_date, "%Y-%m-%d").date() < prune_date
            ]

            # Remove prunable keys
            for key in keys_to_prune:
                del value_dates[key]


def iterate_cards_and_tokens(
    all_printings_path: pathlib.Path,
) -> Iterator[Dict[str, Any]]:
    """
    Grab every card and token object from an AllPrintings file for future iteration
    :param all_printings_path: AllPrintings.json to refer when building
    :return Iterator for all card and token objects
    """
    all_printings_path = all_printings_path.expanduser()

    if not all_printings_path.exists():
        LOGGER.info("AllPrintings not found, downloading cached version")
        util.download_old_all_printings()

    with all_printings_path.open(encoding="utf-8") as f:
        file_contents = json.load(f)

    for value in file_contents.values():
        for card in value.get("cards", []) + value.get("tokens", []):
            yield card


def get_mtgo_to_mtgjson_map(all_printings_path: pathlib.Path) -> Dict[str, str]:
    """
    Construct a mapping from MTGO IDs (Regular & Foil) to MTGJSON UUIDs
    :param all_printings_path: AllPrintings to generate mapping from
    :return MTGO to MTGJSON mapping
    """
    mtgo_to_mtgjson = dict()
    for card in iterate_cards_and_tokens(all_printings_path):
        if "mtgoId" in card:
            mtgo_to_mtgjson[str(card["mtgoId"])] = card["uuid"]
        if "mtgoFoilId" in card:
            mtgo_to_mtgjson[str(card["mtgoFoilId"])] = card["uuid"]

    return mtgo_to_mtgjson


def __get_ch_data() -> Dict[str, Dict[str, str]]:
    """
    Get the stocks data for later use
    :return: All stocks data
    """
    global CH_PRICE_DATA

    if not CH_PRICE_DATA:
        # Ensure Session is created first to pull API keys
        __get_session()
        if not SESSION_TOKEN.get(""):
            LOGGER.warning("No CardHoarder token found, skipping...")
            CH_PRICE_DATA = {"Empty": {}}
            return CH_PRICE_DATA

        # Load cached database
        db_contents = util.get_gist_json_file(GH_DB_URL, GH_DB_FILE)

        # Prune cached database (We only store 3 months of data currently)
        prune_ch_database(db_contents)

        # Update cached version
        all_printings_path = mtgjson4.COMPILED_OUTPUT_DIR.joinpath("AllPrintings.json")
        mtgo_to_mtgjson = get_mtgo_to_mtgjson_map(all_printings_path)

        normal_cards = construct_ch_price_dict(CH_API_URL, mtgo_to_mtgjson)
        foil_cards = construct_ch_price_dict(CH_API_URL + "/foil", mtgo_to_mtgjson)

        for key, value in normal_cards.items():
            if key not in db_contents.keys():
                db_contents[key] = {
                    "mtgo": {},
                    "mtgoFoil": {},
                    "paper": {},
                    "paperFoil": {},
                }

            if "mtgo" not in db_contents[key].keys():
                db_contents[key]["mtgo"] = {}
            if "mtgoFoil" not in db_contents[key].keys():
                db_contents[key]["mtgoFoil"] = {}
            if "paper" not in db_contents[key].keys():
                db_contents[key]["paper"] = {}
            if "paperFoil" not in db_contents[key].keys():
                db_contents[key]["paperFoil"] = {}

            db_contents[key]["mtgo"][TODAY_DATE] = value
            db_contents[key]["mtgoFoil"][TODAY_DATE] = foil_cards.get(key)

        # Save new database to cache
        util.set_gist_json_file(
            GH_API_USER, GH_API_KEY, GH_DB_KEY, GH_DB_FILE, db_contents
        )
        CH_PRICE_DATA = db_contents

    return CH_PRICE_DATA


def construct_ch_price_dict(
    url_to_parse: str, mtgo_to_mtgjson_map: Dict[str, str]
) -> Dict[str, float]:
    """
    Turn CardHoarder API response into MTGJSON
    consumable format.
    :param url_to_parse: URL to pull CH data from
    :param mtgo_to_mtgjson_map: Mapping of MTGO Ids to MTGJSON UUIDs
    :return: MTGJSON dict
    """
    response: Any = __get_session().get(
        url=url_to_parse.format(SESSION_TOKEN.get("")), timeout=5.0
    )

    # Get data and log response
    request_api_response: str = response.content.decode()
    util.print_download_status(response)

    mtgjson_price_map = {}

    # All Entries from CH, cutting off headers
    file_rows: List[str] = request_api_response.splitlines()[2:]

    for file_row in file_rows:
        card_row = file_row.split("\t")

        mtgo_id = card_row[0]
        card_uuid = mtgo_to_mtgjson_map.get(mtgo_id)

        if not card_uuid:
            LOGGER.debug(f"CardHoarder {card_row} unable to be mapped, skipping")
            continue

        if len(card_row) <= 6:
            LOGGER.warning(f"CardHoarder entry {card_row} malformed, skipping")
            continue

        mtgjson_price_map[card_uuid] = float(card_row[5])

    LOGGER.info(mtgjson_price_map)
    return mtgjson_price_map


def get_card_data(mtgjson_uuid: str, limited: bool = False) -> Dict[str, Any]:
    """
    Get full price history of a specific card
    :param mtgjson_uuid: Card to get price history of
    :param limited: Should only return latest value
    :return: Price history
    """
    return_value: Dict[str, Any] = __get_ch_data().get(mtgjson_uuid) or dict()

    if limited:
        for key, value in return_value.items():
            if not isinstance(value, dict):
                continue

            if value:
                max_value = max(value)
                return_value[key] = {max_value: value[max_value]}

        if "uuid" in return_value.keys():
            del return_value["uuid"]

    return return_value
