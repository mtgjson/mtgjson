"""TCGPlayer retrieval and processing."""

import configparser
import contextvars
import json
import logging
import pathlib
from typing import Any, Dict, List, Optional

import mtgjson4
from mtgjson4 import util
import requests

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_TCGPLAYER")
TCGPLAYER_API_VERSION: contextvars.ContextVar = contextvars.ContextVar("API_TCGPLAYER")


def __get_session() -> requests.Session:
    """Get or create a requests session for TCGPlayer."""
    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
        session = requests.Session()
        header_auth = {"Authorization": "Bearer " + _request_tcgplayer_bearer()}
        session.headers.update(header_auth)
        session = util.retryable_session(session)
        SESSION.set(session)
    return session


def _request_tcgplayer_bearer() -> str:
    """
    Attempt to get the latest TCGPlayer Bearer token for
    API access. Use the credentials found in the local
    config to contact the server.
    :return: Empty string or current Bearer token
    """
    if not pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
        LOGGER.error(
            "Unable to import TCGPlayer keys. Config at {} not found".format(
                mtgjson4.CONFIG_PATH
            )
        )
        return ""

    config = configparser.RawConfigParser()
    config.read(mtgjson4.CONFIG_PATH)

    tcg_post = requests.post(
        "https://api.tcgplayer.com/token",
        data={
            "grant_type": "client_credentials",
            "client_id": config.get("TCGPlayer", "client_id"),
            "client_secret": config.get("TCGPlayer", "client_secret"),
        },
    )

    if tcg_post.status_code != 200:
        LOGGER.error("Unable to contact TCGPlayer. Reason: {}".format(tcg_post.reason))
        return ""

    TCGPLAYER_API_VERSION.set(config.get("TCGPlayer", "api_version"))
    request_as_json = json.loads(tcg_post.text)
    return str(request_as_json.get("access_token", ""))


def download(tcgplayer_url: str, params_str: Dict[str, Any] = None) -> str:
    """
    Download content from TCGPlayer with a given URL that
    can include a wildcard for default API version, as well
    as a way to pass in custom params to the URL
    :param tcgplayer_url: URL to get information from
    :param params_str: Additional params to pass to TCGPlayer call
    :return: Data from TCGPlayer API Call
    """
    if params_str is None:
        params_str = {}

    session = __get_session()

    response = session.get(
        url=tcgplayer_url.replace("[API_VERSION]", TCGPLAYER_API_VERSION.get("")),
        params=params_str,
        timeout=5.0,
    )

    LOGGER.info("Downloaded URL: {0}".format(response.url))
    session.close()

    if response.status_code != 200:
        if response.status_code == 404:
            LOGGER.info(
                "Status Code: {} Failed to download from TCGPlayer with URL: {}, Params: {}".format(
                    response.status_code, response.url, params_str
                )
            )
        else:
            LOGGER.warning(
                "Status Code: {} Failed to download from TCGPlayer with URL: {}, Params: {}".format(
                    response.status_code, response.url, params_str
                )
            )
        return ""

    return response.text


def get_group_id(set_code: str) -> int:
    """
    Find the TCGPlayer group ID for a specific set
    :param set_code: Set to find group ID for
    :return: Group ID or Not found (-1)
    """
    offset = 0
    # TCGPlayer will only send 100 results at a time, so we need to
    # page through the data to find the appropriate set
    while True:
        tcg_data = json.loads(
            download(
                "http://api.tcgplayer.com/[API_VERSION]/catalog/categories/1/groups",
                {"limit": "100", "offset": offset},
            )
        )

        if not tcg_data["results"]:
            break

        for set_content in tcg_data["results"]:
            if set_content["abbreviation"] == set_code.upper():
                return int(set_content.get("groupId", -1))

        offset += len(tcg_data["results"])
    return -1


def get_group_id_cards(group_id: int) -> List[Dict[str, Any]]:
    """
    Given a group_id, get all the cards within that set.
    :param group_id: Set to get all cards from
    :return: List of card objects
    """
    if group_id < 0:
        LOGGER.error(
            "Unable to get cards from a negative group_id: {}".format(group_id)
        )
        return []

    cards: List[Dict[str, Any]] = []
    offset = 0

    while True:
        response = download(
            "http://api.tcgplayer.com/[API_VERSION]/catalog/products",
            {
                "categoryId": "1",  # MTG
                "groupId": group_id,
                "productTypes": "Cards",  # Cards only
                "limit": 100,
                "offset": offset,
            },
        )

        if not response:
            break

        tcg_data = json.loads(response)

        if not tcg_data["results"]:
            break

        cards += tcg_data["results"]
        offset += len(tcg_data["results"])

    return cards


def get_card_property(
    card_name: str, card_list: List[Dict[str, Any]], card_field: str
) -> Any:
    """
    Go through the passed in card object list to find the matching
    card from the set and get its attribute.
    :param card_name: Card name to find in the list
    :param card_list: List of TCGPlayer card objects
    :param card_field: Field to pull from TCGPlayer card object
    :return: Value of field
    """
    for card in card_list:
        if card_name.lower() == card["name"].lower():
            return card.get(card_field, None)

    return None
