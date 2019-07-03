"""TCGPlayer retrieval and processing."""
import configparser
import contextvars
import json
import logging
from typing import Any, Dict, List, Optional

import requests
import requests_cache

import mtgjson4
from mtgjson4 import util
from mtgjson4.util import url_keygen

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_TCGPLAYER")
TCGPLAYER_API_VERSION: contextvars.ContextVar = contextvars.ContextVar("API_TCGPLAYER")


def __get_session() -> requests.Session:
    """Get or create a requests session for TCGPlayer."""
    if mtgjson4.USE_CACHE.get():
        requests_cache.install_cache(
            "tcg_cache",
            backend="sqlite",
            expire_after=mtgjson4.SESSION_CACHE_EXPIRE_TCG,
        )

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
    if not mtgjson4.CONFIG_PATH.is_file():
        LOGGER.error(f"Unable to import TCGPlayer keys. Config at {mtgjson4.CONFIG_PATH} not found")
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
        LOGGER.error(f"Unable to contact TCGPlayer. Reason: {tcg_post.reason}")
        return ""

    TCGPLAYER_API_VERSION.set(config.get("TCGPlayer", "api_version"))
    request_as_json = json.loads(tcg_post.text)
    return str(request_as_json.get("access_token", ""))


def download(tcgplayer_url: str, params_str: Dict[str, Any] = None) -> Optional[str]:
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

    try:
        session = __get_session()
    except configparser.NoOptionError:
        return None

    response: Any = session.get(
        url=tcgplayer_url.replace("[API_VERSION]", TCGPLAYER_API_VERSION.get("")),
        params=params_str,
        timeout=5.0,
    )

    util.print_download_status(response)
    session.close()

    if response.status_code != 200:
        if response.status_code == 404:
            LOGGER.info("Status Code: {response.status_code} Failed to download from TCGPlayer with URL: {response.url}, Params: {params_str}")
        else:
            LOGGER.warning(f"Status Code: {response.status_code} Failed to download from TCGPlayer with URL: {response.url}, Params: {params_str}")
        return None

    return str(response.text)


def get_group_id_cards(group_id: int) -> List[Dict[str, Any]]:
    """
    Given a group_id, get all the cards within that set.
    :param group_id: Set to get all cards from
    :return: List of card objects
    """
    if group_id < 0:
        LOGGER.error(f"Unable to get cards from a negative group_id: {group_id}")
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


def get_redirection_url(prod_id: int) -> str:
    """
    Create the URL that can be accessed to get the TCGPlayer URL.
    Also builds up the redirection table, that can be called later.
    :param prod_id: ID of card/object
    :return: URL that can be used
    """
    return f"https://mtgjson.com/links/{url_keygen(prod_id)}"
