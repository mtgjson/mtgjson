"""
MTGStocks operating class for price data
"""

import configparser
import contextvars
import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests
import requests_cache

import mtgjson4
from mtgjson4 import util

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_STOCKS")
SESSION_TOKEN: contextvars.ContextVar = contextvars.ContextVar("SESSION_STOCKS")
STOCKS_DATA: contextvars.ContextVar = contextvars.ContextVar("STOCKS_DATA")

MTG_STOCKS_API_URL: str = "https://api.mtgstocks.com/api/v1/mtgjson?token={}"
MTG_STOCKS_REFERRAL_URL: str = "https://www.mtgstocks.com/prints/{}"


def __get_session() -> requests.Session:
    """
    Get or create a requests session for MTGStocks.
    :return Session data
    """
    if mtgjson4.USE_CACHE.get():
        requests_cache.install_cache(
            "stocks_cache",
            backend="sqlite",
            expire_after=mtgjson4.SESSION_CACHE_EXPIRE_STOCKS,
        )

    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
        session = requests.Session()

        if mtgjson4.CONFIG_PATH.is_file():
            # Open and read MTGJSON secret properties
            config = configparser.RawConfigParser()
            config.read(mtgjson4.CONFIG_PATH)
            SESSION_TOKEN.set(config.get("MTGStocks", "token"))

        session = util.retryable_session(session)
        SESSION.set(session)
    return session


def __get_stocks_data() -> Dict[str, Any]:
    """
    Get the stocks data for later use
    :return: All stocks data
    """
    if not STOCKS_DATA.get(None):
        stocks_file = mtgjson4.RESOURCE_PATH.joinpath("stocks_data.json")

        is_file = stocks_file.is_file()
        cache_expired = (
            is_file
            and time.time() - stocks_file.stat().st_mtime
            > mtgjson4.SESSION_CACHE_EXPIRE_GENERAL
        )

        if (not is_file) or cache_expired:
            # Rebuild set translations
            session = __get_session()
            if not SESSION_TOKEN.get(""):
                LOGGER.warning("No MTGStocks token found, skipping...")
                return {}

            response: Any = session.get(
                url=MTG_STOCKS_API_URL.format(SESSION_TOKEN.get("")), timeout=5.0
            )
            request_api_json: List[Dict[str, Any]] = response.json()
            util.print_download_status(response)

            save_dictionary = {}
            for row in request_api_json:
                save_dictionary[row["tcg_id"]] = row

            with stocks_file.open("w") as f:
                json.dump(save_dictionary, f, indent=4)
                f.write("\n")

        STOCKS_DATA.set(json.load(stocks_file.open("r")))

    return dict(STOCKS_DATA.get())


def get_card_data(tcgplayer_id: int) -> Optional[Dict[str, Any]]:
    """
    Get MTGStocks data for a specific card, identified by TCGPlayer ID
    :param tcgplayer_id: ID to find card by
    :return: MTGStocks map
    """
    return __get_stocks_data().get(str(tcgplayer_id), None)
