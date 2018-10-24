"""WoTC Gatherer retrieval and processing."""

import contextvars
import logging

import bs4
import requests
import requests.adapters

from mtgjson4 import util

LOGGER = logging.getLogger(__name__)

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")

GATHERER_CARD: str = "http://gatherer.wizards.com/Pages/Card/Details.aspx"


def attach_session() -> None:
    """Attach a session for gatherer."""
    session = requests.Session()
    SESSION.set(session)


def download(card_mid: str) -> bs4.BeautifulSoup:
    """
    Download a specific card from gatherer
    :param card_mid: card id to download
    :return: HTML soup parser of the resulting page
    """
    session = util.retryable_session(SESSION.get())
    response = session.get(
        url=GATHERER_CARD,
        params={"multiverseid": str(card_mid), "printed": "true"},
        timeout=5.0,
    )

    soup: bs4.BeautifulSoup = bs4.BeautifulSoup(response.text, "html.parser")
    LOGGER.info("Downloaded URL {}".format(response.url))
    return soup
