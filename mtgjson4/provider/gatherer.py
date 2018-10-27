"""Card information provider for WotC Gatherer."""

import contextvars
import copy
from dataclasses import dataclass
import logging
from typing import List, Optional

import bs4
import requests

from mtgjson4 import util

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")

GATHERER_CARD = "http://gatherer.wizards.com/Pages/Card/Details.aspx"

SYMBOL_MAP = {
    "White": "W",
    "Blue": "U",
    "Black": "B",
    "Red": "R",
    "Green": "G",
    "Colorless": "C",
    "Variable Colorless": "X",
    "Snow": "S",
    "Energy": "E",
    "Phyrexian White": "PW",
    "Phyrexian Blue": "PU",
    "Phyrexian Black": "PB",
    "Phyrexian Red": "PR",
    "Phyrexian Green": "PG",
    "Two or White": "2W",
    "Two or Blue": "2U",
    "Two or Black": "2B",
    "Two or Red": "2R",
    "Two or Green": "2G",
    "White or Blue": "WU",
    "White or Black": "WB",
    "Blue or Black": "UB",
    "Blue or Red": "UR",
    "Black or Red": "BR",
    "Black or Green": "BG",
    "Red or Green": "RG",
    "Red or White": "GU",
    "Green or White": "RW",
    "Green or Blue": "GW",
    "Half a White": "HW",
    "Half a Blue": "HU",
    "Half a Black": "HB",
    "Half a Red": "HR",
    "Half a Green": "HG",
    "Tap": "T",
    "Untap": "Q",
    "Infinite": "∞",
}


def _get_session() -> requests.Session:
    """Get or create a requests session for gatherer."""
    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
        session = requests.Session()
        session = util.retryable_session(session)
        SESSION.set(session)
    return session


@dataclass
class GathererCard:
    """Response payload for fetching a card from Gatherer."""

    card_name: str
    original_types: str
    original_text: str
    flavor_text: str


def get_cards(multiverse_id: str) -> List[GathererCard]:
    """Get card(s) matching a given multiverseId."""
    session = _get_session()
    response = session.get(
        url=GATHERER_CARD,
        params={"multiverseid": multiverse_id, "printed": "true"},
        timeout=5.0,
    )
    LOGGER.info("Retrieved: %s", response.url)
    return parse_cards(response.text)


def parse_cards(gatherer_data: str) -> List[GathererCard]:
    """Parse all cards from a given gatherer page."""
    soup = bs4.BeautifulSoup(gatherer_data, "html.parser")
    columns = soup.findAll("td", class_="rightCol")
    return [_parse_column(c) for c in columns]


def _parse_column(gatherer_column: bs4.element.Tag) -> GathererCard:
    """Parse a single gatherer page 'rightCol' entry."""
    label_to_values = {
        row.find("div", class_="label")
        .getText(strip=True)
        .rstrip(":"): row.find("div", class_="value")
        for row in gatherer_column.findAll("div", class_="row")
    }

    card_name = label_to_values["Card Name"].getText(strip=True)
    card_types = label_to_values["Types"].getText(strip=True)

    if "Flavor Text" in label_to_values:
        flavor_text = "\n".join(
            ft.getText(strip=True)
            for ft in label_to_values["Flavor Text"].findAll(
                "div", class_="flavortextbox"
            )
        )
    else:
        flavor_text = ""

    if "Card Text" in label_to_values:
        card_text = "\n".join(
            _replace_symbols(ct).getText(strip=True)
            for ct in label_to_values["Card Text"].findAll("div", class_="cardtextbox")
        )
    else:
        card_text = ""

    return GathererCard(
        card_name=card_name,
        original_types=card_types,
        original_text=card_text,
        flavor_text=flavor_text,
    )


def _replace_symbols(tag: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
    """Replace all image tags with their mapped symbol."""
    tag_copy = copy.copy(tag)
    images = tag_copy.find_all("img")
    for image in images:
        alt = image["alt"]
        symbol = SYMBOL_MAP.get(alt, alt)
        image.replace_with("{{" + symbol + "}}")
    return tag_copy
