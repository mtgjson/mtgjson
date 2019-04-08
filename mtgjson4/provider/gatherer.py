"""Card information provider for WotC Gatherer."""
import contextvars
import copy
import logging
from typing import Any, List, NamedTuple, Optional

import bs4

from mtgjson4 import util

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_GATHERER")

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


class GathererCard(NamedTuple):
    """Response payload for fetching a card from Gatherer."""

    card_name: str
    original_types: str
    original_text: Optional[str]
    flavor_text: Optional[str]


def get_cards(multiverse_id: str) -> List[GathererCard]:
    """Get card(s) matching a given multiverseId."""
    session = util.get_generic_session()
    response: Any = session.get(
        url=GATHERER_CARD,
        params={"multiverseid": multiverse_id, "printed": "true"},
        timeout=8.0,
    )

    util.print_download_status(response)
    session.close()

    return parse_cards(response.text)


def parse_cards(gatherer_data: str) -> List[GathererCard]:
    """Parse all cards from a given gatherer page."""
    soup = bs4.BeautifulSoup(gatherer_data, "html.parser")
    columns = soup.find_all("td", class_="rightCol")
    return [_parse_column(c) for c in columns]


def _parse_column(gatherer_column: bs4.element.Tag) -> GathererCard:
    """Parse a single gatherer page 'rightCol' entry."""
    label_to_values = {
        row.find("div", class_="label")
        .getText(strip=True)
        .rstrip(":"): row.find("div", class_="value")
        for row in gatherer_column.find_all("div", class_="row")
    }

    card_name = label_to_values["Card Name"].getText(strip=True)
    card_types = label_to_values["Types"].getText(strip=True)

    flavor_lines = []
    if "Flavor Text" in label_to_values:
        for flavorbox in label_to_values["Flavor Text"].find_all(
            "div", class_="flavortextbox"
        ):
            flavor_lines.append(flavorbox.getText(strip=True))

    text_lines = []
    if "Card Text" in label_to_values:
        for textbox in label_to_values["Card Text"].find_all(
            "div", class_="cardtextbox"
        ):
            text_lines.append(_replace_symbols(textbox).getText().strip())

    return GathererCard(
        card_name=card_name,
        original_types=card_types,
        original_text="\n".join(text_lines).strip() or None,
        flavor_text="\n".join(flavor_lines).strip() or None,
    )


def _replace_symbols(tag: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
    """Replace all image tags with their mapped symbol."""
    tag_copy = copy.copy(tag)
    images = tag_copy.find_all("img")
    for image in images:
        alt = image["alt"]
        symbol = SYMBOL_MAP.get(alt, alt)
        image.replace_with("{" + symbol + "}")
    return tag_copy
