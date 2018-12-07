"""Gamepedia retrieval and processing"""
import contextvars
from typing import List

import bs4
from mtgjson4.provider import scryfall
import requests

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_GAMEPEDIA")

MODERN_GAMEPEDIA_URL: str = "https://mtg.gamepedia.com/Modern"


def strip_bad_sf_chars(bad_text: str) -> str:
    """
    Since we're searching Scryfall via name and not set code, we will
    have to strip the names to the bare minimums to get a valid result
    back.
    """
    for bad_char in [" ", ":", "'"]:
        bad_text = bad_text.replace(bad_char, "")

    return bad_text


def get_modern_sets() -> List[str]:
    """
    Pull the modern legal page from Gamepedia and parse it out
    to get the sets that are legal in modern
    :return: List of set codes legal in modern
    """
    modern_page_content = requests.get(MODERN_GAMEPEDIA_URL)

    soup = bs4.BeautifulSoup(modern_page_content.text, "html.parser")
    soup = soup.find("div", class_="div-col columns column-width")
    soup = soup.findAll("a")

    modern_legal_sets = [
        scryfall.get_set_header(strip_bad_sf_chars(x.text)).get("code", "").upper()
        for x in soup
    ]

    return modern_legal_sets
