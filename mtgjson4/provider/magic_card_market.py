"""Magic Card Market retrieval and processing."""

import contextvars
import json
import logging
import re
from typing import Any, Dict, List, Optional

import bs4

import mtgjson4
from mtgjson4 import util

# The offerings of MKM as of 2019-03-19
CARD_MARKET_LANGUAGES: List[Dict[str, str]] = [
    {"code": "en", "lang": "English"},
    {"code": "fr", "lang": "French"},
    {"code": "de", "lang": "German"},
    {"code": "it", "lang": "Italian"},
    {"code": "es", "lang": "Spanish"},
]

CARD_MARKET_URL: str = "https://www.cardmarket.com/{}/Magic/Expansions"
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_MKM")
TRANSLATION_TABLE: contextvars.ContextVar = contextvars.ContextVar("TRANSLATION_TABLE")
LOGGER = logging.getLogger(__name__)


def get_translations(set_code: Optional[str] = None) -> Any:
    """
    Get the translation table that was pre-built OR a specific
    set from the translation table. Will also build the
    table if necessary.
    Return value w/o set_code: {SET_CODE: {SET_LANG: TRANSLATION, ...}, ...}
    Return value w/  set_code: SET_CODE: {SET_LANG: TRANSLATION, ...}
    :return: Translation table
    """
    if not TRANSLATION_TABLE.get(None):
        build_translation_table()

    if set_code:
        return TRANSLATION_TABLE.get()[set_code]
    return TRANSLATION_TABLE.get()


def prebuild_translation_table() -> List[Dict[str, str]]:
    """
    Build a table of dicts that contains the translation
    of each set. Since MKM returns the same order for
    each page, just translated into a different language,
    we can exploit this to build an anonymous array and
    construct it in a later function.
    :return: List[Dict[str, str]] with language: "set name"
    """
    session = util.get_generic_session()
    translation_list: List[Dict[str, str]] = []

    for lang_map in CARD_MARKET_LANGUAGES:
        mkm_url = CARD_MARKET_URL.format(lang_map["code"])
        response: Any = session.get(mkm_url)
        LOGGER.info("Downloaded: {} (Cache = {})".format(mkm_url, response.from_cache))

        # Parse the data and pluck all anchor tags with a set URL
        # inside of their href tag.
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        magic_sets = soup.find_all(
            "a", href=re.compile(r"/{}/Magic/Expansions/.*".format(lang_map["code"]))
        )

        # Pre-fill our table so we can index into it
        if not translation_list:
            translation_list = [{}] * len(magic_sets)

        for index, header in enumerate(magic_sets):
            if translation_list[index]:
                # Merge the two dicts
                translation_list[index] = {
                    **translation_list[index],
                    **{lang_map["lang"]: header.text},
                }
            else:
                translation_list[index] = {lang_map["lang"]: header.text}

    return translation_list


def build_translation_table() -> Dict[str, Dict[str, str]]:
    """
    Calls for the pre-building of the table, then goes through
    the MKM information we have stored in resources and plays
    match maker to create a simple access dictionary for
    future insertions.
    :return: {SET_CODE: {LANGUAGE: TRANSLATED_SET, ...}, ...}
    """
    LOGGER.info("Compiling set translations")
    initial_table = prebuild_translation_table()

    with mtgjson4.RESOURCE_PATH.joinpath("mkm_information.json").open("r") as f:
        mkm_stuff = json.load(f)

    # The Big-O time of this is bad, but there are only a few hundred
    # elements in each, so not _too_ big a deal here.
    translation_table = {}
    for set_content in initial_table:
        for key, value in mkm_stuff.items():
            if value["mcmName"] == set_content["English"]:
                del set_content["English"]
                translation_table[key] = set_content
                break

    TRANSLATION_TABLE.set(translation_table)
    return translation_table
