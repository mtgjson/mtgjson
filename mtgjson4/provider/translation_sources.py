"""Conglomerate of resources for translation retrieval and processing."""

import contextvars
import json
import logging
import pathlib
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import bs4

import mtgjson4
from mtgjson4 import util
from mtgjson4.provider import gamepedia, scryfall

TRANSLATION_URL = "https://magic.wizards.com/{}/products/card-set-archive"
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_TRANSLATIONS")
TRANSLATION_TABLE: contextvars.ContextVar = contextvars.ContextVar("TRANSLATION_TABLE")
LOGGER = logging.getLogger(__name__)

SUPPORTED_LANGUAGES = [
    ("zh-hans", "Chinese Simplified"),
    ("zh-hant", "Chinese Traditional"),
    ("fr", "French"),
    ("de", "German"),
    ("it", "Italian"),
    ("ja", "Japanese"),
    ("ko", "Korean"),
    ("pt-br", "Portuguese (Brazil)"),
    ("ru", "Russian"),
    ("es", "Spanish"),
    ("en", "English"),
]


def get_translations(set_code: Optional[str] = None) -> Any:
    """
    Get the translation table that was pre-built OR a specific
    set from the translation table. Will also build the
    table if necessary.
    Return value w/o set_code: {SET_CODE: {SET_LANG: TRANSLATION, ...}, ...}
    Return value w/  set_code: SET_CODE: {SET_LANG: TRANSLATION, ...}
    :return: Translation table
    """
    table = build_translation_table()
    return

    if not TRANSLATION_TABLE.get(None):
        translation_file = mtgjson4.RESOURCE_PATH.joinpath("set_translations.json")

        # If file cache exists and is current, read it from disk
        # Any other reason, replace the table
        if translation_file.is_file():
            if (
                time.time() - translation_file.stat().st_mtime
                > mtgjson4.SESSION_CACHE_EXPIRE_GATHERER
            ):
                table = build_translation_table()
                with translation_file.open("w") as f:
                    json.dump(table, f, indent=4)
                    f.write("\n")
            else:
                TRANSLATION_TABLE.set(json.load(translation_file.open("r")))
        else:
            table = build_translation_table()
            with translation_file.open("w") as f:
                json.dump(table, f, indent=4)
                f.write("\n")

    if set_code:
        return TRANSLATION_TABLE.get()[set_code]
    return TRANSLATION_TABLE.get()


def download(url: str, encoding: Optional[str] = None) -> str:
    """
    Download a file from a specified source using
    our generic session.
    :param url: URL to download
    :param encoding: URL encoding (if necessary)
    :return: URL content
    """
    session = util.get_generic_session()
    response: Any = session.get(url)
    if encoding:
        response.encoding = encoding
    LOGGER.info("Downloaded: {} (Cache = {})".format(response.url, response.from_cache))
    return str(response.text)


def build_single_language(
    lang: Tuple[str, str], translation_table: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """
    This will take the given language and source the data
    from the Wizards site to create a new entry in each
    option.
    :param translation_table: Partially built table
    :param lang: Tuple of lang to find on wizards, lang to show in MTGJSON
    """
    # Download the localized archive
    session = util.get_generic_session()
    soup = bs4.BeautifulSoup(
        session.get(TRANSLATION_URL.format(lang[0])).text, "html.parser"
    )

    # Find all nodes, which are table rows
    set_lines = soup.find_all("a", href=re.compile(".*node.*"))
    for set_line in set_lines:
        # Pluck out the set icon, as it's how we will link all languages
        icon = set_line.find("span", class_="icon")

        # Skip if we can't find an icon
        if not icon or len(icon) == 1:
            continue

        set_icon_url = icon.find("img")["src"]
        set_name = set_line.find("span", class_="nameSet").text.strip()

        # Update our global table
        if set_icon_url in translation_table.keys():
            translation_table[set_icon_url] = {
                **translation_table[set_icon_url],
                **{lang[1]: set_name},
            }
        else:
            translation_table[set_icon_url] = {lang[1]: set_name}

    return translation_table


def convert_keys_to_set_names(
    table: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:

    return_table = {}
    for key, value in table.items():
        if "English" not in value.keys():
            LOGGER.error("VALUE INCOMPLETE\t{}: {}".format(key, value))
            continue

        new_key = value["English"]
        del value["English"]
        return_table[new_key] = value

    LOGGER.info(json.dumps(return_table))
    return return_table


def remove_and_replace(table, good, bad):
    table[good] = {**table[bad], **table[good]}
    del table[bad]
    return table


def manual_fix_mistakes(table):
    # Fix dominaria
    good_dominaria = "https://magic.wizards.com/sites/mtg/files/images/featured/DAR_Logo_Symbol_Common.png"
    bad_dominaria = "https://magic.wizards.com/sites/mtg/files/images/featured/DAR_CardSetArchive_Symbol.png"
    table = remove_and_replace(table, good_dominaria, bad_dominaria)

    # Fix tempest remastered
    good_tempest_remastered = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/TPR_SetSymbol.png"
    )
    bad_tempest_remastered = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/TPR_SetSymbol.png"
    )
    table = remove_and_replace(table, good_tempest_remastered, bad_tempest_remastered)

    # Fix archenemy
    good_archenemy_nb = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/e01-icon_1.png"
    )
    bad_archenemy_nb = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/e01-icon_0.png"
    )
    table = remove_and_replace(table, good_archenemy_nb, bad_archenemy_nb)

    # Fix planechase
    good_planechase2012 = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/PC2_SetSymbol.png"
    )
    bad_planechase2012 = (
        "https://magic.wizards.com/sites/mtg/files/images/featured/PC2_SetIcon.png"
    )
    table = remove_and_replace(table, good_planechase2012, bad_planechase2012)

    # Fix duel deck
    good_duel_deck_q = "https://magic.wizards.com/sites/mtg/files/images/featured/EN_DDQ_SET_SYMBOL.jpg"
    bad_duel_deck_q = "https://magic.wizards.com/sites/mtg/files/images/featured/DDQ_ExpansionSymbol.png"
    table = remove_and_replace(table, good_duel_deck_q, bad_duel_deck_q)

    return table


def build_translation_table() -> Dict[str, Dict[str, str]]:
    return_table = {}

    for pair in SUPPORTED_LANGUAGES:
        return_table = build_single_language(pair, return_table)

    return_table = manual_fix_mistakes(return_table)

    return convert_keys_to_set_names(return_table)
