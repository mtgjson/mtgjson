"""Magic Card Market retrieval and processing."""

import contextvars
import json
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional

import bs4

import mtgjson4
from mtgjson4 import util

# The offerings of MKM as of 2019-03-19
from mtgjson4.provider import gamepedia, scryfall

CARD_MARKET_LANGUAGES: List[Dict[str, str]] = [
    {"code": "en", "lang": "English"},
    {"code": "fr", "lang": "French"},
    {"code": "de", "lang": "German"},
    {"code": "it", "lang": "Italian"},
    {"code": "es", "lang": "Spanish"},
]

HARERUYA_SET_TRANSLATION = {
    "UBT": "PUMA",
    "M19_2": "M19",
    "MM2015": "MM2",
    "CHRBB": "CHR",
    "4EDBB": "4ED",
}

IG2_REPLACEMENTS = {
    "Magic 2019": "M19",
    "SanDiegoCon": "PS18",
    "Global Series Jiang Yanggu & Mu Yanling": "GS1",
    "Master 25": "A25",
    "Magic Player Rewards": "P11",
    "Commander 2013 Edition": "C13",
    "Commander": "CMD",
    "Friday Night Magic": "F18",
}

WIKIPEDIA_REPLACEMENTS = {"TSP/TSB": "TSP"}

CARD_MARKET_URL: str = "https://www.cardmarket.com/{}/Magic/Expansions"
HARERUYA_URL: str = "http://www.hareruyamtg.com/jp/default.aspx"
WIKIPEDIA_URL: str = "https://pt.wikipedia.org/wiki/Expans%C3%B5es_de_Magic:_The_Gathering"
IG2_URL: str = "http://ig2.cc/sitemap.html"

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


def get_simplified_chinese():
    session = util.get_generic_session()
    response: Any = session.get(IG2_URL)
    response.encoding = "utf-8"  # Get proper Chinese characters
    LOGGER.info("Downloaded: {} (Cache = {})".format(response.url, response.from_cache))

    return_list = {}

    soup = bs4.BeautifulSoup(response.text, "html.parser")
    body = soup.find("div", class_="mainlist")
    set_lines = body.find_all("li")
    for set_line in set_lines:
        a_tags = set_line.find("a")
        if not a_tags:
            continue

        set_name_en = set_line.text.split("-")[1].strip().split('"')[0]
        set_name_ch = a_tags.text.strip()

        if set_name_en in IG2_REPLACEMENTS.keys():
            set_name_en = IG2_REPLACEMENTS[set_name_en]

        return_list[set_name_en] = {"Chinese Simplified": set_name_ch}

    return return_list


def get_portuguese():
    session = util.get_generic_session()
    response: Any = session.get(WIKIPEDIA_URL)
    LOGGER.info("Downloaded: {} (Cache = {})".format(response.url, response.from_cache))

    soup = bs4.BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table", class_="wikitable")

    return_table = {}

    # First table
    rows = tables[0].find_all("tr")
    for row in rows[2:]:
        cols = row.find_all("td")

        set_code = cols[2].text.strip().upper()
        set_name = cols[0].text.strip()

        if set_code in WIKIPEDIA_REPLACEMENTS.keys():
            set_code = WIKIPEDIA_REPLACEMENTS[set_code]

        return_table[set_code] = {"Portuguese (Brazil)": set_name}

    # Second table
    rows = tables[1].find_all("tr")
    for row in rows[2:]:
        cols = row.find_all("td")

        if len(cols) < 4:
            continue

        set_code = cols[3].text.upper().split("[")[0].strip()
        set_name = cols[0].text.strip()

        if not set_name or set_name == "-" or set_name[0] == '"':
            continue

        if set_code in WIKIPEDIA_REPLACEMENTS.keys():
            set_code = WIKIPEDIA_REPLACEMENTS[set_code]

        return_table[set_code] = {"Portuguese (Brazil)": set_name}

    # Third table -- Skipped as there's no translations
    return return_table


def get_japanese():
    session = util.get_generic_session()

    translation_dict: Dict[str, Dict[str, str]] = {}

    response: Any = session.get(HARERUYA_URL)
    LOGGER.info("Downloaded: {} (Cache = {})".format(response.url, response.from_cache))

    soup = bs4.BeautifulSoup(response.text, "html.parser")
    magic_sets_list = soup.find_all("h5")

    for magic_set in magic_sets_list:
        img_tag = magic_set.find("img")
        if not img_tag:
            continue

        jp_set_code = str(pathlib.Path(img_tag["src"]).name.split(".")[0])
        translation_dict[jp_set_code] = {"Japanese": magic_set.text}

    return translation_dict


def get_mkm_languages() -> List[Dict[str, str]]:
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
        LOGGER.info(
            "Downloaded: {} (Cache = {})".format(response.url, response.from_cache)
        )

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

    # Final result table
    combined_table = {}

    with mtgjson4.RESOURCE_PATH.joinpath("mkm_information.json").open("r") as f:
        mkm_stuff = json.load(f)

    # The Big-O time of this is bad, but there are only a few hundred
    # elements in each, so not _too_ big a deal here.
    for set_content in get_mkm_languages():
        for key, value in mkm_stuff.items():
            if value["mcmName"] == set_content["English"]:
                combined_table[key] = set_content
                break

    for word_key, value in get_simplified_chinese().items():
        set_code = scryfall.get_set_header(gamepedia.strip_bad_sf_chars(word_key))[
            "code"
        ].upper()

        if set_code in combined_table:
            combined_table[set_code] = {**combined_table[set_code], **value}
        else:
            combined_table[set_code] = value

    for key, value in get_japanese().items():
        if key in HARERUYA_SET_TRANSLATION.keys():
            key = HARERUYA_SET_TRANSLATION[key]

        if key in combined_table.keys():
            combined_table[key] = {**combined_table[key], **value}
        else:
            combined_table[key] = value

    for key, value in get_portuguese().items():
        if key in combined_table.keys():
            combined_table[key] = {**combined_table[key], **value}
        else:
            combined_table[key] = value

    print(json.dumps(combined_table))

    TRANSLATION_TABLE.set(combined_table)
    return combined_table
