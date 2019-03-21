"""Conglomerate of resources for translation retrieval and processing."""

import contextvars
import json
import logging
import pathlib
import re
import time
from typing import Any, Dict, List, Optional

import bs4

import mtgjson4
from mtgjson4 import util
from mtgjson4.provider import gamepedia, scryfall

WIZARDS_URL: str = "https://magic.wizards.com/"

CARD_MARKET_URL: str = "https://www.cardmarket.com/{}/Magic/Expansions"
JAPANESE_URL: str = "http://www.hareruyamtg.com/jp/default.aspx"
PORTUGUESE_URL: str = "https://pt.wikipedia.org/wiki/Expans%C3%B5es_de_Magic:_The_Gathering"
CHINESE_SIMPLE_URL: str = "http://ig2.cc/sitemap.html"
KOREAN_URL: str = WIZARDS_URL + "ko/products/card-set-archive"

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_MKM")
TRANSLATION_TABLE: contextvars.ContextVar = contextvars.ContextVar("TRANSLATION_TABLE")
LOGGER = logging.getLogger(__name__)

CARD_MARKET_FIXES = [
    {"code": "en", "lang": "English"},
    {"code": "fr", "lang": "French"},
    {"code": "de", "lang": "German"},
    {"code": "it", "lang": "Italian"},
    {"code": "es", "lang": "Spanish"},
]
JAPANESE_FIXES = {
    "UBT": "PUMA",
    "M19_2": "M19",
    "MM2015": "MM2",
    "CHRBB": "CHR",
    "4EDBB": "4ED",
}
CHINESE_SIMPLE_FIXES = {
    "Magic 2019": "M19",
    "SanDiegoCon": "PS18",
    "Global Series Jiang Yanggu & Mu Yanling": "GS1",
    "Master 25": "A25",
    "Magic Player Rewards": "P11",
    "Commander 2013 Edition": "C13",
    "Commander": "CMD",
    "Friday Night Magic": "F18",
}
PORTUGUESE_FIXES = {"TSP/TSB": "TSP"}


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


def get_russian() -> None:
    """
    Get russian language sets
    :return:
    """
    return


def get_traditional_chinese() -> None:
    """
    Get traditional chinese language sets
    :return:
    """
    return


def get_korean() -> Dict[str, Dict[str, str]]:
    """
    Get korean language sets from pre-built and then strip new ones from
    Wizards website
    :return:
    """
    # Korean Cache Start
    with mtgjson4.RESOURCE_PATH.joinpath("ko_set_translations.json").open("r") as f:
        korean_content = json.load(f)

    soup = bs4.BeautifulSoup(download(KOREAN_URL), "html.parser")
    soup = soup.find("div", class_="card-set-archive-table")
    set_lines = soup.find_all("a", href=re.compile(r".*node.*"))

    for set_line in set_lines:
        set_name = set_line.find("span", class_="nameSet").text.strip()

        if set_name not in korean_content.values():
            soup = bs4.BeautifulSoup(
                download(WIZARDS_URL + set_line["href"]), "html.parser"
            )
            soup = soup.find_all("img", attrs={"src": re.compile(r".*media.*")})

            for img_tag in soup:
                set_code = re.match(r".*images/magic/([A-Za-z0-9]*)/.*", img_tag["src"])
                if set_code:
                    korean_content[set_code.group(1).upper()] = set_name
                    break

    with mtgjson4.RESOURCE_PATH.joinpath("ko_set_translations.json").open("w") as f:
        json.dump(korean_content, f, indent=4)
        f.write("\n")
    # Korean Cache End

    # Get the translations now
    return_list = {}
    for key, value in korean_content.items():
        return_list[key] = {"Korean": value}

    return return_list


def get_simplified_chinese() -> Dict[str, Dict[str, str]]:
    """
    Get simplified chinese sets
    :return:
    """
    return_list = {}

    soup = bs4.BeautifulSoup(download(CHINESE_SIMPLE_URL, "utf-8"), "html.parser")
    body = soup.find("div", class_="mainlist")
    set_lines = body.find_all("li")
    for set_line in set_lines:
        a_tags = set_line.find("a")
        if not a_tags:
            continue

        set_name_en = set_line.text.split("-")[1].strip().split('"')[0]
        set_name_ch = a_tags.text.strip()

        if set_name_en in CHINESE_SIMPLE_FIXES.keys():
            set_name_en = CHINESE_SIMPLE_FIXES[set_name_en]

        return_list[set_name_en] = {"Chinese Simplified": set_name_ch}

    return return_list


def get_portuguese() -> Dict[str, Dict[str, str]]:
    """
    Get portuguese sets
    :return:
    """
    return_table = {}

    soup = bs4.BeautifulSoup(download(PORTUGUESE_URL), "html.parser")
    tables = soup.find_all("table", class_="wikitable")

    # First table
    rows = tables[0].find_all("tr")
    for row in rows[2:]:
        cols = row.find_all("td")

        set_code = cols[2].text.strip().upper()
        set_name = cols[0].text.strip()

        if set_code in PORTUGUESE_FIXES.keys():
            set_code = PORTUGUESE_FIXES[set_code]

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

        if set_code in PORTUGUESE_FIXES.keys():
            set_code = PORTUGUESE_FIXES[set_code]

        return_table[set_code] = {"Portuguese (Brazil)": set_name}

    # Third table -- Skipped as there's no translations
    return return_table


def get_japanese() -> Dict[str, Dict[str, str]]:
    """
    Get japanese sets
    :return:
    """
    translation_dict: Dict[str, Dict[str, str]] = {}

    soup = bs4.BeautifulSoup(download(JAPANESE_URL), "html.parser")
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
    translation_list: List[Dict[str, str]] = []

    for lang_map in CARD_MARKET_FIXES:
        mkm_url = CARD_MARKET_URL.format(lang_map["code"])

        # Parse the data and pluck all anchor tags with a set URL
        # inside of their href tag.
        soup = bs4.BeautifulSoup(download(mkm_url), "html.parser")
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
    Calls for the building of each language, then goes through
    and plays match maker to create a simple access dictionary
    with all of the necessary data.
    :return: {SET_CODE: {LANGUAGE: TRANSLATED_SET, ...}, ...}
    """
    LOGGER.info("Compiling set translations")

    # Final result table
    combined_table = {}

    with mtgjson4.RESOURCE_PATH.joinpath("mkm_information.json").open("r") as f:
        mkm_stuff = json.load(f)

    # English, French, German, Italian, Spanish
    # NOTE: Refactor as this could be slow
    for set_content in get_mkm_languages():
        for key, value in mkm_stuff.items():
            if value["mcmName"] == set_content["English"]:
                combined_table[key] = set_content
                break

    # Chinese
    for word_key, value in get_simplified_chinese().items():
        set_code = scryfall.get_set_header(gamepedia.strip_bad_sf_chars(word_key))[
            "code"
        ].upper()
        if set_code in combined_table:
            combined_table[set_code] = {**combined_table[set_code], **value}
        else:
            combined_table[set_code] = value

    # Japanese
    for key, value in get_japanese().items():
        if key in JAPANESE_FIXES.keys():
            key = JAPANESE_FIXES[key]

        if key in combined_table.keys():
            combined_table[key] = {**combined_table[key], **value}
        else:
            combined_table[key] = value

    # Portuguese (Brazil)
    for key, value in get_portuguese().items():
        if key in combined_table.keys():
            combined_table[key] = {**combined_table[key], **value}
        else:
            combined_table[key] = value

    # Korean
    for key, value in get_korean().items():
        if key in combined_table.keys():
            combined_table[key] = {**combined_table[key], **value}
        else:
            combined_table[key] = value

    # Strip English afterwards (not necessary in the file)
    for key, value in combined_table.items():
        if "English" in value.keys():
            del value["English"]

    TRANSLATION_TABLE.set(combined_table)
    return combined_table
