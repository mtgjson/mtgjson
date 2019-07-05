"""File information provider for WotC Website."""

import contextvars
import json
import logging
import re
import time
from typing import Any, Dict, List, Match, Optional, Tuple

import bs4

import mtgjson4
from mtgjson4 import util
from mtgjson4.provider import gamepedia, scryfall
import unidecode

TRANSLATION_URL: str = "https://magic.wizards.com/{}/products/card-set-archive"
COMP_RULES: str = "https://magic.wizards.com/en/game-info/gameplay/rules-and-formats/rules"

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_WIZARDS")
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


def download_from_wizards(url: str) -> str:
    """
    Generic download class for Wizards URLs
    :param url: URL to download (prob from Wizards website)
    :return: Text from page
    """
    session = util.get_generic_session()
    response: Any = session.get(url=url, timeout=5.0)
    util.print_download_status(response)
    session.close()
    return str(response.text)


def get_comp_rules() -> str:
    """
    Download the comp rules from Wizards site and return it
    :return: Comp rules text
    """
    response = download_from_wizards(COMP_RULES)

    # Get the comp rules from the website (as it changes often)
    # Also split up the regex find so we only have the URL
    comp_rules_url: str = re.findall(r"href=\".*\.txt\"", response)[0][6:-1]
    response = download_from_wizards(comp_rules_url)

    return response


def compile_comp_types_output() -> Dict[str, Any]:
    """
    Give a compiled dictionary result of the super, sub, and types
    that can be found in the MTG comprehensive rules book.
    :return: Dict of the different types
    """
    comp_rules = get_comp_rules()
    return get_card_types(comp_rules)


def compile_comp_output() -> Dict[str, Any]:
    """
    Give a compiled dictionary result of the key phrases that can be
    found in the MTG comprehensive rule book.
    :return: Dict of abilities (both kinds), actions
    """
    comp_rules = get_comp_rules()

    return {
        # Deprecation in 4.3, Removal in 4.4
        "AbilityWords": get_ability_words(comp_rules),
        # Deprecation in 4.3, Removal in 4.4
        "KeywordActions": get_keyword_actions(comp_rules),
        # Deprecation in 4.3, Removal in 4.4
        "KeywordAbilities": get_keyword_abilities(comp_rules),
        "abilityWords": get_ability_words(comp_rules),
        "keywordActions": get_keyword_actions(comp_rules),
        "keywordAbilities": get_keyword_abilities(comp_rules),
        "meta": {
            "version": mtgjson4.__VERSION__,
            "date": mtgjson4.__VERSION_DATE__,
            "pricesDate": mtgjson4.__PRICE_UPDATE_DATE__,
        },
    }


def get_ability_words(comp_rules: str) -> List[str]:
    """
    Go through the ability words and put them into a list
    :return: List of abilities, sorted
    """
    for line in comp_rules.split("\r\n"):
        if "Ability words" in line:
            # Isolate all of the ability words, capitalize the words,
            # and remove the . from the end of the string
            line = unidecode.unidecode(
                line.split("The ability words are")[1].strip()[:-1]
            )
            result = [x.strip().lower() for x in line.split(",")]
            result[-1] = result[-1][4:]  # Address the "and" bit of the last element
            return result

    return []


def parse_comp_internal(
    comp_rules: str, top_delim: str, bottom_delim: str, rule_start: str
) -> List[str]:
    """
    Do the heavy handling to parse out specific sub-sections of rules.
    :param comp_rules: Rules to parse
    :param top_delim: Section to parse
    :param bottom_delim: Section to cut away
    :param rule_start: What rules to pull that rule from
    :return: List of asked for components
    """
    # Keyword actions are found in section XXX
    comp_rules = comp_rules.split(top_delim)[2].split(bottom_delim)[0]

    # Windows line endings... yuck
    valid_line_segments = comp_rules.split("\r\n")

    # XXX.1 is just a description of what rule XXX includes.
    # XXX.2 starts the action for _most_ sections
    keyword_index = 2
    return_list: List[str] = []

    for line in valid_line_segments:
        # Keywords are defined as "XXX.# Name"
        # We will want to ignore subset lines like "XXX.#a"
        if f"{rule_start}.{keyword_index}" in line:
            # Break the line into "Rule Number | Keyword"
            keyword = line.split(" ", 1)[1].lower()
            return_list.append(keyword)
            # Get next keyword, so we can pass over the non-relevant lines
            keyword_index += 1

    return sorted(return_list)


def get_keyword_actions(comp_rules: str) -> List[str]:
    """
    Go through the keyword actions and put them into a list
    :return: List of keyword actions, sorted
    """
    return parse_comp_internal(
        comp_rules, "701. Keyword Actions", "702. Keyword Abilities", "701"
    )


def get_keyword_abilities(comp_rules: str) -> List[str]:
    """
    Go through the keyword abilities and put them into a list
    :return: List of keywords, sorted
    """
    return parse_comp_internal(
        comp_rules, "702. Keyword Abilities", "703. Turn-Based Actions", "702"
    )


def regex_str_to_list(regex_match: Optional[Match]) -> List[str]:
    """
    Take a regex match object and turn a string in
    format "a, b, c, ..., and z." into [a,b,c,...,z]
    :param regex_match: Regex match object
    :return: List of strings
    """
    if not regex_match:
        return []

    # Get only the sentence with the types
    card_types = regex_match.group(1).split(". ")[0]

    # Split the types by comma
    card_types_split: List[str] = card_types.split(", ")

    # If there are only two elements, split by " and " instead
    if len(card_types_split) == 1:
        card_types_split = card_types.split(" and ")
    else:
        # Replace the last one from "and XYZ" to just "XYZ"
        card_types_split[-1] = card_types_split[-1].split(" ", 1)[1]

    #
    for index, value in enumerate(card_types_split):
        card_types_split[index] = value.split(" (")[0].lower()

    return card_types_split


def get_card_types(comp_rules: str) -> Dict[str, Any]:
    """
    Get all possible card super, sub, and types from the rules.
    :param comp_rules: Comp rules from method
    :return: Card types for return_value
    """

    # Only act on a sub-set of the rules to save time
    comp_rules = (
        comp_rules.split("205. Type Line")[2]
        .split("206. Expansion Symbol")[0]
        .replace("\r", "\n")
    )

    # Different regex searches needed for the data
    card_types = re.search(r".*The card types are (.*)\.", comp_rules)
    regex_type = {
        "artifact": re.search(r".*The artifact types are (.*)\.", comp_rules),
        "conspiracy": None,
        "creature": re.search(r".*The creature types are (.*)\.", comp_rules),
        "enchantment": re.search(r".*The enchantment types are (.*)\.", comp_rules),
        "instant": re.search(r".*The spell types are (.*)\.", comp_rules),
        "land": re.search(r".*The land types are (.*)\.", comp_rules),
        "phenomenon": None,
        "plane": re.search(r".*The planar types are (.*)\.", comp_rules),
        "planeswalker": re.search(r".*planeswalker types are (.*)\.", comp_rules),
        "scheme": None,
        "sorcery": re.search(r".*The spell types are (.*)\.", comp_rules),
        "tribal": None,
        "vanguard": None,
    }

    super_types = regex_str_to_list(
        re.search(r".*The supertypes are (.*)\.", comp_rules)
    )

    types_dict = {}
    for card_type in regex_str_to_list(card_types):
        types_dict[card_type] = {
            "subTypes": regex_str_to_list(regex_type[card_type]),
            "superTypes": super_types,
        }

    return {
        "meta": {
            "version": mtgjson4.__VERSION__,
            "date": mtgjson4.__VERSION_DATE__,
            "pricesDate": mtgjson4.__PRICE_UPDATE_DATE__,
        },
        "types": types_dict,
    }


def get_translations(set_code: Optional[str] = None) -> Any:
    """
    Get the translation table that was pre-built OR a specific
    set from the translation table. Will also build the
    table if necessary.
    Return value w/o set_code: {SET_CODE: {SET_LANG: TRANSLATION, ...}, ...}
    Return value w/  set_code: SET_CODE: {SET_LANG: TRANSLATION, ...}
    :param set_code Set code for specific entry
    :return: Translation table
    """
    if not TRANSLATION_TABLE.get(None):
        translation_file = mtgjson4.RESOURCE_PATH.joinpath("set_translations.json")

        is_file = translation_file.is_file()
        cache_expired = (
            is_file
            and time.time() - translation_file.stat().st_mtime
            > mtgjson4.SESSION_CACHE_EXPIRE_GENERAL
        )

        if (not is_file) or cache_expired:
            # Rebuild set translations
            table = build_translation_table()
            with translation_file.open("w") as f:
                json.dump(table, f, indent=4)
                f.write("\n")

        TRANSLATION_TABLE.set(json.load(translation_file.open("r")))

    if set_code:
        # If we have an exact match, return it
        if set_code in TRANSLATION_TABLE.get().keys():
            return TRANSLATION_TABLE.get()[set_code]

        LOGGER.warning(
            f"(Wizards) Unable to find good enough translation match for {set_code}"
        )
        return {}

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

    util.print_download_status(response)
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
    soup = bs4.BeautifulSoup(download(TRANSLATION_URL.format(lang[0])), "html.parser")

    # Find all nodes, which are table (set) rows
    set_lines = soup.find_all("a", href=re.compile(".*(node|content).*"))
    for set_line in set_lines:
        # Pluck out the set icon, as it's how we will link all languages
        icon = set_line.find("span", class_="icon")

        # Skip if we can't find an icon
        if not icon or len(icon) == 1:
            set_name = set_line.find("span", class_="nameSet")
            if set_name:
                LOGGER.warning(f"Unable to find set icon for {set_name.text.strip()}")
            continue

        # Update our global table
        set_name = set_line.find("span", class_="nameSet").text.strip()
        set_icon_url = icon.find("img")["src"]
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
    """
    Now that the table is complete, we need to replace the keys
    that were URLs with actual set names, so we can work with
    them.
    :param table: Completed translation table
    :return: Updated translation table w/ correct keys
    """
    return_table = {}
    for key, value in table.items():
        if "English" not in value.keys():
            LOGGER.error(f"VALUE INCOMPLETE\t{key}: {value}")
            continue

        new_key = value["English"]
        del value["English"]
        return_table[new_key] = value

    LOGGER.info(json.dumps(return_table))
    return return_table


def remove_and_replace(
    table: Dict[str, Dict[str, str]], good: str, bad: str
) -> Dict[str, Dict[str, str]]:
    """
    Small helper method to combine two columns and remove
    the duplicate
    :param table: Translation table
    :param good: Key to keep and merge into
    :param bad: Key to delete after merging
    :return: Translation table fixed
    """
    table[good] = {**table[bad], **table[good]}
    del table[bad]
    return table


def manual_fix_urls(table: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    Wizards has some problems, this corrects them to allow
    seamless integration of all sets
    :param table: Translation table needing fixing
    :return: Corrected translated table
    """
    # Fix dominaria
    good_dominaria = "https://magic.wizards.com/sites/mtg/files/images/featured/DAR_Logo_Symbol_Common.png"
    bad_dominaria = "https://magic.wizards.com/sites/mtg/files/images/featured/DAR_CardSetArchive_Symbol.png"
    table = remove_and_replace(table, good_dominaria, bad_dominaria)

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


def set_names_to_set_codes(
    table: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """
    The set names from Wizard's website are slightly incorrect.
    This function will clean them up and make them ready to go
    :param table: Translation Table
    :return: Fixed Translation Table
    """
    with mtgjson4.RESOURCE_PATH.joinpath("wizards_set_name_fixes.json").open("r") as f:
        set_name_fixes = json.load(f)

    for key, value in set_name_fixes.items():
        table[value] = table[key]
        del table[key]

    # Build new table with set codes instead of set names
    new_table = {}
    for key, value in table.items():
        if key:
            sf_header = scryfall.get_set_header(gamepedia.strip_bad_sf_chars(key))
            new_table[sf_header["code"].upper()] = value

    return new_table


def build_translation_table() -> Dict[str, Dict[str, str]]:
    """
    Helper method to create the translation table for
    the end user. Should only be called once per week
    based on how the cache system works.
    :return: New translation table
    """
    translation_table: Dict[str, Dict[str, str]] = {}

    for pair in SUPPORTED_LANGUAGES:
        translation_table = build_single_language(pair, translation_table)

    # Oh Wizards...
    translation_table = manual_fix_urls(translation_table)
    translation_table = convert_keys_to_set_names(translation_table)
    translation_table = set_names_to_set_codes(translation_table)

    return translation_table
