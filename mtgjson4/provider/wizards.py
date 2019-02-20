"""File information provider for WotC Website."""

import contextvars
import logging
import re
from typing import Any, Dict, List, Match, Optional

import mtgjson4
from mtgjson4 import util
import unidecode

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION_WIZARDS")

COMP_RULES: str = "https://magic.wizards.com/en/game-info/gameplay/rules-and-formats/rules"


def download_from_wizards(url: str) -> str:
    """
    Generic download class for Wizards URLs
    :param url: URL to download (prob from Wizards website)
    :return: Text from page
    """
    session = util.get_generic_session()
    response = session.get(url=url, timeout=5.0)
    response.encoding = "windows-1252"  # WHY DO THEY DO THIS

    LOGGER.info("Downloaded URL: {0}".format(response.url))
    session.close()
    return response.text


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
        "meta": {"version": mtgjson4.__VERSION__, "date": mtgjson4.__VERSION_DATE__},
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
        if "{0}.{1}".format(rule_start, keyword_index) in line:
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
        .replace("\r", "")
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
        "meta": {"version": mtgjson4.__VERSION__, "date": mtgjson4.__VERSION_DATE__},
        "types": types_dict,
    }
