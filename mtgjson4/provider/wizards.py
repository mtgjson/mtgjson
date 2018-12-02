"""File information provider for WotC Website."""

import contextvars
import logging
import re
import string
from typing import Dict, List

from mtgjson4 import util

import unidecode

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")

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

    LOGGER.info("Retrieved: %s", response.url)

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


def compile_comp_output() -> Dict[str, List[str]]:
    """
    Give a compiled dictionary result of the key phrases that can be
    found in the MTG comprehensive rule book.
    :return: Dict of abilities (both kinds), actions
    """
    comp_rules = get_comp_rules()

    return {
        "AbilityWords": get_ability_words(comp_rules),
        "KeywordActions": get_keyword_actions(comp_rules),
        "KeywordAbilities": get_keyword_abilities(comp_rules),
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
            result = [string.capwords(x.strip()) for x in line.split(",")]
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
            keyword = line.split(" ", 1)[1]
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
