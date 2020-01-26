"""
MTGJSON Keywords container
"""
import re
from typing import Any, Dict, List

import unidecode

from ..classes import MtgjsonMetaObject
from ..providers import WizardsProvider
from ..utils import parse_magic_rules_subset, to_camel_case


class MtgjsonKeywordsObject:
    """
    Keywords container
    """

    ability_words: List[str]
    keyword_actions: List[str]
    keyword_abilities: List[str]
    meta: MtgjsonMetaObject

    def __init__(self) -> None:
        self.meta = MtgjsonMetaObject()
        self.ability_words = self.get_magic_ability_words()
        self.keyword_actions = self.get_keyword_actions()
        self.keyword_abilities = self.get_keyword_abilities()

    @staticmethod
    def get_magic_ability_words() -> List[str]:
        """
        Go through the ability words and put them into a list
        :return: List of abilities, sorted
        """
        for line in WizardsProvider().get_magic_rules().split("\r\r"):
            if "Ability words" in line:
                # Isolate all of the ability words, capitalize the words,
                line = unidecode.unidecode(
                    line.split("The ability words are")[1].strip()
                ).split("\r\n")[0]

                result = [x.strip().lower() for x in line.split(",")]

                # Address the "and" bit of the last element, and the period
                result[-1] = result[-1][4:-1]
                return result

        return []

    def get_keyword_actions(self) -> List[str]:
        """
        Go through the keyword actions and put them into a list
        :return: List of keyword actions, sorted
        """
        return self.parse_magic_rules(
            WizardsProvider().get_magic_rules(),
            "701. Keyword Actions",
            "702. Keyword Abilities",
            "701",
        )

    def get_keyword_abilities(self) -> List[str]:
        """
        Go through the keyword abilities and put them into a list
        :return: List of keywords, sorted
        """
        return self.parse_magic_rules(
            WizardsProvider().get_magic_rules(),
            "702. Keyword Abilities",
            "703. Turn-Based Actions",
            "702",
        )

    @staticmethod
    def parse_magic_rules(
        magic_rules: str, start_header: str, end_header: str, rule_to_read: str
    ) -> List[str]:
        """
        Do the heavy handling to parse out specific sub-sections of rules.
        :param magic_rules: Rules to parse
        :param start_header: Section to parse
        :param end_header: Section to cut away
        :param rule_to_read: What rules to pull that rule from
        :return: List of asked for components
        """
        # Keyword actions are found in section XXX
        valid_line_segments = parse_magic_rules_subset(
            magic_rules, start_header, end_header
        ).split("\n")

        # XXX.1 is just a description of what rule XXX includes.
        # XXX.2 starts the action for _most_ sections
        keyword_index = 2
        return_list: List[str] = []

        for line in valid_line_segments:
            # Keywords are defined as "XXX.# Name"
            # We will want to ignore subset lines like "XXX.#a"
            regex_search = re.findall(f"{rule_to_read}.{keyword_index}. (.*)", line)
            if regex_search:
                # Break the line into "Rule Number | Keyword"
                return_list.append(regex_search[0].lower())
                # Get next keyword, so we can pass over the non-relevant lines
                keyword_index += 1

        return sorted(return_list)

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
