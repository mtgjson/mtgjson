"""
Wizards Site 3rd party provider
"""

import re
import string
from typing import Any, Dict, List, Match, Optional, Union

import bs4
import requests

import simplejson as json
from singleton_decorator import singleton
import unidecode

from ..classes import MtgjsonTranslationsObject
from ..consts import RESOURCE_PATH, WIZARDS_SUPPORTED_LANGUAGES
from ..providers.abstract_provider import AbstractProvider
from ..utils import get_thread_logger
from .scryfall_provider import ScryfallProvider


@singleton
class WizardsProvider(AbstractProvider):
    """
    Wizards Site Container
    """

    TRANSLATION_URL: str = "https://magic.wizards.com/{}/products/card-set-archive"
    magic_rules_url: str = "https://magic.wizards.com/en/game-info/gameplay/rules-and-formats/rules"
    translation_table: Dict[str, Dict[str, str]] = {}
    magic_rules: str = ""

    def __init__(self) -> None:
        self.logger = get_thread_logger()
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Dict[str, Union[str, int]] = None
    ) -> requests.Response:
        """
        Download from Wizard's website
        :param url: URL to get
        :param params: Not used
        :return: Response
        """
        session = self.session_pool.popleft()
        response = session.get(url)
        self.session_pool.append(session)
        self.log_download(response)
        return response

    def get_translation_for_set(self, set_code: str) -> MtgjsonTranslationsObject:
        """
        Get translations for a specific set, if it exists
        :param set_code: Set code
        :return: Set translations
        """
        if not self.translation_table:
            self.logger.info("Initializing Translation Table")
            self.build_translation_table()

        if set_code in self.translation_table.keys():
            return MtgjsonTranslationsObject(self.translation_table[set_code])

        return MtgjsonTranslationsObject()

    def build_single_language(
        self,
        short_lang_code: str,
        long_lang_name: str,
        translation_table: Dict[str, Dict[str, str]],
    ) -> Dict[str, Dict[str, str]]:
        """
        This will take the given language and source the data
        from the Wizards site to create a new entry in each
        option.
        :param translation_table: Partially built table
        :param short_lang_code: Short hand lang from Wizards
        :param long_lang_name: Language of MTGJSON
        """
        # Download the localized archive
        soup = bs4.BeautifulSoup(
            self.download(self.TRANSLATION_URL.format(short_lang_code)).content,
            "html.parser",
        )

        # Find all nodes, which are table (set) rows
        set_lines = soup.find_all("a", href=re.compile(".*(node|content).*"))
        for set_line in set_lines:
            # Pluck out the set icon, as it's how we will link all languages
            icon = set_line.find("span", class_="icon")

            # Skip if we can't find an icon
            if not icon or len(icon) == 1:
                set_name = set_line.find("span", class_="nameSet")
                if set_name:
                    self.logger.warning(
                        f"Unable to find set icon for {set_name.text.strip()}"
                    )
                continue

            # Update our global table
            set_name = set_line.find("span", class_="nameSet").text.strip()
            set_icon_url = icon.find("img")["src"]
            if set_icon_url in translation_table.keys():
                translation_table[set_icon_url] = {
                    **translation_table[set_icon_url],
                    **{long_lang_name: set_name},
                }
            else:
                translation_table[set_icon_url] = {long_lang_name: set_name}

        return translation_table

    def convert_keys_to_set_names(
        self, table: Dict[str, Dict[str, str]]
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
                self.logger.warning(f"VALUE INCOMPLETE\t{key}: {value}")
                continue

            new_key = value["English"]
            del value["English"]
            return_table[new_key] = value

        return return_table

    def build_translation_table(self) -> None:
        """
        Helper method to create the translation table for
        the end user. Should only be called once per week
        based on how the cache system works.
        :return: New translation table
        """
        translation_table: Dict[str, Dict[str, str]] = {}

        for short_code, long_name in WIZARDS_SUPPORTED_LANGUAGES:
            translation_table = self.build_single_language(
                short_code, long_name, translation_table
            )

        # Oh Wizards...
        translation_table = self.convert_keys_to_set_names(translation_table)
        translation_table = self.set_names_to_set_codes(translation_table)

        self.translation_table = translation_table

    @staticmethod
    def set_names_to_set_codes(
        table: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        """
        The set names from Wizard's website are slightly incorrect.
        This function will clean them up and make them ready to go
        :param table: Translation Table
        :return: Fixed Translation Table
        """
        with RESOURCE_PATH.joinpath("wizards_set_name_fixes.json").open() as f:
            set_name_fixes = json.load(f)

        for key, value in set_name_fixes.items():
            table[value] = table[key]
            del table[key]

        # Build new table with set codes instead of set names
        new_table = {}
        for key, value in table.items():
            if not key:
                continue

            # Strip chars not in line with Scryfall's API
            key = key.translate({ord(i): None for i in ":'’.& "})

            set_header = ScryfallProvider().download(
                f"{ScryfallProvider().ALL_SETS_URL}/{key}"
            )
            if set_header:
                new_table[set_header["code"].upper()] = value

        return new_table

    # Handle building up components from rules text
    def get_magic_rules(self) -> str:
        """
        Download the comp rules from Wizards site
        :return Comprehensive Magic Rules
        """
        if self.magic_rules:
            return self.magic_rules

        response = self.download(self.magic_rules_url).content.decode()

        # Get the comp rules from the website (as it changes often)
        # Also split up the regex find so we only have the URL
        self.magic_rules_url = str(re.findall(r"href=\".*\.txt\"", response)[0][6:-1])
        response = (
            self.download(self.magic_rules_url).content.decode().replace("â€™", "'")
        )

        self.magic_rules = response
        return self.magic_rules

    def get_magic_ability_words(self) -> List[str]:
        """
        Go through the ability words and put them into a list
        :return: List of abilities, sorted
        """
        for line in self.get_magic_rules().split("\r\r"):
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
            self.get_magic_rules(),
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
            self.get_magic_rules(),
            "702. Keyword Abilities",
            "703. Turn-Based Actions",
            "702",
        )

    @staticmethod
    def parse_magic_rules_subset(
        magic_rules: str, start_header: str, end_header: str
    ) -> str:
        """
        Split up the magic rules to get a smaller working subset for parsing
        :param magic_rules: Magic rules to split up
        :param start_header: Start of content
        :param end_header: End of content
        :return: Smaller set of content
        """
        # Keyword actions are found in section XXX
        magic_rules = magic_rules.split(start_header)[2].split(end_header)[0]

        # Windows line endings... yuck
        valid_line_segments = "\n".join(magic_rules.split("\r\n"))

        return valid_line_segments

    def parse_magic_rules(
        self, magic_rules: str, start_header: str, end_header: str, rule_to_read: str
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
        valid_line_segments = self.parse_magic_rules_subset(
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

    def get_card_types(self) -> Dict[str, Any]:
        """
        Get all possible card super, sub, and types from the rules.
        :return: Card types for return_value
        """

        comp_rules = self.parse_magic_rules_subset(
            self.get_magic_rules(), "205. Type Line", "206. Expansion Symbol"
        )

        card_types = {
            "artifact": ScryfallProvider().get_catalog_entry("artifact"),
            "conspiracy": [],
            "creature": ScryfallProvider().get_catalog_entry("creature"),
            "enchantment": ScryfallProvider().get_catalog_entry("enchantment"),
            "instant": ScryfallProvider().get_catalog_entry("spell"),
            "land": ScryfallProvider().get_catalog_entry("land"),
            "phenomenon": [],
            "plane": self.regex_str_to_list(
                re.search(r".*planar types are (.*)\.", comp_rules)
            ),
            "planeswalker": ScryfallProvider().get_catalog_entry("planeswalker"),
            "scheme": [],
            "sorcery": ScryfallProvider().get_catalog_entry("spell"),
            "tribal": [],
            "vanguard": [],
        }

        super_types = self.regex_str_to_list(
            re.search(r".*supertypes are (.*)\.", comp_rules)
        )

        types_dict = {}
        for key, value in card_types.items():
            types_dict[key] = {"subTypes": value, "superTypes": super_types}

        return types_dict

    @staticmethod
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

        for index, value in enumerate(card_types_split):
            card_types_split[index] = string.capwords(value.split(" (")[0])

        return card_types_split
