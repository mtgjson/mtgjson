"""
Wizards Site 3rd party provider
"""

import re
from typing import Dict, Union

import bs4
import requests

import simplejson as json
from singleton_decorator import singleton

from ..classes import MtgjsonTranslationsObject
from ..consts import RESOURCE_PATH, WIZARDS_SUPPORTED_LANGUAGES
from ..providers.abstract_provider import AbstractProvider
from ..utils import get_thread_logger
from .scryfall_provider import ScryfallProvider


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


@singleton
class WizardsProvider(AbstractProvider):
    """
    Wizards Site Container
    """

    TRANSLATION_URL: str = "https://magic.wizards.com/{}/products/card-set-archive"
    COMP_RULES: str = "https://magic.wizards.com/en/game-info/gameplay/rules-and-formats/rules"
    translation_table: Dict[str, Dict[str, str]] = {}

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
        translation_table = set_names_to_set_codes(translation_table)

        self.translation_table = translation_table
