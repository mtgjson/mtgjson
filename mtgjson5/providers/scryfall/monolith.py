"""
Scryfall 3rd party provider
"""
import argparse
import logging
import pathlib
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set, Union

import ratelimit
import requests.exceptions
from singleton_decorator import singleton

from ... import constants
from ...mtgjson_config import MtgjsonConfig
from ...providers.abstract import AbstractProvider
from . import sf_utils

LOGGER = logging.getLogger(__name__)


@singleton
class ScryfallProvider(AbstractProvider):
    """
    Scryfall container
    """

    class_id: str = "sf"
    ALL_SETS_URL: str = "https://api.scryfall.com/sets/"
    CARDS_URL: str = "https://api.scryfall.com/cards/"
    CARDS_URL_ALL_DETAIL_BY_SET_CODE: str = "https://api.scryfall.com/cards/search?include_extras=true&include_variations=true&order=set&q=e%3A{}&unique=prints"
    CARDS_WITHOUT_LIMITS_URL: str = "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)%20or%20(o:deck%20o:have%20o:up%20o:to%20o:cards%20o:named)"
    CARDS_IN_BASE_SET_URL: str = "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20is:booster%20unique:prints"
    CARDS_IN_SET: str = (
        "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20unique:prints"
    )
    TYPE_CATALOG: str = "https://api.scryfall.com/catalog/{0}"
    CARDS_WITH_ALCHEMY_SPELLBOOK_URL = "https://api.scryfall.com/cards/search?q=is:alchemy%20and%20oracle:/conjure|draft|%27s%20spellbook/&include_extras=true"
    SPELLBOOK_SEARCH_URL = (
        "https://api.scryfall.com/cards/search?q=spellbook:%22{}%22&include_extras=true"
    )
    cards_without_limits: Set[str]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.cards_without_limits = set(self.generate_cards_without_limits())

    def _build_http_header(self) -> Dict[str, str]:
        return sf_utils.build_http_header()

    def download_all_pages(
        self,
        starting_url: Optional[str],
        params: Optional[Dict[str, Union[str, int]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Connects to Scryfall API and goes through all redirects to get the
        card data from their several pages via multiple API calls
        :param starting_url: First Page URL
        :param params: Params to pass to Scryfall API
        """
        all_cards: List[Dict[str, Any]] = []

        page_downloaded = 1
        starting_url = f"{starting_url}&page={page_downloaded}"

        while starting_url:
            LOGGER.debug(f"Downloading page {page_downloaded} -- {starting_url}")
            page_downloaded += 1

            response: Dict[str, Any] = self.download(starting_url, params)
            if response["object"] == "error":
                if response["code"] != "not_found":
                    LOGGER.warning(f"Unable to download {starting_url}: {response}")
                break

            data_response: List[Dict[str, Any]] = response.get("data", [])
            all_cards.extend(data_response)

            # Go to the next page, if it exists
            if not response.get("has_more"):
                break

            starting_url = re.sub(
                r"&page=\d+", f"&page={page_downloaded}", starting_url, count=1
            )

        return all_cards

    @ratelimit.sleep_and_retry
    @ratelimit.limits(calls=15, period=1)
    def download(
        self,
        url: str,
        params: Optional[Dict[str, Union[str, int]]] = None,
        retry_ttl: int = 3,
    ) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        :param retry_ttl: How many times to retry if Chunk Error
        """
        try:
            response = self.session.get(url, timeout=30)
            self.log_download(response)
        except requests.exceptions.ChunkedEncodingError as error:
            if retry_ttl:
                LOGGER.warning(f"Download failed: {error}... Retrying")
                time.sleep(3 - retry_ttl)
                return self.download(url, params, retry_ttl - 1)

            LOGGER.error(f"Download failed: {error}... Maxed out retries, returning empty dict")
            return {}

        try:
            return response.json()
        except ValueError as error:
            if "504" in response.text:
                LOGGER.warning("Scryfall 504 error, sleeping...")
            else:
                LOGGER.error(
                    f"Unable to convert response to JSON for URL: {url} -> {error}; Message = {response.text}"
                )

            time.sleep(5)
            return self.download(url, params)

    def download_cards(self, set_code: str) -> List[Dict[str, Any]]:
        """
        Get all cards from Scryfall API for a particular set code
        :param set_code: Set to download (Ex: AER, M19)
        :return: List of all card objects
        """
        LOGGER.info(f"Downloading {set_code} cards")
        scryfall_cards = self.download_all_pages(
            self.CARDS_URL_ALL_DETAIL_BY_SET_CODE.format(set_code)
        )

        # Return sorted by card name, and by card number if the same name is found
        return sorted(
            scryfall_cards, key=lambda card: (card["name"], card["collector_number"])
        )

    def generate_cards_without_limits(self) -> List[str]:
        """
        Grab all cards that can have as many copies
        in a deck as the player wants
        :return: Set of valid cards
        """

        return self.__get_card_names(self.CARDS_WITHOUT_LIMITS_URL)

    def get_alchemy_cards_with_spellbooks(self) -> List[str]:
        """
        Grab all cards that have alchemy spellbooks associated
        :return Set of valid cards
        """
        return self.__get_card_names(self.CARDS_WITH_ALCHEMY_SPELLBOOK_URL)

    def get_card_names_in_spellbook(self, card_name: str) -> List[str]:
        """
        Grab all cards that are within a specific card_name's alchemy spellbook
        :param card_name Card to find spellbook entries for
        :return Set of spellbook cards
        """
        return self.__get_card_names(self.SPELLBOOK_SEARCH_URL.format(card_name))

    def get_catalog_entry(self, catalog_key: str) -> List[str]:
        """
        Grab the Scryfall catalog of appropriate types
        :param catalog_key: Type to find
        :return: List of values found
        """
        catalog_data = self.download(self.TYPE_CATALOG.format(catalog_key))
        if catalog_data["object"] == "error":
            LOGGER.error(f"Unable to build {catalog_key}. Not found")
            return []

        return list(catalog_data["data"])

    def get_all_scryfall_sets(self) -> List[str]:
        """
        Grab all sets that Scryfall currently supports
        :return: Scryfall sets
        """
        scryfall_sets = self.download(self.ALL_SETS_URL)

        if scryfall_sets["object"] == "error":
            LOGGER.error(f"Downloading Scryfall data failed: {scryfall_sets}")
            return []

        # Get _ALL_ Scryfall sets
        scryfall_set_codes = [
            set_obj["code"].upper() for set_obj in scryfall_sets["data"]
        ]

        # Remove Scryfall token sets (but leave extra sets)
        scryfall_set_codes = [
            set_code
            for set_code in scryfall_set_codes
            if not (set_code.startswith("t") and set_code[1:] in scryfall_set_codes)
        ]

        return sorted(scryfall_set_codes)

    @staticmethod
    def get_sets_already_built() -> List[str]:
        """
        Grab sets that have already been compiled by the system
        :return: List of all set codes found
        """
        json_output_files: List[pathlib.Path] = list(
            MtgjsonConfig().output_path.glob("**/*.json")
        )

        set_codes_found = list(
            {file.stem for file in json_output_files}
            - set()  # MtgjsonStructuresObject().get_all_compiled_file_names())
        )

        LOGGER.info(f"Sets Built Already: {', '.join(set_codes_found)}")

        set_codes_found = [
            set_code[:-1] if set_code[:-1] in constants.BAD_FILE_NAMES else set_code
            for set_code in set_codes_found
        ]

        return set_codes_found

    def get_sets_to_build(self, args: argparse.Namespace) -> List[str]:
        """
        Grab what sets to build given build params
        :param args: CLI args
        :return: List of sets to construct, alphabetically
        """
        if args.resume_build:
            # Exclude sets we have already built
            args.skip_sets.extend(self.get_sets_already_built())

        if not args.all_sets:
            # We have a sub-set list, so only return what we want
            return sorted(list(set(args.sets) - set(args.skip_sets)))

        scryfall_sets = self.get_all_scryfall_sets()

        # Remove Scryfall token sets (but leave extra sets)
        non_token_sets = {
            s
            for s in scryfall_sets
            if not (s.startswith("T") and s[1:] in scryfall_sets)
        }

        # Remove sets to skip
        return_list = list(non_token_sets - set(args.skip_sets))

        return sorted(return_list)

    def __get_card_names(self, url: str) -> List[str]:
        """
        Get the card names from a URL search
        :param url: URL on Scryfall to query
        :return All unique card names found
        """
        return list({card["name"] for card in self.download(url).get("data", {})})
