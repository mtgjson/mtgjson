"""Scryfall retrieval and processing."""

import configparser
import logging
import pathlib
from typing import Any, Dict, List, Optional, Set, Tuple

import mtgjson4
from mtgjson4 import util
import requests
import requests.adapters


class Scryfall:
    """
    Scryfall downloader class
    """

    def __init__(self) -> None:
        self.api_sets = "https://api.scryfall.com/sets/"
        self.api_card = "https://api.scryfall.com/cards/"
        self.logger = logging.getLogger(__name__)
        self.session = self.__get_session()

    def __get_session(self) -> requests.Session:
        """Get or create a requests session for Scryfall."""
        tmp_session = requests.Session()

        if pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
            # Open and read MTGJSON secret properties
            config = configparser.RawConfigParser()
            config.read(mtgjson4.CONFIG_PATH)
            header_auth = {
                "Authorization": "Bearer " + config.get("Scryfall", "client_secret")
            }
            tmp_session.headers.update(header_auth)
            self.logger.info("Fetching from Scryfall with authentication")
        else:
            self.logger.warning("Fetching from Scryfall WITHOUT authentication")

        tmp_session = util.retryable_session(tmp_session)
        return tmp_session

    def download(self, scryfall_url: str) -> Dict[str, Any]:
        """
        Get the data from Scryfall in JSON format using our secret keys
        :param scryfall_url: URL to download JSON data from
        :return: JSON object of the Scryfall data
        """
        response = self.session.get(url=scryfall_url, timeout=5.0)
        request_api_json: Dict[str, Any] = response.json()

        self.logger.info("Downloaded URL: {0}".format(scryfall_url))
        return request_api_json

    def get_set_header(self, set_name: str) -> Dict[str, Any]:
        """
        Get just the header (not card contents) of a set by its name
        :param set_name:
        :return:
        """
        set_api_json: Dict[str, Any] = self.download(self.api_sets + set_name)
        if set_api_json["object"] == "error":
            self.logger.warning(
                "Set header api download failed for {0}: {1}".format(
                    set_name, set_api_json
                )
            )
            return {}

        return set_api_json

    def get_set(self, set_code: str) -> List[Dict[str, Any]]:
        """
        Connects to Scryfall API and goes through all redirects to get the
        card data from their several pages via multiple API calls.
        :param set_code: Set to download (Ex: AER, M19)
        :return: List of all card objects
        """
        self.logger.info("Downloading set {} information".format(set_code))
        set_api_json: Dict[str, Any] = self.download(self.api_sets + set_code)
        if set_api_json["object"] == "error":
            self.logger.warning(
                "Set api download failed for {0}: {1}".format(set_code, set_api_json)
            )
            return []

        cards_api_url: Optional[str] = set_api_json.get("search_uri")

        # All cards in the set structure
        scryfall_cards: List[Dict[str, Any]] = []

        # For each page, append all the data, go to next page
        page_downloaded: int = 1
        while cards_api_url is not None:
            self.logger.info(
                "Downloading page {0} of card data for {1}".format(
                    page_downloaded, set_code
                )
            )
            page_downloaded += 1

            cards_api_json: Dict[str, Any] = self.download(cards_api_url)
            if cards_api_json["object"] == "error":
                self.logger.error(
                    "Error downloading {0}: {1}".format(set_code, cards_api_json)
                )
                return scryfall_cards

            for card in cards_api_json["data"]:
                scryfall_cards.append(card)

            if cards_api_json.get("has_more"):
                cards_api_url = cards_api_json.get("next_page")
            else:
                cards_api_url = None

        return scryfall_cards

    def parse_rulings(self, rulings_url: str) -> List[Dict[str, str]]:
        """
        Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
        :param rulings_url: URL to get Scryfall JSON data from
        :return: MTGJSON rulings list
        """
        rules_api_json: Dict[str, Any] = self.download(rulings_url)
        if rules_api_json["object"] == "error":
            self.logger.error(
                "Error downloading URL {0}: {1}".format(rulings_url, rules_api_json)
            )

        sf_rules: List[Dict[str, str]] = []
        mtgjson_rules: List[Dict[str, str]] = []

        for rule in rules_api_json["data"]:
            sf_rules.append(rule)

        for sf_rule in sf_rules:
            mtgjson_rule: Dict[str, str] = {
                "date": sf_rule["published_at"],
                "text": sf_rule["comment"],
            }
            mtgjson_rules.append(mtgjson_rule)

        return mtgjson_rules

    @staticmethod
    def parse_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
        """
        Given a card type string, split it up into its raw components: super, sub, and type
        :param card_type: Card type string to parse
        :return: Tuple (super, type, sub) of the card's attributes
        """
        sub_types: List[str] = []
        super_types: List[str] = []
        types: List[str] = []

        if "—" in card_type:
            split_type: List[str] = card_type.split("—")
            supertypes_and_types: str = split_type[0]
            subtypes: str = split_type[1]
            sub_types = [x for x in subtypes.split(" ") if x]
        else:
            supertypes_and_types = card_type

        for value in supertypes_and_types.split(" "):
            if value in mtgjson4.SUPERTYPES:
                super_types.append(value)
            elif value:
                types.append(value)

        return super_types, types, sub_types

    @staticmethod
    def parse_legalities(sf_card_legalities: Dict[str, str]) -> Dict[str, str]:
        """
        Given a Scryfall legalities dictionary, convert it to MTGJSON format
        :param sf_card_legalities: Scryfall legalities
        :return: MTGJSON legalities
        """
        card_legalities: Dict[str, str] = {}
        for key, value in sf_card_legalities.items():
            if value != "not_legal":
                card_legalities[key] = value.capitalize()

        return card_legalities

    def parse_foreign(
        self, sf_prints_url: str, card_name: str, set_name: str
    ) -> List[Dict[str, str]]:
        """
        Get the foreign printings information for a specific card
        :param sf_prints_url: URL to get prints from
        :param card_name: Card name to parse (needed for double faced)
        :param set_name: Set name
        :return: Foreign entries object
        """
        card_foreign_entries: List[Dict[str, str]] = []

        # Add information to get all languages
        sf_prints_url = sf_prints_url.replace(
            "&unique=prints", "+lang%3Aany&unique=prints"
        )

        prints_api_json: Dict[str, Any] = self.download(sf_prints_url)
        if prints_api_json["object"] == "error":
            self.logger.error(
                "No data found for {0}: {1}".format(sf_prints_url, prints_api_json)
            )
            return []

        for foreign_card in prints_api_json["data"]:
            if set_name != foreign_card["set"] or foreign_card["lang"] == "en":
                continue

            card_foreign_entry: Dict[str, str] = {}
            try:
                card_foreign_entry["language"] = mtgjson4.LANGUAGE_MAP[
                    foreign_card["lang"]
                ]
            except IndexError:
                self.logger.warning("Unable to get language {}".format(foreign_card))

            try:
                card_foreign_entry["multiverseId"] = foreign_card["multiverse_ids"][0]
            except IndexError:
                self.logger.warning(
                    "Unable to get multiverseId {}".format(foreign_card["name"])
                )

            if "card_faces" in foreign_card:
                if (
                    card_name.lower()
                    == foreign_card["name"].split("/")[0].strip().lower()
                ):
                    face = 0
                else:
                    face = 1

                foreign_card = foreign_card["card_faces"][face]
                self.logger.info(
                    "Split card found: Using face {0} for {1}".format(face, card_name)
                )

            card_foreign_entry["name"] = foreign_card.get("printed_name")
            card_foreign_entry["text"] = foreign_card.get("printed_text")
            card_foreign_entry["flavorText"] = foreign_card.get("flavor_text")
            card_foreign_entry["type"] = foreign_card.get("printed_type_line")

            card_foreign_entries.append(card_foreign_entry)

        return card_foreign_entries

    def parse_printings(self, sf_prints_url: str) -> List[str]:
        """
        Given a Scryfall printings URL, extract all sets a card was printed in
        :param sf_prints_url: URL to extract data from
        :return: List of all sets a specific card was printed in
        """
        card_sets: Set[str] = set()

        prints_api_json: Dict[str, Any] = self.download(sf_prints_url)
        if prints_api_json["object"] == "error":
            self.logger.error("Bad download: {}".format(sf_prints_url))
            return []

        for card in prints_api_json["data"]:
            card_sets.add(card.get("set").upper())

        return list(card_sets)
