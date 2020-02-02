"""
Scryfall 3rd party provider
"""
import logging
from typing import Any, Dict, List, Set, Union

import requests
from singleton_decorator import singleton

from ..providers.abstract_provider import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class ScryfallProvider(AbstractProvider):
    """
    Scryfall container
    """

    class_id: str = "sf"
    ALL_SETS_URL: str = "https://api.scryfall.com/sets/"
    CARDS_URL: str = "https://api.scryfall.com/cards/"
    VARIATIONS_URL: str = "https://api.scryfall.com/cards/search?q=is%3Avariation%20set%3A{0}&unique=prints"
    CARDS_WITHOUT_LIMITS_URL: str = "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)"
    CARDS_IN_BASE_SET_URL: str = "https://api.scryfall.com/cards/search?order=set&q=set:{0}%20is:booster%20unique:prints"
    TYPE_CATALOG: str = "https://api.scryfall.com/catalog/{0}-types"
    cards_without_limits: Set[str]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.cards_without_limits = self.generate_cards_without_limits()

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for Scryfall
        :return: Authorization header
        """
        config = self.get_configs()
        if not self.get_configs().get("Scryfall", "client_secret"):
            LOGGER.warning(
                "Scryfall keys not established. Defaulting to non-authorized mode"
            )
            return {}

        headers: Dict[str, str] = {
            "Authorization": f"Bearer {config.get('Scryfall', 'client_secret')}",
            "Connection": "Keep-Alive",
        }
        return headers

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = requests.Session()
        session.headers.update(self.session_header)
        response = session.get(url)
        self.log_download(response)
        return response.json()

    def download_cards(self, set_code: str) -> List[Dict[str, Any]]:
        """
        Connects to Scryfall API and goes through all redirects to get the
        card data from their several pages via multiple API calls.
        :param set_code: Set to download (Ex: AER, M19)
        :return: List of all card objects
        """
        LOGGER.info(f"Downloading {set_code} information")
        set_api_json: Dict[str, Any] = self.download(self.ALL_SETS_URL + set_code)
        if set_api_json["object"] == "error":
            if not set_api_json["details"].startswith("No Magic set found"):
                LOGGER.warning(
                    f"Set api download failed for {set_code}: {set_api_json}"
                )
            else:
                LOGGER.error("Idk")
            return []

        # All cards in the set structure
        scryfall_cards: List[Dict[str, Any]] = []

        # Download both normal card and variations
        for cards_api_url in [
            set_api_json.get("search_uri"),
            self.VARIATIONS_URL.format(set_code),
        ]:
            # For each page, append all the data, go to next page
            page_downloaded: int = 1
            while cards_api_url:
                LOGGER.info(f"Downloading {set_code} card data page")
                page_downloaded += 1

                cards_api_json: Dict[str, Any] = self.download(cards_api_url)
                if cards_api_json["object"] == "error":
                    if not cards_api_json["details"].startswith(
                        "Your query didn’t match"
                    ):
                        LOGGER.warning(
                            f"Error downloading {set_code}: {cards_api_json}"
                        )
                    break

                # Append all cards on this page
                for card_obj in cards_api_json["data"]:
                    scryfall_cards.append(card_obj)

                # Go to the next page, if it exists
                if not cards_api_json.get("has_more"):
                    break

                cards_api_url = cards_api_json.get("next_page")

        # Return sorted by card name, and by card number if the same name is found
        return sorted(
            scryfall_cards, key=lambda card: (card["name"], card["collector_number"])
        )

    def generate_cards_without_limits(self) -> Set[str]:
        """
        Grab all cards that can have as many copies
        in a deck as the player wants
        :return: Set of valid cards
        """

        return {
            card["name"]
            for card in self.download(self.CARDS_WITHOUT_LIMITS_URL)["data"]
        }

    def get_catalog_entry(self, catalog_key: str) -> List[str]:
        """
        Grab the Scryfall catalog of appropriate types
        :param catalog_key: Type to find
        :return: List of values found
        """
        return list(self.download(self.TYPE_CATALOG.format(catalog_key))["data"])
