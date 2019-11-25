"""
Scryfall 3rd party provider
"""
import logging
from typing import Dict, Any, List

from singleton.singleton import Singleton

from mtgjson5.globals import init_logger
from mtgjson5.providers.abstract_provider import AbstractProvider


@Singleton
class ScryfallProvider(AbstractProvider):
    """
    Scryfall container
    """

    ALL_SETS: str = "https://api.scryfall.com/sets/"
    VARIATIONS: str = "https://api.scryfall.com/cards/search?q=is%3Avariation%20set%3A{0}&unique=prints"

    def __init__(self, use_cache: bool = True):
        init_logger()
        super().__init__(self._build_http_header(), use_cache)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for Scryfall
        :return: Authorization header
        """
        headers: Dict[str, str] = {}

        config = self.get_configs()
        if config.get("Scryfall", "client_secret"):
            headers = {
                "Authorization": f"Bearer {config.get('Scryfall', 'client_secret')}"
            }

        return headers

    def download(self, url: str) -> Dict[str, Any]:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        """
        session = self.session_pool.popleft()
        response = session.get(url)
        self.session_pool.append(session)

        self.log_download(response)

        return response.json()

    def download_cards(
        self, set_code: str, in_booster: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Connects to Scryfall API and goes through all redirects to get the
        card data from their several pages via multiple API calls.
        :param set_code: Set to download (Ex: AER, M19)
        :param in_booster: Cards that can appear in booster or not
        :return: List of all card objects
        """
        logging.info(f"Downloading set {set_code} information")
        set_api_json: Dict[str, Any] = self.download(self.ALL_SETS + set_code)
        if set_api_json["object"] == "error":
            if not set_api_json["details"].startswith("No Magic set found"):
                logging.warning(
                    f"Set api download failed for {set_code}: {set_api_json}"
                )
            return []

        # All cards in the set structure
        scryfall_cards: List[Dict[str, Any]] = []

        # Download both normal card and variations
        for cards_api_url in [
            set_api_json.get("search_uri"),
            self.VARIATIONS.format(set_code),
        ]:
            # For each page, append all the data, go to next page
            page_downloaded: int = 1
            while cards_api_url:
                logging.info(
                    f"Downloading page {page_downloaded} of card data for {set_code}"
                )
                page_downloaded += 1

                cards_api_json: Dict[str, Any] = self.download(cards_api_url)
                if cards_api_json["object"] == "error":
                    if not cards_api_json["details"].startswith(
                        "Your query didn’t match"
                    ):
                        logging.warning(
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
