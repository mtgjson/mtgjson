"""
CardHoarder 3rd party provider
"""
import datetime
from typing import Any, Dict, List

from singleton_decorator import singleton

from ..providers.abstract_provider import AbstractProvider
from ..utils import get_thread_logger

LOGGER = get_thread_logger()


@singleton
class CardhoarderProvider(AbstractProvider):
    """
    CardHoarder container
    """

    ch_api_url: str = "https://www.cardhoarder.com/affiliates/pricefile/{}"
    today_date: str = datetime.datetime.today().strftime("%Y-%m-%d")

    def __init__(self) -> None:
        """
        Initializer
        """
        get_thread_logger()
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for CardHoarder
        :return: Authorization header
        """
        headers: Dict[str, str] = {}

        config = self.get_configs()
        if not config.get("CardHoarder", "token"):
            self.ch_api_url = ""
        self.ch_api_url = self.ch_api_url.format(config.get("CardHoarder", "token"))

        return headers

    def download(self, url: str, params: Dict[str, str] = None) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = self.session_pool.popleft()
        response = session.get(url)
        self.session_pool.append(session)

        self.log_download(response)

        return response.content.decode()

    def convert_cardhoarder_to_mtgjson(self, url_to_parse: str) -> Dict[str, float]:
        """
        Download CardHoarder cards and convert them into a more
        consumable format for further processing.
        :param url_to_parse: URL to download CardHoarder cards from
        :return: Consumable dictionary
        """
        request_api_response: str = self.download(url_to_parse)

        mtgjson_to_price = {}

        # All Entries from CH, cutting off headers
        card_rows: List[str] = request_api_response.split("\n")[2:]

        for card_row in card_rows:
            split_row = card_row.split("\t")
            # We're only indexing cards with MTGJSON UUIDs
            if len(split_row[-1]) > 3:
                # Last Row = UUID, 5th Row = Price
                mtgjson_to_price[split_row[-1]] = float(split_row[5])

        return mtgjson_to_price

    def generate_today_price_dict(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        normal_cards = self.convert_cardhoarder_to_mtgjson(self.ch_api_url)
        foil_cards = self.convert_cardhoarder_to_mtgjson(self.ch_api_url + "/foil")

        db_contents: Dict[str, Dict[str, Dict[str, float]]] = {}

        self._construct_for_cards(db_contents, normal_cards, "mtgo")
        self._construct_for_cards(db_contents, foil_cards, "mtgoFoil")
        return db_contents

    def _construct_for_cards(
        self,
        semi_completed_data: Dict[str, Dict[str, Dict[str, float]]],
        cards: Dict[str, float],
        card_type: str,
    ) -> None:
        """
        Construct MTGJSON price output for a single day given a card set
        :param semi_completed_data: MTGJSON set to update
        :param cards: Cards to iterate
        :param card_type: Printing Type
        """
        for key, value in cards.items():
            if key not in semi_completed_data.keys():
                semi_completed_data[key] = {
                    "mtgo": {},
                    "mtgoFoil": {},
                    "paper": {},
                    "paperFoil": {},
                }
            semi_completed_data[key][card_type][self.today_date] = float(value)
