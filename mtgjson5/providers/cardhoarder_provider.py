"""
CardHoarder 3rd party provider
"""
import logging
from typing import Any, Dict, List, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract_provider import AbstractProvider
from ..utils import retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class CardHoarderProvider(AbstractProvider):
    """
    CardHoarder container
    """

    ch_api_url: str = "https://www.cardhoarder.com/affiliates/pricefile/{}"

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for CardHoarder
        :return: Authorization header
        """
        headers: Dict[str, str] = {}
        __keys_found: bool

        config = self.get_configs()
        if config.get("CardHoarder", "token"):
            self.__keys_found = True
            self.ch_api_url = self.ch_api_url.format(config.get("CardHoarder", "token"))
        else:
            LOGGER.info("CardHoarder keys not established. Skipping pricing")
            self.__keys_found = False
            self.ch_api_url = ""

        return headers

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)

        response = session.get(url)
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
        card_rows: List[str] = request_api_response.splitlines()[2:]

        for card_row in card_rows:
            split_row = card_row.split("\t")
            # We're only indexing cards with MTGJSON UUIDs
            if len(split_row[-1]) > 3:
                # Last Row = UUID, 5th Row = Price
                mtgjson_to_price[split_row[-1]] = float(split_row[5])

        return mtgjson_to_price

    def generate_today_price_dict(self) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        if not self.__keys_found:
            return {}

        normal_cards = self.convert_cardhoarder_to_mtgjson(self.ch_api_url)
        foil_cards = self.convert_cardhoarder_to_mtgjson(self.ch_api_url + "/foil")

        db_contents: Dict[str, MtgjsonPricesObject] = {}

        self._construct_for_cards(db_contents, normal_cards, True)
        self._construct_for_cards(db_contents, foil_cards)
        return db_contents

    def _construct_for_cards(
        self,
        semi_completed_data: Dict[str, MtgjsonPricesObject],
        cards: Dict[str, float],
        is_mtgo_normal: bool = False,
    ) -> None:
        """
        Construct MTGJSON price output for a single day given a card set
        :param semi_completed_data: MTGJSON set to update
        :param cards: Cards to iterate
        """
        for key, value in cards.items():
            if key not in semi_completed_data.keys():
                semi_completed_data[key] = MtgjsonPricesObject(
                    "mtgo", "cardhoarder", self.today_date
                )

            if is_mtgo_normal:
                semi_completed_data[key].sell_normal = float(value)
            else:
                semi_completed_data[key].sell_foil = float(value)
