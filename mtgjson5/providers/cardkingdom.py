"""
Card Kingdom 3rd party provider
"""
import logging
import pathlib
from typing import Any, Dict, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class CardKingdomProvider(AbstractProvider):
    """
    Card Kingdom container
    """

    api_url: str = "https://api.cardkingdom.com/api/pricelist"

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
        return {}

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content
        Api calls always return JSON
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)

        response = session.get(url)
        self.log_download(response)

        return response.json()

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        request_api_response: Dict[str, Any] = self.download(self.api_url)

        # Start with non-foil IDs
        card_kingdom_id_to_mtgjson = generate_card_mapping(
            all_printings_path, ("identifiers", "cardKingdomId"), ("uuid",)
        )

        # Then add in foil IDs
        card_kingdom_id_to_mtgjson.update(
            generate_card_mapping(
                all_printings_path, ("identifiers", "cardKingdomFoilId"), ("uuid",)
            )
        )

        today_dict: Dict[str, MtgjsonPricesObject] = {}

        card_rows = request_api_response.get("data", [])
        for card in card_rows:
            card_id = str(card["id"])
            if card_id not in card_kingdom_id_to_mtgjson:
                continue

            mtgjson_uuid = card_kingdom_id_to_mtgjson[card_id]

            if mtgjson_uuid not in today_dict:
                today_dict[mtgjson_uuid] = MtgjsonPricesObject(
                    "paper", "cardkingdom", self.today_date, "USD"
                )

            if card["is_foil"] == "true":
                today_dict[mtgjson_uuid].sell_foil = float(card["price_retail"])
                today_dict[mtgjson_uuid].buy_foil = float(card["price_buy"])
            else:
                today_dict[mtgjson_uuid].sell_normal = float(card["price_retail"])
                today_dict[mtgjson_uuid].buy_normal = float(card["price_buy"])

        return today_dict
