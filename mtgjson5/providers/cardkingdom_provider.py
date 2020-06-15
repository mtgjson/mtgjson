"""
Card Kingdom 3rd party provider
"""
import json
import logging
import pathlib
from typing import Any, Dict, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract_provider import AbstractProvider
from ..utils import retryable_session

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
        translation_table = self.generate_card_kingdom_to_mtgjson_map(
            all_printings_path
        )

        today_dict: Dict[str, MtgjsonPricesObject] = {}

        card_rows = request_api_response.get("data", [])
        for card in card_rows:
            if card["id"] in translation_table.keys():
                mtgjson_uuid = translation_table[card["id"]]

                if mtgjson_uuid not in today_dict:
                    today_dict[mtgjson_uuid] = MtgjsonPricesObject(
                        "paper", "cardkingdom", self.today_date
                    )

                if card["is_foil"] == "true":
                    today_dict[mtgjson_uuid].sell_foil = float(card["price_retail"])
                    today_dict[mtgjson_uuid].buy_foil = float(card["price_buy"])
                else:
                    today_dict[mtgjson_uuid].sell_normal = float(card["price_retail"])
                    today_dict[mtgjson_uuid].buy_normal = float(card["price_buy"])

        return today_dict

    @staticmethod
    def generate_card_kingdom_to_mtgjson_map(
        all_printings_path: pathlib.Path,
    ) -> Dict[str, str]:
        """
        Generate a TCGPlayerID -> MTGJSON UUID map that can be used
        across the system.
        :param all_printings_path: Path to JSON compiled version
        :return: Map of TCGPlayerID -> MTGJSON UUID
        """
        with all_printings_path.expanduser().open(encoding="utf-8") as f:
            file_contents = json.load(f).get("data", {})

        dump_map: Dict[str, str] = {}
        for value in file_contents.values():
            for card in value.get("cards", []) + value.get("tokens", []):
                if "cardKingdomId" in card.keys():
                    dump_map[card["cardKingdomId"]] = card["uuid"]
                if "cardKingdomFoilId" in card.keys():
                    dump_map[card["cardKingdomFoilId"]] = card["uuid"]

        return dump_map
