"""
MultiverseBridge 3rd party provider
"""
import logging
import pathlib
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


@singleton
class MultiverseBridgeProvider(AbstractProvider):
    """
    MultiverseBridge container
    """

    class_id: str = "mb"

    ROSETTA_STONE_SETS_URL = "https://www.multiversebridge.com/api/v1/sets"
    ROSETTA_STONE_CARDS_URL = "https://cdn.multiversebridge.com/mtgjson_build.json"
    rosetta_stone_cards: Dict[str, Any]
    rosetta_stone_sets: Dict[str, int]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.rosetta_stone_cards = defaultdict(list)
        self.rosetta_stone_sets = {}

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        response = self.session.get(url)
        self.log_download(response)
        if not response.ok:
            LOGGER.error(
                f"MultiverseBridge Download Error ({response.status_code}): {response.content.decode()}"
            )
            time.sleep(5)
            return self.download(url, params)
        return response.json()

    def parse_rosetta_stone_cards(self, rosetta_rows: List[Dict[str, Any]]) -> None:
        """
        Convert Rosetta Stone Card data into an index-able hashmap
        :param rosetta_rows: Rows from the API
        """
        for rosetta_row in rosetta_rows:
            self.rosetta_stone_cards[rosetta_row["scryfall_id"]].append(rosetta_row)

    def parse_rosetta_stone_sets(self, rosetta_rows: List[Dict[str, Any]]) -> None:
        """
        Convert Rosetta Stone Set data into index-able hashmap
        :param rosetta_rows: Rows from the API
        """
        for rosetta_row in rosetta_rows:
            self.rosetta_stone_sets[rosetta_row["mtgjson_code"]] = rosetta_row["cs_id"]

    def get_rosetta_stone_cards(self) -> Dict[str, Any]:
        """
        Cache a copy of the Rosetta Stone from MB and give it back when needed
        :return Rosetta Stone of Card IDs
        """
        if not self.rosetta_stone_cards:
            self.parse_rosetta_stone_cards(self.download(self.ROSETTA_STONE_CARDS_URL))
        return self.rosetta_stone_cards

    def get_rosetta_stone_sets(self) -> Dict[str, int]:
        """
        Cache a copy of the Rosetta Stone's Set IDs from MB and give it back when needed
        :return Rosetta Stone of Set IDs
        """
        if not self.rosetta_stone_sets:
            self.parse_rosetta_stone_sets(self.download(self.ROSETTA_STONE_SETS_URL))
        return self.rosetta_stone_sets

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for Paper from CardSphere
        :return MTGJSON prices single day structure
        """
        request_api_response: List[Dict[str, Any]] = self.download(
            self.ROSETTA_STONE_CARDS_URL
        )

        cardsphere_id_to_mtgjson: Dict[str, Set[Any]] = generate_card_mapping(
            all_printings_path, ("identifiers", "cardsphereId"), ("uuid",)
        )

        default_prices_obj = MtgjsonPricesObject(
            "paper", "cardsphere", self.today_date, "USD"
        )

        LOGGER.info("Building CardSphere retail data")
        return super().generic_generate_today_price_dict(
            third_party_to_mtgjson=cardsphere_id_to_mtgjson,
            price_data_rows=request_api_response,
            card_platform_id_key="cs_id",
            default_prices_object=default_prices_obj,
            foil_key="is_foil",
            retail_key="price",
        )
