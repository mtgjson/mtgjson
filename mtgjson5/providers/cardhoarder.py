"""
CardHoarder 3rd party provider
"""
import logging
import pathlib
from typing import Any, Dict, List, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import get_all_cards_and_tokens, retryable_session

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

        if "CardHoarder" not in config.sections():
            LOGGER.warning("CardHoarder section not established. Skipping upload")
            self.__keys_found = False
            self.ch_api_url = ""
            return headers

        if config.get("CardHoarder", "token"):
            self.__keys_found = True
            self.ch_api_url = self.ch_api_url.format(config.get("CardHoarder", "token"))
        else:
            LOGGER.info("CardHoarder keys values missing. Skipping pricing")
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

    def convert_cardhoarder_to_mtgjson(
        self, url_to_parse: str, mtgo_to_mtgjson_map: Dict[str, str]
    ) -> Dict[str, float]:
        """
        Download CardHoarder cards and convert them into a more
        consumable format for further processing.
        :param url_to_parse: URL to download CardHoarder cards from
        :param mtgo_to_mtgjson_map: Mapping for translating incoming data
        :return: Consumable dictionary
        """
        mtgjson_price_map = {}

        request_api_response: str = self.download(url_to_parse)

        # All Entries from CH, cutting off headers
        file_rows: List[str] = request_api_response.splitlines()[2:]
        for file_row in file_rows:
            card_row = file_row.split("\t")

            mtgo_id = card_row[0]
            card_uuid = mtgo_to_mtgjson_map.get(mtgo_id)

            if not card_uuid:
                LOGGER.debug(f"CardHoarder {card_row} unable to be mapped, skipping")
                continue

            if len(card_row) <= 6:
                LOGGER.warning(f"CardHoarder entry {card_row} malformed, skipping")
                continue

            mtgjson_price_map[card_uuid] = float(card_row[5])

        return mtgjson_price_map

    def generate_today_price_dict(
        self, all_printings_path: Any
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        if not self.__keys_found:
            return {}

        mtgo_to_mtgjson_map = self.get_mtgo_to_mtgjson_map(all_printings_path)

        normal_cards = self.convert_cardhoarder_to_mtgjson(
            self.ch_api_url, mtgo_to_mtgjson_map
        )
        foil_cards = self.convert_cardhoarder_to_mtgjson(
            self.ch_api_url + "/foil", mtgo_to_mtgjson_map
        )

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
                    "mtgo", "cardhoarder", self.today_date, "USD"
                )

            if is_mtgo_normal:
                semi_completed_data[key].sell_normal = float(value)
            else:
                semi_completed_data[key].sell_foil = float(value)

    @staticmethod
    def get_mtgo_to_mtgjson_map(all_printings_path: pathlib.Path) -> Dict[str, str]:
        """
        Construct a mapping from MTGO IDs (Regular & Foil) to MTGJSON UUIDs
        :param all_printings_path: AllPrintings to generate mapping from
        :return MTGO to MTGJSON mapping
        """
        mtgo_to_mtgjson = dict()
        for card in get_all_cards_and_tokens(all_printings_path):
            identifiers = card["identifiers"]
            if "mtgoId" in identifiers:
                mtgo_to_mtgjson[identifiers["mtgoId"]] = card["uuid"]
            if "mtgoFoilId" in identifiers:
                mtgo_to_mtgjson[identifiers["mtgoFoilId"]] = card["uuid"]

        return mtgo_to_mtgjson
