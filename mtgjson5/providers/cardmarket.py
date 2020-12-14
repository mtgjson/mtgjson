"""
MKM 3rd party provider
"""
import base64
import io
import json
import logging
import math
import os
import pathlib
import time
import zlib
from typing import Any, Dict, List, Optional, Union

import mkmsdk.exceptions
import pandas
from mkmsdk.api_map import _API_MAP
from mkmsdk.mkm import Mkm
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..consts import RESOURCE_PATH
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


@singleton
class CardMarketProvider(AbstractProvider):
    """
    MKM container
    """

    connection: Mkm
    set_map: Dict[str, Dict[str, Any]]

    __keys_found: bool

    def __init__(self, headers: Dict[str, str] = None):
        super().__init__(headers or {})

        config = self.get_configs()

        if "CardMarket" not in config.sections():
            LOGGER.warning("CardMarket section not established. Skipping requests")
            self.__keys_found = False
            return

        os.environ["MKM_APP_TOKEN"] = config.get("CardMarket", "app_token")
        os.environ["MKM_APP_SECRET"] = config.get("CardMarket", "app_secret")
        os.environ["MKM_ACCESS_TOKEN"] = config.get(
            "CardMarket", "mkm_access_token", fallback=""
        )
        os.environ["MKM_ACCESS_TOKEN_SECRET"] = config.get(
            "CardMarket", "mkm_access_token_secret", fallback=""
        )

        if not (os.environ["MKM_APP_TOKEN"] and os.environ["MKM_APP_SECRET"]):
            LOGGER.warning("CardMarket keys values missing. Skipping requests")
            self.__keys_found = False
            return

        self.__keys_found = True

        self.connection = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])
        self.set_map = {}
        self.__init_set_map()

    def _get_card_market_data(self) -> io.StringIO:
        """
        Download and reformat Card Market price data for further processing
        :return Card Market data ready for Pandas consumption
        """
        # Response comes gzip'd, then base64'd
        mkm_response = self.connection.market_place.price_guide().json()

        price_data = base64.b64decode(mkm_response["priceguidefile"])  # Un-base64
        price_data = zlib.decompress(price_data, 16 + zlib.MAX_WBITS)  # Un-gzip
        decoded_data = price_data.decode("utf-8")  # byte array to string
        return io.StringIO(decoded_data)

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure from Card Market
        :return MTGJSON prices single day structure
        """
        if not self.__keys_found:
            return {}

        mtgjson_id_map = generate_card_mapping(
            all_printings_path, ("identifiers", "mcmId"), ("uuid",)
        )

        price_data = pandas.read_csv(self._get_card_market_data())
        data_frame_columns = list(price_data.columns)

        product_id_index = data_frame_columns.index("idProduct")
        avg_sell_price_index = data_frame_columns.index("AVG1")
        avg_foil_price_index = data_frame_columns.index("Foil AVG1")

        today_dict: Dict[str, MtgjsonPricesObject] = {}
        for row in price_data.iterrows():
            columns: List[float] = [
                -1 if math.isnan(value) else value for value in row[1].tolist()
            ]

            product_id = str(int(columns[product_id_index]))
            if product_id in mtgjson_id_map.keys():
                mtgjson_uuid = mtgjson_id_map[product_id]
                avg_sell_price = columns[avg_sell_price_index]
                avg_foil_price = columns[avg_foil_price_index]

                if mtgjson_uuid not in today_dict.keys():
                    if avg_sell_price == -1 and avg_foil_price == -1:
                        continue

                    today_dict[mtgjson_uuid] = MtgjsonPricesObject(
                        "paper", "cardmarket", self.today_date, "EUR"
                    )

                if avg_sell_price != -1:
                    today_dict[mtgjson_uuid].sell_normal = avg_sell_price

                if avg_foil_price != -1:
                    today_dict[mtgjson_uuid].sell_foil = avg_foil_price

        return today_dict

    def __init_set_map(self) -> None:
        """
        Construct a mapping for all set components from MKM
        """
        mkm_resp = self.connection.market_place.expansions(game=1)
        if mkm_resp.status_code != 200:
            LOGGER.error(f"Unable to download MKM correctly: {mkm_resp}")
            return

        for set_content in mkm_resp.json()["expansion"]:
            self.set_map[set_content["enName"].lower()] = {
                "mcmId": set_content["idExpansion"],
                "mcmName": set_content["enName"],
            }

        # Update the set map with manual overrides
        with RESOURCE_PATH.joinpath("mkm_set_name_fixes.json").open() as f:
            mkm_set_name_fixes = json.load(f)

        for old_set_name, new_set_name in mkm_set_name_fixes.items():
            if old_set_name.lower() not in self.set_map:
                LOGGER.warning(
                    f"MKM Manual override {old_set_name} to {new_set_name} not found"
                )
                continue

            self.set_map[new_set_name.lower()] = self.set_map[old_set_name.lower()]
            del self.set_map[old_set_name.lower()]

    def get_set_id(self, set_name: str) -> Optional[int]:
        """
        Get MKM Set ID from pre-generated map
        :param set_name: Set to get ID from
        :return: Set ID
        """
        if not self.__keys_found:
            return None

        if set_name.lower() in self.set_map.keys():
            return int(self.set_map[set_name.lower()]["mcmId"])
        return None

    def get_extras_set_id(self, set_name: str) -> Optional[int]:
        """
        Get "Extras" MKM Set ID from pre-generated map
        For "Throne of Eldraine" it will return the mcmId for "Throne of Eldraine: Extras"
        :param set_name: Set to get ID from
        :return: Set ID
        """
        if not self.__keys_found:
            return None

        extras_set_name = f"{set_name.lower()}: extras"
        if extras_set_name in self.set_map.keys():
            return int(self.set_map[extras_set_name]["mcmId"])
        return None

    def get_set_name(self, set_name: str) -> Optional[str]:
        """
        Get MKM Set Name from pre-generated map
        :param set_name: Set to get Name from
        :return: Set Name
        """
        if not self.__keys_found:
            return None

        if set_name.lower() in self.set_map.keys():
            return str(self.set_map[set_name.lower()]["mcmName"])
        return None

    def _build_http_header(self) -> Dict[str, str]:
        """
        Generate HTTP Header -- Not Used
        :return: Nothing
        """
        return dict()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download Content -- Not Used
        :param url:
        :param params:
        :return:
        """
        return None

    def get_mkm_cards(self, mcm_id: Optional[int]) -> Dict[str, Dict[str, Any]]:
        """
        Initialize the MKM global with the cards found in the set
        :param mcm_id: Set's ID, if possible
        """
        if mcm_id is None:
            return {}

        mkm_resp = None
        for _ in range(5):
            try:
                mkm_resp = self.connection.market_place.expansion_singles(
                    1, expansion=mcm_id
                )
                break
            except mkmsdk.exceptions.ConnectionError as exception:
                LOGGER.warning(
                    f"MKM Had a connection error trying to build {mcm_id}: {exception}"
                )
                time.sleep(10)

        if mkm_resp is None:
            LOGGER.error("MKM had a critical failure. Skipping this import.")
            return {}

        # {SetNum: Object, ... }
        set_in_progress = {}
        for set_content in mkm_resp.json()["single"]:
            if not set_content["number"]:
                set_content["number"] = ""

            # Remove leading zeroes
            set_content["number"].lstrip("0")

            # Split cards get two entries
            for name in set_content["enName"].split("//"):
                name_no_special_chars = name.strip().lower()
                set_in_progress[name_no_special_chars] = set_content

        return set_in_progress
