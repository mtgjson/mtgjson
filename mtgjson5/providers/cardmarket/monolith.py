"""
MKM 3rd party provider
"""
import json
import logging
import os
import pathlib
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

import mkmsdk.exceptions
from mkmsdk.api_map import _API_MAP
from mkmsdk.mkm import Mkm
from singleton_decorator import singleton

from ... import constants
from ...classes import MtgjsonPricesObject
from ...mtgjson_config import MtgjsonConfig
from ...providers.abstract import AbstractProvider
from ...utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


@singleton
class CardMarketProvider(AbstractProvider):
    """
    MKM container
    """
    CARD_PRICES_URL: str = "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json"

    connection: Mkm
    set_map: Dict[str, Dict[str, Any]]

    def __init__(self, headers: Optional[Dict[str, str]] = None, init_map: bool = True):
        super().__init__(headers or {})
        self.set_map = {}

        if not MtgjsonConfig().has_section("CardMarket"):
            LOGGER.warning(
                "CardMarket config section not established. Skipping requests"
            )
            return

        os.environ["MKM_APP_TOKEN"] = MtgjsonConfig().get("CardMarket", "app_token")
        os.environ["MKM_APP_SECRET"] = MtgjsonConfig().get("CardMarket", "app_secret")
        os.environ["MKM_ACCESS_TOKEN"] = MtgjsonConfig().get(
            "CardMarket", "mkm_access_token", fallback=""
        )
        os.environ["MKM_ACCESS_TOKEN_SECRET"] = MtgjsonConfig().get(
            "CardMarket", "mkm_access_token_secret", fallback=""
        )

        if not (os.environ["MKM_APP_TOKEN"] and os.environ["MKM_APP_SECRET"]):
            LOGGER.warning("CardMarket keys values missing. Skipping requests")
            return

        self.connection = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])

        if init_map:
            self.__init_set_map()

    def _get_card_market_data(self):
        data = self.download(self.CARD_PRICES_URL)
        for card_entry in data.get("priceGuides", []):
            card_id = card_entry.get("idProduct")
            avg_1_day = card_entry.get("avg1")
            avg_1_day_foil = card_entry.get("avg1-foil")
            yield card_id, avg_1_day, avg_1_day_foil

    def generate_today_price_dict(
            self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure from Card Market
        :return MTGJSON prices single day structure
        """
        mtgjson_finish_map = generate_card_mapping(
            all_printings_path,
            ("identifiers", "mcmId"),
            ("finishes",),
        )

        mtgjson_id_map = generate_card_mapping(
            all_printings_path, ("identifiers", "mcmId"), ("uuid",)
        )

        today_dict: Dict[str, MtgjsonPricesObject] = {}
        for (product_id, avg_1_day, avg_1_day_foil) in self._get_card_market_data():
            if product_id in mtgjson_id_map:
                mtgjson_uuids = mtgjson_id_map[product_id]
                for mtgjson_uuid in mtgjson_uuids:
                    if mtgjson_uuid not in today_dict:
                        today_dict[mtgjson_uuid] = MtgjsonPricesObject(
                            "paper", "cardmarket", self.today_date, "EUR"
                        )

                    if avg_1_day_foil:
                        today_dict[mtgjson_uuid].sell_normal = avg_1_day

                    if avg_1_day_foil:
                        if "etched" in mtgjson_finish_map.get(product_id, []):
                            today_dict[mtgjson_uuid].sell_etched = avg_1_day_foil
                        else:
                            today_dict[mtgjson_uuid].sell_foil = avg_1_day_foil

        return today_dict

    def __init_set_map(self) -> None:
        """
        Construct a mapping for all set components from MKM
        """
        try:
            mkm_resp = self.connection.market_place.expansions(game=1)
        except mkmsdk.exceptions.ConnectionError as exception:
            LOGGER.error(f"Unable to download MKM correctly: {exception}")
            return

        if mkm_resp.status_code != 200:
            LOGGER.error(f"Unable to download MKM correctly: {mkm_resp}")
            return

        try:
            mkm_body_json = mkm_resp.json()
        except json.JSONDecodeError as exception:
            LOGGER.error(
                f"Unable to download MKM correctly: {exception} from {mkm_resp.text}"
            )
            return

        for set_content in mkm_body_json["expansion"]:
            self.set_map[set_content["enName"].lower()] = {
                "mcmId": set_content["idExpansion"],
                "mcmName": set_content["enName"],
            }

        # Update the set map with manual overrides
        with constants.RESOURCE_PATH.joinpath("mkm_set_name_fixes.json").open(
            encoding="utf-8"
        ) as f:
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
        if not self.set_map:
            return None

        if set_name.lower() in self.set_map:
            return int(self.set_map[set_name.lower()]["mcmId"])
        return None

    def get_extras_set_id(self, set_name: str) -> Optional[int]:
        """
        Get "Extras" MKM Set ID from pre-generated map
        For "Throne of Eldraine" it will return the mcmId for "Throne of Eldraine: Extras"
        :param set_name: Set to get ID from
        :return: Set ID
        """
        if not self.set_map:
            return None

        extras_set_name = f"{set_name.lower()}: extras"
        if extras_set_name in self.set_map:
            return int(self.set_map[extras_set_name]["mcmId"])
        return None

    def get_set_name(self, set_name: str) -> Optional[str]:
        """
        Get MKM Set Name from pre-generated map
        :param set_name: Set to get Name from
        :return: Set Name
        """
        if not self.set_map:
            return None

        if set_name.lower() in self.set_map:
            return str(self.set_map[set_name.lower()]["mcmName"])
        return None

    def _build_http_header(self) -> Dict[str, str]:
        """
        Generate HTTP Header -- Not Used
        :return: Nothing
        """
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        response = self.session.get(url)
        self.log_download(response)
        return response.json()

    def get_mkm_cards(self, mcm_id: Optional[int]) -> Dict[str, List[Dict[str, Any]]]:
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
        set_in_progress = defaultdict(list)
        try:
            for set_content in mkm_resp.json()["single"]:
                if not set_content["number"]:
                    set_content["number"] = ""

                # Remove leading zeroes
                set_content["number"].lstrip("0")

                # Split cards get two entries
                for name in set_content["enName"].split("//"):
                    name_no_special_chars = name.strip().lower()
                    if "token" in name_no_special_chars:
                        name_no_special_chars = name_no_special_chars.split(" (", 1)[0]
                    set_in_progress[name_no_special_chars].append(set_content)
        except json.JSONDecodeError as exception:
            LOGGER.warning(
                f"MKM had a parsing failure trying to build {mcm_id}: {exception}"
            )

        for key in set_in_progress.keys():
            set_in_progress[key].sort(key=lambda x: x.get("number"))
        return set_in_progress
