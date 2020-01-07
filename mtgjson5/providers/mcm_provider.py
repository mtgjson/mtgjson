"""
MKM 3rd party provider
"""

import os
from typing import Any, Dict, Optional

from mkmsdk.api_map import _API_MAP
from mkmsdk.mkm import Mkm
from singleton_decorator import singleton

from ..providers.abstract_provider import AbstractProvider
from ..utils import get_thread_logger

LOGGER = get_thread_logger()


@singleton
class McmProvider(AbstractProvider):
    """
    MKM container
    """

    connection: Mkm
    set_map: Dict[str, Dict[str, Any]]

    def __init__(self, headers: Dict[str, str] = None):
        super().__init__(headers or {})

        config = self.get_configs()
        os.environ["MKM_APP_TOKEN"] = config.get("CardMarket", "app_token")
        os.environ["MKM_APP_SECRET"] = config.get("CardMarket", "app_secret")
        os.environ["MKM_ACCESS_TOKEN"] = ""
        os.environ["MKM_ACCESS_TOKEN_SECRET"] = ""

        self.connection = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])
        self.set_map = {}
        self.__init_set_map()

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

    def get_set_id(self, set_name: str) -> Optional[int]:
        """
        Get MKM Set ID from pre-generated map
        :param set_name: Set to get ID from
        :return: Set ID
        """
        if set_name.lower() in self.set_map.keys():
            return int(self.set_map[set_name.lower()]["mcmId"])
        return None

    def get_set_name(self, set_name: str) -> Optional[str]:
        """
        Get MKM Set Name from pre-generated map
        :param set_name: Set to get Name from
        :return: Set Name
        """
        if set_name.lower() in self.set_map.keys():
            return str(self.set_map[set_name.lower()]["mcmName"])
        return None

    def _build_http_header(self) -> Dict[str, str]:
        """
        Generate HTTP Header -- Not Used
        :return: Nothing
        """
        return {}

    def download(self, url: str, params: Dict[str, str] = None) -> Any:
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

        mkm_resp = self.connection.market_place.expansion_singles(1, expansion=mcm_id)

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
