"""
MTGBan 3rd party provider
"""
import logging
from typing import Any, Dict, Optional, Union

import requests
from singleton_decorator import singleton

from ..mtgjson_config import MtgjsonConfig
from ..providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class MTGBanProvider(AbstractProvider):
    """
    MTGBan container
    """

    api_url: str = "https://www.mtgban.com/api/mtgjson/ck.json?sig={}"
    __mtgjson_to_card_kingdom: Dict[str, Dict[str, Dict[str, str]]]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.__mtgjson_to_card_kingdom = {}

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for MTGBan
        :return: Authorization header
        """
        headers: Dict[str, str] = {}
        __keys_found: bool

        if not MtgjsonConfig().has_section("MTGBan"):
            LOGGER.warning("MTGBan section not established. Skipping alerts")
            self.__keys_found = False
            self.api_url = ""
            return headers

        if MtgjsonConfig().has_option("MTGBan", "api_key"):
            self.__keys_found = True
            self.api_url = self.api_url.format(MtgjsonConfig().get("MTGBan", "api_key"))
        else:
            LOGGER.info("MTGBan keys values missing. Skipping imports")
            self.__keys_found = False
            self.api_url = ""

        return headers

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download a URL
        :param url: URL to download from
        :param params: Options for URL download
        """
        response = self.session.get(url)
        self.log_download(response)

        try:
            return response.json()
        except requests.exceptions.RequestException as e:
            LOGGER.error("Unable to download from MTGBan: %s", e)
            return {}

    def get_mtgjson_to_card_kingdom(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        """
        Get MTGJSON to Card Kingdom translation table
        :return Compiled table for future use
        """
        if not self.__keys_found:
            return {}

        if not self.__mtgjson_to_card_kingdom:
            self.__mtgjson_to_card_kingdom = self.download(self.api_url)

        return self.__mtgjson_to_card_kingdom
