"""
MTGBan 3rd party provider
"""
import logging
from typing import Any, Dict, Union

from singleton_decorator import singleton

from ..providers.abstract import AbstractProvider
from ..utils import retryable_session

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

        config = self.get_configs()
        if "MTGBan" not in config.sections():
            LOGGER.warning("MTGBan section not established. Skipping alerts")
            self.__keys_found = False
            self.api_url = ""
            return headers

        if config.get("MTGBan", "api_key"):
            self.__keys_found = True
            self.api_url = self.api_url.format(config.get("MTGBan", "api_key"))
        else:
            LOGGER.info("MTGBan keys values missing. Skipping imports")
            self.__keys_found = False
            self.api_url = ""

        return headers

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download a URL
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)

        response = session.get(url)
        self.log_download(response)

        return response.json()

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
