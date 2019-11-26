"""
Whats In Standard 3rd party provider
"""
import datetime
from typing import Any, Dict, Set

import dateutil.parser
from mtgjson5.globals import init_thread_logger
from mtgjson5.providers.abstract_provider import AbstractProvider
from singleton.singleton import Singleton


@Singleton
class WhatsInStandardProvider(AbstractProvider):
    """
    Whats In Standard API provider
    """

    API_ENDPOINT: str = "https://whatsinstandard.com/api/v6/standard.json"
    SET_CODES: Set[str]

    def __init__(self, use_cache: bool = True):
        init_thread_logger()
        super().__init__(self._build_http_header(), use_cache)

        self.SET_CODES = self.standard_legal_set_codes()

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(self, url: str, params: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Download content from Whats in Standard
        Api calls always return JSON from them
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = self.session_pool.popleft()
        response = session.get(url, params=params)
        self.session_pool.append(session)
        self.log_download(response)
        return response.json()

    def standard_legal_set_codes(self) -> Set[str]:
        """
        Get all set codes from sets that are currently legal in Standard
        :return: Set Codes legal in standard
        """
        api_response = self.download(self.API_ENDPOINT)

        standard_set_codes = {
            set_object.get("code", "").upper()
            for set_object in api_response["sets"]
            if (
                dateutil.parser.parse(set_object["enterDate"]["exact"] or "9999")
                <= datetime.datetime.now()
                <= dateutil.parser.parse(set_object["exitDate"]["exact"] or "9999")
            )
        }

        return standard_set_codes
