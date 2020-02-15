"""
Whats In Standard 3rd party provider
"""
import datetime
from typing import Any, Dict, Set, Union

import dateutil.parser
from singleton_decorator import singleton

from ..providers.abstract_provider import AbstractProvider
from ..utils import retryable_session


@singleton
class WhatsInStandardProvider(AbstractProvider):
    """
    Whats In Standard API provider
    """

    API_ENDPOINT: str = "https://whatsinstandard.com/api/v6/standard.json"
    set_codes: Set[str]
    standard_legal_sets: Set[str]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.set_codes = self.standard_legal_set_codes()
        self.standard_legal_sets = set()

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from Whats in Standard
        Api calls always return JSON from them
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)
        response = session.get(url, params=params)
        self.log_download(response)
        return response.json()

    def standard_legal_set_codes(self) -> Set[str]:
        """
        Get all set codes from sets that are currently legal in Standard
        :return: Set Codes legal in standard
        """
        if self.standard_legal_sets:
            return self.standard_legal_sets

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

        self.standard_legal_sets = standard_set_codes

        return standard_set_codes
