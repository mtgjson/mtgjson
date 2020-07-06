"""
Whats In Standard 3rd party provider
"""
import datetime
import logging
import time
from typing import Any, Dict, Set, Union

import dateutil.parser
from singleton_decorator import singleton

from ..providers.abstract import AbstractProvider
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
        """
        Class Initializer
        """
        super().__init__(self._build_http_header())
        self.logger = logging.getLogger(__name__)
        self.standard_legal_sets = set()
        self.set_codes = self.standard_legal_set_codes()

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header -- unused
        :return: Authorization header
        """
        return dict()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from Whats in Standard
        Api calls always return JSON from them
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        response = session.get(url)
        self.log_download(response)
        if not response.ok:
            self.logger.error(
                f"WhatsInStandard Download Error ({response.status_code}): {response.content.decode()}"
            )
            time.sleep(5)
            return self.download(url, params)

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
