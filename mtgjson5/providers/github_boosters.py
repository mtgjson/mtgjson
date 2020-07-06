"""
Boosters via GitHub 3rd party provider
"""
import logging
from typing import Any, Dict, Optional, Union

from singleton_decorator import singleton

from ..providers.abstract import AbstractProvider
from ..utils import retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubBoostersProvider(AbstractProvider):
    """
    GitHubBoostersProvider container
    """

    booster_api_url: str = "https://github.com/taw/magic-sealed-data/blob/master/experimental_export_for_mtgjson.json?raw=true"
    booster_data: Dict[str, Any]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.booster_data = self.download(self.booster_api_url)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return dict()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        session = retryable_session()

        response = session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(
            f"Error downloading GitHub Boosters: {response} --- {response.text}"
        )
        return {}

    def get_set_booster_data(self, set_code: str) -> Optional[Dict[str, Any]]:
        """
        Grab an individual set's booster variable, if it exists
        :param set_code: Set to pull data from (case insensitive)
        :return Booster data, if applicable
        """
        LOGGER.info(f"Getting booster data for {set_code}")
        return self.booster_data.get(set_code.upper())
