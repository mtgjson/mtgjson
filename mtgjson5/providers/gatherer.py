"""
Wizards Gatherer 3rd party provider
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from ..mtgjson_config import MtgjsonConfig
from ..providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class GathererProvider(AbstractProvider):
    """
    Gatherer Container
    """

    _GATHERER_ID_MAPPING_URL = "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/gatherer_mapping.json?raw=True"
    _multiverse_id_to_data: Dict[str, List[Dict[str, str]]]

    def __init__(self) -> None:
        """
        Class Initializer
        """
        super().__init__(self._build_http_header())
        self._multiverse_id_to_data = self.download(self._GATHERER_ID_MAPPING_URL)

    def _build_http_header(self) -> Dict[str, str]:
        __github_token = MtgjsonConfig().get("GitHub", "api_token")
        return {"Authorization": f"Bearer {__github_token}"}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        response = self.session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(
            f"Error downloading GitHub Boosters: {response} --- {response.text}"
        )
        return {}

    def get_cards(self, multiverse_id: str) -> List[Dict[str, str]]:
        """
        Get card(s) matching a given multiverseId
        :param multiverse_id: Multiverse ID of the card
        :return All found cards matching description
        """
        return self._multiverse_id_to_data.get(multiverse_id, [])
