import logging
from typing import Any, Dict, Optional, Union

from singleton_decorator import singleton

from ...mtgjson_config import MtgjsonConfig
from ...providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubUuidCacheProvider(AbstractProvider):
    sf_to_mtgjson_cache: Dict[str, Dict[str, str]]
    __cache_url: str = (
        "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/scryfall_to_mtgjson_uuid_mapping.json?raw=true"
    )

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.sf_to_mtgjson_cache = self.download(self.__cache_url)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
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

        LOGGER.error(f"Error downloading GitHub UUID: {response} --- {response.text}")
        return {}

    def get_uuid(self, scryfall_uuid: str, side: Optional[str]) -> Optional[str]:
        """
        Try to get the cached version of the UUID
        """
        side_with_default = side if side else "a"
        return self.sf_to_mtgjson_cache.get(scryfall_uuid, {}).get(side_with_default)
