"""
UUID Cache Provider for MTGJSONv5 Legacy UUIDs
"""

import json
import logging
from typing import Any

from singleton_decorator import singleton

from .. import constants
from .abstract import AbstractProvider


LOGGER = logging.getLogger(__name__)


@singleton
class UuidCacheProvider(AbstractProvider):
    """
    UUID Cache Provider for MTGJSONv5 Legacy UUIDs
    """

    sf_to_mtgjson_cache: dict[str, dict[str, str]]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        with constants.RESOURCE_PATH.joinpath(
            "legacy_mtgjson_v5_uuid_mapping.json"
        ).open(encoding="utf-8") as f:
            self.sf_to_mtgjson_cache = json.load(f)

    def _build_http_header(self) -> dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return {}

    def download(
        self, url: str, params: dict[str, str | int] | None = None
    ) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        return {}

    def get_uuid(self, scryfall_uuid: str, side: str) -> str | None:
        """
        Try to get the cached version of the UUID
        """
        return self.sf_to_mtgjson_cache.get(scryfall_uuid, {}).get(side)
