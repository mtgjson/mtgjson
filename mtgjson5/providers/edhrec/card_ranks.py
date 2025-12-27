import logging
from typing import Any

from singleton_decorator import singleton

from ...mtgjson_config import MtgjsonConfig
from ...providers.abstract import AbstractProvider


LOGGER = logging.getLogger(__name__)


@singleton
class EdhrecProviderCardRanks(AbstractProvider):
    __keys_found: bool
    __api_url: str
    __data_table: dict[str, dict[str, Any]]

    def __init__(self) -> None:
        super().__init__(self._build_http_header())
        self.__data_table = {}

    def download(
        self, url: str, params: dict[str, str | int] | None = None
    ) -> Any:
        response = self.session.get(url)
        self.log_download(response)

        return response.json()

    def get_salt_rating(self, card_name: str) -> float | None:
        if not self.__data_table:
            self.__generate_data_table()

        salt_full_float = self.__data_table.get(card_name, {}).get("salt")
        return round(salt_full_float, 2) if salt_full_float else None

    def _build_http_header(self) -> dict[str, str]:
        self.__keys_found = MtgjsonConfig().has_option("EDHRec", "api_url")
        self.__api_url = MtgjsonConfig().get("EDHRec", "api_url")

        if not self.__keys_found:
            LOGGER.info("EDHRec keys values missing. Skipping imports")

        return {}

    def __generate_data_table(self) -> None:
        if not self.__keys_found:
            return

        raw_json_data = self.download(self.__api_url)
        for entry in raw_json_data:
            entry_name = entry["name"]
            del entry["name"]
            self.__data_table[entry_name] = entry
