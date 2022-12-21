import logging
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from mtgjson5.constants import LANGUAGE_MAP
from mtgjson5.providers.abstract import AbstractProvider
from mtgjson5.utils import retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class ScryfallProviderSetLanguageDetector(AbstractProvider):
    FIRST_CARD_URL = "https://api.scryfall.com/cards/search?q=set:{}&unique=prints&include_extras=true"
    LANG_QUERY_URL = 'https://api.scryfall.com/cards/search?q=set:{}%20number:"{}"%20lang:any&unique=prints&include_extras=true'

    def __init__(self) -> None:
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        session = retryable_session()
        response = session.get(url)
        self.log_download(response)
        return response.json()

    def get_set_printing_languages(self, set_code: str) -> List[str]:
        first_card_response = self.download(self.FIRST_CARD_URL.format(set_code))

        if first_card_response.get("object") != "list":
            LOGGER.warning(
                f"Unable to get set printing languages for {set_code} due to bad response: {first_card_response}"
            )
            return []
        if first_card_response.get("total_cards", 0) < 1:
            LOGGER.warning(
                f"Unable to get set printing languages for {set_code} due to no cards in set: {first_card_response}"
            )
            return []

        first_card_number = first_card_response["data"][0].get("collector_number")
        lang_response = self.download(
            self.LANG_QUERY_URL.format(set_code, first_card_number)
        )
        set_languages = {
            LANGUAGE_MAP.get(card.get("lang")) for card in lang_response.get("data", [])
        }

        return list(filter(None, set_languages))
